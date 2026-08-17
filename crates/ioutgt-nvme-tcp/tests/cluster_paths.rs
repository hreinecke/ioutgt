//! End-to-end cluster paths: a target opening a Sheepdog namespace registers
//! its own fabric address as a holder of that volume, advertises every holder
//! as a discovery-log path, and hands the registration back on shutdown.
//!
//! Its own test binary on purpose: it calls [`ioutgt_harness::shutdown`], which
//! quiesces every target in the process, so no other test may be running
//! alongside it.
//!
//! The cluster is a minimal in-process fake `sheep` — the five requests this
//! path makes, against one ACL object and one volume in it — so the harness
//! wiring is exercised for real (namespace open → `REGISTER_VDI` → holder
//! read-back → `Subsystem::set_ports` → discovery log → `UNREGISTER_VDI`) with
//! no cluster.

// Test-only offset arithmetic on a 64-bit host; values are small and bounded.
#![allow(clippy::cast_possible_truncation)]

mod common;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use common::{Client, ascii, connect_discovery, get_disc_log};
use ioutgt_control::config::BackendConfig;
use ioutgt_nvme::fabrics;

// --- wire constants (include/sheepdog_proto.h) -----------------------------
const HDR: usize = 48;
const SD_OP_READ_OBJ: u8 = 0x02;
const SD_OP_GET_VDI_INFO: u8 = 0x14;
const SD_OP_REGISTER_VDI: u8 = 0x19;
const SD_OP_UNREGISTER_VDI: u8 = 0x1A;
const SD_OP_GET_VDI_COPIES: u8 = 0xAB;
const SD_RES_NO_VDI: u32 = 0x08;
const SD_RES_VDI_NOT_LOCKED: u32 = 0x10;
const SD_RES_VDI_DENIED: u32 = 0x1E;
const SD_VDI_FLAG_ACL: u32 = 0x01;
const SD_INODE_HEADER_SIZE: usize = 4664;
const SD_MAX_VDI_LEN: usize = 256;
const VDI_BIT: u64 = 1 << 63;
const VDI_STATE: usize = 1432;
const LOCK_STATE_SHARED: u32 = 3;

/// The ACL object this cluster holds, named after the subsystem NQN, and the
/// one volume in it — the subsystem's single namespace.
const ACL_NQN: &str = "nqn.2026-06.io.ioutgt:cluster";
const ACL_VID: u32 = 0x0000_4711;
const VOL: &str = "vol";
const VOL_VID: u32 = 0x0000_4712;
/// 16 MiB in 4 MiB objects.
const VOL_SIZE: u64 = 16 << 20;
const VOL_SHIFT: u8 = 22;

/// Another target already serving the volume when this one starts: the peer
/// whose path must show up in our discovery log, and must survive our
/// shutdown.
const PEER: &str = "10.9.8.7:4420";

/// The fake cluster's whole state: who holds the shared lock on [`VOL_VID`],
/// with each holder's registration count (`sheep` refcounts repeats of one
/// owner into a single participant entry).
type Participants = Arc<Mutex<Vec<(SocketAddr, u32)>>>;

fn u16le(b: &[u8], o: usize) -> u16 {
    u16::from_le_bytes(b[o..o + 2].try_into().unwrap())
}
fn u32le(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}
fn u64le(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}

/// An inode object: the ACL object's (its name, a size, and the flag that makes
/// it an ACL rather than a volume) or the volume's (its name, its size, and the
/// ACL it belongs to, followed by an all-zero — unallocated — object map).
fn inode(vid: u32) -> Vec<u8> {
    let mut b = vec![0u8; SD_INODE_HEADER_SIZE];
    if vid == ACL_VID {
        b[..ACL_NQN.len()].copy_from_slice(ACL_NQN.as_bytes());
        b[536..544].copy_from_slice(&(1u64 << 22).to_le_bytes()); // vdi_size
        b[554] = 1; // nr_copies
        b[555] = VOL_SHIFT;
        b[592..596].copy_from_slice(&SD_VDI_FLAG_ACL.to_le_bytes());
        return b;
    }
    b[..VOL.len()].copy_from_slice(VOL.as_bytes());
    b[536..544].copy_from_slice(&VOL_SIZE.to_le_bytes());
    b[554] = 1;
    b[555] = VOL_SHIFT;
    b[572..576].copy_from_slice(&ACL_VID.to_le_bytes()); // acl_id
    b.resize(
        SD_INODE_HEADER_SIZE + 4 * (VOL_SIZE >> VOL_SHIFT) as usize,
        0,
    );
    b
}

/// The `GET_VDI_COPIES` payload: one `vdi_state` for the volume, carrying its
/// participant list.
fn vdi_states(list: &[(SocketAddr, u32)]) -> Vec<u8> {
    let mut vs = vec![0u8; VDI_STATE];
    vs[0..4].copy_from_slice(&VOL_VID.to_le_bytes());
    if list.is_empty() {
        return vs; // lock_state stays 0: nobody holds it
    }
    vs[20..24].copy_from_slice(&LOCK_STATE_SHARED.to_le_bytes());
    vs[64..68].copy_from_slice(&(list.len() as u32).to_le_bytes());
    for (i, (owner, count)) in list.iter().enumerate() {
        // participants_state[i]: SHARED_LOCK_STATE_SHARED, count above it.
        vs[68 + i * 4..72 + i * 4].copy_from_slice(&(2u32 | (count << 8)).to_le_bytes());
        let nid = 192 + i * 40;
        match owner.ip() {
            std::net::IpAddr::V4(v4) => vs[nid + 12..nid + 16].copy_from_slice(&v4.octets()),
            std::net::IpAddr::V6(v6) => vs[nid..nid + 16].copy_from_slice(&v6.octets()),
        }
        vs[nid + 16..nid + 18].copy_from_slice(&owner.port().to_le_bytes());
    }
    vs
}

/// The owner a `vdi_lock` request names: addr[16] at 24, port at 40 (IPv4 in
/// the last four bytes, the leading twelve zero).
fn req_owner(hdr: &[u8; HDR]) -> SocketAddr {
    let addr: [u8; 16] = hdr[24..40].try_into().unwrap();
    let ip = if addr[12] != 0 && addr[..12].iter().all(|&b| b == 0) {
        std::net::IpAddr::V4(<[u8; 4]>::try_from(&addr[12..]).unwrap().into())
    } else {
        std::net::IpAddr::V6(addr.into())
    };
    SocketAddr::new(ip, u16le(hdr, 40))
}

fn resp(opcode: u8, id: u32, result: u32, data_len: u32) -> [u8; HDR] {
    let mut r = [0u8; HDR];
    r[0] = 0x02; // proto_ver
    r[1] = opcode;
    r[8..12].copy_from_slice(&id.to_le_bytes());
    r[12..16].copy_from_slice(&data_len.to_le_bytes());
    r[16..20].copy_from_slice(&result.to_le_bytes());
    r
}

fn cstr(field: &[u8]) -> String {
    let end = field.iter().position(|&b| b == 0).unwrap_or(field.len());
    String::from_utf8_lossy(&field[..end]).into_owned()
}

/// The vid a name resolves to under `acl`: the cluster admits a name only from
/// inside the ACL its inode records (the ACL object itself being in none).
fn resolve(name: &str, acl: u32) -> Result<u32, u32> {
    match (name, acl) {
        (ACL_NQN, 0) => Ok(ACL_VID),
        (VOL, ACL_VID) => Ok(VOL_VID),
        (ACL_NQN | VOL, _) => Err(SD_RES_VDI_DENIED),
        _ => Err(SD_RES_NO_VDI),
    }
}

fn serve_conn(mut sock: TcpStream, holders: Participants) -> std::io::Result<()> {
    loop {
        let mut hdr = [0u8; HDR];
        if sock.read_exact(&mut hdr).is_err() {
            return Ok(()); // peer closed
        }
        let opcode = hdr[1];
        let id = u32le(&hdr, 8);
        let data_length = u32le(&hdr, 12) as usize;
        let oid = u64le(&hdr, 16);
        let offset = u32le(&hdr, 40) as usize;

        match opcode {
            SD_OP_GET_VDI_INFO => {
                let mut p = vec![0u8; data_length];
                sock.read_exact(&mut p)?;
                match resolve(&cstr(&p[..SD_MAX_VDI_LEN]), u32le(&hdr, 36)) {
                    Ok(vid) => {
                        let mut r = resp(opcode, id, 0, 0);
                        r[24..28].copy_from_slice(&vid.to_le_bytes()); // vdi_id
                        sock.write_all(&r)?;
                    }
                    Err(res) => sock.write_all(&resp(opcode, id, res, 0))?,
                }
            }
            // The lock op: a lookup by name under the request's ACL, with the
            // holder's address supplied by the client rather than the gateway.
            SD_OP_REGISTER_VDI => {
                let mut p = vec![0u8; data_length];
                sock.read_exact(&mut p)?;
                let res = match resolve(&cstr(&p[..SD_MAX_VDI_LEN]), u32le(&hdr, 44)) {
                    Err(res) => res,
                    Ok(_) => {
                        // add_participant: a repeat from one owner bumps its
                        // count rather than taking a second slot.
                        let owner = req_owner(&hdr);
                        let mut list = holders.lock().unwrap();
                        match list.iter_mut().find(|(addr, _)| *addr == owner) {
                            Some(entry) => entry.1 += 1,
                            None => list.push((owner, 1)),
                        }
                        0
                    }
                };
                sock.write_all(&resp(opcode, id, res, 0))?;
            }
            SD_OP_UNREGISTER_VDI => {
                let owner = req_owner(&hdr);
                let mut list = holders.lock().unwrap();
                let result = match list.iter().position(|(addr, _)| *addr == owner) {
                    Some(i) => {
                        list[i].1 -= 1;
                        if list[i].1 == 0 {
                            list.remove(i); // the cluster compacts the list
                        }
                        0
                    }
                    None => SD_RES_VDI_NOT_LOCKED,
                };
                drop(list);
                sock.write_all(&resp(opcode, id, result, 0))?;
            }
            SD_OP_GET_VDI_COPIES => {
                let states = vdi_states(&holders.lock().unwrap());
                sock.write_all(&resp(opcode, id, 0, states.len() as u32))?;
                sock.write_all(&states)?;
            }
            SD_OP_READ_OBJ => {
                let inode = inode(((oid & !VDI_BIT) >> 32) as u32);
                let end = (offset + data_length).min(inode.len());
                let slice = &inode[offset.min(inode.len())..end];
                sock.write_all(&resp(opcode, id, 0, slice.len() as u32))?;
                sock.write_all(slice)?;
            }
            other => sock.write_all(&resp(other, id, 0x01, 0))?, // UNKNOWN
        }
    }
}

/// Spawn the fake sheep; returns its address.
fn spawn_fake_sheep(holders: Participants) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for sock in listener.incoming() {
            let Ok(sock) = sock else { break };
            sock.set_nodelay(true).ok();
            let holders = Arc::clone(&holders);
            std::thread::spawn(move || {
                let _ = serve_conn(sock, holders);
            });
        }
    });
    addr
}

#[test]
fn a_cluster_namespace_registers_and_advertises_every_holder() {
    let peer: SocketAddr = PEER.parse().unwrap();
    // The cluster already has one target serving the volume when we start.
    let holders: Participants = Arc::new(Mutex::new(vec![(peer, 1)]));
    let cluster = spawn_fake_sheep(Arc::clone(&holders));

    let mut config = ioutgt_nvme_tcp::TargetConfig::single_memory(ACL_NQN, 16);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 2;
    config.subsystems[0].namespaces[0].backend = BackendConfig::Sheepdog {
        addr: cluster.to_string(),
        vdi: VOL.into(),
        tag: None,
        acl: Some(ACL_NQN.into()),
        lock: true,
    };
    let addr = ioutgt_nvme_tcp::spawn_target(config).expect("target start");

    // Opening the namespace registered this target as a holder of the volume,
    // under the address it listens on — one registration per open.
    assert_eq!(
        *holders.lock().unwrap(),
        vec![(peer, 1), (addr, 1)],
        "the target joined the volume's holders"
    );

    // ...and seeded the subsystem's paths from the holder list it read back,
    // so a host connecting here learns the peer too.
    let mut client = Client::handshake(addr, false, false);
    connect_discovery(&mut client, common::HOSTNQN);
    client.enable_controller(2);
    let header = get_disc_log(&mut client, 3, 0, 16);
    let numrec = u64::from_le_bytes(header[8..16].try_into().unwrap());
    assert_eq!(numrec, 2, "one entry per holder of the volume");

    let log = get_disc_log(&mut client, 4, 0, 3072);
    let paths: Vec<_> = (1..=2)
        .map(|i| {
            let entry = &log[1024 * i..1024 * (i + 1)];
            assert_eq!(entry[0], fabrics::trtype::TCP, "entry {i} trtype");
            assert_eq!(entry[1], fabrics::adrfam::IPV4, "entry {i} adrfam");
            assert_eq!(entry[2], fabrics::subtype::NVM, "entry {i} subtype");
            assert_eq!(
                ascii(&entry[256..512]),
                ACL_NQN,
                "entry {i} subnqn: both paths lead to the same subsystem"
            );
            (
                ascii(&entry[512..768]),
                ascii(&entry[32..64]),
                u16::from_le_bytes([entry[4], entry[5]]),
            )
        })
        .collect();
    assert_eq!(
        paths,
        vec![
            // PORTID is the holder's index in the address-sorted holder list,
            // which every target serving the volume computes the same way.
            (peer.ip().to_string(), peer.port().to_string(), 0),
            (addr.ip().to_string(), addr.port().to_string(), 1),
        ]
    );
    drop(client);

    // Shutdown hands the registration back, so the peer's discovery log stops
    // advertising a target that is no longer there.
    assert_eq!(ioutgt_harness::shutdown(), 1, "one namespace released");
    assert_eq!(
        *holders.lock().unwrap(),
        vec![(peer, 1)],
        "only the peer still serves the volume"
    );
}
