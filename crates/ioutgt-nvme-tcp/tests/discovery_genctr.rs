//! The discovery log's `GENCTR`, and the Discovery Log Page Change async event
//! that sends a host back to read it.
//!
//! A cluster subsystem's generation starts as the ACL object's inode
//! `vdi_epoch` — the cluster's own version of the group — and moves from there
//! on both of the things that can change what the log says: a target joining or
//! leaving the volumes' holder list (which no cluster counter records, so the
//! target counts it itself) and the epoch advancing under it (`dog acl add
//! vdi`). Either way a discovery controller with a parked AER is told, rather
//! than being left to poll a page it has no reason to think has changed.
//!
//! Its own test binary, like the other cluster tests: it drives
//! [`ioutgt_harness::refresh_clusters`], which visits every cluster tracked in
//! the process.
//!
//! The cluster is a minimal in-process fake `sheep`: one ACL object with a
//! `vdi_epoch` the test bumps, one volume in it, and a participant list the
//! test edits behind the target's back.

// Test-only offset arithmetic on a 64-bit host; values are small and bounded.
#![allow(clippy::cast_possible_truncation)]

mod common;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use common::{Client, connect_discovery, get_disc_log};
use ioutgt_control::config::{BackendConfig, SheepdogAcl};
use ioutgt_nvme::identify::IdentifyController;
use ioutgt_nvme::{spec, status};
use zerocopy::FromBytes;

// --- wire constants (include/sheepdog_proto.h) -----------------------------
const HDR: usize = 48;
const SD_OP_READ_OBJ: u8 = 0x02;
const SD_OP_GET_VDI_INFO: u8 = 0x14;
const SD_OP_REGISTER_VDI: u8 = 0x19;
const SD_OP_UNREGISTER_VDI: u8 = 0x1A;
/// Sheep-internal local op: one VDI's shared-lock participant list, each with
/// the owner string it registered under.
const SD_OP_GET_VDI_LOCK_STATE: u8 = 0xD1;
const SD_RES_NO_VDI: u32 = 0x08;
const SD_RES_VDI_DENIED: u32 = 0x1E;
const SD_VDI_FLAG_ACL: u32 = 0x01;
const SD_INODE_HEADER_SIZE: usize = 4664;
const SD_MAX_VDI_LEN: usize = 256;
/// Offset of the inode header's `vdi_epoch` — the ACL object's version.
const INO_OFF_VDI_EPOCH: usize = 544;
const VDI_BIT: u64 = 1 << 63;
/// `sizeof(struct vdi_lock_state)`, the `GET_VDI_LOCK_STATE` record.
const VDI_LOCK_STATE: usize = 316;
const VLS_OFF_COUNT: usize = 8;
const VLS_OFF_INDEX: usize = 12;
const VLS_OFF_OWNER: usize = 16;

const ACL_NQN: &str = "nqn.2026-06.io.ioutgt:genctr";
const ACL_VID: u32 = 0x0000_4711;
const VOL: &str = "vol";
const VOL_VID: u32 = 0x0000_4712;
/// 16 MiB in 4 MiB objects.
const VOL_SIZE: u64 = 16 << 20;
const VOL_SHIFT: u8 = 22;

/// A second target serving the volume, joining after this one is up.
const PEER: &str = "10.9.8.7:4420";

/// The epoch the ACL object carries when the target enumerates the cluster.
/// Not 1: the point is that the target reports the cluster's number, not a
/// counter of its own that happens to start there.
const EPOCH0: u64 = 7;

/// The fake cluster: the volume's participant slots and the ACL object's
/// `vdi_epoch`, both editable from the test.
struct Sheep {
    holders: Mutex<Vec<Option<(SocketAddr, u32)>>>,
    epoch: AtomicU64,
}

fn u32le(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}
fn u64le(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}

/// An inode object: the ACL object's (name, size, epoch and the flag that
/// makes it an ACL) or the volume's, followed by an all-zero object map.
fn inode(sheep: &Sheep, vid: u32) -> Vec<u8> {
    let mut b = vec![0u8; SD_INODE_HEADER_SIZE];
    if vid == ACL_VID {
        b[..ACL_NQN.len()].copy_from_slice(ACL_NQN.as_bytes());
        b[536..544].copy_from_slice(&(1u64 << 22).to_le_bytes()); // vdi_size
        let epoch = sheep.epoch.load(Ordering::Relaxed);
        b[INO_OFF_VDI_EPOCH..INO_OFF_VDI_EPOCH + 8].copy_from_slice(&epoch.to_le_bytes());
        b[554] = 1; // nr_copies
        b[555] = VOL_SHIFT;
        b[592..596].copy_from_slice(&SD_VDI_FLAG_ACL.to_le_bytes());
        // max_data_id_nr (528) + data_vdi_id[0] = VOL_VID: VOL is a declared
        // member, not just a volume that happens to name this ACL — the
        // namespace-membership refresh removes anything the ACL does not
        // list.
        b[528..532].copy_from_slice(&1u32.to_le_bytes());
        b.resize(SD_INODE_HEADER_SIZE + 4, 0);
        b[SD_INODE_HEADER_SIZE..SD_INODE_HEADER_SIZE + 4].copy_from_slice(&VOL_VID.to_le_bytes());
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

/// The `GET_VDI_LOCK_STATE` payload for the volume: one `vdi_lock_state`
/// record per occupied participant slot — a free slot is skipped entirely
/// rather than sent as a placeholder, since each record carries its own slot
/// index and nothing here reads the records positionally.
fn vdi_lock_states(slots: &[Option<(SocketAddr, u32)>]) -> Vec<u8> {
    let mut out = Vec::new();
    for (i, owner, count) in slots
        .iter()
        .enumerate()
        .filter_map(|(i, slot)| slot.map(|(owner, count)| (i, owner, count)))
    {
        let mut rec = vec![0u8; VDI_LOCK_STATE];
        rec[VLS_OFF_COUNT..VLS_OFF_COUNT + 4].copy_from_slice(&count.to_le_bytes());
        rec[VLS_OFF_INDEX..VLS_OFF_INDEX + 4].copy_from_slice(&(i as u32).to_le_bytes());
        let text = owner.to_string();
        rec[VLS_OFF_OWNER..VLS_OFF_OWNER + text.len()].copy_from_slice(text.as_bytes());
        out.extend_from_slice(&rec);
    }
    out
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

/// The vid a name resolves to under `acl`.
fn resolve(name: &str, acl: u32) -> Result<u32, u32> {
    match (name, acl) {
        (ACL_NQN, 0) => Ok(ACL_VID),
        (VOL, ACL_VID) => Ok(VOL_VID),
        (ACL_NQN | VOL, _) => Err(SD_RES_VDI_DENIED),
        _ => Err(SD_RES_NO_VDI),
    }
}

fn serve_conn(mut sock: TcpStream, sheep: &Sheep) -> std::io::Result<()> {
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
            SD_OP_REGISTER_VDI => {
                // The vid is already resolved (no name lookup here); the
                // owner travels as the payload.
                let mut p = vec![0u8; data_length];
                sock.read_exact(&mut p)?;
                let owner: SocketAddr = cstr(&p).parse().expect("test owner is ip:port");
                let mut slots = sheep.holders.lock().unwrap();
                let mine = slots
                    .iter()
                    .position(|s| s.is_some_and(|(addr, _)| addr == owner));
                match mine {
                    Some(i) => slots[i].as_mut().unwrap().1 += 1,
                    None => match slots.iter().position(Option::is_none) {
                        Some(free) => slots[free] = Some((owner, 1)),
                        None => slots.push(Some((owner, 1))),
                    },
                }
                sock.write_all(&resp(opcode, id, 0, 0))?;
            }
            SD_OP_UNREGISTER_VDI => {
                // Now a write op too: the owner travels as the payload here
                // as well.
                let mut p = vec![0u8; data_length];
                sock.read_exact(&mut p)?;
                let owner: SocketAddr = cstr(&p).parse().expect("test owner is ip:port");
                let mut slots = sheep.holders.lock().unwrap();
                if let Some(i) = slots
                    .iter()
                    .position(|s| s.is_some_and(|(addr, _)| addr == owner))
                {
                    let count = &mut slots[i].as_mut().unwrap().1;
                    *count -= 1;
                    if *count == 0 {
                        slots[i] = None;
                    }
                }
                drop(slots);
                sock.write_all(&resp(opcode, id, 0, 0))?;
            }
            SD_OP_GET_VDI_LOCK_STATE => {
                let states = vdi_lock_states(&sheep.holders.lock().unwrap());
                sock.write_all(&resp(opcode, id, 0, states.len() as u32))?;
                sock.write_all(&states)?;
            }
            SD_OP_READ_OBJ => {
                let inode = inode(sheep, ((oid & !VDI_BIT) >> 32) as u32);
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
fn spawn_fake_sheep(sheep: Arc<Sheep>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for sock in listener.incoming() {
            let Ok(sock) = sock else { break };
            sock.set_nodelay(true).ok();
            let sheep = Arc::clone(&sheep);
            std::thread::spawn(move || {
                let _ = serve_conn(sock, &sheep);
            });
        }
    });
    addr
}

/// The discovery log's header fields: `(genctr, numrec)`.
fn disc_header(client: &mut Client, cid: u16) -> (u64, u64) {
    let header = get_disc_log(client, cid, 0, 16);
    (
        u64::from_le_bytes(header[..8].try_into().unwrap()),
        u64::from_le_bytes(header[8..16].try_into().unwrap()),
    )
}

/// Re-read every tracked cluster, as the refresh thread does on its tick.
fn refresh() {
    assert!(
        ioutgt_harness::refresh_clusters() > 0,
        "a cluster is tracked"
    );
}

#[test]
fn the_discovery_log_generation_tracks_the_cluster_and_raises_an_aen() {
    let peer: SocketAddr = PEER.parse().unwrap();
    let sheep = Arc::new(Sheep {
        holders: Mutex::new(Vec::new()),
        epoch: AtomicU64::new(EPOCH0),
    });
    let cluster = spawn_fake_sheep(Arc::clone(&sheep));

    // A whole-cluster subsystem: the namespace is a volume on the cluster (so
    // opening it registers this target as a holder and gives the subsystem a
    // path list), and the subsystem is the export of the ACL object holding it
    // (so its host list and its discovery generation come off that object).
    let mut config = ioutgt_nvme_tcp::TargetConfig::single_memory(ACL_NQN, 16);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    // Whole-cluster mode's own invariant (`acl_subsystem`): NSID *is* the
    // vid, since that is the only cluster-wide count of a member ACL has.
    // The namespace-membership refresh diffs `data_vdi_id[]` against nsids on
    // that assumption, so this vid-matching-ACL-member test has to keep it.
    config.subsystems[0].namespaces[0].nsid = VOL_VID;
    config.subsystems[0].namespaces[0].backend = BackendConfig::Sheepdog {
        addr: cluster.to_string(),
        vdi: VOL.into(),
        tag: None,
        acl: Some(ACL_NQN.into()),
        lock: true,
    };
    config.subsystems[0].sheepdog_acl = Some(SheepdogAcl {
        cluster,
        vid: ACL_VID,
        epoch: EPOCH0,
        lock: true,
    });
    let addr = ioutgt_nvme_tcp::spawn_target(config).expect("target start");

    let mut client = Client::handshake(addr, false, false);
    connect_discovery(&mut client, common::HOSTNQN);
    client.enable_controller(2);

    // A discovery controller says it can report the change, or the host never
    // enables the event (it masks its AEC against OAES).
    let id = client.identify(spec::cns::CONTROLLER, 0, 3);
    let id = IdentifyController::read_from_bytes(&id).expect("identify controller");
    assert_eq!(
        id.oaes.get() & ioutgt_nvme::AEN_CFG_DISC_CHANGE,
        ioutgt_nvme::AEN_CFG_DISC_CHANGE,
        "OAES advertises the discovery log page change notice"
    );

    // The generation the host first sees is the cluster's own for the ACL
    // object, not a placeholder and not one past it: seeding the path list at
    // startup is not a change anyone could have missed.
    assert_eq!(
        disc_header(&mut client, 4),
        (EPOCH0, 1),
        "genctr is the ACL's vdi_epoch; one path, this target"
    );

    // A second target registers on the volume. The cluster records nothing
    // about it beyond the participant list — `vdi_epoch` does not move — so
    // this is the change only the target's own holder read can find.
    client.post_aer(5);
    sheep.holders.lock().unwrap().push(Some((peer, 1)));
    refresh();

    let cqe = client.recv_response();
    assert_eq!(cqe.cid.get(), 5, "the parked AER completed");
    assert_eq!(cqe.status.get() >> 1, status::SUCCESS);
    // Type Notice (2), info Discovery Log Page Changed (F0h), log page 70h.
    assert_eq!(cqe.result.get(), 0x0070_F002);

    let (genctr, numrec) = disc_header(&mut client, 6);
    assert_eq!(numrec, 2, "the peer is a second path to the subsystem");
    assert_eq!(
        genctr,
        EPOCH0 + 1,
        "a path change the cluster does not count, the target does"
    );

    // A refresh that finds nothing changed raises nothing and leaves the
    // generation alone — a host that re-reads must see the same log.
    refresh();
    assert_eq!(disc_header(&mut client, 7), (EPOCH0 + 1, 2));

    // `dog acl add vdi`: the cluster bumps the ACL object's epoch. Past the
    // local count, so it is the generation from here on.
    client.post_aer(8);
    sheep.epoch.store(EPOCH0 + 20, Ordering::Relaxed);
    refresh();

    let cqe = client.recv_response();
    assert_eq!(cqe.cid.get(), 8, "the epoch change reached the parked AER");
    assert_eq!(cqe.result.get(), 0x0070_F002);
    assert_eq!(
        disc_header(&mut client, 9),
        (EPOCH0 + 20, 2),
        "the cluster's epoch, once it is ahead of the local count"
    );

    // An epoch that has fallen behind what this target already counted is not
    // applied: the generation only ever moves forward.
    sheep.epoch.store(1, Ordering::Relaxed);
    refresh();
    assert_eq!(disc_header(&mut client, 10), (EPOCH0 + 20, 2));

    drop(client);
    assert_eq!(ioutgt_harness::shutdown(), 1, "one namespace released");
}
