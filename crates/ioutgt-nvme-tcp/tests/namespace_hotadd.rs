//! Whole-cluster mode's namespace table stays live: a subsystem *is* the
//! export of a Sheepdog ACL object, and the ACL's member VDIs — not just its
//! member names or its `vdi_epoch` — are what the refresh thread keeps
//! current. `dog acl add vdi`/`remove vdi` on a running cluster adds or
//! removes a namespace here, the connected controller's parked AER completes
//! with the NS_ATTR notice, and the new nsid shows up wherever a host asks
//! (`nvme list-ns`, Identify Namespace) — no target restart.
//!
//! The cluster is a minimal in-process fake `sheep` answering the requests
//! this path makes — the name lookup and the inode read, for the ACL object
//! and for each member, out of a `data_vdi_id[]` array the test edits
//! between refreshes — plus `REGISTER_VDI`/`UNREGISTER_VDI` for the tests
//! that lock their namespaces, tracking which vids currently hold the
//! cluster's shared lock.

// Test-only offset arithmetic on a 64-bit host; values are small and bounded.
#![allow(clippy::cast_possible_truncation)]

mod common;

use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use common::{Client, rw_sqe};
use ioutgt_control::config::SheepdogAcl;
use ioutgt_nvme::identify::IdentifyNamespace;
use ioutgt_nvme::{spec, status};
use zerocopy::{FromBytes, IntoBytes};

// --- wire constants (include/sheepdog_proto.h, include/internal_proto.h) --
const HDR: usize = 48;
const SD_OP_READ_OBJ: u8 = 0x02;
const SD_OP_GET_VDI_INFO: u8 = 0x14;
const SD_OP_REGISTER_VDI: u8 = 0x19;
const SD_OP_UNREGISTER_VDI: u8 = 0x1A;
const SD_RES_NO_VDI: u32 = 0x08;
const SD_RES_VDI_DENIED: u32 = 0x1E;
const SD_VDI_FLAG_ACL: u32 = 0x01;
const SD_INODE_HEADER_SIZE: usize = 4664;
const SD_MAX_VDI_LEN: usize = 256;
const VDI_BIT: u64 = 1 << 63;

const ACL_NQN: &str = "nqn.2026-06.io.ioutgt:hotadd";
const ACL_VID: u32 = 0x0000_4711;
const NS1: &str = "ns1";
const NS1_VID: u32 = 0x0000_2001;
const NS2: &str = "ns2";
const NS2_VID: u32 = 0x0000_2002;
/// 4 MiB, one object.
const VOL_SIZE: u64 = 4 << 20;
const VOL_SHIFT: u8 = 22;

fn u32le(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}
fn u64le(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}
fn cstr(field: &[u8]) -> String {
    let end = field.iter().position(|&b| b == 0).unwrap_or(field.len());
    String::from_utf8_lossy(&field[..end]).into_owned()
}

/// The NSIDs named by an active-NSID-list payload (zero-terminated, 4 bytes
/// little-endian each).
fn active_nsids(payload: &[u8]) -> Vec<u32> {
    payload
        .chunks_exact(4)
        .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
        .take_while(|&n| n != 0)
        .collect()
}

/// The fake cluster: one ACL object and the `data_vdi_id[]` array the test
/// edits to simulate `dog acl add vdi`/`remove vdi`. Zero entries are holes,
/// exactly as a removal leaves them on a real cluster.
struct Sheep {
    members: Mutex<Vec<u32>>,
    /// The vids currently holding this cluster's shared lock — `REGISTER_VDI`
    /// adds, `UNREGISTER_VDI` (which carries the vid directly, no lookup)
    /// removes.
    registered: Mutex<HashSet<u32>>,
}

impl Sheep {
    fn inode(&self, vid: u32) -> Vec<u8> {
        let mut b = vec![0u8; SD_INODE_HEADER_SIZE];
        match vid {
            ACL_VID => {
                b[..ACL_NQN.len()].copy_from_slice(ACL_NQN.as_bytes());
                b[536..544].copy_from_slice(&(1u64 << 22).to_le_bytes()); // vdi_size
                b[554] = 1; // nr_copies
                b[555] = VOL_SHIFT;
                b[592..596].copy_from_slice(&SD_VDI_FLAG_ACL.to_le_bytes());
                let members = self.members.lock().unwrap();
                b[528..532].copy_from_slice(&(members.len() as u32).to_le_bytes());
                b.resize(SD_INODE_HEADER_SIZE + members.len() * 4, 0);
                for (i, &vid) in members.iter().enumerate() {
                    let off = SD_INODE_HEADER_SIZE + i * 4;
                    b[off..off + 4].copy_from_slice(&vid.to_le_bytes());
                }
            }
            _ => {
                let name = if vid == NS1_VID { NS1 } else { NS2 };
                b[..name.len()].copy_from_slice(name.as_bytes());
                b[536..544].copy_from_slice(&VOL_SIZE.to_le_bytes());
                b[554] = 1;
                b[555] = VOL_SHIFT;
                b[572..576].copy_from_slice(&ACL_VID.to_le_bytes()); // acl_id
                b.resize(
                    SD_INODE_HEADER_SIZE + 4 * (VOL_SIZE >> VOL_SHIFT) as usize,
                    0,
                );
            }
        }
        b
    }
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

/// The vid a name resolves to under `acl` (the ACL object itself is in none).
fn resolve(name: &str, acl: u32) -> Result<u32, u32> {
    match (name, acl) {
        (NS1, ACL_VID) => Ok(NS1_VID),
        (NS2, ACL_VID) => Ok(NS2_VID),
        (NS1 | NS2, _) => Err(SD_RES_VDI_DENIED),
        (ACL_NQN, 0) => Ok(ACL_VID),
        (ACL_NQN, _) => Err(SD_RES_VDI_DENIED),
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
            SD_OP_READ_OBJ => {
                let inode = sheep.inode(((oid & !VDI_BIT) >> 32) as u32);
                let end = (offset + data_length).min(inode.len());
                let slice = &inode[offset.min(inode.len())..end];
                sock.write_all(&resp(opcode, id, 0, slice.len() as u32))?;
                sock.write_all(slice)?;
            }
            SD_OP_REGISTER_VDI => {
                // The vid is already resolved (sd_req.vdi_lock.vid, at
                // header offset 16); the payload is now the owner string,
                // which this fake has no use for — it only tracks whether a
                // vid is registered, not by whom.
                let mut p = vec![0u8; data_length];
                sock.read_exact(&mut p)?;
                let vid = u32le(&hdr, 16);
                sheep.registered.lock().unwrap().insert(vid);
                sock.write_all(&resp(opcode, id, 0, 0))?;
            }
            SD_OP_UNREGISTER_VDI => {
                // Now a write op too: the owner string travels as the
                // payload here as well, and must be drained off the wire
                // even though this fake ignores it.
                let mut p = vec![0u8; data_length];
                sock.read_exact(&mut p)?;
                let vid = u32le(&hdr, 16);
                sheep.registered.lock().unwrap().remove(&vid);
                sock.write_all(&resp(opcode, id, 0, 0))?;
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

/// Re-read every tracked cluster, as the refresh thread does on its tick.
fn refresh() {
    assert!(
        ioutgt_harness::refresh_clusters() > 0,
        "the ACL object is tracked"
    );
}

#[test]
fn a_vdi_added_to_a_live_acl_becomes_a_namespace() {
    let sheep = Arc::new(Sheep {
        members: Mutex::new(vec![NS1_VID]),
        registered: Mutex::new(HashSet::new()),
    });
    let cluster = spawn_fake_sheep(Arc::clone(&sheep));

    // The subsystem as whole-cluster mode built it: NSID *is* the vid
    // (`acl_subsystem`'s own convention — the namespace-membership refresh
    // relies on it to tell an ACL's members from a subsystem's namespaces).
    let mut config = ioutgt_nvme_tcp::TargetConfig::single_memory(ACL_NQN, 16);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    config.subsystems[0].namespaces[0].nsid = NS1_VID;
    config.subsystems[0].namespaces[0].backend = ioutgt_control::config::BackendConfig::Sheepdog {
        addr: cluster.to_string(),
        vdi: NS1.into(),
        tag: None,
        acl: Some(ACL_NQN.into()),
        lock: false,
    };
    config.subsystems[0].sheepdog_acl = Some(SheepdogAcl {
        cluster,
        vid: ACL_VID,
        epoch: 1,
        lock: false,
    });
    let addr = ioutgt_nvme_tcp::spawn_target(config).expect("target start");

    let mut admin = Client::handshake(addr, false, false);
    let (sqe, mut data) = common::connect_sqe(0, 32, 0xFFFF, 1);
    data.subsysnqn = [0; 256];
    data.subsysnqn[..ACL_NQN.len()].copy_from_slice(ACL_NQN.as_bytes());
    admin.send_capsule(&sqe, data.as_bytes());
    let cqe = admin.recv_response();
    assert_eq!(
        cqe.status.get() >> 1,
        status::SUCCESS,
        "connect to {ACL_NQN}"
    );
    admin.enable_controller(2);

    // Baseline inventory: the one namespace the target started with.
    let list = admin.identify(spec::cns::ACTIVE_NS_LIST, 0, 3);
    assert_eq!(active_nsids(&list), vec![NS1_VID]);

    // `dog acl add vdi`: a second member joins the ACL's array.
    admin.post_aer(4);
    sheep.members.lock().unwrap().push(NS2_VID);
    refresh();

    // The parked AER completes with the NS_ATTR notice — the same one
    // `ADD_NAMESPACE` over the control socket raises.
    let cqe = admin.recv_response();
    assert_eq!(cqe.cid.get(), 4, "the parked AER completed");
    assert_eq!(cqe.status.get() >> 1, status::SUCCESS);
    assert_eq!(cqe.result.get(), 0x0004_0002, "NS_ATTR_CHANGED notice");

    // The new nsid is in the inventory, and Identify Namespace answers for
    // it — not a placeholder, a real backend the target opened just now.
    let list = admin.identify(spec::cns::ACTIVE_NS_LIST, 0, 5);
    assert_eq!(active_nsids(&list), vec![NS1_VID, NS2_VID]);
    let ns = admin.identify(spec::cns::NAMESPACE, NS2_VID, 6);
    let ns = IdentifyNamespace::read_from_bytes(&ns).expect("identify namespace");
    assert_eq!(
        u64::from_le_bytes(ns.nsze.get().to_le_bytes()) * 512,
        VOL_SIZE,
        "the hot-added volume's own size, not a guess"
    );

    // A refresh that finds nothing changed raises nothing and leaves the
    // inventory alone.
    refresh();
    let list = admin.identify(spec::cns::ACTIVE_NS_LIST, 0, 7);
    assert_eq!(active_nsids(&list), vec![NS1_VID, NS2_VID]);

    // `dog acl remove vdi`: NS1 leaves the array (a hole, not a shorter
    // array — the same shape a real removal leaves).
    admin.post_aer(8);
    sheep.members.lock().unwrap()[0] = 0;
    refresh();

    let cqe = admin.recv_response();
    assert_eq!(cqe.cid.get(), 8, "the removal reached the parked AER too");
    assert_eq!(cqe.result.get(), 0x0004_0002);
    let list = admin.identify(spec::cns::ACTIVE_NS_LIST, 0, 9);
    assert_eq!(active_nsids(&list), vec![NS2_VID], "NS1 is gone");

    // NS1's nsid is now inactive (below NN, no backend) rather than invalid:
    // the same distinction a config-file namespace removed at runtime gets.
    let ns = admin.identify(spec::cns::NAMESPACE, NS1_VID, 10);
    assert_eq!(
        ns,
        IdentifyNamespace::zeroed().as_bytes(),
        "inactive, not an error — NS1_VID is still <= NN (NS2_VID)"
    );
}

/// A hot-removed namespace's cluster lock releases even when the IO queue
/// that once touched it never sends another command afterward.
///
/// `Subsystem::remove_namespace` only swaps the admin thread's own `Arc` —
/// an IO queue caches the whole table (`NsCache`, one atomic generation
/// check per command) and only refreshes it on its *next* command. Without
/// an explicit release at removal time, a queue that goes quiet right after
/// caching a snapshot that still includes the removed namespace (host
/// stopped sending it IO, ANA failover moved elsewhere, or simply no IO
/// arrives) would keep that namespace's backend alive — and its Sheepdog
/// lock held — indefinitely, even though the admin thread and `nvme
/// list-ns` already agree it is gone.
#[test]
fn a_hot_removed_locked_vdi_releases_its_lock_without_further_io() {
    let sheep = Arc::new(Sheep {
        members: Mutex::new(vec![NS1_VID]),
        registered: Mutex::new(HashSet::new()),
    });
    let cluster = spawn_fake_sheep(Arc::clone(&sheep));

    let mut config = ioutgt_nvme_tcp::TargetConfig::single_memory(ACL_NQN, 16);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    config.subsystems[0].namespaces[0].nsid = NS1_VID;
    config.subsystems[0].namespaces[0].backend = ioutgt_control::config::BackendConfig::Sheepdog {
        addr: cluster.to_string(),
        vdi: NS1.into(),
        tag: None,
        acl: Some(ACL_NQN.into()),
        lock: true,
    };
    config.subsystems[0].sheepdog_acl = Some(SheepdogAcl {
        cluster,
        vid: ACL_VID,
        epoch: 1,
        lock: true,
    });
    let addr = ioutgt_nvme_tcp::spawn_target(config).expect("target start");
    assert!(
        sheep.registered.lock().unwrap().contains(&NS1_VID),
        "NS1 locks at startup"
    );

    let mut admin = Client::handshake(addr, false, false);
    let (sqe, mut data) = common::connect_sqe(0, 32, 0xFFFF, 1);
    data.subsysnqn = [0; 256];
    data.subsysnqn[..ACL_NQN.len()].copy_from_slice(ACL_NQN.as_bytes());
    admin.send_capsule(&sqe, data.as_bytes());
    let cqe = admin.recv_response();
    assert_eq!(
        cqe.status.get() >> 1,
        status::SUCCESS,
        "connect to {ACL_NQN}"
    );
    let cntlid = u16::try_from(cqe.result.get() & 0xFFFF).expect("cntlid fits u16");
    admin.enable_controller(2);

    // `dog acl add vdi`: NS2 joins, locked the same way NS1 is.
    sheep.members.lock().unwrap().push(NS2_VID);
    refresh();
    assert_eq!(
        active_nsids(&admin.identify(spec::cns::ACTIVE_NS_LIST, 0, 3)),
        vec![NS1_VID, NS2_VID]
    );
    assert!(
        sheep.registered.lock().unwrap().contains(&NS2_VID),
        "NS2 locks once hot-added"
    );

    // An IO queue connects and sends exactly one command — enough to cache a
    // table snapshot that includes NS2 — and then never sends another.
    let mut io = Client::handshake(addr, false, false);
    let (sqe, mut data) = common::connect_sqe(1, 32, cntlid, 1);
    data.subsysnqn = [0; 256];
    data.subsysnqn[..ACL_NQN.len()].copy_from_slice(ACL_NQN.as_bytes());
    io.send_capsule(&sqe, data.as_bytes());
    assert_eq!(
        io.recv_response().status.get() >> 1,
        status::SUCCESS,
        "io connect"
    );
    // A read of an untouched (hole) object needs no `WRITE_OBJ` support from
    // the fake — `ns_cache` is populated ahead of the read/write split, so
    // this is enough to cache a table snapshot that includes NS2.
    let mut sqe = rw_sqe(spec::io_opcode::READ, 0x10, 0, 7, 4096, true);
    sqe.nsid.set(NS1_VID);
    io.send_capsule(&sqe, &[]);
    let (_, payload) = io.recv_pdu();
    assert_eq!(payload, vec![0u8; 4096], "an untouched object reads zeroes");
    assert_eq!(
        io.recv_response().status.get() >> 1,
        status::SUCCESS,
        "one read, to populate the queue's table cache"
    );

    // `dog acl remove vdi --force`: NS2 leaves the array. The admin thread's
    // table drops it...
    sheep.members.lock().unwrap().retain(|&v| v != NS2_VID);
    refresh();
    assert_eq!(
        active_nsids(&admin.identify(spec::cns::ACTIVE_NS_LIST, 0, 4)),
        vec![NS1_VID],
        "NS2 is gone from the table"
    );

    // ...and so must the cluster lock, even though `io`'s queue never sent
    // another command after caching a snapshot that still had NS2 in it.
    assert!(
        !sheep.registered.lock().unwrap().contains(&NS2_VID),
        "NS2's lock must release without the IO queue revisiting it"
    );
    assert!(
        sheep.registered.lock().unwrap().contains(&NS1_VID),
        "NS1, still exported, stays locked"
    );
}
