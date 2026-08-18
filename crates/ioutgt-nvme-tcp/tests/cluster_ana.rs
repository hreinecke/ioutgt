//! End-to-end ANA for cluster namespaces: a subsystem holding Sheepdog
//! volumes reports Asymmetric Namespace Access, and each namespace lands in
//! the optimized group exactly when the `sheep` this target talks to stores
//! that volume's inode object itself.
//!
//! The cluster is a minimal in-process fake `sheep` answering the three
//! requests this path makes — the name lookup, the inode read, and the
//! `EXIST` locality probe — with one volume stored locally and one not.
//! Locking is off (`lock: false`), which is also the point: ANA does not ride
//! on VDI registration.

// Test-only offset arithmetic on a 64-bit host; values are small and bounded.
#![allow(clippy::cast_possible_truncation)]

mod common;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use common::Client;
use ioutgt_control::config::{BackendConfig, NamespaceConfig};
use ioutgt_nvme::identify::{IdentifyController, IdentifyNamespace, anacap, cmic};
use ioutgt_nvme::spec::{self, ana};
use ioutgt_nvme::status;
use zerocopy::{FromBytes, IntoBytes};

// --- wire constants (include/sheepdog_proto.h) -----------------------------
const HDR: usize = 48;
const SD_OP_READ_OBJ: u8 = 0x02;
const SD_OP_GET_VDI_INFO: u8 = 0x14;
const SD_OP_EXIST: u8 = 0xBD;
const SD_RES_NO_OBJ: u32 = 0x02;
const SD_RES_NO_VDI: u32 = 0x08;
const SD_RES_VDI_DENIED: u32 = 0x1E;
const SD_VDI_FLAG_ACL: u32 = 0x01;
const SD_SHEEP_PROTO_VER: u8 = 0x0a;
const SD_INODE_HEADER_SIZE: usize = 4664;
const SD_MAX_VDI_LEN: usize = 256;
const VDI_BIT: u64 = 1 << 63;

/// The vid of the ACL object naming a subsystem, and of the two volumes in
/// it: one whose inode object this node stores, one whose it does not.
const ACL_VID: u32 = 0x0000_4711;
const NEAR: &str = "near";
const NEAR_VID: u32 = 0x0000_4712;
const FAR: &str = "far";
const FAR_VID: u32 = 0x0000_4713;
/// 16 MiB in 4 MiB objects.
const VOL_SIZE: u64 = 16 << 20;
const VOL_SHIFT: u8 = 22;

/// The fake cluster: one ACL object, the two volumes in it, and which volume's
/// inode object this particular gateway keeps in its own store.
struct Sheep {
    /// The ACL object's name — the NQN of the subsystem it belongs to. One per
    /// target, so tests in this binary do not share a subsystem.
    acl_nqn: String,
    /// The vid whose inode object is stored here; every other object lives on
    /// some other node. `0` for a gateway storing nothing itself.
    local: AtomicU32,
}

fn u32le(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}
fn u64le(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}

/// An inode object: the ACL object's, or a volume's followed by an all-zero
/// (unallocated) object map.
fn inode(sheep: &Sheep, vid: u32) -> Vec<u8> {
    let mut b = vec![0u8; SD_INODE_HEADER_SIZE];
    if vid == ACL_VID {
        b[..sheep.acl_nqn.len()].copy_from_slice(sheep.acl_nqn.as_bytes());
        b[536..544].copy_from_slice(&(1u64 << 22).to_le_bytes()); // vdi_size
        b[554] = 1; // nr_copies
        b[555] = VOL_SHIFT;
        b[592..596].copy_from_slice(&SD_VDI_FLAG_ACL.to_le_bytes());
        return b;
    }
    let name = if vid == NEAR_VID { NEAR } else { FAR };
    b[..name.len()].copy_from_slice(name.as_bytes());
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

/// The vid a name resolves to under `acl` (the ACL object itself is in none).
fn resolve(sheep: &Sheep, name: &str, acl: u32) -> Result<u32, u32> {
    match (name, acl) {
        (NEAR, ACL_VID) => Ok(NEAR_VID),
        (FAR, ACL_VID) => Ok(FAR_VID),
        (NEAR | FAR, _) => Err(SD_RES_VDI_DENIED),
        _ if name == sheep.acl_nqn && acl == 0 => Ok(ACL_VID),
        _ if name == sheep.acl_nqn => Err(SD_RES_VDI_DENIED),
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
                match resolve(sheep, &cstr(&p[..SD_MAX_VDI_LEN]), u32le(&hdr, 36)) {
                    Ok(vid) => {
                        let mut r = resp(opcode, id, 0, 0);
                        r[24..28].copy_from_slice(&vid.to_le_bytes()); // vdi_id
                        sock.write_all(&r)?;
                    }
                    Err(res) => sock.write_all(&resp(opcode, id, res, 0))?,
                }
            }
            SD_OP_READ_OBJ => {
                let inode = inode(sheep, ((oid & !VDI_BIT) >> 32) as u32);
                let end = (offset + data_length).min(inode.len());
                let slice = &inode[offset.min(inode.len())..end];
                sock.write_all(&resp(opcode, id, 0, slice.len() as u32))?;
                sock.write_all(slice)?;
            }
            // "Do you store this object yourself?" — a local op, answered out
            // of this node's own store. Sheep-internal opcodes carry the sheep
            // protocol version, not the client one.
            SD_OP_EXIST => {
                assert_eq!(hdr[0], SD_SHEEP_PROTO_VER, "EXIST is a sheep-internal op");
                assert_eq!(oid & VDI_BIT, VDI_BIT, "locality is asked of the inode");
                let vid = ((oid & !VDI_BIT) >> 32) as u32;
                let result = if vid == sheep.local.load(Ordering::Relaxed) {
                    0
                } else {
                    SD_RES_NO_OBJ
                };
                sock.write_all(&resp(opcode, id, result, 0))?;
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

fn sheepdog_ns(nsid: u32, cluster: SocketAddr, acl_nqn: &str, vdi: &str) -> NamespaceConfig {
    NamespaceConfig {
        nsid,
        backend: BackendConfig::Sheepdog {
            addr: cluster.to_string(),
            vdi: vdi.into(),
            tag: None,
            acl: Some(acl_nqn.into()),
            lock: false,
        },
        uuid: None,
    }
}

/// Start a target whose subsystem is `nqn`, serving `volumes` off a fake
/// cluster that stores `local`'s inode object. Returns the target's address
/// and the cluster, so a test can move an object under it.
fn spawn_target(nqn: &str, volumes: &[&str], local: u32) -> (SocketAddr, Arc<Sheep>) {
    let sheep = Arc::new(Sheep {
        acl_nqn: nqn.into(),
        local: AtomicU32::new(local),
    });
    let cluster = spawn_fake_sheep(Arc::clone(&sheep));

    let mut config = ioutgt_nvme_tcp::TargetConfig::single_memory(nqn, 16);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    config.subsystems[0].namespaces = volumes
        .iter()
        .enumerate()
        .map(|(i, vdi)| sheepdog_ns(i as u32 + 1, cluster, nqn, vdi))
        .collect();
    let addr = ioutgt_nvme_tcp::spawn_target(config).expect("target start");
    (addr, sheep)
}

/// An admin connection to subsystem `nqn` with the controller enabled, ready
/// for commands.
fn admin_client(addr: SocketAddr, nqn: &str) -> Client {
    let mut client = Client::handshake(addr, false, false);
    let (sqe, mut data) = common::connect_sqe(0, 32, 0xFFFF, 1);
    data.subsysnqn = [0; 256];
    data.subsysnqn[..nqn.len()].copy_from_slice(nqn.as_bytes());
    client.send_capsule(&sqe, data.as_bytes());
    let cqe = client.recv_response();
    assert_eq!(cqe.status.get() >> 1, status::SUCCESS, "connect to {nqn}");
    client.enable_controller(2);
    client
}

/// Parse an ANA log page into `(grpid, state, nsids)` per group, checking the
/// descriptors tile the page exactly.
fn groups(log: &[u8]) -> Vec<(u32, u8, Vec<u32>)> {
    let header = ana::LogHeader::read_from_bytes(&log[..16]).expect("ana header");
    let mut off = 16;
    let mut groups = Vec::new();
    for _ in 0..header.ngrps.get() {
        let desc = ana::GroupDesc::read_from_bytes(&log[off..off + 32]).expect("group desc");
        off += 32;
        let nsids: Vec<u32> = (0..desc.nnsids.get() as usize)
            .map(|i| u32le(log, off + i * 4))
            .collect();
        off += nsids.len() * 4;
        groups.push((desc.grpid.get(), desc.state, nsids));
    }
    assert_eq!(off, log.len(), "no trailing bytes past the last group");
    groups
}

/// The change count in an ANA log page header.
fn chgcnt(log: &[u8]) -> u64 {
    ana::LogHeader::read_from_bytes(&log[..16])
        .expect("ana header")
        .chgcnt
        .get()
}

#[test]
fn cluster_namespaces_report_ana_by_object_locality() {
    let nqn = "nqn.2026-06.io.ioutgt:ana";
    let (addr, _sheep) = spawn_target(nqn, &[NEAR, FAR], NEAR_VID);
    let mut admin = admin_client(addr, nqn);

    // Identify Controller: the whole ANA field set, which the host validates
    // together before it will use the log page at all.
    let ctrl = admin.identify(spec::cns::CONTROLLER, 0, 3);
    let ctrl = IdentifyController::read_from_bytes(&ctrl).expect("identify controller");
    assert_eq!(
        ctrl.cmic & cmic::ANA_REPORTING,
        cmic::ANA_REPORTING,
        "a cluster subsystem reports ANA"
    );
    assert_eq!(ctrl.anagrpmax.get(), 2, "one group per ANA state we report");
    assert_eq!(ctrl.nanagrpid.get(), 2, "both groups are always reported");
    assert_ne!(ctrl.anatt, 0, "ANATT must be a real transition timeout");
    assert_eq!(
        ctrl.anacap,
        anacap::OPTIMIZED | anacap::NON_OPTIMIZED,
        "only the two states locality maps to, and no STATIC_GRPID: a \
         namespace moves group when its locality changes"
    );
    assert_eq!(
        ctrl.oaes.get() & ioutgt_nvme::AEN_CFG_ANA_CHANGE,
        ioutgt_nvme::AEN_CFG_ANA_CHANGE,
        "OAES must offer the ANA Change notice or the host never enables it"
    );
    // MNAN sizes the host's ANA log buffer; zero or above NN is rejected.
    assert!(
        (1..=ctrl.nn.get()).contains(&ctrl.mnan.get()),
        "MNAN {} out of range for NN {}",
        ctrl.mnan.get(),
        ctrl.nn.get()
    );

    // Identify Namespace: the group each namespace is in.
    for (nsid, grpid) in [(1u32, 1u32), (2, 2)] {
        let ns = admin.identify(spec::cns::NAMESPACE, nsid, 4);
        let ns = IdentifyNamespace::read_from_bytes(&ns).expect("identify namespace");
        assert_eq!(
            ns.anagrpid.get(),
            grpid,
            "nsid {nsid} belongs to ANA group {grpid}"
        );
    }

    // The ANA log page itself: two group descriptors, the local volume
    // optimized and the remote one not.
    let log = common::get_log_page(&mut admin, spec::log_page::ANA, 0, 5, 0, 4096);
    assert_ne!(chgcnt(&log), 0, "the change count is live");
    assert_eq!(
        groups(&log),
        vec![(1, 0x01, vec![1]), (2, 0x02, vec![2])],
        "group 1 optimized holds the local volume, group 2 non-optimized the \
         remote one"
    );

    // RGO (LSP bit 0): the same groups without the NSID lists.
    let rgo = common::get_log_page(&mut admin, spec::log_page::ANA, ana::LSP_RGO, 6, 0, 4096);
    assert_eq!(rgo.len(), 16 + 2 * 32, "groups only");
    assert_eq!(
        groups(&rgo),
        vec![(1, 0x01, vec![]), (2, 0x02, vec![])],
        "the groups and their states, with no namespaces listed"
    );

    // A log-page offset windows into the same bytes.
    let tail = common::get_log_page(&mut admin, spec::log_page::ANA, 0, 7, 16, 4096);
    assert_eq!(tail, log[16..], "LPO skips the header");
}

/// A subsystem with no cluster storage reports no ANA at all: every path to a
/// local namespace is the same path, and an ANA log page saying so would only
/// give the host something to poll.
#[test]
fn local_namespaces_report_no_ana() {
    let mut config = ioutgt_nvme_tcp::TargetConfig::single_memory(common::NQN, 16);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    let addr = ioutgt_nvme_tcp::spawn_target(config).expect("target start");
    let mut admin = admin_client(addr, common::NQN);

    let ctrl = admin.identify(spec::cns::CONTROLLER, 0, 3);
    let ctrl = IdentifyController::read_from_bytes(&ctrl).expect("identify controller");
    assert_eq!(
        ctrl.cmic & cmic::ANA_REPORTING,
        0,
        "no ANA without a cluster"
    );
    assert_eq!(ctrl.nanagrpid.get(), 0);
    assert_eq!(ctrl.anagrpmax.get(), 0);
    assert_eq!(
        ctrl.oaes.get() & ioutgt_nvme::AEN_CFG_ANA_CHANGE,
        0,
        "no ANA Change notices to offer"
    );

    // ...and the log page it would come with is not there either.
    let mut sqe = spec::Sqe::zeroed();
    sqe.opcode = spec::admin_opcode::GET_LOG_PAGE;
    sqe.flags = spec::CMD_FLAGS_SGL_METABUF;
    sqe.cid.set(8);
    sqe.cdw10.set(u32::from(spec::log_page::ANA) | (255 << 16));
    sqe.dptr.length.set(1024);
    sqe.dptr.sgl_type = spec::sgl::TYPE_TRANSPORT_DATA_BLOCK;
    admin.send_capsule(&sqe, &[]);
    let cqe = admin.recv_response();
    assert_eq!(
        cqe.status.get() >> 1,
        status::INVALID_LOG_PAGE | status::DNR,
        "ANA log page rejected on a subsystem that does not report ANA"
    );
}

/// The host learns about a locality change from an async event, not by
/// polling: move the volume's inode object onto this node and the parked AER
/// completes with the ANA Change notice, after which the log page reads back
/// optimized.
#[test]
fn a_locality_change_raises_an_ana_change_notice() {
    let nqn = "nqn.2026-06.io.ioutgt:ana-change";
    // Nothing stored here yet: the one volume is reachable, not preferred.
    let (addr, sheep) = spawn_target(nqn, &[FAR], 0);
    let mut admin = admin_client(addr, nqn);
    let before = common::get_log_page(&mut admin, spec::log_page::ANA, 0, 3, 0, 4096);
    assert_eq!(groups(&before), vec![(1, 0x01, vec![]), (2, 0x02, vec![1])]);

    admin.post_aer(4);
    // The volume's inode object lands on this node, and the refresh notices.
    sheep.local.store(FAR_VID, Ordering::Relaxed);
    assert!(
        ioutgt_harness::refresh_clusters() > 0,
        "a cluster is tracked"
    );

    let cqe = admin.recv_response();
    assert_eq!(cqe.cid.get(), 4, "the parked AER completed");
    assert_eq!(cqe.status.get() >> 1, status::SUCCESS);
    // Type Notice (2), info ANA Change (3), log page to re-read (0Ch).
    assert_eq!(cqe.result.get(), 0x000C_0302);

    let after = common::get_log_page(&mut admin, spec::log_page::ANA, 0, 5, 0, 4096);
    assert_eq!(
        groups(&after),
        vec![(1, 0x01, vec![1]), (2, 0x02, vec![])],
        "the namespace moved to the optimized group"
    );
    assert!(
        chgcnt(&after) > chgcnt(&before),
        "the change count advanced with the change"
    );
}
