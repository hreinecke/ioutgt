//! A cluster subsystem's host ACL is the Sheepdog ACL object's member list,
//! and it stays that way: the refresh thread re-reads the members, so `dog acl
//! add member` / `dog acl remove member` on a running cluster decides who may
//! Connect next — no target restart.
//!
//! The cluster is a minimal in-process fake `sheep` answering the one request
//! this path makes, a `READ_OBJ` of the ACL object's inode, out of a member
//! list the test edits between refreshes.

// Test-only offset arithmetic on a 64-bit host; values are small and bounded.
#![allow(clippy::cast_possible_truncation)]

mod common;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use ioutgt_control::config::SheepdogAcl;
use ioutgt_nvme::status;
use zerocopy::IntoBytes;

// --- wire constants (include/sheepdog_proto.h) -----------------------------
const HDR: usize = 48;
const SD_OP_READ_OBJ: u8 = 0x02;
const SD_INODE_HEADER_SIZE: usize = 4664;
/// Offset of the inode header's `metadata[]` area, which on an ACL object
/// holds the member names `dog acl add member` writes.
const INO_OFF_METADATA: usize = 600;
const SD_MAX_VDI_LEN: usize = 256;
const VDI_BIT: u64 = 1 << 63;

const NQN: &str = "nqn.2026-06.io.ioutgt:acl-members";
/// The vid of the ACL object the subsystem is built from.
const ACL_VID: u32 = 0x0000_4711;

const HOST_A: &str = "nqn.2014-08.org.nvmexpress:uuid:host-a";
const HOST_B: &str = "nqn.2014-08.org.nvmexpress:uuid:host-b";
const HOST_C: &str = "nqn.2014-08.org.nvmexpress:uuid:host-c";

/// The fake cluster: one ACL object whose member list the test rewrites, and a
/// switch for pulling the cluster out from under a refresh.
struct Sheep {
    /// The ACL inode's member-name slots, holes ("") included.
    members: Mutex<Vec<String>>,
    /// Answer nothing and drop the connection — a gateway that went away.
    broken: AtomicBool,
}

impl Sheep {
    /// Rewrite the ACL object's member list, as `dog acl add`/`remove member`
    /// would.
    fn set_members(&self, members: &[&str]) {
        *self.members.lock().unwrap() = members.iter().map(|m| (*m).to_string()).collect();
    }

    /// The ACL object's inode, with the member names in their slots.
    fn inode(&self) -> Vec<u8> {
        let mut b = vec![0u8; SD_INODE_HEADER_SIZE];
        b[..NQN.len()].copy_from_slice(NQN.as_bytes());
        for (i, member) in self.members.lock().unwrap().iter().enumerate() {
            let off = INO_OFF_METADATA + i * SD_MAX_VDI_LEN;
            b[off..off + member.len()].copy_from_slice(member.as_bytes());
        }
        b
    }
}

fn u32le(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}
fn u64le(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(b[o..o + 8].try_into().unwrap())
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

fn serve_conn(mut sock: TcpStream, sheep: &Sheep) -> std::io::Result<()> {
    loop {
        let mut hdr = [0u8; HDR];
        if sock.read_exact(&mut hdr).is_err() {
            return Ok(()); // peer closed
        }
        if sheep.broken.load(Ordering::Relaxed) {
            return Ok(());
        }
        let opcode = hdr[1];
        let id = u32le(&hdr, 8);
        let data_length = u32le(&hdr, 12) as usize;
        let oid = u64le(&hdr, 16);
        let offset = u32le(&hdr, 40) as usize;

        match opcode {
            SD_OP_READ_OBJ => {
                assert_eq!(oid, VDI_BIT | (u64::from(ACL_VID) << 32), "the ACL inode");
                let inode = sheep.inode();
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

/// Admin-queue Connect to [`NQN`] as `hostnqn`; the phase-stripped status.
fn connect_as(addr: SocketAddr, hostnqn: &str) -> u16 {
    let mut client = common::Client::handshake(addr, false, false);
    let (sqe, mut data) = common::connect_sqe(0, 32, 0xFFFF, 1);
    data.subsysnqn = [0; 256];
    data.subsysnqn[..NQN.len()].copy_from_slice(NQN.as_bytes());
    data.hostnqn = [0; 256];
    data.hostnqn[..hostnqn.len()].copy_from_slice(hostnqn.as_bytes());
    client.send_capsule(&sqe, data.as_bytes());
    client.recv_response().status.get() >> 1
}

const DENIED: u16 = status::CONNECT_INVALID_HOST | status::DNR;

/// Re-read every tracked cluster, as the refresh thread does on its tick.
fn refresh() {
    assert!(
        ioutgt_harness::refresh_clusters() > 0,
        "the ACL object is tracked"
    );
}

#[test]
fn acl_membership_changes_reach_a_running_target() {
    let sheep = Arc::new(Sheep {
        members: Mutex::new(Vec::new()),
        broken: AtomicBool::new(false),
    });
    sheep.set_members(&[HOST_A]);
    let cluster = spawn_fake_sheep(Arc::clone(&sheep));

    // The subsystem as whole-cluster mode built it: the ACL's one member is
    // its host list, and the ACL object it came from is recorded so the
    // refresh can go back to it. The namespace is local — nothing here needs
    // the cluster's data path.
    let mut config = ioutgt_nvme_tcp::TargetConfig::single_memory(NQN, 8);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    config.subsystems[0].allow_any_host = false;
    config.subsystems[0].allowed_hosts = vec![HOST_A.into()];
    config.subsystems[0].sheepdog_acl = Some(SheepdogAcl {
        cluster,
        vid: ACL_VID,
        epoch: 1,
        lock: false,
    });
    let addr = ioutgt_nvme_tcp::spawn_target(config).expect("target start");

    assert_eq!(connect_as(addr, HOST_A), status::SUCCESS, "the one member");
    assert_eq!(connect_as(addr, HOST_B), DENIED, "not a member");

    // `dog acl add member <acl> host-b`, in the slot after the one host-a has.
    sheep.set_members(&[HOST_A, HOST_B]);
    refresh();
    assert_eq!(connect_as(addr, HOST_B), status::SUCCESS, "member now");
    assert_eq!(connect_as(addr, HOST_A), status::SUCCESS, "still a member");

    // `dog acl remove member <acl> host-a` zeroes its slot in place: a hole,
    // not the end of the list — the member behind it stays a member.
    sheep.set_members(&["", HOST_B]);
    refresh();
    assert_eq!(connect_as(addr, HOST_A), DENIED, "removed from the ACL");
    assert_eq!(connect_as(addr, HOST_B), status::SUCCESS, "past the hole");

    // A cluster that will not answer leaves the last membership it did state
    // in force, rather than flapping the door open (or shut) on a gateway
    // hiccup: the empty list written here is not read.
    sheep.broken.store(true, Ordering::Relaxed);
    sheep.set_members(&[]);
    refresh();
    assert_eq!(connect_as(addr, HOST_A), DENIED, "last known list stands");
    assert_eq!(connect_as(addr, HOST_B), status::SUCCESS);

    // ...and once it answers again, an ACL with no members left constrains
    // nobody, so the subsystem is open to any host — including one that was
    // never on it.
    sheep.broken.store(false, Ordering::Relaxed);
    refresh();
    assert_eq!(connect_as(addr, HOST_A), status::SUCCESS);
    assert_eq!(
        connect_as(addr, HOST_C),
        status::SUCCESS,
        "no members, no ACL"
    );
}
