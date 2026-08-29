//! `--portaddr` overrides the address a locked Sheepdog namespace registers
//! as the volume's holder, instead of the port's own bound listen address —
//! for a port whose listen address (a wildcard bind behind a NAT, an
//! ingress rewriting the port) is not what the rest of the cluster should
//! reach it at.
//!
//! Its own test binary, like the other cluster tests: it calls
//! [`ioutgt_harness::shutdown`], which quiesces every target in the process,
//! so no other test may be running alongside it.
//!
//! The cluster is a minimal in-process fake `sheep` — just enough of
//! `GET_VDI_INFO`/`READ_OBJ`/`REGISTER_VDI` to open one VDI and capture the
//! owner string the registration named.

// Test-only offset arithmetic on a 64-bit host; values are small and bounded.
#![allow(clippy::cast_possible_truncation)]

mod common;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use common::NQN;
use ioutgt_control::config::BackendConfig;

// --- wire constants (include/sheepdog_proto.h) -----------------------------
const HDR: usize = 48;
const SD_OP_READ_OBJ: u8 = 0x02;
const SD_OP_GET_VDI_INFO: u8 = 0x14;
const SD_OP_REGISTER_VDI: u8 = 0x19;
const SD_RES_NO_VDI: u32 = 0x08;
const SD_INODE_HEADER_SIZE: usize = 4664;
const SD_MAX_VDI_LEN: usize = 256;

const VOL: &str = "vol";
const VOL_VID: u32 = 0x0000_4712;
/// 16 MiB in 4 MiB objects.
const VOL_SIZE: u64 = 16 << 20;
const VOL_SHIFT: u8 = 22;

fn u32le(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}
fn cstr(field: &[u8]) -> String {
    let end = field.iter().position(|&b| b == 0).unwrap_or(field.len());
    String::from_utf8_lossy(&field[..end]).into_owned()
}

/// The volume's inode: no ACL, in no ACL, with an all-zero (unallocated)
/// object map.
fn inode() -> Vec<u8> {
    let mut b = vec![0u8; SD_INODE_HEADER_SIZE];
    b[..VOL.len()].copy_from_slice(VOL.as_bytes());
    b[536..544].copy_from_slice(&VOL_SIZE.to_le_bytes());
    b[554] = 1; // nr_copies
    b[555] = VOL_SHIFT;
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

fn serve_conn(
    mut sock: TcpStream,
    registered: Arc<Mutex<Option<SocketAddr>>>,
) -> std::io::Result<()> {
    loop {
        let mut hdr = [0u8; HDR];
        if sock.read_exact(&mut hdr).is_err() {
            return Ok(()); // peer closed
        }
        let opcode = hdr[1];
        let id = u32le(&hdr, 8);
        let data_length = u32le(&hdr, 12) as usize;
        let offset = u32le(&hdr, 40) as usize;

        match opcode {
            SD_OP_GET_VDI_INFO => {
                let mut p = vec![0u8; data_length];
                sock.read_exact(&mut p)?;
                if cstr(&p[..SD_MAX_VDI_LEN]) == VOL {
                    let mut r = resp(opcode, id, 0, 0);
                    r[24..28].copy_from_slice(&VOL_VID.to_le_bytes()); // vdi_id
                    sock.write_all(&r)?;
                } else {
                    sock.write_all(&resp(opcode, id, SD_RES_NO_VDI, 0))?;
                }
            }
            SD_OP_READ_OBJ => {
                let inode = inode();
                let end = (offset + data_length).min(inode.len());
                let slice = &inode[offset.min(inode.len())..end];
                sock.write_all(&resp(opcode, id, 0, slice.len() as u32))?;
                sock.write_all(slice)?;
            }
            // The vid is already resolved (no name lookup here); the owner
            // travels as the payload.
            SD_OP_REGISTER_VDI => {
                let mut p = vec![0u8; data_length];
                sock.read_exact(&mut p)?;
                let owner: SocketAddr = cstr(&p).parse().expect("test owner is ip:port");
                *registered.lock().unwrap() = Some(owner);
                sock.write_all(&resp(opcode, id, 0, 0))?;
            }
            other => sock.write_all(&resp(other, id, 0x01, 0))?, // UNKNOWN
        }
    }
}

/// Spawn the fake sheep; returns its address.
fn spawn_fake_sheep(registered: Arc<Mutex<Option<SocketAddr>>>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for sock in listener.incoming() {
            let Ok(sock) = sock else { break };
            sock.set_nodelay(true).ok();
            let registered = Arc::clone(&registered);
            std::thread::spawn(move || {
                let _ = serve_conn(sock, registered);
            });
        }
    });
    addr
}

#[test]
fn portaddr_override_replaces_the_bound_listen_address() {
    let registered: Arc<Mutex<Option<SocketAddr>>> = Arc::new(Mutex::new(None));
    let cluster = spawn_fake_sheep(Arc::clone(&registered));
    let portaddr: SocketAddr = "203.0.113.9:5555".parse().unwrap();

    let mut config = ioutgt_nvme_tcp::TargetConfig::single_memory(NQN, 16);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    config.sheepdog_portaddr = Some(portaddr);
    config.subsystems[0].namespaces[0].backend = BackendConfig::Sheepdog {
        addr: cluster.to_string(),
        vdi: VOL.into(),
        tag: None,
        acl: None,
        lock: true,
    };
    let addr = ioutgt_nvme_tcp::spawn_target(config).expect("target start");

    assert_eq!(
        *registered.lock().unwrap(),
        Some(portaddr),
        "the --portaddr override registers instead of the port's own bound address {addr}"
    );

    assert_eq!(ioutgt_harness::shutdown(), 1, "one namespace released");
}
