//! `--backend sheepdog:HOST` expansion against an in-process fake `sheep`.
//!
//! Covers the whole-cluster path of [`ioutgt_control::cli::subsystems`]: the
//! VDI bitmap and inode reads it issues are plain blocking TCP (no io_uring,
//! no queue threads), so a ~60-line fake gateway exercises it end to end — one
//! subsystem per ACL object named by the ACL, NSID assignment, snapshot
//! filtering, and namespace UUIDs taken from the VDIs' inodes.

// Test-only length arithmetic on a 64-bit host; the payloads are kilobytes.
#![allow(clippy::cast_possible_truncation)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};

use ioutgt_control::cli::subsystems;
use ioutgt_control::config::BackendConfig;

const HDR: usize = 48;
const SD_OP_READ_OBJ: u8 = 0x02;
const SD_OP_READ_VDIS: u8 = 0x15;
const SD_NR_VDIS: u32 = 1 << 24;
const SD_MAX_VDI_LEN: usize = 256;
const SD_INODE_HEADER_SIZE: usize = 4664;
const SD_VDI_FLAG_ACL: u32 = 0x01;

const ACL_A: u32 = 0x10;
const ACL_B: u32 = 0x11;
const NQN_A: &str = "nqn.2026-06.io.ioutgt:grp-a";
const NQN_B: &str = "nqn.2026-06.io.ioutgt:grp-b";

/// One inode of the fake cluster: (vid, name, size, snapshot, acl_id, is_acl).
struct Inode {
    vid: u32,
    name: &'static str,
    size: u64,
    snapshot: bool,
    acl_id: u32,
    is_acl: bool,
    /// The inode carries a `uuid[16]`; false for one written by a `sheep`
    /// predating the field (all-zero, i.e. no identity of its own).
    has_uuid: bool,
}

/// A volume in ACL `acl_id`.
const fn vol(vid: u32, name: &'static str, size: u64, snapshot: bool, acl_id: u32) -> Inode {
    Inode {
        vid,
        name,
        size,
        snapshot,
        acl_id,
        is_acl: false,
        has_uuid: true,
    }
}

/// A volume whose inode predates `uuid[16]`.
const fn legacy_vol(vid: u32, name: &'static str, size: u64, acl_id: u32) -> Inode {
    Inode {
        has_uuid: false,
        ..vol(vid, name, size, false, acl_id)
    }
}

/// An ACL object (`dog acl create`), which belongs to no ACL itself.
const fn acl(vid: u32, name: &'static str) -> Inode {
    Inode {
        vid,
        name,
        size: 4 << 20,
        snapshot: false,
        acl_id: 0,
        is_acl: true,
        has_uuid: true,
    }
}

/// Two ACLs with two volumes each (one of them a snapshot), plus a volume in
/// no ACL — deliberately in neither vid nor name order.
const CLUSTER: &[Inode] = &[
    vol(0x20, "vol-b", 64 << 20, false, ACL_A),
    vol(0x21, "vol-a", 32 << 20, false, ACL_A),
    vol(0x22, "vol-a", 32 << 20, true, ACL_A), // a snapshot of vol-a
    vol(0x23, "vol-c", 16 << 20, false, ACL_B),
    legacy_vol(0x25, "vol-d", 8 << 20, ACL_B), // no inode uuid to export
    vol(0x24, "loose", 8 << 20, false, 0),     // in no ACL: exported by nobody
    acl(ACL_A, NQN_A),
    acl(ACL_B, NQN_B),
];

/// The `uuid[16]` the fake cluster generated for `vid` when the VDI was
/// created: one per VDI and never all-zero.
fn fixture_uuid(vid: u32) -> [u8; 16] {
    let mut uuid = [0xa5u8; 16];
    uuid[..4].copy_from_slice(&vid.to_be_bytes());
    uuid
}

fn inode_bytes(inode: &Inode) -> Vec<u8> {
    let mut b = vec![0u8; SD_INODE_HEADER_SIZE];
    b[..inode.name.len()].copy_from_slice(inode.name.as_bytes());
    if inode.snapshot {
        b[520..528].copy_from_slice(&1u64.to_le_bytes()); // snap_ctime
        b[SD_MAX_VDI_LEN..SD_MAX_VDI_LEN + 5].copy_from_slice(b"daily");
    }
    b[536..544].copy_from_slice(&inode.size.to_le_bytes()); // vdi_size
    b[554] = 1; // nr_copies
    b[555] = 22; // 4 MiB objects
    b[572..576].copy_from_slice(&inode.acl_id.to_le_bytes());
    if inode.has_uuid {
        b[576..592].copy_from_slice(&fixture_uuid(inode.vid));
    }
    let flags = if inode.is_acl { SD_VDI_FLAG_ACL } else { 0 };
    b[592..596].copy_from_slice(&flags.to_le_bytes());
    b
}

/// Serve `READ_VDIS` and inode `READ_OBJ`s for [`CLUSTER`].
fn serve(mut sock: TcpStream) -> std::io::Result<()> {
    loop {
        let mut hdr = [0u8; HDR];
        if sock.read_exact(&mut hdr).is_err() {
            return Ok(()); // peer closed
        }
        let data_length = u32::from_le_bytes(hdr[12..16].try_into().unwrap()) as usize;
        let oid = u64::from_le_bytes(hdr[16..24].try_into().unwrap());
        let vid = ((oid & 0x00FF_FFFF_0000_0000) >> 32) as u32;

        let payload = match hdr[1] {
            SD_OP_READ_VDIS => {
                let mut bitmap = vec![0u8; (SD_NR_VDIS / 8) as usize];
                for inode in CLUSTER {
                    bitmap[(inode.vid / 8) as usize] |= 1 << (inode.vid % 8);
                }
                bitmap
            }
            SD_OP_READ_OBJ => match CLUSTER.iter().find(|inode| inode.vid == vid) {
                Some(inode) => inode_bytes(inode),
                None => {
                    let mut resp = [0u8; HDR];
                    resp[16..20].copy_from_slice(&2u32.to_le_bytes()); // NO_OBJ
                    sock.write_all(&resp)?;
                    continue;
                }
            },
            _ => Vec::new(),
        };
        let payload = &payload[..payload.len().min(data_length)];
        let mut resp = [0u8; HDR];
        resp[0] = 0x02; // proto_ver
        resp[1] = hdr[1];
        resp[12..16].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        sock.write_all(&resp)?;
        sock.write_all(payload)?;
    }
}

fn spawn_fake_sheep() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for sock in listener.incoming() {
            let Ok(sock) = sock else { break };
            std::thread::spawn(move || {
                let _ = serve(sock);
            });
        }
    });
    addr
}

#[test]
fn cluster_spec_expands_to_one_subsystem_per_acl() {
    let addr = spawn_fake_sheep();
    let subsystems = subsystems(&format!("sheepdog:{addr}"), 64, "nqn.unused").unwrap();

    // One subsystem per ACL object, named by the ACL rather than by the
    // --subsys-nqn flag, sorted by name; the volume in no ACL is exported by
    // none of them.
    let mapped: Vec<_> = subsystems
        .iter()
        .map(|subsys| {
            let namespaces: Vec<_> = subsys
                .namespaces
                .iter()
                .map(|ns| match &ns.backend {
                    // Cluster mode locks every VDI it exports, under the ACL
                    // whose subsystem serves it (`?nolock` opts out).
                    BackendConfig::Sheepdog {
                        addr: cluster,
                        vdi,
                        tag,
                        acl,
                        lock,
                    } => {
                        assert!(lock, "nsid {}: cluster VDIs are locked", ns.nsid);
                        assert!(tag.is_none(), "nsid {}: heads only", ns.nsid);
                        assert_eq!(acl.as_deref(), Some(subsys.nqn.as_str()));
                        assert_eq!(cluster, &addr.to_string());
                        (ns.nsid, vdi.clone())
                    }
                    other => panic!("expected a sheepdog backend, got {other:?}"),
                })
                .collect();
            (subsys.nqn.as_str(), namespaces)
        })
        .collect();
    assert_eq!(
        mapped,
        vec![
            // Both writable VDIs of ACL A, each on the NSID its vid (its
            // VDI-bitmap position) dictates rather than one assigned by
            // listing order; the snapshot is skipped.
            (
                NQN_A,
                vec![(0x21, "vol-a".to_string()), (0x20, "vol-b".to_string())]
            ),
            (
                NQN_B,
                vec![(0x23, "vol-c".to_string()), (0x25, "vol-d".to_string())]
            ),
        ]
    );

    // One serial per ACL: the group's cluster-wide identity, not the target's.
    assert_eq!(subsystems[0].serial, format!("SHEEPDOG{ACL_A:06X}"));
    assert_ne!(subsystems[0].serial, subsystems[1].serial);

    // Identity comes from the cluster — the VDI's own inode uuid — not from
    // the exporting subsystem, so one volume looks the same through any
    // target, and through QEMU or `dog vdi list --json` for that matter.
    let ns = &subsystems[0].namespaces;
    assert_eq!(ns[0].uuid, Some(fixture_uuid(0x21))); // vol-a
    assert_eq!(ns[1].uuid, Some(fixture_uuid(0x20))); // vol-b
    assert_ne!(ns[0].uuid, ns[1].uuid);

    // An inode with no uuid of its own falls back to one derived from the
    // VDI's name and vid, which are just as cluster-wide.
    assert_eq!(
        subsystems[1].namespaces[1].uuid, // vol-d
        Some(ioutgt_core::subsystem::namespace_uuid(
            "sheepdog:vol-d",
            0x25
        )),
    );
}

/// A single-VDI spec still builds exactly one subsystem, the flag-named one,
/// and never touches the cluster.
#[test]
fn vdi_spec_keeps_the_flag_nqn() {
    let built = subsystems("sheepdog:sheep0/vol%grp", 64, "nqn.2026-06.io.ioutgt:x").unwrap();
    assert_eq!(built.len(), 1);
    assert_eq!(built[0].nqn, "nqn.2026-06.io.ioutgt:x");
    match &built[0].namespaces[..] {
        [ns] => {
            assert_eq!(ns.nsid, 1);
            match &ns.backend {
                BackendConfig::Sheepdog { vdi, acl, .. } => {
                    assert_eq!(vdi, "vol");
                    assert_eq!(acl.as_deref(), Some("grp"));
                }
                other => panic!("expected a sheepdog backend, got {other:?}"),
            }
        }
        other => panic!("expected one namespace, got {other:?}"),
    }
}
