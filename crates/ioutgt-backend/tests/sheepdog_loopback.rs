//! End-to-end `SheepdogBackend` test against an in-process fake `sheep`.
//!
//! A tiny `TcpListener`-backed server speaks the real 48-byte Sheepdog wire
//! protocol against an in-memory object store, letting the backend exercise its
//! full path (blocking VDI lookup + inode read at open, then io_uring
//! object read/write/allocate/CoW on a real `QueueRuntime`) with no cluster.
//! The same fake serves the cluster-wide VDI bitmap and models the cluster's
//! ACL scoping — a lookup only sees names whose inode `acl_id` matches the ACL
//! it carries — covering `ioutgt_backend::list_vdis` and
//! `ioutgt_backend::list_acls`.

// Test-only offset/size arithmetic on a 64-bit host; values are small and bounded.
#![allow(clippy::cast_possible_truncation)]

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use ioutgt_backend::{SheepdogBackend, list_acls, list_vdis};
use ioutgt_core::buf::AlignedBuf;
use ioutgt_core::{Backend, BackendError, LbaRange};
use ioutgt_uring::{QueueRuntime, RingConfig};

// --- wire constants / oid math (mirroring the backend) ---------------------
const SD_OP_CREATE_AND_WRITE_OBJ: u8 = 0x01;
const SD_OP_READ_OBJ: u8 = 0x02;
const SD_OP_WRITE_OBJ: u8 = 0x03;
const SD_OP_LOCK_VDI: u8 = 0x12;
const SD_OP_RELEASE_VDI: u8 = 0x13;
const SD_OP_GET_VDI_INFO: u8 = 0x14;
const SD_OP_READ_VDIS: u8 = 0x15;
const SD_RES_VDI_LOCKED: u32 = 0x07;
const SD_RES_VDI_NOT_LOCKED: u32 = 0x10;
const SD_RES_VDI_DENIED: u32 = 0x1E;
/// `LOCK_TYPE_NORMAL`: no ACL, and the exclusive lock such an open takes.
const SD_ACL_NONE: u32 = 0;
const SD_VDI_FLAG_ACL: u32 = 0x01;
const SD_FLAG_CMD_COW: u16 = 0x02;
const VDI_BIT: u64 = 1 << 63;
const SD_INODE_HEADER_SIZE: u64 = 4664;
const SD_MAX_VDI_LEN: usize = 256;
const SD_NR_VDIS: u32 = 1 << 24;
const HDR: usize = 48;

/// The vid owning an object id (inode or data).
fn oid_to_vid(oid: u64) -> u32 {
    ((oid & 0x00FF_FFFF_0000_0000) >> 32) as u32
}
fn u16le(b: &[u8], o: usize) -> u16 {
    u16::from_le_bytes(b[o..o + 2].try_into().unwrap())
}
fn u32le(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}
fn u64le(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}

/// One VDI in the fake cluster.
struct Vdi {
    name: String,
    tag: String,
    /// Non-zero marks a snapshot (frozen, read-only).
    snap_ctime: u64,
    vdi_size: u64,
    block_size_shift: u8,
    nr_copies: u8,
    /// The vid of the ACL object this VDI belongs to (0: none).
    acl_id: u32,
    /// `SD_VDI_FLAG_ACL` marks the VDI as an ACL object itself.
    vdi_flags: u32,
    data_vdi_id: Vec<u32>,
}

impl Vdi {
    fn new(name: &str, vdi_size: u64, block_size_shift: u8) -> Vdi {
        let nr_objects = vdi_size.div_ceil(1u64 << block_size_shift) as usize;
        Vdi {
            name: name.into(),
            tag: String::new(),
            snap_ctime: 0,
            vdi_size,
            block_size_shift,
            nr_copies: 1,
            acl_id: SD_ACL_NONE,
            vdi_flags: 0,
            data_vdi_id: vec![0u32; nr_objects],
        }
    }

    fn snapshot(mut self, tag: &str) -> Vdi {
        self.tag = tag.into();
        self.snap_ctime = 0x5f5e_0100;
        self
    }

    /// Put this volume in the ACL object with vid `acl`.
    fn in_acl(mut self, acl: u32) -> Vdi {
        self.acl_id = acl;
        self
    }

    /// Make this VDI an ACL object (`dog acl create`) rather than a volume.
    fn acl_object(mut self) -> Vdi {
        self.vdi_flags |= SD_VDI_FLAG_ACL;
        self
    }

    fn object_size(&self) -> usize {
        1usize << self.block_size_shift
    }

    /// Bytes of the inode object (header fields + the data_vdi_id[] map).
    fn inode_bytes(&self) -> Vec<u8> {
        let mut b = vec![0u8; SD_INODE_HEADER_SIZE as usize + self.data_vdi_id.len() * 4];
        b[..self.name.len()].copy_from_slice(self.name.as_bytes());
        b[SD_MAX_VDI_LEN..SD_MAX_VDI_LEN + self.tag.len()].copy_from_slice(self.tag.as_bytes());
        b[520..528].copy_from_slice(&self.snap_ctime.to_le_bytes());
        b[536..544].copy_from_slice(&self.vdi_size.to_le_bytes()); // vdi_size
        b[554] = self.nr_copies;
        b[555] = self.block_size_shift;
        b[572..576].copy_from_slice(&self.acl_id.to_le_bytes());
        b[592..596].copy_from_slice(&self.vdi_flags.to_le_bytes());
        for (i, v) in self.data_vdi_id.iter().enumerate() {
            let o = SD_INODE_HEADER_SIZE as usize + i * 4;
            b[o..o + 4].copy_from_slice(&v.to_le_bytes());
        }
        b
    }
}

/// A VDI lock held in the fake cluster: `LOCK_TYPE_NORMAL` (no ACL), which
/// stands alone, or a lock under an ACL, shared among the holders naming that
/// same ACL.
#[derive(Debug, PartialEq, Eq)]
enum Lock {
    Exclusive,
    Shared { acl: u32, holders: u32 },
}

struct Store {
    vdis: BTreeMap<u32, Vdi>,
    objects: HashMap<u64, Vec<u8>>,
    /// Vids currently under a `LOCK_VDI`. Only `RELEASE_VDI` clears one — a
    /// closing connection deliberately does not, so a test can tell an
    /// explicit release from a socket that merely went away.
    locks: BTreeMap<u32, Lock>,
}

impl Store {
    /// The cluster VDI bitmap `READ_VDIS` returns (one LSB-first bit per vid).
    fn vdi_bitmap(&self) -> Vec<u8> {
        let mut bitmap = vec![0u8; (SD_NR_VDIS / 8) as usize];
        for vid in self.vdis.keys() {
            bitmap[(vid / 8) as usize] |= 1 << (vid % 8);
        }
        bitmap
    }

    /// Lock `vid` under ACL `acl` (0 = `LOCK_TYPE_NORMAL`) by sheepdog's
    /// compatibility rule: holders naming the same ACL stack, a no-ACL holder
    /// stands alone. Anything else shuts a newcomer out, so `false`
    /// (→ `SD_RES_VDI_LOCKED`) means someone incompatible already holds it.
    fn take_lock(&mut self, vid: u32, acl: u32) -> bool {
        match (self.locks.get_mut(&vid), acl) {
            (Some(Lock::Shared { acl: held, holders }), _) if *held == acl => {
                *holders += 1;
                true
            }
            (Some(_), _) => false,
            (None, SD_ACL_NONE) => {
                self.locks.insert(vid, Lock::Exclusive);
                true
            }
            (None, acl) => {
                self.locks.insert(vid, Lock::Shared { acl, holders: 1 });
                true
            }
        }
    }

    /// Drop one holder of `vid`'s lock; `false` if it was not held under `acl`
    /// at all — the cluster only lets go of a lock the caller names correctly.
    fn release_lock(&mut self, vid: u32, acl: u32) -> bool {
        match self.locks.get_mut(&vid) {
            Some(Lock::Shared { acl: held, .. }) if *held != acl => false,
            Some(Lock::Exclusive) if acl != SD_ACL_NONE => false,
            Some(Lock::Shared { holders, .. }) if *holders > 1 => {
                *holders -= 1;
                true
            }
            Some(_) => {
                self.locks.remove(&vid);
                true
            }
            None => false,
        }
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

/// Reply with `bytes[offset..offset+data_length]`, the trim-to-what-exists
/// behavior a real `sheep` has.
fn send_slice(
    sock: &mut TcpStream,
    opcode: u8,
    id: u32,
    bytes: &[u8],
    offset: usize,
    data_length: usize,
) -> std::io::Result<()> {
    let end = (offset + data_length).min(bytes.len());
    let slice = &bytes[offset.min(bytes.len())..end];
    sock.write_all(&resp(opcode, id, 0, slice.len() as u32))?;
    sock.write_all(slice)
}

fn serve_conn(mut sock: TcpStream, store: Arc<Mutex<Store>>) -> std::io::Result<()> {
    loop {
        let mut hdr = [0u8; HDR];
        if sock.read_exact(&mut hdr).is_err() {
            return Ok(()); // peer closed
        }
        let opcode = hdr[1];
        let flags = u16le(&hdr, 2);
        let id = u32le(&hdr, 8);
        let data_length = u32le(&hdr, 12) as usize;
        let oid = u64le(&hdr, 16);
        let cow_oid = u64le(&hdr, 24);
        let offset = u64le(&hdr, 40) as usize;

        // Writes/creates carry a payload after the header.
        let mut payload = vec![0u8; if is_write(opcode) { data_length } else { 0 }];
        if !payload.is_empty() {
            sock.read_exact(&mut payload)?;
        }

        let mut st = store.lock().unwrap();
        match opcode {
            SD_OP_GET_VDI_INFO | SD_OP_LOCK_VDI => {
                // payload was not consumed above (not a write opcode); drain it.
                let mut p = vec![0u8; data_length];
                sock.read_exact(&mut p)?;
                let name = cstr(&p[..SD_MAX_VDI_LEN]);
                let tag = cstr(&p[SD_MAX_VDI_LEN..]);
                let acl = u32le(&hdr, 36); // sd_req.vdi.acl / lock type
                let named = st
                    .vdis
                    .iter()
                    .find(|(_, vdi)| vdi.name == name && vdi.tag == tag)
                    .map(|(&vid, vdi)| (vid, vdi.acl_id));
                match named {
                    // The cluster resolves a name only within the ACL the
                    // request carries; from outside it, the VDI is denied.
                    Some((_, vdi_acl)) if vdi_acl != acl => {
                        sock.write_all(&resp(opcode, id, SD_RES_VDI_DENIED, 0))?;
                    }
                    // LOCK_VDI answers as GET_VDI_INFO does, and takes the
                    // lock on the way — unless an incompatible holder has it.
                    Some((vid, _)) if opcode == SD_OP_LOCK_VDI && !st.take_lock(vid, acl) => {
                        sock.write_all(&resp(opcode, id, SD_RES_VDI_LOCKED, 0))?;
                    }
                    Some((vid, _)) => {
                        let mut r = resp(opcode, id, 0, 0);
                        r[24..28].copy_from_slice(&vid.to_le_bytes()); // vdi_id
                        sock.write_all(&r)?;
                    }
                    None => sock.write_all(&resp(opcode, id, 0x08, 0))?, // NO_VDI
                }
            }
            SD_OP_RELEASE_VDI => {
                let vid = u32le(&hdr, 24); // base_vdi_id
                let result = if st.release_lock(vid, u32le(&hdr, 36)) {
                    0
                } else {
                    SD_RES_VDI_NOT_LOCKED
                };
                sock.write_all(&resp(opcode, id, result, 0))?;
            }
            SD_OP_READ_VDIS => {
                let bitmap = st.vdi_bitmap();
                send_slice(&mut sock, opcode, id, &bitmap, 0, data_length)?;
            }
            SD_OP_READ_OBJ => {
                let vid = oid_to_vid(oid);
                let Some(vdi) = st.vdis.get(&vid) else {
                    sock.write_all(&resp(opcode, id, 0x02, 0))?; // NO_OBJ
                    continue;
                };
                let bytes = if oid & VDI_BIT != 0 {
                    vdi.inode_bytes()
                } else {
                    let osz = vdi.object_size();
                    st.objects
                        .get(&oid)
                        .cloned()
                        .unwrap_or_else(|| vec![0u8; osz])
                };
                send_slice(&mut sock, opcode, id, &bytes, offset, data_length)?;
            }
            SD_OP_WRITE_OBJ => {
                let vid = oid_to_vid(oid);
                if oid & VDI_BIT != 0 {
                    // Inode map update: data_vdi_id[idx] = payload.
                    let idx = (offset - SD_INODE_HEADER_SIZE as usize) / 4;
                    let vdi = st.vdis.get_mut(&vid).expect("inode of a known vid");
                    if idx < vdi.data_vdi_id.len() && payload.len() == 4 {
                        vdi.data_vdi_id[idx] = u32le(&payload, 0);
                    }
                } else {
                    let osz = st.vdis[&vid].object_size();
                    let obj = st.objects.entry(oid).or_insert_with(|| vec![0u8; osz]);
                    obj[offset..offset + payload.len()].copy_from_slice(&payload);
                }
                sock.write_all(&resp(opcode, id, 0, 0))?;
            }
            SD_OP_CREATE_AND_WRITE_OBJ => {
                let osz = st.vdis[&oid_to_vid(oid)].object_size();
                let mut obj = if flags & SD_FLAG_CMD_COW != 0 {
                    st.objects
                        .get(&cow_oid)
                        .cloned()
                        .unwrap_or_else(|| vec![0u8; osz])
                } else {
                    vec![0u8; osz]
                };
                obj[offset..offset + payload.len()].copy_from_slice(&payload);
                st.objects.insert(oid, obj);
                sock.write_all(&resp(opcode, id, 0, 0))?;
            }
            other => {
                sock.write_all(&resp(other, id, 0x01, 0))?; // UNKNOWN
            }
        }
    }
}

fn cstr(field: &[u8]) -> String {
    let end = field.iter().position(|&b| b == 0).unwrap_or(field.len());
    String::from_utf8_lossy(&field[..end]).into_owned()
}

fn is_write(opcode: u8) -> bool {
    matches!(opcode, SD_OP_WRITE_OBJ | SD_OP_CREATE_AND_WRITE_OBJ)
}

/// Spawn the fake sheep; returns its address and the shared store.
fn spawn_fake_sheep(store: Arc<Mutex<Store>>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for sock in listener.incoming() {
            let Ok(sock) = sock else { break };
            sock.set_nodelay(true).ok();
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                let _ = serve_conn(sock, store);
            });
        }
    });
    addr
}

const TEST_VID: u32 = 0x00ab_cdef;
const ACL_VID: u32 = 0x0000_4711;
const ACL_NQN: &str = "nqn.2026-06.io.ioutgt:grp";

fn fresh_store(block_size_shift: u8, vdi_size: u64) -> Arc<Mutex<Store>> {
    let mut vdis = BTreeMap::new();
    vdis.insert(TEST_VID, Vdi::new("testvdi", vdi_size, block_size_shift));
    Arc::new(Mutex::new(Store {
        vdis,
        objects: HashMap::new(),
        locks: BTreeMap::new(),
    }))
}

/// [`fresh_store`] with `testvdi` moved into an ACL object named [`ACL_NQN`],
/// the shape a target exporting a subsystem per ACL sees.
fn acl_store(block_size_shift: u8, vdi_size: u64) -> Arc<Mutex<Store>> {
    let store = fresh_store(block_size_shift, vdi_size);
    {
        let mut st = store.lock().unwrap();
        st.vdis
            .insert(ACL_VID, Vdi::new(ACL_NQN, 1 << 22, 22).acl_object());
        st.vdis.get_mut(&TEST_VID).unwrap().acl_id = ACL_VID;
    }
    store
}

/// The vids the fake cluster currently has locked.
fn locked(store: &Arc<Mutex<Store>>) -> Vec<u32> {
    store.lock().unwrap().locks.keys().copied().collect()
}

/// How many holders share `vid`'s lock (0 if it is free or exclusive).
fn holders(store: &Arc<Mutex<Store>>, vid: u32) -> u32 {
    match store.lock().unwrap().locks.get(&vid) {
        Some(&Lock::Shared { holders, .. }) => holders,
        _ => 0,
    }
}

fn filled(len: usize, seed: u8) -> AlignedBuf {
    let mut b = AlignedBuf::zeroed(len);
    for (i, x) in b.iter_mut().enumerate() {
        *x = (i as u8).wrapping_mul(31).wrapping_add(seed);
    }
    b
}

#[test]
fn read_hole_write_alloc_overwrite_roundtrip() {
    // 64 KiB data objects, 256 KiB volume (4 objects), 512-byte LBAs.
    let store = fresh_store(16, 256 * 1024);
    let addr = spawn_fake_sheep(Arc::clone(&store));
    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    let be = SheepdogBackend::open(addr, "testvdi", None, None, true).unwrap();
    assert_eq!(be.block_shift(), 9);
    assert_eq!(be.nr_blocks(), 256 * 1024 / 512);

    let map_entry = |idx: usize| store.lock().unwrap().vdis[&TEST_VID].data_vdi_id[idx];

    rt.block_on(async move {
        // 1. Unallocated volume reads back as zeros.
        let mut out = AlignedBuf::zeroed(8192);
        be.read(0, &mut out[..8192]).await.unwrap();
        assert!(out.iter().all(|&b| b == 0), "hole reads zero");

        // 2. Write into object 0 (allocates it), read back.
        let pat = filled(8192, 7);
        be.write(16, &pat[..8192]).await.unwrap(); // slba 16 -> byte 8192
        let mut back = AlignedBuf::zeroed(8192);
        be.read(16, &mut back[..8192]).await.unwrap();
        assert_eq!(&back[..], &pat[..], "allocated read-back");
        assert_ne!(map_entry(0), 0, "inode map entry persisted");

        // 3. Overwrite the same region (fast path) with a new pattern.
        let pat2 = filled(8192, 200);
        be.write(16, &pat2[..8192]).await.unwrap();
        be.read(16, &mut back[..8192]).await.unwrap();
        assert_eq!(&back[..], &pat2[..], "overwrite read-back");

        // 4. Boundary-spanning write: 8 KiB straddling the 64 KiB object seam
        //    (byte 60 KiB .. 68 KiB → objects 0 and 1).
        let span = filled(8192, 99);
        let slba = (60 * 1024) / 512;
        be.write(slba, &span[..8192]).await.unwrap();
        let mut sback = AlignedBuf::zeroed(8192);
        be.read(slba, &mut sback[..8192]).await.unwrap();
        assert_eq!(&sback[..], &span[..], "cross-object read-back");
        assert_ne!(map_entry(1), 0, "object 1 allocated");

        // 5. write_zeroes clears a sub-range.
        be.write_zeroes(LbaRange { slba: 16, nlb: 8 })
            .await
            .unwrap(); // 4 KiB
        be.read(16, &mut back[..8192]).await.unwrap();
        assert!(back[..4096].iter().all(|&b| b == 0), "zeroed prefix");
        assert_eq!(&back[4096..], &pat2[4096..], "tail untouched");

        // 6. flush is a no-op success; out-of-range is rejected.
        be.flush().await.unwrap();
        let err = be.read(be.nr_blocks(), &mut back[..512]).await.unwrap_err();
        assert_eq!(err, BackendError::OutOfRange);
    });
}

/// Under an ACL the VDI lock is shared among the targets naming that ACL.
#[test]
fn shared_lock_stacks_across_targets_and_unwinds_on_drop() {
    let store = acl_store(16, 256 * 1024);
    let addr = spawn_fake_sheep(Arc::clone(&store));

    let be = SheepdogBackend::open(addr, "testvdi", None, Some(ACL_NQN), true).unwrap();
    assert_eq!(holders(&store, TEST_VID), 1, "the open took the lock");

    // A second target may serve the same VDI — that is what the shared lock
    // is for — and joins the holders rather than displacing the first.
    let second = SheepdogBackend::open(addr, "testvdi", None, Some(ACL_NQN), true).unwrap();
    assert_eq!(holders(&store, TEST_VID), 2);

    // An explicitly unlocked open goes through too, and disturbs nothing.
    let waived = SheepdogBackend::open(addr, "testvdi", None, Some(ACL_NQN), false).unwrap();
    assert_eq!(holders(&store, TEST_VID), 2);
    drop(waived);
    assert_eq!(holders(&store, TEST_VID), 2);

    // Each drop hands back exactly one participant's hold.
    drop(second);
    assert_eq!(holders(&store, TEST_VID), 1, "the first target still holds");
    drop(be);
    assert!(locked(&store).is_empty(), "the last drop freed the VDI");
}

/// A VDI in no ACL locks exclusively, so a second target is kept out — the
/// counterpart of the shared case above.
#[test]
fn a_vdi_outside_any_acl_locks_exclusively() {
    let store = fresh_store(16, 256 * 1024);
    let addr = spawn_fake_sheep(Arc::clone(&store));

    let be = SheepdogBackend::open(addr, "testvdi", None, None, true).unwrap();
    assert_eq!(
        store.lock().unwrap().locks[&TEST_VID],
        Lock::Exclusive,
        "no ACL means LOCK_TYPE_NORMAL"
    );
    let err = SheepdogBackend::open(addr, "testvdi", None, None, true)
        .err()
        .expect("a second exclusive open is refused");
    assert_eq!(err.kind(), std::io::ErrorKind::ResourceBusy);
    drop(be);
    assert!(locked(&store).is_empty(), "the drop freed the VDI");
}

/// The lock is shared only among holders naming the same ACL: a client holding
/// the VDI exclusively (a QEMU guest, say) still keeps the target out.
#[test]
fn an_exclusive_holder_locks_the_target_out() {
    let store = acl_store(16, 256 * 1024);
    store
        .lock()
        .unwrap()
        .locks
        .insert(TEST_VID, Lock::Exclusive);
    let addr = spawn_fake_sheep(Arc::clone(&store));

    let err = SheepdogBackend::open(addr, "testvdi", None, Some(ACL_NQN), true)
        .err()
        .expect("an exclusively locked VDI is refused");
    assert_eq!(err.kind(), std::io::ErrorKind::ResourceBusy);
    assert_eq!(locked(&store), vec![TEST_VID], "the holder keeps its lock");

    // Waiving the lock is the escape hatch for exclusion arranged elsewhere.
    let waived = SheepdogBackend::open(addr, "testvdi", None, Some(ACL_NQN), false).unwrap();
    drop(waived);
    assert_eq!(
        store.lock().unwrap().locks[&TEST_VID],
        Lock::Exclusive,
        "an unlocked open neither takes nor releases anything"
    );
}

/// The ACL is the cluster's access-control scope, not a label the target may
/// pick: a member VDI is unreachable without it, and an ordinary VDI is not an
/// ACL to open things under.
#[test]
fn an_acl_scopes_which_names_resolve() {
    let store = acl_store(16, 256 * 1024);
    store
        .lock()
        .unwrap()
        .vdis
        .insert(0x00_0055, Vdi::new("loose", 1 << 20, 22));
    let addr = spawn_fake_sheep(Arc::clone(&store));

    // A member named from outside its ACL, and a non-member named from inside
    // one: the cluster denies both.
    for (vdi, acl) in [("testvdi", None), ("loose", Some(ACL_NQN))] {
        let err = SheepdogBackend::open(addr, vdi, None, acl, true)
            .err()
            .unwrap_or_else(|| panic!("{vdi} should be denied under {acl:?}"));
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied, "{vdi}");
    }
    assert!(locked(&store).is_empty(), "a denied open takes no lock");

    // An ordinary VDI is refused as an ACL even though the name resolves.
    let err = SheepdogBackend::open(addr, "testvdi", None, Some("loose"), true)
        .err()
        .expect("'loose' is no ACL object");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

    // Named correctly, the member opens.
    let be = SheepdogBackend::open(addr, "testvdi", None, Some(ACL_NQN), true).unwrap();
    assert_eq!(be.nr_blocks(), 256 * 1024 / 512);
}

/// A failed open must not walk away holding the lock it took to get there.
#[test]
fn lock_released_when_the_open_fails_after_taking_it() {
    // A zero-sized VDI passes the name lookup (so the lock is taken) and then
    // fails the inode check.
    let store = fresh_store(16, 256 * 1024);
    store
        .lock()
        .unwrap()
        .vdis
        .insert(0x00_0042, Vdi::new("empty", 0, 22));
    let addr = spawn_fake_sheep(Arc::clone(&store));

    let err = SheepdogBackend::open(addr, "empty", None, None, true)
        .err()
        .expect("a zero-sized VDI is rejected");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    assert!(locked(&store).is_empty(), "a failed open leaves no lock");
}

/// A cluster of loose volumes plus one snapshot, deliberately in neither vid
/// nor name order, and one ACL object holding two of them.
fn enumeration_store() -> Arc<Mutex<Store>> {
    let store = fresh_store(16, 256 * 1024);
    {
        let mut st = store.lock().unwrap();
        st.vdis.insert(0x00_0002, Vdi::new("zeta", 1 << 20, 22));
        st.vdis.insert(0x00_0010, Vdi::new("alpha", 4 << 20, 22));
        st.vdis
            .insert(0x00_0011, Vdi::new("alpha", 4 << 20, 22).snapshot("daily"));
        // Unnamed / zero-sized inodes are not exportable volumes.
        st.vdis.insert(0x00_0012, Vdi::new("", 1 << 20, 22));
        st.vdis.insert(0x00_0013, Vdi::new("empty", 0, 22));
        // An ACL object and its two members, plus an empty second ACL.
        st.vdis
            .insert(ACL_VID, Vdi::new(ACL_NQN, 1 << 22, 22).acl_object());
        st.vdis
            .insert(0x00_0020, Vdi::new("shared", 8 << 20, 22).in_acl(ACL_VID));
        st.vdis.insert(
            0x00_0021,
            Vdi::new("shared", 8 << 20, 22)
                .snapshot("daily")
                .in_acl(ACL_VID),
        );
        st.vdis.insert(
            0x00_0030,
            Vdi::new("nqn.2026-06.io.ioutgt:idle", 1 << 22, 22).acl_object(),
        );
    }
    store
}

#[test]
fn cluster_enumeration_lists_every_vdi() {
    let store = enumeration_store();
    let addr = spawn_fake_sheep(Arc::clone(&store));

    let vdis = list_vdis(addr).unwrap();
    let seen: Vec<_> = vdis
        .iter()
        .map(|v| (v.name.as_str(), v.tag.as_str(), v.vid, v.size, v.acl))
        .collect();
    assert_eq!(
        seen,
        vec![
            ("alpha", "", 0x10, 4 << 20, 0),
            ("alpha", "daily", 0x11, 4 << 20, 0),
            ("shared", "", 0x20, 8 << 20, ACL_VID),
            ("shared", "daily", 0x21, 8 << 20, ACL_VID),
            ("testvdi", "", TEST_VID, 256 * 1024, 0),
            ("zeta", "", 0x02, 1 << 20, 0),
        ],
        "sorted by (name, tag), ACL objects and unnamed/empty inodes skipped"
    );
    assert!(
        vdis.iter().filter(|v| v.snapshot).count() == 2,
        "both snapshots flagged"
    );

    // Every enumerated head is openable under the ACL the listing reports, and
    // reports the size the listing reports.
    for vdi in vdis.iter().filter(|v| !v.snapshot) {
        let acl = (vdi.acl != 0).then_some(ACL_NQN);
        let be = SheepdogBackend::open(addr, &vdi.name, None, acl, false).unwrap();
        assert_eq!(be.nr_blocks() * 512, vdi.size, "{} size", vdi.name);
    }

    // The snapshot is openable by tag and refuses writes. Being frozen, it
    // takes no lock even though this open asks for one.
    let snap = SheepdogBackend::open(addr, "alpha", Some("daily"), None, true).unwrap();
    assert!(locked(&store).is_empty(), "a snapshot open locks nothing");
    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    rt.block_on(async move {
        let data = AlignedBuf::zeroed(512);
        assert_eq!(
            snap.write(0, &data[..512]).await.unwrap_err(),
            BackendError::Unsupported,
            "snapshots are read-only"
        );
    });
}

/// `list_acls` is what maps a cluster onto subsystems: every ACL object, named
/// as the cluster names it, with the members that name it back.
#[test]
fn acl_enumeration_groups_members_under_their_acl() {
    let store = enumeration_store();
    // A member whose ACL object no longer exists belongs to no subsystem; it
    // is dropped with a warning rather than inventing one.
    store
        .lock()
        .unwrap()
        .vdis
        .insert(0x00_0040, Vdi::new("orphan", 1 << 20, 22).in_acl(0x00_0099));
    let addr = spawn_fake_sheep(Arc::clone(&store));

    let acls = list_acls(addr).unwrap();
    let seen: Vec<_> = acls
        .iter()
        .map(|acl| {
            let members: Vec<_> = acl
                .vdis
                .iter()
                .map(|v| (v.name.as_str(), v.tag.as_str(), v.vid))
                .collect();
            (acl.name.as_str(), acl.vid, members)
        })
        .collect();
    assert_eq!(
        seen,
        vec![
            // Sorted by ACL name; an ACL with no members is still an ACL.
            (
                "nqn.2026-06.io.ioutgt:grp",
                ACL_VID,
                vec![("shared", "", 0x20), ("shared", "daily", 0x21)]
            ),
            ("nqn.2026-06.io.ioutgt:idle", 0x30, vec![]),
        ]
    );
}
