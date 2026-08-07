//! End-to-end `SheepdogBackend` test against an in-process fake `sheep`.
//!
//! A tiny `TcpListener`-backed server speaks the real 48-byte Sheepdog wire
//! protocol against an in-memory object store, letting the backend exercise its
//! full path (blocking VDI lookup + inode read at open, then io_uring
//! object read/write/allocate/CoW on a real `QueueRuntime`) with no cluster.
//! The same fake serves the cluster-wide VDI bitmap, covering
//! `ioutgt_backend::list_vdis`.

// Test-only offset/size arithmetic on a 64-bit host; values are small and bounded.
#![allow(clippy::cast_possible_truncation)]

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use ioutgt_backend::{SheepdogBackend, list_vdis};
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
const LOCK_TYPE_SHARED: u32 = 1;
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
            data_vdi_id: vec![0u32; nr_objects],
        }
    }

    fn snapshot(mut self, tag: &str) -> Vdi {
        self.tag = tag.into();
        self.snap_ctime = 0x5f5e_0100;
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
        for (i, v) in self.data_vdi_id.iter().enumerate() {
            let o = SD_INODE_HEADER_SIZE as usize + i * 4;
            b[o..o + 4].copy_from_slice(&v.to_le_bytes());
        }
        b
    }
}

/// A VDI lock held in the fake cluster: `LOCK_TYPE_NORMAL`, which stands
/// alone, or `LOCK_TYPE_SHARED`, which counts its participants.
#[derive(Debug, PartialEq, Eq)]
enum Lock {
    Exclusive,
    Shared(u32),
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

    /// Take `lock_type` on `vid` under sheepdog's compatibility rule: shared
    /// participants stack, an exclusive holder stands alone. Either kind shuts
    /// the other out, so `false` (→ `SD_RES_VDI_LOCKED`) means someone
    /// incompatible already holds it.
    fn take_lock(&mut self, vid: u32, lock_type: u32) -> bool {
        match (self.locks.get_mut(&vid), lock_type) {
            (Some(Lock::Shared(holders)), LOCK_TYPE_SHARED) => {
                *holders += 1;
                true
            }
            (Some(_), _) => false,
            (None, LOCK_TYPE_SHARED) => {
                self.locks.insert(vid, Lock::Shared(1));
                true
            }
            (None, _) => {
                self.locks.insert(vid, Lock::Exclusive);
                true
            }
        }
    }

    /// Drop one holder of `vid`'s lock; `false` if it was not held at all.
    fn release_lock(&mut self, vid: u32) -> bool {
        match self.locks.get_mut(&vid) {
            Some(Lock::Shared(holders)) if *holders > 1 => {
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
                let found = st
                    .vdis
                    .iter()
                    .find(|(_, vdi)| vdi.name == name && vdi.tag == tag)
                    .map(|(&vid, _)| vid);
                match found {
                    // LOCK_VDI answers as GET_VDI_INFO does, and takes the
                    // lock on the way — unless an incompatible holder has it.
                    Some(vid)
                        if opcode == SD_OP_LOCK_VDI
                            && !st.take_lock(vid, u32le(&hdr, 36) /* type */) =>
                    {
                        sock.write_all(&resp(opcode, id, SD_RES_VDI_LOCKED, 0))?;
                    }
                    Some(vid) => {
                        let mut r = resp(opcode, id, 0, 0);
                        r[24..28].copy_from_slice(&vid.to_le_bytes()); // vdi_id
                        sock.write_all(&r)?;
                    }
                    None => sock.write_all(&resp(opcode, id, 0x08, 0))?, // NO_VDI
                }
            }
            SD_OP_RELEASE_VDI => {
                let vid = u32le(&hdr, 24); // base_vdi_id
                let result = if st.release_lock(vid) {
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

fn fresh_store(block_size_shift: u8, vdi_size: u64) -> Arc<Mutex<Store>> {
    let mut vdis = BTreeMap::new();
    vdis.insert(TEST_VID, Vdi::new("testvdi", vdi_size, block_size_shift));
    Arc::new(Mutex::new(Store {
        vdis,
        objects: HashMap::new(),
        locks: BTreeMap::new(),
    }))
}

/// The vids the fake cluster currently has locked.
fn locked(store: &Arc<Mutex<Store>>) -> Vec<u32> {
    store.lock().unwrap().locks.keys().copied().collect()
}

/// How many shared participants hold `vid` (0 if it is free or exclusive).
fn holders(store: &Arc<Mutex<Store>>, vid: u32) -> u32 {
    match store.lock().unwrap().locks.get(&vid) {
        Some(&Lock::Shared(holders)) => holders,
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
    let be = SheepdogBackend::open(addr, "testvdi", None, true).unwrap();
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

#[test]
fn shared_lock_stacks_across_targets_and_unwinds_on_drop() {
    let store = fresh_store(16, 256 * 1024);
    let addr = spawn_fake_sheep(Arc::clone(&store));

    let be = SheepdogBackend::open(addr, "testvdi", None, true).unwrap();
    assert_eq!(holders(&store, TEST_VID), 1, "the open took the lock");

    // A second target may serve the same VDI — that is what the shared lock
    // is for — and joins the holders rather than displacing the first.
    let second = SheepdogBackend::open(addr, "testvdi", None, true).unwrap();
    assert_eq!(holders(&store, TEST_VID), 2);

    // An explicitly unlocked open goes through too, and disturbs nothing.
    let waived = SheepdogBackend::open(addr, "testvdi", None, false).unwrap();
    assert_eq!(holders(&store, TEST_VID), 2);
    drop(waived);
    assert_eq!(holders(&store, TEST_VID), 2);

    // Each drop hands back exactly one participant's hold.
    drop(second);
    assert_eq!(holders(&store, TEST_VID), 1, "the first target still holds");
    drop(be);
    assert!(locked(&store).is_empty(), "the last drop freed the VDI");
}

/// The shared lock is shared only among shared holders: a client holding the
/// VDI exclusively (a QEMU guest, say) still keeps the target out.
#[test]
fn an_exclusive_holder_locks_the_target_out() {
    let store = fresh_store(16, 256 * 1024);
    store
        .lock()
        .unwrap()
        .locks
        .insert(TEST_VID, Lock::Exclusive);
    let addr = spawn_fake_sheep(Arc::clone(&store));

    let err = SheepdogBackend::open(addr, "testvdi", None, true)
        .err()
        .expect("an exclusively locked VDI is refused");
    assert_eq!(err.kind(), std::io::ErrorKind::ResourceBusy);
    assert_eq!(locked(&store), vec![TEST_VID], "the holder keeps its lock");

    // Waiving the lock is the escape hatch for exclusion arranged elsewhere.
    let waived = SheepdogBackend::open(addr, "testvdi", None, false).unwrap();
    drop(waived);
    assert_eq!(
        store.lock().unwrap().locks[&TEST_VID],
        Lock::Exclusive,
        "an unlocked open neither takes nor releases anything"
    );
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

    let err = SheepdogBackend::open(addr, "empty", None, true)
        .err()
        .expect("a zero-sized VDI is rejected");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    assert!(locked(&store).is_empty(), "a failed open leaves no lock");
}

#[test]
fn cluster_enumeration_lists_every_vdi() {
    // A cluster of three writable VDIs plus one snapshot, deliberately in
    // neither vid nor name order.
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
    }
    let addr = spawn_fake_sheep(Arc::clone(&store));

    let vdis = list_vdis(addr).unwrap();
    let seen: Vec<_> = vdis
        .iter()
        .map(|v| (v.name.as_str(), v.tag.as_str(), v.vid, v.size, v.snapshot))
        .collect();
    assert_eq!(
        seen,
        vec![
            ("alpha", "", 0x10, 4 << 20, false),
            ("alpha", "daily", 0x11, 4 << 20, true),
            ("testvdi", "", TEST_VID, 256 * 1024, false),
            ("zeta", "", 0x02, 1 << 20, false),
        ],
        "sorted by (name, tag), snapshots flagged, unnamed/empty skipped"
    );

    // Every enumerated head is openable by the name the listing reports, and
    // reports the size the listing reports.
    for vdi in vdis.iter().filter(|v| !v.snapshot) {
        let be = SheepdogBackend::open(addr, &vdi.name, None, false).unwrap();
        assert_eq!(be.nr_blocks() * 512, vdi.size, "{} size", vdi.name);
    }

    // The snapshot is openable by tag and refuses writes. Being frozen, it
    // takes no lock even though this open asks for one.
    let snap = SheepdogBackend::open(addr, "alpha", Some("daily"), true).unwrap();
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
