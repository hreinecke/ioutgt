//! End-to-end `SheepdogBackend` test against an in-process fake `sheep`.
//!
//! A tiny `TcpListener`-backed server speaks the real 48-byte Sheepdog wire
//! protocol against an in-memory object store, letting the backend exercise its
//! full path (the control plane's VDI lookup + inode read at open, then
//! io_uring object read/write/allocate/CoW on a real `QueueRuntime`) with no
//! cluster.
//! The same fake serves the cluster-wide VDI bitmap and models the cluster's
//! ACL scoping — a lookup only sees names whose inode `acl_id` matches the ACL
//! it carries — covering `ioutgt_backend::list_vdis` and
//! `ioutgt_backend::list_acls`.

// Test-only offset/size arithmetic on a 64-bit host; values are small and bounded.
#![allow(clippy::cast_possible_truncation)]

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use ioutgt_backend::{
    SheepdogBackend, VdiHolder, cluster_ana_state, list_acls, list_vdis, vdi_holders,
};
use ioutgt_core::buf::AlignedBuf;
use ioutgt_core::{Backend, BackendError, LbaRange};
use ioutgt_uring::{QueueRuntime, RingConfig};

// --- wire constants / oid math (mirroring the backend) ---------------------
const SD_OP_CREATE_AND_WRITE_OBJ: u8 = 0x01;
const SD_OP_READ_OBJ: u8 = 0x02;
const SD_OP_WRITE_OBJ: u8 = 0x03;
const SD_OP_GET_VDI_INFO: u8 = 0x14;
const SD_OP_READ_VDIS: u8 = 0x15;
const SD_OP_REGISTER_VDI: u8 = 0x19;
const SD_OP_UNREGISTER_VDI: u8 = 0x1A;
const SD_OP_GET_VDI_COPIES: u8 = 0xAB;
/// Sheep-internal local op: the node/zone topology this node currently sees.
const SD_OP_GET_NODE_LIST: u8 = 0x82;
const SD_SHEEP_PROTO_VER: u8 = 0x0a;
const SD_RES_VDI_LOCKED: u32 = 0x07;
const SD_RES_VDI_NOT_LOCKED: u32 = 0x10;
const SD_RES_VDI_DENIED: u32 = 0x1E;
const SD_RES_BUFFER_SMALL: u32 = 0x88;
/// `LOCK_TYPE_NORMAL`: no ACL, and the exclusive lock such an open takes.
const SD_ACL_NONE: u32 = 0;
/// `LOCK_STATE_UNLOCKED` / `LOCK_STATE_SHARED`.
const LOCK_STATE_UNLOCKED: u32 = 1;
const LOCK_STATE_SHARED: u32 = 3;
/// `sizeof(struct vdi_state)`, the GET_VDI_COPIES record.
const VDI_STATE: usize = 1432;
/// `SD_MAX_COPIES`: participants a VDI's lock can have.
const SD_MAX_COPIES: usize = 31;
const SD_VDI_FLAG_ACL: u32 = 0x01;
const SD_FLAG_CMD_COW: u16 = 0x02;
const VDI_BIT: u64 = 1 << 63;
const SD_INODE_HEADER_SIZE: u64 = 4664;
const SD_MAX_VDI_LEN: usize = 256;
/// `offsetof(struct sd_inode_header, metadata)` — an ACL object's member-name
/// table.
const INO_OFF_METADATA: usize = 600;
const SD_NR_VDIS: u32 = 1 << 24;
const HDR: usize = 48;

// `struct sd_node` (GET_NODE_LIST): rb_node(24, unread) + node_id(40:
// addr[16]@24, port@40) + nr_vnodes(u16)@64 + pad(2) + zone(u32)@68 +
// space(u64, unread) = 80 bytes.
const SD_NODE_SIZE: usize = 80;
const NODE_OFF_ADDR: usize = 24;
const NODE_OFF_PORT: usize = 40;
const NODE_OFF_NR_VNODES: usize = 64;
const NODE_OFF_ZONE: usize = 68;

/// One `GET_NODE_LIST` record: only what the ANA placement ring reads.
fn node_record(addr: SocketAddr, nr_vnodes: u16, zone: u32) -> [u8; SD_NODE_SIZE] {
    let mut rec = [0u8; SD_NODE_SIZE];
    if let std::net::IpAddr::V4(v4) = addr.ip() {
        rec[NODE_OFF_ADDR + 12..NODE_OFF_ADDR + 16].copy_from_slice(&v4.octets());
    } else {
        panic!("test nodes are IPv4");
    }
    rec[NODE_OFF_PORT..NODE_OFF_PORT + 2].copy_from_slice(&addr.port().to_le_bytes());
    rec[NODE_OFF_NR_VNODES..NODE_OFF_NR_VNODES + 2].copy_from_slice(&nr_vnodes.to_le_bytes());
    rec[NODE_OFF_ZONE..NODE_OFF_ZONE + 4].copy_from_slice(&zone.to_le_bytes());
    rec
}

/// The oid of data object `idx` of a vid.
fn data_oid(vid: u32, idx: u64) -> u64 {
    (u64::from(vid) << 32) | idx
}

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
    /// Leave the inode's `uuid[16]` all-zero, as a `sheep` predating the
    /// field wrote it.
    no_uuid: bool,
    data_vdi_id: Vec<u32>,
    /// An ACL object's member names (`dog acl add member`), one per
    /// fixed-width slot of the inode header's `metadata[]`; an empty string is
    /// the hole `dog acl remove member` leaves.
    members: Vec<String>,
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
            no_uuid: false,
            data_vdi_id: vec![0u32; nr_objects],
            members: Vec::new(),
        }
    }

    /// An inode from a `sheep` that never wrote `uuid[16]`.
    fn no_uuid(mut self) -> Vdi {
        self.no_uuid = true;
        self
    }

    /// The `uuid[16]` this VDI was created with: one per (name, tag), and
    /// never all-zero, so it is always distinguishable from "unset".
    fn uuid(&self) -> Option<[u8; 16]> {
        if self.no_uuid {
            return None;
        }
        let mut uuid = [0x5du8; 16];
        for (i, b) in format!("{}@{}", self.name, self.tag).bytes().enumerate() {
            uuid[i % 16] ^= b.rotate_left(i as u32 % 8);
        }
        Some(uuid)
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

    /// Make this VDI an ACL object (`dog acl create`) rather than a volume,
    /// listing `members` in the `data_vdi_id[]` array `dog acl add` writes —
    /// a `0` entry being the hole `dog acl remove` leaves behind.
    fn acl_object(mut self, members: &[u32]) -> Vdi {
        self.vdi_flags |= SD_VDI_FLAG_ACL;
        self.data_vdi_id = members.to_vec();
        self
    }

    /// The ACL's member names (`dog acl add member`), in slot order — an
    /// empty one being the hole a `dog acl remove member` left behind.
    fn members(mut self, members: &[&str]) -> Vdi {
        self.members = members.iter().map(|m| (*m).to_string()).collect();
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
        // max_data_id_nr: the object-map extent of a volume, the member-array
        // extent (holes included) of an ACL object.
        b[528..532].copy_from_slice(&(self.data_vdi_id.len() as u32).to_le_bytes());
        b[536..544].copy_from_slice(&self.vdi_size.to_le_bytes()); // vdi_size
        b[554] = self.nr_copies;
        b[555] = self.block_size_shift;
        b[572..576].copy_from_slice(&self.acl_id.to_le_bytes());
        if let Some(uuid) = self.uuid() {
            b[576..592].copy_from_slice(&uuid);
        }
        b[592..596].copy_from_slice(&self.vdi_flags.to_le_bytes());
        // metadata[]: the ACL's member names, one per SD_MAX_VDI_LEN slot.
        for (i, member) in self.members.iter().enumerate() {
            let o = INO_OFF_METADATA + i * SD_MAX_VDI_LEN;
            b[o..o + member.len()].copy_from_slice(member.as_bytes());
        }
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
    /// Vids currently locked by a `REGISTER_VDI`. Only `UNREGISTER_VDI` clears
    /// one — a closing connection deliberately does not, so a test can tell an
    /// explicit release from a socket that merely went away.
    locks: BTreeMap<u32, Lock>,
    /// The participant list `REGISTER_VDI` builds, per vid: the owner each
    /// registration named, and how many it has (`sheep` refcounts repeats of
    /// one owner into a single entry rather than listing it twice).
    participants: BTreeMap<u32, Vec<(SocketAddr, u32)>>,
    /// The cluster's node/zone topology (`GET_NODE_LIST`), as `(addr, zone,
    /// nr_vnodes)`: only [`SD_OP_GET_NODE_LIST`] looks at it — every other op
    /// is answered cluster-wide, as a gateway, whatever this list says. Empty
    /// by default: only tests exercising ANA placement need it.
    nodes: Vec<(SocketAddr, u32, u16)>,
    /// Oids whose `READ_OBJ` is answered late, off a thread of its own, so a
    /// pipelining client gets its responses out of order — which a real
    /// `sheep` does routinely (each request goes to a worker and is answered
    /// in completion order).
    slow_reads: HashSet<u64>,
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

impl Store {
    /// `add_participant`: a repeat registration from one owner bumps its
    /// count, a new owner takes the next free slot (`SD_MAX_COPIES` of them).
    fn add_participant(&mut self, vid: u32, owner: SocketAddr) -> bool {
        let list = self.participants.entry(vid).or_default();
        if let Some(entry) = list.iter_mut().find(|(addr, _)| *addr == owner) {
            entry.1 += 1;
            return true;
        }
        if list.len() >= SD_MAX_COPIES {
            return false;
        }
        list.push((owner, 1));
        true
    }

    /// `del_participant`: decrement, and compact the list when the last
    /// registration of an owner goes.
    fn del_participant(&mut self, vid: u32, owner: SocketAddr) -> bool {
        let Some(list) = self.participants.get_mut(&vid) else {
            return false;
        };
        let Some(i) = list.iter().position(|(addr, _)| *addr == owner) else {
            return false;
        };
        list[i].1 -= 1;
        if list[i].1 == 0 {
            list.remove(i);
        }
        if list.is_empty() {
            self.participants.remove(&vid);
        }
        true
    }

    /// The `GET_VDI_COPIES` payload: one `vdi_state` per VDI the cluster
    /// knows, carrying its participant list.
    fn vdi_states(&self) -> Vec<u8> {
        let mut out = vec![0u8; self.vdis.len() * VDI_STATE];
        for (i, vid) in self.vdis.keys().enumerate() {
            let vs = &mut out[i * VDI_STATE..(i + 1) * VDI_STATE];
            vs[0..4].copy_from_slice(&vid.to_le_bytes());
            let empty = Vec::new();
            let list = self.participants.get(vid).unwrap_or(&empty);
            let state = if list.is_empty() {
                LOCK_STATE_UNLOCKED
            } else {
                LOCK_STATE_SHARED
            };
            vs[20..24].copy_from_slice(&state.to_le_bytes());
            vs[64..68].copy_from_slice(&(list.len() as u32).to_le_bytes());
            for (j, (owner, count)) in list.iter().enumerate() {
                // participants_state[j]: SHARED_LOCK_STATE_SHARED, count above.
                let word = 2u32 | (count << 8);
                vs[68 + j * 4..72 + j * 4].copy_from_slice(&word.to_le_bytes());
                let nid = 192 + j * 40;
                match owner.ip() {
                    std::net::IpAddr::V4(v4) => {
                        vs[nid + 12..nid + 16].copy_from_slice(&v4.octets());
                    }
                    std::net::IpAddr::V6(v6) => {
                        vs[nid..nid + 16].copy_from_slice(&v6.octets());
                    }
                }
                vs[nid + 16..nid + 18].copy_from_slice(&owner.port().to_le_bytes());
            }
        }
        out
    }
}

/// The owner a `vdi_lock` request names: addr[16] at 24, port at 40.
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

/// Reply with `bytes[offset..offset+data_length]`, the trim-to-what-exists
/// behavior a real `sheep` has.
fn send_slice(
    out: &Mutex<TcpStream>,
    opcode: u8,
    id: u32,
    bytes: &[u8],
    offset: usize,
    data_length: usize,
) -> std::io::Result<()> {
    let end = (offset + data_length).min(bytes.len());
    let slice = &bytes[offset.min(bytes.len())..end];
    let mut sock = out.lock().unwrap();
    sock.write_all(&resp(opcode, id, 0, slice.len() as u32))?;
    sock.write_all(slice)
}

/// How late a `slow_reads` response is: long enough that a request issued
/// after it is answered first, short enough not to drag the suite.
const SLOW_READ: std::time::Duration = std::time::Duration::from_millis(60);

fn serve_conn(mut sock: TcpStream, store: Arc<Mutex<Store>>) -> std::io::Result<()> {
    // Requests are read in order; responses may be written by a delayed
    // responder thread, so the write half is shared and one lock covers a
    // whole (header, payload) reply.
    let out = Arc::new(Mutex::new(sock.try_clone()?));
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
            SD_OP_GET_VDI_INFO => {
                // payload was not consumed above (not a write opcode); drain it.
                let mut p = vec![0u8; data_length];
                sock.read_exact(&mut p)?;
                let name = cstr(&p[..SD_MAX_VDI_LEN]);
                let tag = cstr(&p[SD_MAX_VDI_LEN..]);
                let acl = u32le(&hdr, 36); // sd_req.vdi.acl
                let named = st
                    .vdis
                    .iter()
                    .find(|(_, vdi)| vdi.name == name && vdi.tag == tag)
                    .map(|(&vid, vdi)| (vid, vdi.acl_id));
                match named {
                    // The cluster resolves a name only within the ACL the
                    // request carries; from outside it, the VDI is denied.
                    Some((_, vdi_acl)) if vdi_acl != acl => {
                        write_resp(&out, &resp(opcode, id, SD_RES_VDI_DENIED, 0))?;
                    }
                    Some((vid, _)) => {
                        let mut r = resp(opcode, id, 0, 0);
                        r[24..28].copy_from_slice(&vid.to_le_bytes()); // vdi_id
                        write_resp(&out, &r)?;
                    }
                    None => write_resp(&out, &resp(opcode, id, 0x08, 0))?, // NO_VDI
                }
            }
            SD_OP_REGISTER_VDI => {
                // The lock op, looking its VDI up by name as GET_VDI_INFO does
                // but naming its own owner: under an ACL it joins the volume's
                // participant list, outside one it claims the volume alone.
                let mut p = vec![0u8; data_length];
                sock.read_exact(&mut p)?;
                let name = cstr(&p[..SD_MAX_VDI_LEN]);
                let acl = u32le(&hdr, 44); // sd_req.vdi_lock.acl
                let owner = req_owner(&hdr);
                let named = st
                    .vdis
                    .iter()
                    .find(|(_, vdi)| vdi.name == name && vdi.tag.is_empty())
                    .map(|(&vid, vdi)| (vid, vdi.acl_id));
                let result = match named {
                    None => 0x08, // NO_VDI
                    Some((_, vdi_acl)) if vdi_acl != acl => SD_RES_VDI_DENIED,
                    Some((vid, _)) if !st.take_lock(vid, acl) => SD_RES_VDI_LOCKED,
                    // The exclusive lock has one owner and no list; only a
                    // shared one records its holders as participants.
                    Some((vid, _)) if acl != SD_ACL_NONE && !st.add_participant(vid, owner) => {
                        st.release_lock(vid, acl);
                        0x15 // NO_SPACE: SD_MAX_COPIES participants already
                    }
                    Some(_) => 0,
                };
                write_resp(&out, &resp(opcode, id, result, 0))?;
            }
            SD_OP_UNREGISTER_VDI => {
                let vid = u32le(&hdr, 16); // sd_req.vdi_lock.vid
                let acl = u32le(&hdr, 44);
                let owner = req_owner(&hdr);
                let held = st.release_lock(vid, acl);
                let listed = acl == SD_ACL_NONE || st.del_participant(vid, owner);
                let result = if held && listed {
                    0
                } else {
                    SD_RES_VDI_NOT_LOCKED
                };
                write_resp(&out, &resp(opcode, id, result, 0))?;
            }
            SD_OP_GET_VDI_COPIES => {
                // The whole table or nothing: a caller that asked for too
                // small a buffer is told to come back with a bigger one.
                let states = st.vdi_states();
                if data_length < states.len() {
                    write_resp(&out, &resp(opcode, id, SD_RES_BUFFER_SMALL, 0))?;
                } else {
                    send_slice(&out, opcode, id, &states, 0, data_length)?;
                }
            }
            SD_OP_READ_VDIS => {
                let bitmap = st.vdi_bitmap();
                send_slice(&out, opcode, id, &bitmap, 0, data_length)?;
            }
            SD_OP_READ_OBJ => {
                let vid = oid_to_vid(oid);
                let Some(vdi) = st.vdis.get(&vid) else {
                    write_resp(&out, &resp(opcode, id, 0x02, 0))?; // NO_OBJ
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
                if st.slow_reads.contains(&oid) {
                    let out = Arc::clone(&out);
                    std::thread::spawn(move || {
                        std::thread::sleep(SLOW_READ);
                        let _ = send_slice(&out, opcode, id, &bytes, offset, data_length);
                    });
                } else {
                    send_slice(&out, opcode, id, &bytes, offset, data_length)?;
                }
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
                write_resp(&out, &resp(opcode, id, 0, 0))?;
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
                write_resp(&out, &resp(opcode, id, 0, 0))?;
            }
            // A local op: answered out of this node's own membership view
            // rather than routed anywhere.
            SD_OP_GET_NODE_LIST => {
                assert_eq!(
                    hdr[0], SD_SHEEP_PROTO_VER,
                    "GET_NODE_LIST is a sheep-internal op"
                );
                let bytes: Vec<u8> = st
                    .nodes
                    .iter()
                    .flat_map(|&(addr, zone, nr_vnodes)| node_record(addr, nr_vnodes, zone))
                    .collect();
                send_slice(&out, opcode, id, &bytes, 0, data_length)?;
            }
            other => {
                write_resp(&out, &resp(other, id, 0x01, 0))?; // UNKNOWN
            }
        }
    }
}

/// Write a header-only response under the shared write half.
fn write_resp(out: &Mutex<TcpStream>, hdr: &[u8; HDR]) -> std::io::Result<()> {
    out.lock().unwrap().write_all(hdr)
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
    spawn_counting_sheep(store, Arc::new(AtomicUsize::new(0)))
}

/// [`spawn_fake_sheep`], counting the connections the target opens to it.
fn spawn_counting_sheep(store: Arc<Mutex<Store>>, conns: Arc<AtomicUsize>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for sock in listener.incoming() {
            let Ok(sock) = sock else { break };
            conns.fetch_add(1, Ordering::SeqCst);
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
/// The hostnqns `dog acl add member` put on that ACL: the hosts it admits.
const HOST_A: &str = "nqn.2014-08.org.nvmexpress:uuid:host-a";
const HOST_B: &str = "nqn.2014-08.org.nvmexpress:uuid:host-b";

fn fresh_store(block_size_shift: u8, vdi_size: u64) -> Arc<Mutex<Store>> {
    let mut vdis = BTreeMap::new();
    vdis.insert(TEST_VID, Vdi::new("testvdi", vdi_size, block_size_shift));
    Arc::new(Mutex::new(Store {
        vdis,
        objects: HashMap::new(),
        locks: BTreeMap::new(),
        participants: BTreeMap::new(),
        nodes: Vec::new(),
        slow_reads: HashSet::new(),
    }))
}

/// [`fresh_store`] with `testvdi` moved into an ACL object named [`ACL_NQN`],
/// the shape a target exporting a subsystem per ACL sees.
fn acl_store(block_size_shift: u8, vdi_size: u64) -> Arc<Mutex<Store>> {
    let store = fresh_store(block_size_shift, vdi_size);
    {
        let mut st = store.lock().unwrap();
        st.vdis.insert(
            ACL_VID,
            Vdi::new(ACL_NQN, 1 << 22, 22).acl_object(&[TEST_VID]),
        );
        st.vdis.get_mut(&TEST_VID).unwrap().acl_id = ACL_VID;
    }
    store
}

/// The fabric address of target `n`: what an open registers as the volume's
/// holder, and what the cluster hands back as a path to it.
fn target(n: u8) -> SocketAddr {
    SocketAddr::from(([10, 0, 0, n], 4420))
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
    let be = SheepdogBackend::open(addr, "testvdi", None, None, Some(target(1))).unwrap();
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

    let be = SheepdogBackend::open(addr, "testvdi", None, Some(ACL_NQN), Some(target(1))).unwrap();
    assert_eq!(holders(&store, TEST_VID), 1, "the open took the lock");

    // A second target may serve the same VDI — that is what the shared lock
    // is for — and joins the holders rather than displacing the first.
    let second =
        SheepdogBackend::open(addr, "testvdi", None, Some(ACL_NQN), Some(target(2))).unwrap();
    assert_eq!(holders(&store, TEST_VID), 2);

    // An explicitly unlocked open goes through too, and disturbs nothing.
    let waived = SheepdogBackend::open(addr, "testvdi", None, Some(ACL_NQN), None).unwrap();
    assert_eq!(holders(&store, TEST_VID), 2);
    drop(waived);
    assert_eq!(holders(&store, TEST_VID), 2);

    // Each drop hands back exactly one participant's hold.
    drop(second);
    assert_eq!(holders(&store, TEST_VID), 1, "the first target still holds");
    drop(be);
    assert!(locked(&store).is_empty(), "the last drop freed the VDI");
}

/// The shutdown path: a target asked to stop releases its VDI lock while the
/// backend is still alive (its queue threads hold `Arc`s to it, so no drop is
/// coming), and the later drop must not release it a second time.
#[test]
fn an_explicit_release_hands_the_lock_back_before_drop() {
    let store = fresh_store(16, 256 * 1024);
    let addr = spawn_fake_sheep(Arc::clone(&store));

    let be = SheepdogBackend::open(addr, "testvdi", None, None, Some(target(1))).unwrap();
    assert_eq!(locked(&store), vec![TEST_VID], "the open took the lock");

    be.release_lock();
    assert!(locked(&store).is_empty(), "shutdown freed the VDI");

    // Idempotent: neither a repeat call nor the drop that follows it names
    // the lock again — the cluster would refuse a release it does not hold,
    // and a *new* holder's lock must survive both.
    be.release_lock();
    let next = SheepdogBackend::open(addr, "testvdi", None, None, Some(target(2))).unwrap();
    drop(be);
    assert_eq!(
        locked(&store),
        vec![TEST_VID],
        "the released backend's drop left the next holder alone"
    );
    drop(next);
    assert!(locked(&store).is_empty());

    // A backend that never locked (`?nolock`) shuts down just as quietly.
    let waived = SheepdogBackend::open(addr, "testvdi", None, None, None).unwrap();
    waived.release_lock();
    assert!(locked(&store).is_empty());
}

/// A VDI in no ACL locks exclusively, so a second target is kept out — the
/// counterpart of the shared case above.
#[test]
fn a_vdi_outside_any_acl_locks_exclusively() {
    let store = fresh_store(16, 256 * 1024);
    let addr = spawn_fake_sheep(Arc::clone(&store));

    let be = SheepdogBackend::open(addr, "testvdi", None, None, Some(target(1))).unwrap();
    assert_eq!(
        store.lock().unwrap().locks[&TEST_VID],
        Lock::Exclusive,
        "no ACL means LOCK_TYPE_NORMAL"
    );
    let err = SheepdogBackend::open(addr, "testvdi", None, None, Some(target(2)))
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

    let err = SheepdogBackend::open(addr, "testvdi", None, Some(ACL_NQN), Some(target(1)))
        .err()
        .expect("an exclusively locked VDI is refused");
    assert_eq!(err.kind(), std::io::ErrorKind::ResourceBusy);
    assert_eq!(locked(&store), vec![TEST_VID], "the holder keeps its lock");

    // Waiving the lock is the escape hatch for exclusion arranged elsewhere.
    let waived = SheepdogBackend::open(addr, "testvdi", None, Some(ACL_NQN), None).unwrap();
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
        let err = SheepdogBackend::open(addr, vdi, None, acl, Some(target(1)))
            .err()
            .unwrap_or_else(|| panic!("{vdi} should be denied under {acl:?}"));
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied, "{vdi}");
    }
    assert!(locked(&store).is_empty(), "a denied open takes no lock");

    // An ordinary VDI is refused as an ACL even though the name resolves.
    let err = SheepdogBackend::open(addr, "testvdi", None, Some("loose"), Some(target(1)))
        .err()
        .expect("'loose' is no ACL object");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

    // Named correctly, the member opens.
    let be = SheepdogBackend::open(addr, "testvdi", None, Some(ACL_NQN), Some(target(1))).unwrap();
    assert_eq!(be.nr_blocks(), 256 * 1024 / 512);
}

/// A failed open must not walk away holding a lock: the registration is the
/// last thing it takes, after the inode is in hand.
#[test]
fn a_failed_open_leaves_no_registration() {
    // A zero-sized VDI passes the name lookup and fails the inode check.
    let store = fresh_store(16, 256 * 1024);
    store
        .lock()
        .unwrap()
        .vdis
        .insert(0x00_0042, Vdi::new("empty", 0, 22));
    let addr = spawn_fake_sheep(Arc::clone(&store));

    let err = SheepdogBackend::open(addr, "empty", None, None, Some(target(1)))
        .err()
        .expect("a zero-sized VDI is rejected");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    assert!(locked(&store).is_empty(), "a failed open leaves no lock");
}

/// The VDI's identity comes from the cluster: the `uuid[16]` `sheep` wrote
/// into the inode at creation, which a target reports as the namespace UUID
/// instead of inventing one.
#[test]
fn the_inode_uuid_is_read_back_as_the_vdi_identity() {
    let store = fresh_store(16, 256 * 1024);
    store
        .lock()
        .unwrap()
        .vdis
        .insert(0x00_0050, Vdi::new("legacy", 1 << 20, 22).no_uuid());
    let addr = spawn_fake_sheep(Arc::clone(&store));

    let expected = store.lock().unwrap().vdis[&TEST_VID].uuid();
    assert!(expected.is_some(), "the fixture wrote a uuid");
    let be = SheepdogBackend::open(addr, "testvdi", None, None, None).unwrap();
    assert_eq!(be.uuid(), expected, "open reports the inode's uuid");

    // An inode from a sheep predating the field carries an all-zero uuid,
    // which is "unset" rather than an identity to hand a host.
    let legacy = SheepdogBackend::open(addr, "legacy", None, None, None).unwrap();
    assert_eq!(legacy.uuid(), None);

    // The enumeration path reads the same field, so a target mapping the
    // cluster and one opening a single VDI agree on every volume's identity.
    let vdis = list_vdis(addr).unwrap();
    let seen: Vec<_> = vdis.iter().map(|v| (v.name.as_str(), v.uuid)).collect();
    assert_eq!(seen, vec![("legacy", None), ("testvdi", expected)]);
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
        // An ACL object and its two members — with the hole a third, since
        // removed, member left in the array — plus an empty second ACL. Two
        // hosts are members of the first (again around a hole), none of the
        // second.
        st.vdis.insert(
            ACL_VID,
            Vdi::new(ACL_NQN, 1 << 22, 22)
                .acl_object(&[0x00_0020, 0, 0x00_0021])
                .members(&[HOST_A, "", HOST_B]),
        );
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
            Vdi::new("nqn.2026-06.io.ioutgt:idle", 1 << 22, 22).acl_object(&[]),
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
    // Each VDI carries its own cluster-assigned uuid — a snapshot's differs
    // from its head's, so the two never collide as namespace identities.
    let uuids: Vec<_> = vdis.iter().map(|v| v.uuid.expect(&v.name)).collect();
    let mut unique = uuids.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), uuids.len(), "one identity per VDI");

    // Every enumerated head is openable under the ACL the listing reports, and
    // reports the size the listing reports.
    for vdi in vdis.iter().filter(|v| !v.snapshot) {
        let acl = (vdi.acl != 0).then_some(ACL_NQN);
        let be = SheepdogBackend::open(addr, &vdi.name, None, acl, None).unwrap();
        assert_eq!(be.nr_blocks() * 512, vdi.size, "{} size", vdi.name);
    }

    // The snapshot is openable by tag and refuses writes. Being frozen, it
    // takes no lock even though this open asks for one.
    let snap = SheepdogBackend::open(addr, "alpha", Some("daily"), None, Some(target(1))).unwrap();
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
/// as the cluster names it, holding the VDIs its own `data_vdi_id[]` lists and
/// the member names its own `metadata[]` lists.
#[test]
fn acl_enumeration_follows_the_acls_member_array() {
    let store = enumeration_store();
    {
        let mut st = store.lock().unwrap();
        // A VDI whose ACL object no longer exists belongs to no subsystem; it
        // is dropped with a warning rather than inventing one.
        st.vdis
            .insert(0x00_0040, Vdi::new("orphan", 1 << 20, 22).in_acl(0x00_0099));
        // `dog acl add` writes the array entry before the member's acl_id, so
        // a half-completed add leaves a listed VDI whose inode disagrees...
        st.vdis
            .insert(0x00_0041, Vdi::new("halfadded", 1 << 20, 22));
        // ...and a VDI naming the ACL that the ACL does not list is not a
        // member of it either. Both are dropped with a warning: the cluster
        // resolves neither name under this ACL.
        st.vdis
            .insert(0x00_0042, Vdi::new("unlisted", 1 << 20, 22).in_acl(ACL_VID));
        let acl = st.vdis.get_mut(&ACL_VID).unwrap();
        acl.data_vdi_id.push(0x00_0041);
        acl.data_vdi_id.push(0x00_0050); // listed, but no such VDI at all
    }
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
            (acl.name.as_str(), acl.vid, acl.max_data_id_nr, members)
        })
        .collect();
    assert_eq!(
        seen,
        vec![
            // Sorted by ACL name; an ACL with no members is still an ACL.
            // max_data_id_nr counts array slots, not usable members: the hole
            // and the two unresolvable entries are in it too.
            (
                "nqn.2026-06.io.ioutgt:grp",
                ACL_VID,
                5,
                vec![("shared", "", 0x20), ("shared", "daily", 0x21)]
            ),
            ("nqn.2026-06.io.ioutgt:idle", 0x30, 0, vec![]),
        ]
    );

    // The host side of the ACL: the names `dog acl add member` wrote, in slot
    // order and without the hole a removal left. An ACL nobody was added to
    // lists none — and so admits none.
    let hosts: Vec<_> = acls.iter().map(|acl| acl.hosts.clone()).collect();
    assert_eq!(
        hosts,
        vec![vec![HOST_A.to_string(), HOST_B.to_string()], vec![]]
    );
}

/// The discovery path: opening a volume under an ACL registers this target's
/// fabric address as one of its holders, and reading the holders back names
/// every target serving it — the paths a subsystem advertises.
#[test]
fn the_holders_of_a_volume_are_the_targets_serving_it() {
    let store = acl_store(16, 256 * 1024);
    let addr = spawn_fake_sheep(Arc::clone(&store));
    let (us, peer) = (target(1), target(2));

    let be = SheepdogBackend::open(addr, "testvdi", None, Some(ACL_NQN), Some(us)).unwrap();
    assert_eq!(be.vid(), TEST_VID);
    assert_eq!(be.owner(), Some(us), "the open registered our own address");
    assert_eq!(
        vdi_holders(addr, &[be.vid()]).unwrap(),
        vec![vec![VdiHolder {
            addr: us,
            index: 0,
            registrations: 1
        }]]
    );

    // A second target opens the same volume: both are paths to it now, each
    // with its own participant slot.
    let other = SheepdogBackend::open(addr, "testvdi", None, Some(ACL_NQN), Some(peer)).unwrap();
    assert_eq!(
        vdi_holders(addr, &[TEST_VID]).unwrap()[0],
        vec![
            VdiHolder {
                addr: us,
                index: 0,
                registrations: 1
            },
            VdiHolder {
                addr: peer,
                index: 1,
                registrations: 1
            },
        ]
    );

    // Shutting one down hands its registration back and the cluster compacts
    // the list — the other keeps serving the volume.
    other.release_lock();
    assert_eq!(
        vdi_holders(addr, &[TEST_VID]).unwrap()[0],
        vec![VdiHolder {
            addr: us,
            index: 0,
            registrations: 1
        }]
    );

    // Nobody holds an unopened volume, and a vid the cluster never heard of
    // has no list at all: both answer empty, in the order asked.
    drop(be);
    assert_eq!(
        vdi_holders(addr, &[TEST_VID, ACL_VID, 0x00_dead]).unwrap(),
        vec![vec![], vec![], vec![]]
    );
}

/// A volume in no ACL is held exclusively, which has an owner but no
/// participant list: nobody else can serve it, so it advertises no paths.
#[test]
fn an_exclusively_held_volume_reports_no_holders() {
    let store = fresh_store(16, 256 * 1024);
    let addr = spawn_fake_sheep(Arc::clone(&store));

    let be = SheepdogBackend::open(addr, "testvdi", None, None, Some(target(1))).unwrap();
    assert_eq!(locked(&store), vec![TEST_VID]);
    assert_eq!(vdi_holders(addr, &[be.vid()]).unwrap(), vec![vec![]]);

    // An open that took no registration has none to retake either.
    let waived = SheepdogBackend::open(addr, "testvdi", None, None, None).unwrap();
    waived.reregister().unwrap();
    assert_eq!(locked(&store), vec![TEST_VID], "and took none doing so");
}

/// A cluster that lost this target's registration — a `sheep` restart, or an
/// eviction and rejoin — gets it back from the refresh, without the namespace
/// being reopened.
#[test]
fn a_dropped_registration_is_retaken() {
    let store = acl_store(16, 256 * 1024);
    let addr = spawn_fake_sheep(Arc::clone(&store));
    let us = target(1);

    let be = SheepdogBackend::open(addr, "testvdi", None, Some(ACL_NQN), Some(us)).unwrap();
    {
        let mut st = store.lock().unwrap();
        st.locks.clear();
        st.participants.clear();
    }
    assert!(vdi_holders(addr, &[TEST_VID]).unwrap()[0].is_empty());

    be.reregister().unwrap();
    assert_eq!(
        vdi_holders(addr, &[TEST_VID]).unwrap()[0],
        vec![VdiHolder {
            addr: us,
            index: 0,
            registrations: 1
        }]
    );

    // A volume deleted and recreated under this namespace's feet resolves to a
    // different vid; registering on it would advertise a path to storage this
    // namespace is not reading, so it is refused.
    {
        let mut st = store.lock().unwrap();
        let vdi = st.vdis.remove(&TEST_VID).unwrap();
        st.vdis.insert(0x00_0099, vdi);
    }
    let err = be.reregister().unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::StaleNetworkFileHandle);

    // A backend that has already handed its registration back has nothing to
    // retake: a refresh arriving behind a shutdown must not undo it.
    be.release_lock();
    be.reregister().unwrap();
    assert!(store.lock().unwrap().participants.is_empty());
}

/// The holder list arrives whole however many VDIs the cluster has: the fetch
/// retries with a bigger buffer for as long as `sheep` says it is too small.
#[test]
fn holder_list_survives_a_cluster_larger_than_one_buffer() {
    let store = acl_store(16, 256 * 1024);
    {
        // Past DEFAULT_VDI_STATE_COUNT (512), so the first request bounces.
        let mut st = store.lock().unwrap();
        for i in 0..600u32 {
            st.vdis
                .insert(0x10_0000 + i, Vdi::new("filler", 1 << 22, 22));
        }
    }
    let addr = spawn_fake_sheep(Arc::clone(&store));
    let us = target(1);

    let be = SheepdogBackend::open(addr, "testvdi", None, Some(ACL_NQN), Some(us)).unwrap();
    assert_eq!(
        vdi_holders(addr, &[be.vid()]).unwrap()[0],
        vec![VdiHolder {
            addr: us,
            index: 0,
            registrations: 1
        }]
    );
}

/// Object placement — the fact behind ANA. `GET_NODE_LIST` names every node's
/// zone; the ring built from it decides which zone owns each vid's inode
/// object — a fact about the cluster's topology and the object id, the same
/// however many targets ask and through whichever gateway.
#[test]
fn cluster_ana_state_reports_the_placement_ring() {
    const REMOTE_VID: u32 = 0x00ab_cdf0;
    let store = fresh_store(16, 256 * 1024);
    let addr = spawn_fake_sheep(Arc::clone(&store));
    {
        let mut st = store.lock().unwrap();
        st.vdis
            .insert(REMOTE_VID, Vdi::new("remotevdi", 256 * 1024, 16));
        st.nodes = vec![
            // The node we are actually connected to: its zone is the
            // "optimized" one.
            (addr, 1, 128),
            // A peer that stores data too, in a different zone.
            (target(9), 2, 128),
            // A pure gateway: it claims a zone but stores nothing
            // (`nr_vnodes` 0), so no object should ever land on it.
            (target(10), 3, 0),
        ];
    }

    let vids: Vec<u32> = (0..128).collect();
    let state = cluster_ana_state(addr, &vids).unwrap();

    // The zone we are connected through, straight out of the node list.
    assert_eq!(state.own_zone, Some(1));
    // Every zone that stores data — the gateway-only zone 3 is excluded, the
    // same filter `sheep`'s own zone count applies.
    assert_eq!(state.zones, vec![1, 2]);
    // Every vid landed on one of the two data-storing zones, and — spread
    // over two equally-weighted zones, 128 of them — both zones actually won
    // some, not just one by chance.
    let seen: std::collections::HashSet<u32> = state.grpids.iter().flatten().copied().collect();
    assert_eq!(seen, std::collections::HashSet::from([1, 2]));

    // TEST_VID and REMOTE_VID answer too, positionally, same as any other vid.
    let two = cluster_ana_state(addr, &[TEST_VID, REMOTE_VID]).unwrap();
    assert!(two.grpids.iter().all(|g| matches!(g, Some(1) | Some(2))));

    // A cluster with no data-storing nodes at all resolves nothing — the
    // vid simply has no zone to report.
    store.lock().unwrap().nodes.clear();
    let none = cluster_ana_state(addr, &[TEST_VID]).unwrap();
    assert_eq!(none.own_zone, None);
    assert!(none.zones.is_empty());
    assert_eq!(none.grpids, vec![None]);

    // Unlike the old per-vid locality probe, there is no zero-vid fast path
    // that skips connecting: the placement ring needs the node list even to
    // answer nothing, so an address that would refuse fails outright.
    let nowhere: SocketAddr = "127.0.0.1:1".parse().unwrap();
    assert!(cluster_ana_state(nowhere, &[]).is_err());
}

/// One connection per thread, pipelined: a queue thread runs every namespace's
/// object IO over a single socket, and routes each response back to its caller
/// by the request id the server echoes — including when the cluster answers
/// out of order, which it does whenever one request outruns another.
#[test]
fn one_connection_carries_concurrent_requests_answered_out_of_order() {
    // 64 KiB data objects, 256 KiB volume; object 0 is answered late.
    let store = fresh_store(16, 256 * 1024);
    let conns = Arc::new(AtomicUsize::new(0));
    let addr = spawn_counting_sheep(Arc::clone(&store), Arc::clone(&conns));
    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    let be = Rc::new(SheepdogBackend::open(addr, "testvdi", None, None, Some(target(1))).unwrap());
    // The open's own lookups went over one-shot control-plane connections.
    let before = conns.load(Ordering::SeqCst);

    let obj = 64 * 1024 / 512; // LBAs per object
    let done = Rc::new(RefCell::new(Vec::new()));
    rt.block_on({
        let (be, done) = (Rc::clone(&be), Rc::clone(&done));
        async move {
            // Give both objects distinct contents to read back.
            for (i, seed) in [(0u64, 11u8), (1, 22)] {
                let pat = filled(4096, seed);
                be.write(i * obj, &pat[..4096]).await.unwrap();
            }
            store
                .lock()
                .unwrap()
                .slow_reads
                .insert(data_oid(TEST_VID, 0));

            // Both reads are in flight on the one connection at once; the
            // second is answered first.
            let readers: Vec<_> = [(0u64, 11u8), (1, 22)]
                .into_iter()
                .map(|(i, seed)| {
                    let (be, done) = (Rc::clone(&be), Rc::clone(&done));
                    tokio::task::spawn_local(async move {
                        let mut got = AlignedBuf::zeroed(4096);
                        be.read(i * obj, &mut got[..4096]).await.unwrap();
                        assert_eq!(
                            &got[..],
                            &filled(4096, seed)[..],
                            "object {i} got its own response"
                        );
                        done.borrow_mut().push(i);
                    })
                })
                .collect();
            for r in readers {
                r.await.unwrap();
            }
        }
    });

    assert_eq!(*done.borrow(), vec![1, 0], "the late response came last");
    assert_eq!(
        conns.load(Ordering::SeqCst) - before,
        1,
        "all of that data path went over one connection"
    );
}
