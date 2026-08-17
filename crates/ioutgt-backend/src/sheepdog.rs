//! Sheepdog cluster backend: serves a namespace from a named VDI on a
//! [Sheepdog](https://github.com/sheepdog/sheepdog) distributed-storage
//! cluster, over the plain-TCP client/gateway protocol. [`list_acls`]
//! enumerates the cluster's ACL objects and their member VDIs, so a target
//! can export one subsystem per ACL and one namespace per member.
//!
//! # Protocol
//!
//! A `sheep` gateway listens on TCP port 7000. There is no handshake and no
//! authentication: after connect, the client sends 48-byte little-endian
//! request headers (`proto_ver = 0x02`) optionally followed by a data payload,
//! and reads a 48-byte response header (carrying an `SD_RES_*` result and a
//! `data_length`) optionally followed by payload. Requests carry an `id` the
//! server echoes; a connection may pipeline requests, but this backend keeps
//! **one request in flight per connection** (a per-thread connection pool) so
//! no response demultiplexing is needed.
//!
//! A VDI is looked up by name → a 24-bit `vid` (or the whole cluster is
//! enumerated from the VDI bitmap, `READ_VDIS`). Its *inode* object holds the
//! volume size, the data-object size (`block_size_shift`, default 4 MiB), the
//! replication factor, and a `data_vdi_id[]` array mapping each object index to
//! the vid that owns that data object (`0` = unallocated hole). A logical byte
//! offset `off` maps to `idx = off / object_size`, `in_obj = off % object_size`,
//! and the object id `oid = (vid << 32) | idx`.
//!
//! # Identity
//!
//! `sheep` generates a `uuid[16]` into every inode header when the VDI is
//! created, and `dog vdi list --json` reports it: the cluster's own identity
//! for the volume, outliving any target that exports it. [`VdiInfo::uuid`] and
//! [`SheepdogBackend::uuid`] hand it up so a target can report it verbatim as
//! the namespace UUID (Identify CNS 03h) — one volume then looks like one
//! namespace (`/dev/disk/by-id`) through every target fronting the cluster.
//! An inode written by an older `sheep` has the field all-zero (the field was
//! carved out of `__unused[]`), reported as `None`.
//!
//! # ACLs
//!
//! An *ACL object* is an ordinary VDI carrying `SD_VDI_FLAG_ACL` in its inode
//! `vdi_flags`; the volumes it grants access to name it back in their own
//! inode `acl_id`. The ACL is the cluster's access-control scope: every VDI
//! lookup (`GET_VDI_INFO`, `LOCK_VDI`) carries an ACL id, and `sheep` only
//! matches a name against inodes whose `acl_id` equals it — a VDI inside an
//! ACL is invisible (`SD_RES_VDI_DENIED`) to a lookup that does not name it,
//! and vice versa. ACL id `0` is both "belongs to no ACL" and, for the lock
//! ops, `LOCK_TYPE_NORMAL`.
//!
//! The membership list itself lives in the ACL object's inode: `dog acl add`
//! writes the member's vid into the ACL's `data_vdi_id[]` array — the same
//! array an ordinary VDI uses as its object map — and sizes it with the header
//! field `max_data_id_nr`. `dog acl remove` clears an entry in place, so the
//! array is sparse: a zero is a hole, not the end of the list. [`list_acls`]
//! walks `data_vdi_id[0..max_data_id_nr]` and hands both the members and the
//! slot count up, the latter for a target to report as Identify Controller
//! `nn` (each member's vid *is* its NSID).
//!
//! # Fit with the engine
//!
//! The backend struct holds only `Send + Sync` state (cluster address, learned
//! geometry, and the mutable `data_vdi_id[]` map as an atomic array). The TCP
//! connections are `!Send` (their io_uring ops bind to `Reactor::current()`) so
//! they live in a `thread_local` pool, lazily dialed on each queue thread with
//! the reactor's [`ops::connect`]. Reads/writes issue raw io_uring send/recv on
//! a pooled connection; the request/response headers live in the awaiting
//! slot-task frame, the same cancellation envelope as `FileBackend`'s vectored
//! IO (memory outlives the op until queue-teardown drain).
//!
//! # VDI locking (and who serves a volume)
//!
//! Opening a writable VDI takes the cluster's VDI lock, and the ACL id the
//! request carries picks the lock's kind. A VDI opened under its ACL takes the
//! *shared* lock: every holder naming the same ACL joins the participant list,
//! so a pair of targets serving one ACL can export the same volume on two
//! paths. A VDI opened outside any ACL takes `LOCK_TYPE_NORMAL` and stands
//! alone. The two shut each other out — a volume a QEMU guest is already
//! running from is refused rather than corrupted, and so is a volume locked
//! under a *different* ACL. Snapshot opens are read-only and never lock;
//! `lock = None` opts out, for setups whose exclusion is arranged elsewhere.
//!
//! The op that takes it is `REGISTER_VDI`, not `LOCK_VDI`: the two are the
//! same name lookup with the same lock as a side effect, but `REGISTER_VDI`
//! records an owner the **client** supplies, where `LOCK_VDI` records the
//! `sheep` gateway that relayed the request. So [`SheepdogBackend::open`]
//! takes the address this target's fabric listens on and registers *that* as
//! the volume's holder (`dog vdi lock lock <vdi> -A <acl> --owner <ip:port>`
//! is the same call), and the participant list `sheep` keeps for the VDI
//! becomes the cluster's own record of **which targets serve this volume, at
//! which address**. [`vdi_holders`] reads it back (`GET_VDI_COPIES`, the `dog
//! vdi lock list` query) so a target can publish its peers as the subsystem's
//! discovery-log entries — one NVMe-oF "port" per holder — and a host
//! connecting to any one of them learns every path.
//!
//! One registration per open; `sheep` refcounts repeats from one owner into a
//! single participant entry rather than listing it twice, and
//! [`SheepdogBackend::release_lock`] takes exactly the one back — at drop, or
//! earlier on the shutdown path where the backend outlives the target it
//! served. A holder that goes away without releasing (a `SIGKILL`) stays in
//! the list until the cluster evicts the node, which is what
//! [`SheepdogBackend::reregister`] exists for on the way back in.
//!
//! Sharing a VDI is only safe for readers and for writers that never race on
//! the same object: this backend caches `data_vdi_id[]` at open and does not
//! implement sheepdog's inode-invalidation notifications, so an object one
//! holder allocates stays a hole in another's map — it reads zeroes there, and
//! allocating it in turn loses one of the two writes. Multipath (one initiator
//! reaching one volume two ways) is the intended case; two independent writers
//! are not.
//!
//! # Writes
//!
//! Writes bypass the object cache (`SD_FLAG_CMD_DIRECT`) so each is durable and
//! [`SheepdogBackend::flush`] is a no-op. Writing an unallocated object
//! allocates it (`CREATE_AND_WRITE_OBJ`) and persists the new `data_vdi_id[]`
//! entry back into the inode; writing an object owned by a parent (snapshot)
//! copies-on-write (`SD_FLAG_CMD_COW`). Overwrites of already-owned objects take
//! the lock-free fast path (a single atomic load).

// This backend does object-offset and array-index arithmetic where u64 byte
// offsets convert to usize slice indices and back: every such value is bounded
// by the data-object size (<= 4 MiB) or the object-map length, and the target
// is 64-bit Linux, so these conversions cannot truncate.
#![allow(clippy::cast_possible_truncation)]

use std::cell::Cell;
use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, TcpStream};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};

use ioutgt_core::buf::AlignedBuf;
use ioutgt_core::{Backend, BackendError, LbaRange};
use ioutgt_uring::ops;

// ---------------------------------------------------------------------------
// Wire constants (include/sheepdog_proto.h)
// ---------------------------------------------------------------------------

const SD_PROTO_VER: u8 = 0x02;
/// `SD_SHEEP_PROTO_VER` — the version byte of the sheep-internal request
/// family (every opcode `>= 0x80`, e.g. `GET_VDI_COPIES`).
const SD_SHEEP_PROTO_VER: u8 = 0x0a;
/// Default `sheep` client port.
pub const SD_LISTEN_PORT: u16 = 7000;

const SD_OP_CREATE_AND_WRITE_OBJ: u8 = 0x01;
const SD_OP_READ_OBJ: u8 = 0x02;
const SD_OP_WRITE_OBJ: u8 = 0x03;
const SD_OP_GET_VDI_INFO: u8 = 0x14;
const SD_OP_READ_VDIS: u8 = 0x15;
const SD_OP_REGISTER_VDI: u8 = 0x19;
const SD_OP_UNREGISTER_VDI: u8 = 0x1A;
const SD_OP_GET_VDI_COPIES: u8 = 0xAB;

/// `LOCK_TYPE_NORMAL` — the ACL id of a VDI belonging to no ACL, and the
/// exclusive (single-holder) VDI lock such an open takes. Any non-zero ACL id
/// is a real ACL object's vid, and locks that VDI *shared* among the holders
/// naming that same ACL.
const SD_ACL_NONE: u32 = 0;

/// `SD_VDI_FLAG_ACL` — the inode `vdi_flags` bit marking a VDI as an ACL
/// object rather than a volume.
const SD_VDI_FLAG_ACL: u32 = 0x0000_0001;

const SD_FLAG_CMD_WRITE: u16 = 0x01;
const SD_FLAG_CMD_COW: u16 = 0x02;
const SD_FLAG_CMD_DIRECT: u16 = 0x08;

const SD_RES_SUCCESS: u32 = 0x00;
const SD_RES_VDI_LOCKED: u32 = 0x07;
const SD_RES_NO_VDI: u32 = 0x08;
const SD_RES_NO_TAG: u32 = 0x0E;
const SD_RES_VDI_NOT_LOCKED: u32 = 0x10;
const SD_RES_NO_SPACE: u32 = 0x15;
const SD_RES_READONLY: u32 = 0x1A;
const SD_RES_VDI_DENIED: u32 = 0x1E;
const SD_RES_BUFFER_SMALL: u32 = 0x88;

const VDI_BIT: u64 = 1 << 63;
const VDI_SPACE_SHIFT: u32 = 32;
const SD_MAX_VDI_LEN: usize = 256;
const SD_MAX_VDI_TAG_LEN: usize = 256;

/// `SD_NR_VDIS` — vids in the cluster-wide VDI bitmap (one bit each).
const SD_NR_VDIS: u32 = 1 << 24;
/// Bytes of that bitmap, the `READ_VDIS` payload size.
const SD_VDI_BITMAP_SIZE: usize = (SD_NR_VDIS / 8) as usize;

/// Fixed request/response header size.
const SD_HDR_SIZE: usize = 48;

/// `offsetof(struct sd_inode, data_vdi_id)` — start of the object map.
const SD_INODE_HEADER_SIZE: u64 = 4664;
/// Bytes of the inode holding its named fields (everything ahead of the
/// `__unused[]` padding): all this backend ever reads outside the object map.
const SD_INODE_META_SIZE: usize = 596;
/// `SD_INODE_DATA_INDEX` — entries in `data_vdi_id[]` (max 4 TiB at 4 MiB).
const SD_INODE_DATA_INDEX: u64 = 1 << 20;

// Inode header field offsets (little-endian).
const INO_OFF_NAME: usize = 0;
const INO_OFF_TAG: usize = SD_MAX_VDI_LEN;
const INO_OFF_SNAP_CTIME: usize = 520;
const INO_OFF_MAX_DATA_ID_NR: usize = 528;
const INO_OFF_VDI_SIZE: usize = 536;
const INO_OFF_NR_COPIES: usize = 554;
const INO_OFF_BLOCK_SIZE_SHIFT: usize = 555;
const INO_OFF_ACL_ID: usize = 572;
const INO_OFF_UUID: usize = 576;
const INO_OFF_VDI_FLAGS: usize = 592;

/// Bytes of the inode's `uuid[16]`.
const SD_UUID_LEN: usize = 16;

// `struct node_id` (include/internal_proto.h): addr[16], port, io_addr[16],
// io_port, pad[4]. A node's address is 16 bytes whatever its family — an IPv4
// address sits in the last 4, the leading 12 zero (sheepdog's own encoding,
// not the IPv6-mapped `::ffff:` form) — and its port is host order, like every
// other integer on this wire.
const SD_NODE_ID_SIZE: usize = 40;
const NID_OFF_ADDR: usize = 0;
const NID_OFF_PORT: usize = 16;

// `struct vdi_state` (include/internal_proto.h), the GET_VDI_COPIES payload:
// one fixed-size record per VDI the cluster knows.
const SD_MAX_COPIES: usize = 31;
const SD_VDI_STATE_SIZE: usize = 1432;
const VS_OFF_VID: usize = 0;
const VS_OFF_LOCK_STATE: usize = 20;
const VS_OFF_NR_PARTICIPANTS: usize = 64;
const VS_OFF_PARTICIPANTS_STATE: usize = 68;
const VS_OFF_PARTICIPANTS: usize = 192;

/// `LOCK_STATE_SHARED` — the lock state of a VDI held by a participant list
/// (the other two being `UNLOCKED` = 1 and `LOCKED` = 2, the exclusive one).
const LOCK_STATE_SHARED: u32 = 3;

/// Records [`acl_holders`] asks for in its first `GET_VDI_COPIES` (the
/// `DEFAULT_VDI_STATE_COUNT` `dog` uses); a cluster with more VDIs answers
/// `SD_RES_BUFFER_SMALL` and the request is retried with twice the buffer.
const SD_VDI_STATE_BATCH: usize = 512;

/// Ceiling on that doubling: 8 Mi records is far past any real cluster
/// (`SD_NR_VDIS` is 16 Mi vids, and the reply would be 11 GiB), so a server
/// that keeps answering `BUFFER_SMALL` is failed rather than followed.
const SD_VDI_STATE_MAX: usize = 8 << 20;

/// NVMe logical block shift (512 B), matching the other backends' static path.
const BLOCK_SHIFT: u8 = 9;

/// Bound on the `UNREGISTER_VDI` round trip at drop time.
const LOCK_RELEASE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Sentinel stored in the object map while a create is in flight. A real vid is
/// 24-bit, so `u32::MAX` can never collide with one (nor with `0` = hole).
const VID_INFLIGHT: u32 = u32::MAX;

/// Inode object id for a vid.
fn vid_to_vdi_oid(vid: u32) -> u64 {
    VDI_BIT | (u64::from(vid) << VDI_SPACE_SHIFT)
}

/// Data object id for object index `idx` of a vid.
fn vid_to_data_oid(vid: u32, idx: u64) -> u64 {
    (u64::from(vid) << VDI_SPACE_SHIFT) | (idx & 0xFFFF_FFFF)
}

/// Encode a 48-byte `obj` request header (little-endian). `offset` is the
/// in-object byte offset; other unused header fields stay zero (compatible with
/// both the upstream and QEMU header layouts, whose bytes coincide here).
#[allow(clippy::too_many_arguments)]
fn encode_obj_req(
    hdr: &mut [u8; SD_HDR_SIZE],
    opcode: u8,
    flags: u16,
    id: u32,
    data_length: u32,
    oid: u64,
    cow_oid: u64,
    copies: u8,
    offset: u64,
) {
    hdr.fill(0);
    hdr[0] = SD_PROTO_VER;
    hdr[1] = opcode;
    hdr[2..4].copy_from_slice(&flags.to_le_bytes());
    // hdr[4..8] epoch = 0 (the gateway fills it).
    hdr[8..12].copy_from_slice(&id.to_le_bytes());
    hdr[12..16].copy_from_slice(&data_length.to_le_bytes());
    hdr[16..24].copy_from_slice(&oid.to_le_bytes());
    hdr[24..32].copy_from_slice(&cow_oid.to_le_bytes());
    hdr[32] = copies;
    hdr[40..48].copy_from_slice(&offset.to_le_bytes());
}

/// Encode a 48-byte `vdi` request header (the VDI lookup family).
/// `base_vdi_id` names the VDI for ops that take one directly rather than by
/// name; `acl` is the ACL id the request runs under, the scope `sheep`
/// resolves the name in (`0` = no ACL, `LOCK_TYPE_NORMAL`).
fn encode_vdi_req(
    hdr: &mut [u8; SD_HDR_SIZE],
    opcode: u8,
    flags: u16,
    data_length: u32,
    snapid: u32,
    base_vdi_id: u32,
    acl: u32,
) {
    hdr.fill(0);
    hdr[0] = SD_PROTO_VER;
    hdr[1] = opcode;
    hdr[2..4].copy_from_slice(&flags.to_le_bytes());
    hdr[12..16].copy_from_slice(&data_length.to_le_bytes());
    // vdi union: base_vdi_id at 24, snapid at 32, acl at 36.
    hdr[24..28].copy_from_slice(&base_vdi_id.to_le_bytes());
    hdr[32..36].copy_from_slice(&snapid.to_le_bytes());
    hdr[36..40].copy_from_slice(&acl.to_le_bytes());
}

/// Encode a 48-byte `vdi_lock` request header — the register/unregister pair,
/// whose distinguishing feature is that the *client* names the lock's owner
/// (`addr`/`port`) instead of the `sheep` gateway naming itself.
///
/// `REGISTER_VDI` looks its VDI up by name, from the payload the caller sends
/// after this header, and leaves `vid` zero; `UNREGISTER_VDI` carries the vid
/// and no payload. `acl` is both the scope the name resolves in and the lock
/// kind, and `index` is a participant slot the server recomputes from the
/// owner anyway.
fn encode_vdi_lock_req(
    hdr: &mut [u8; SD_HDR_SIZE],
    opcode: u8,
    flags: u16,
    data_length: u32,
    vid: u32,
    owner: SocketAddr,
    acl: u32,
) {
    hdr.fill(0);
    hdr[0] = SD_PROTO_VER;
    hdr[1] = opcode;
    hdr[2..4].copy_from_slice(&flags.to_le_bytes());
    hdr[12..16].copy_from_slice(&data_length.to_le_bytes());
    // vdi_lock union: vid at 16, snapid at 20, addr[16] at 24, port at 40,
    // index at 42, acl at 44.
    hdr[16..20].copy_from_slice(&vid.to_le_bytes());
    hdr[24..40].copy_from_slice(&encode_node_addr(owner.ip()));
    hdr[40..42].copy_from_slice(&owner.port().to_le_bytes());
    hdr[44..48].copy_from_slice(&acl.to_le_bytes());
}

/// Encode a 48-byte sheep-internal request header (opcode `>= 0x80`, hence
/// [`SD_SHEEP_PROTO_VER`]) that carries nothing but a reply-buffer size —
/// which is all `GET_VDI_COPIES` takes.
fn encode_sheep_req(hdr: &mut [u8; SD_HDR_SIZE], opcode: u8, data_length: u32) {
    hdr.fill(0);
    hdr[0] = SD_SHEEP_PROTO_VER;
    hdr[1] = opcode;
    hdr[12..16].copy_from_slice(&data_length.to_le_bytes());
}

/// A node id's `addr[16]`: an IPv4 address in the last four bytes with the
/// leading twelve zero, an IPv6 address verbatim (`str_to_addr`).
fn encode_node_addr(ip: std::net::IpAddr) -> [u8; 16] {
    let mut addr = [0u8; 16];
    match ip {
        std::net::IpAddr::V4(v4) => addr[12..].copy_from_slice(&v4.octets()),
        std::net::IpAddr::V6(v6) => addr.copy_from_slice(&v6.octets()),
    }
    addr
}

/// Decode a node id's `addr[16]`, undoing [`encode_node_addr`]: the IPv4 form
/// is "the leading twelve bytes are zero and the thirteenth is not", the same
/// test `addr_to_str` applies before printing a node.
fn decode_node_addr(addr: &[u8; 16]) -> std::net::IpAddr {
    if addr[12] != 0 && addr[..12].iter().all(|&b| b == 0) {
        let v4: [u8; 4] = addr[12..].try_into().expect("4 bytes");
        std::net::IpAddr::V4(v4.into())
    } else {
        std::net::IpAddr::V6((*addr).into())
    }
}

/// The `result` (`SD_RES_*`) field of a response header.
fn resp_result(hdr: &[u8; SD_HDR_SIZE]) -> u32 {
    u32::from_le_bytes(hdr[16..20].try_into().expect("4 bytes"))
}

/// The `data_length` field of a response header.
fn resp_data_length(hdr: &[u8; SD_HDR_SIZE]) -> u32 {
    u32::from_le_bytes(hdr[12..16].try_into().expect("4 bytes"))
}

/// Map an `SD_RES_*` result to a backend error.
fn sd_res_to_backend(res: u32) -> BackendError {
    match res {
        SD_RES_NO_SPACE => BackendError::NoSpace,
        SD_RES_READONLY => BackendError::Unsupported,
        _ => BackendError::Io(libc::EIO),
    }
}

fn io_to_backend(err: &io::Error) -> BackendError {
    BackendError::Io(err.raw_os_error().unwrap_or(libc::EIO))
}

// ---------------------------------------------------------------------------
// Per-thread connection pool (data path)
// ---------------------------------------------------------------------------

thread_local! {
    /// Idle connections keyed by cluster address, reused across requests on
    /// this queue thread. Connections are `!Send`, so they never leave the
    /// thread that dialed them.
    static POOL: std::cell::RefCell<HashMap<SocketAddr, Vec<OwnedFd>>> =
        std::cell::RefCell::new(HashMap::new());
    /// Per-thread request-id counter (echoed by the server; informational,
    /// since only one request is ever in flight per connection).
    static NEXT_ID: Cell<u32> = const { Cell::new(0) };
}

fn next_id() -> u32 {
    NEXT_ID.with(|c| {
        let id = c.get().wrapping_add(1);
        c.set(id);
        id
    })
}

/// Create a non-blocking-free stream socket suitable for `IORING_OP_CONNECT`,
/// with `TCP_NODELAY` set.
fn new_stream_socket(addr: &SocketAddr) -> io::Result<OwnedFd> {
    let domain = match addr {
        SocketAddr::V4(_) => libc::AF_INET,
        SocketAddr::V6(_) => libc::AF_INET6,
    };
    // SAFETY: plain socket(2) with constant args; the returned fd is owned.
    let fd = unsafe { libc::socket(domain, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: fresh fd, exclusively owned.
    let owned = unsafe { OwnedFd::from_raw_fd(fd) };
    let one: libc::c_int = 1;
    // SAFETY: valid fd; `one` outlives the call; length matches its type.
    unsafe {
        libc::setsockopt(
            owned.as_raw_fd(),
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            std::ptr::from_ref(&one).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
    Ok(owned)
}

/// Take an idle pooled connection for `addr`, or dial a fresh one.
async fn get_conn(addr: SocketAddr) -> io::Result<OwnedFd> {
    if let Some(fd) = POOL.with(|p| p.borrow_mut().get_mut(&addr).and_then(Vec::pop)) {
        return Ok(fd);
    }
    let fd = new_stream_socket(&addr)?;
    ops::connect(fd.as_raw_fd(), &addr)?.await?;
    Ok(fd)
}

/// Return a healthy connection to the pool for reuse.
fn put_conn(addr: SocketAddr, fd: OwnedFd) {
    POOL.with(|p| p.borrow_mut().entry(addr).or_default().push(fd));
}

/// Send all of `ptr..ptr+len` on `fd`, resuming across short sends.
///
/// # Safety
/// `ptr..ptr+len` must stay valid (reads) until the returned future completes
/// or the reactor drains — the raw-op contract of [`ops::send_raw`].
async unsafe fn send_all(fd: RawFd, ptr: *const u8, len: usize) -> io::Result<()> {
    let mut off = 0usize;
    while off < len {
        let want = u32::try_from(len - off).unwrap_or(u32::MAX);
        // SAFETY: `ptr.add(off)` stays within the caller's buffer; the buffer
        // is kept valid per this function's safety contract.
        let n = unsafe { ops::send_raw(fd, ptr.add(off), want) }?.await?;
        if n == 0 {
            return Err(io::Error::from(io::ErrorKind::WriteZero));
        }
        off += n as usize;
    }
    Ok(())
}

/// Receive up to `len` bytes into `ptr`, resuming across short receives.
/// Returns the number of bytes read; fewer than `len` only on EOF.
///
/// # Safety
/// `ptr..ptr+len` must stay valid and unaliased for writes until the returned
/// future completes or the reactor drains — the contract of [`ops::recv_raw`].
async unsafe fn recv_all(fd: RawFd, ptr: *mut u8, len: usize) -> io::Result<usize> {
    let mut off = 0usize;
    while off < len {
        let want = u32::try_from(len - off).unwrap_or(u32::MAX);
        // SAFETY: `ptr.add(off)` stays within the caller's buffer, kept valid
        // per this function's safety contract.
        let n = unsafe { ops::recv_raw(fd, ptr.add(off), want) }?.await? as usize;
        if n == 0 {
            break; // EOF
        }
        off += n;
    }
    Ok(off)
}

// ---------------------------------------------------------------------------
// Backend
// ---------------------------------------------------------------------------

/// A namespace backed by a Sheepdog VDI. See the module docs.
pub struct SheepdogBackend {
    addr: SocketAddr,
    /// The VDI's name, kept for [`SheepdogBackend::reregister`] — the lock
    /// ops resolve a volume by name, never by vid.
    name: String,
    /// The (writable head) VDI id.
    vid: u32,
    /// The ACL this VDI is served under ([`SD_ACL_NONE`] for a VDI in no
    /// ACL): the scope its name was looked up in and its lock taken under,
    /// so `UNREGISTER_VDI` can name the same one back.
    acl: u32,
    /// The address this target registered as the VDI's holder — its own
    /// fabric address, which is what the cluster hands other targets as a
    /// path to this volume. `None` when the open took no lock.
    owner: Option<SocketAddr>,
    /// The inode's `uuid[16]`, or `None` when the cluster never wrote one.
    uuid: Option<[u8; SD_UUID_LEN]>,
    /// Data-object size in bytes (`1 << inode.block_size_shift`).
    object_size: u32,
    /// Replication factor to request on object writes.
    nr_copies: u8,
    nr_blocks: u64,
    /// True for a snapshot (writes rejected).
    read_only: bool,
    /// `data_vdi_id[]`: object index → owning vid (`0` hole, [`VID_INFLIGHT`]
    /// during a create). Lock-free reads; only first-touch writes contend.
    data_map: Box<[AtomicU32]>,
    /// The connection this VDI's cluster lock was taken on, parked here for
    /// the backend's lifetime and used once more to release the lock on drop
    /// or at [`SheepdogBackend::release_lock`]. The registration itself lives
    /// in the cluster's VDI state rather than on this connection; keeping it
    /// is what makes the release one warm round trip, and its presence is
    /// this backend's "still registered" flag. `None` when locking is off,
    /// the VDI is a read-only snapshot, or the lock has already gone back.
    /// Control-plane state only (open, shutdown, drop) — never the IO path,
    /// hence a plain `Mutex`.
    lock_conn: Mutex<Option<TcpStream>>,
}

impl SheepdogBackend {
    /// Look up `vdi` (optionally at snapshot `tag`) on the cluster at `addr`,
    /// read its inode, and build the backend. Performs a small synchronous
    /// handshake (blocking `TcpStream`) once at startup — off the io_uring path.
    ///
    /// `acl` names the ACL object the VDI belongs to; every lookup and lock
    /// runs under it, so it must match the VDI's inode `acl_id` or the cluster
    /// will not admit the name at all ([`io::ErrorKind::PermissionDenied`]).
    /// `None` addresses a VDI in no ACL.
    ///
    /// `lock` is the address this target's fabric serves on, and asks for the
    /// cluster's VDI lock in one move: the VDI is registered as held by that
    /// address until this backend releases it — shared with the other holders
    /// naming the same ACL, exclusive when there is no ACL — and the cluster
    /// hands the address to anyone asking who serves the volume
    /// ([`vdi_holders`]). An unspecified IP (the target bound the wildcard) is
    /// resolved to the local end of a route to the cluster, since `0.0.0.0` is
    /// nothing a host can connect to. A VDI already locked incompatibly — by a
    /// QEMU guest, or under a different ACL — fails the open with
    /// [`io::ErrorKind::ResourceBusy`]. `None` takes no lock, and so does a
    /// snapshot (`tag`), which is read-only and can keep nobody out.
    pub fn open(
        addr: SocketAddr,
        vdi: &str,
        tag: Option<&str>,
        acl: Option<&str>,
        lock: Option<SocketAddr>,
    ) -> io::Result<SheepdogBackend> {
        if vdi.is_empty() || vdi.len() >= SD_MAX_VDI_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid VDI name",
            ));
        }
        let mut stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true).ok();

        let acl = match acl {
            Some(name) => sync_lookup_acl(&mut stream, name)?,
            None => SD_ACL_NONE,
        };
        let vid = sync_lookup_vdi(&mut stream, vdi, tag, acl)?;
        let mut backend = Self::with_inode(addr, vdi, vid, acl, &mut stream)?;

        // A frozen snapshot serves nobody's writes, so it needn't keep anyone
        // out — and the cluster would not record a useful holder for it.
        if let Some(fabric) = lock.filter(|_| !backend.read_only) {
            let owner = advertised_addr(fabric, addr)?;
            sync_register_vdi(&mut stream, vdi, owner, acl)?;
            backend.owner = Some(owner);
            backend.lock_conn = Mutex::new(Some(stream));
        }
        Ok(backend)
    }

    /// Read `vid`'s inode over `stream` and assemble the (unregistered)
    /// backend.
    fn with_inode(
        addr: SocketAddr,
        name: &str,
        vid: u32,
        acl: u32,
        stream: &mut TcpStream,
    ) -> io::Result<SheepdogBackend> {
        // Inode metadata, then the data_vdi_id[] slice sized to the volume.
        let inode_oid = vid_to_vdi_oid(vid);
        let mut header = vec![0u8; SD_INODE_META_SIZE];
        sync_read_obj(stream, inode_oid, 0, &mut header)?;

        let snap_ctime = read_u64(&header, INO_OFF_SNAP_CTIME);
        let vdi_size = read_u64(&header, INO_OFF_VDI_SIZE);
        let nr_copies = header[INO_OFF_NR_COPIES].max(1);
        let block_size_shift = header[INO_OFF_BLOCK_SIZE_SHIFT];
        if !(9..=31).contains(&block_size_shift) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("inode block_size_shift {block_size_shift} out of range"),
            ));
        }
        if vdi_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "VDI has zero size",
            ));
        }
        let object_size = 1u32 << block_size_shift;
        let nr_objects = vdi_size
            .div_ceil(u64::from(object_size))
            .min(SD_INODE_DATA_INDEX);

        let mut map_bytes = vec![0u8; nr_objects as usize * 4];
        if !map_bytes.is_empty() {
            sync_read_obj(stream, inode_oid, SD_INODE_HEADER_SIZE, &mut map_bytes)?;
        }
        let data_map: Box<[AtomicU32]> = (0..nr_objects as usize)
            .map(|i| AtomicU32::new(read_u32(&map_bytes, i * 4)))
            .collect();

        Ok(SheepdogBackend {
            addr,
            name: name.to_owned(),
            vid,
            acl,
            owner: None,
            uuid: read_uuid(&header),
            object_size,
            nr_copies,
            nr_blocks: vdi_size >> BLOCK_SHIFT,
            read_only: snap_ctime != 0,
            data_map,
            lock_conn: Mutex::new(None),
        })
    }

    /// The VDI's cluster-assigned UUID (inode `uuid[16]`), for a target to
    /// report as this namespace's NVMe UUID. `None` when the inode carries
    /// none — an all-zero field, as written by a `sheep` predating it.
    pub fn uuid(&self) -> Option<[u8; SD_UUID_LEN]> {
        self.uuid
    }

    /// The cluster this VDI lives on.
    pub fn cluster(&self) -> SocketAddr {
        self.addr
    }

    /// The VDI id: the volume's cluster-wide identity, and the handle
    /// [`vdi_holders`] takes to ask who else serves it.
    pub fn vid(&self) -> u32 {
        self.vid
    }

    /// The address this backend registered as the volume's holder, or `None`
    /// if it took no lock. It is one of the addresses [`vdi_holders`] reports
    /// — this target's own path to the namespace.
    pub fn owner(&self) -> Option<SocketAddr> {
        self.owner
    }

    /// Hand the VDI lock back now instead of at drop, for a target shutting
    /// down while its backends are still shared: the queue threads hold
    /// `Arc`s to this namespace, so nothing here is dropped before the
    /// process exits and only an explicit release keeps the next opener —
    /// a restarted target, a QEMU guest — from finding the VDI locked, and
    /// keeps this target out of the other targets' discovery logs.
    ///
    /// Idempotent and cheap when there is no lock to give back (`?nolock`,
    /// a snapshot, an already-released backend): a later drop finds the
    /// connection gone and does nothing.
    pub fn release_lock(&self) {
        // A poisoned lock still holds a usable connection: a shutdown path
        // has nowhere better to go than to try the release anyway.
        let conn = self
            .lock_conn
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let (Some(mut stream), Some(owner)) = (conn, self.owner) {
            let _ = stream.set_write_timeout(Some(LOCK_RELEASE_TIMEOUT));
            let _ = stream.set_read_timeout(Some(LOCK_RELEASE_TIMEOUT));
            release_registration(&mut stream, self.vid, owner, self.acl);
        }
    }

    /// Take this VDI's registration again after the cluster lost it — a
    /// `sheep` restart, or this node being evicted from the cluster's view
    /// and rejoining. Without it the target keeps serving a volume the
    /// cluster no longer lists it as a holder of, so its peers stop
    /// advertising the path.
    ///
    /// Call it only when [`vdi_holders`] no longer reports
    /// [`SheepdogBackend::owner`]: a registration on top of a live one is a
    /// second one, which the single release will not take back. A no-op for a
    /// backend that took no lock, and for one that has already handed it back
    /// ([`SheepdogBackend::release_lock`]) — a shutdown must not be undone by
    /// a refresh arriving behind it.
    ///
    /// The name must still resolve to the vid this backend serves. A name
    /// that now points somewhere else is a volume deleted and recreated under
    /// this target's feet — registering on it would claim a path to storage
    /// this namespace is not reading, so it is refused (the namespace itself
    /// is stale and wants a restart).
    pub fn reregister(&self) -> io::Result<()> {
        // Held across the round trip, so a release cannot slip in behind the
        // registration this is about to take.
        let mut conn = self
            .lock_conn
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (Some(owner), true) = (self.owner, conn.is_some()) else {
            return Ok(());
        };
        let mut stream = TcpStream::connect(self.addr)?;
        stream.set_nodelay(true).ok();
        let vid = sync_lookup_vdi(&mut stream, &self.name, None, self.acl)?;
        if vid != self.vid {
            return Err(io::Error::new(
                io::ErrorKind::StaleNetworkFileHandle,
                format!(
                    "VDI '{}' is now {vid:x}, not the {:x} this namespace serves",
                    self.name, self.vid
                ),
            ));
        }
        sync_register_vdi(&mut stream, &self.name, owner, self.acl)?;
        // The connection the open left behind is dead if the cluster restarted
        // under us; the release goes over this one instead.
        *conn = Some(stream);
        Ok(())
    }

    /// Issue one object request on a pooled connection, releasing the
    /// connection back to the pool on success and closing it on error.
    /// `write` is the payload to send (writes); `read` is the destination for
    /// the response payload (reads).
    #[allow(clippy::too_many_arguments)]
    async fn obj_request(
        &self,
        opcode: u8,
        flags: u16,
        oid: u64,
        cow_oid: u64,
        offset: u64,
        write: Option<&[u8]>,
        read: Option<&mut [u8]>,
    ) -> Result<(), BackendError> {
        let fd = get_conn(self.addr).await.map_err(|e| io_to_backend(&e))?;
        match self
            .obj_request_on(
                fd.as_raw_fd(),
                opcode,
                flags,
                oid,
                cow_oid,
                offset,
                write,
                read,
            )
            .await
        {
            Ok(()) => {
                put_conn(self.addr, fd);
                Ok(())
            }
            // Drop (close) a connection whose protocol state is now uncertain.
            Err(e) => Err(e),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn obj_request_on(
        &self,
        fd: RawFd,
        opcode: u8,
        flags: u16,
        oid: u64,
        cow_oid: u64,
        offset: u64,
        write: Option<&[u8]>,
        read: Option<&mut [u8]>,
    ) -> Result<(), BackendError> {
        let data_length = match (write, &read) {
            (Some(w), _) => w.len(),
            (None, Some(r)) => r.len(),
            (None, None) => 0,
        };
        let data_length = u32::try_from(data_length).map_err(|_| BackendError::Io(libc::EINVAL))?;

        let mut hdr = [0u8; SD_HDR_SIZE];
        encode_obj_req(
            &mut hdr,
            opcode,
            flags,
            next_id(),
            data_length,
            oid,
            cow_oid,
            self.nr_copies,
            offset,
        );

        // Request: header, then payload for writes. `hdr` and the payload live
        // in this awaiting frame (the slot task's), valid until the ops'
        // terminal CQEs — the FileBackend raw-op envelope.
        // SAFETY: `hdr` is a live local held across the await.
        unsafe { send_all(fd, hdr.as_ptr(), SD_HDR_SIZE) }
            .await
            .map_err(|e| io_to_backend(&e))?;
        if let Some(w) = write {
            // SAFETY: `w` is the caller's buffer, valid across the await.
            unsafe { send_all(fd, w.as_ptr(), w.len()) }
                .await
                .map_err(|e| io_to_backend(&e))?;
        }

        // Response header.
        let mut resp = [0u8; SD_HDR_SIZE];
        // SAFETY: `resp` is a live local held across the await.
        let got = unsafe { recv_all(fd, resp.as_mut_ptr(), SD_HDR_SIZE) }
            .await
            .map_err(|e| io_to_backend(&e))?;
        if got < SD_HDR_SIZE {
            return Err(BackendError::Io(libc::EIO));
        }
        let result = resp_result(&resp);
        let resp_len = resp_data_length(&resp) as usize;

        // Response payload for reads (server may trim trailing zeroes).
        if let Some(dst) = read {
            let n = resp_len.min(dst.len());
            let got = if n > 0 {
                // SAFETY: `dst` is the caller's buffer, valid across the await.
                unsafe { recv_all(fd, dst.as_mut_ptr(), n) }
                    .await
                    .map_err(|e| io_to_backend(&e))?
            } else {
                0
            };
            if got < dst.len() {
                dst[got..].fill(0);
            }
        }

        if result != SD_RES_SUCCESS {
            return Err(sd_res_to_backend(result));
        }
        Ok(())
    }

    /// Read one object slice `[in_obj, in_obj+dst.len())` into `dst`,
    /// zero-filling unallocated (hole) objects.
    async fn read_object(&self, idx: u64, in_obj: u64, dst: &mut [u8]) -> Result<(), BackendError> {
        let vid = self.map_load(idx)?;
        if vid == 0 || vid == VID_INFLIGHT {
            dst.fill(0);
            return Ok(());
        }
        self.obj_request(
            SD_OP_READ_OBJ,
            0,
            vid_to_data_oid(vid, idx),
            0,
            in_obj,
            None,
            Some(dst),
        )
        .await
    }

    /// Write one object slice `[in_obj, in_obj+data.len())`, allocating or
    /// copying-on-write the object as needed and publishing the map entry.
    async fn write_object(&self, idx: u64, in_obj: u64, data: &[u8]) -> Result<(), BackendError> {
        loop {
            let cur = self.map_load(idx)?;
            if cur == self.vid {
                // Fast path: overwrite an object we already own.
                return self
                    .obj_request(
                        SD_OP_WRITE_OBJ,
                        SD_FLAG_CMD_WRITE | SD_FLAG_CMD_DIRECT,
                        vid_to_data_oid(self.vid, idx),
                        0,
                        in_obj,
                        Some(data),
                        None,
                    )
                    .await;
            }
            if cur == VID_INFLIGHT {
                // Another task on this thread is creating it; briefly yield to
                // the reactor (parks on a ring timer) so its create completes.
                if let Ok(s) = ops::sleep(std::time::Duration::from_micros(50)) {
                    let _ = s.await;
                }
                continue;
            }
            // `cur == 0` (hole) or a parent's vid (snapshot → copy-on-write).
            let slot = &self.data_map[idx as usize];
            if slot
                .compare_exchange(cur, VID_INFLIGHT, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue; // lost the claim; re-read and retry
            }
            let cow = (cur != 0).then_some(cur);
            let res = self.create_object(idx, in_obj, data, cow).await;
            match res {
                Ok(()) => {
                    slot.store(self.vid, Ordering::Release);
                    return Ok(());
                }
                Err(e) => {
                    slot.store(cur, Ordering::Release); // restore prior state
                    return Err(e);
                }
            }
        }
    }

    /// Create (and write) a new data object, optionally copying-on-write from a
    /// parent object, then persist the `data_vdi_id[idx]` entry into the inode.
    async fn create_object(
        &self,
        idx: u64,
        in_obj: u64,
        data: &[u8],
        cow: Option<u32>,
    ) -> Result<(), BackendError> {
        let mut flags = SD_FLAG_CMD_WRITE | SD_FLAG_CMD_DIRECT;
        let cow_oid = match cow {
            Some(parent) => {
                flags |= SD_FLAG_CMD_COW;
                vid_to_data_oid(parent, idx)
            }
            None => 0,
        };
        self.obj_request(
            SD_OP_CREATE_AND_WRITE_OBJ,
            flags,
            vid_to_data_oid(self.vid, idx),
            cow_oid,
            in_obj,
            Some(data),
            None,
        )
        .await?;

        // Persist the map entry into the inode object (4-byte LE vid at the
        // slot's offset within data_vdi_id[]).
        let entry = self.vid.to_le_bytes();
        let inode_off = SD_INODE_HEADER_SIZE + idx * 4;
        self.obj_request(
            SD_OP_WRITE_OBJ,
            SD_FLAG_CMD_WRITE | SD_FLAG_CMD_DIRECT,
            vid_to_vdi_oid(self.vid),
            0,
            inode_off,
            Some(&entry),
            None,
        )
        .await
    }

    fn map_load(&self, idx: u64) -> Result<u32, BackendError> {
        self.data_map
            .get(idx as usize)
            .map(|a| a.load(Ordering::Acquire))
            .ok_or(BackendError::OutOfRange)
    }
}

impl Drop for SheepdogBackend {
    /// Hand the VDI lock back so the next opener — a restarted target, a
    /// guest — is not locked out. Only a clean teardown reaches this:
    /// a killed target leaves the lock with the cluster to reclaim.
    fn drop(&mut self) {
        self.release_lock();
    }
}

impl Backend for SheepdogBackend {
    fn block_shift(&self) -> u8 {
        BLOCK_SHIFT
    }

    fn nr_blocks(&self) -> u64 {
        self.nr_blocks
    }

    async fn read(&self, slba: u64, buf: &mut [u8]) -> Result<(), BackendError> {
        self.check_range(slba, (buf.len() as u64) >> BLOCK_SHIFT)?;
        let obj = u64::from(self.object_size);
        let mut off = slba << BLOCK_SHIFT;
        let mut done = 0usize;
        while done < buf.len() {
            let idx = off / obj;
            let in_obj = off % obj;
            let chunk = (buf.len() - done).min((obj - in_obj) as usize);
            self.read_object(idx, in_obj, &mut buf[done..done + chunk])
                .await?;
            off += chunk as u64;
            done += chunk;
        }
        Ok(())
    }

    async fn write(&self, slba: u64, buf: &[u8]) -> Result<(), BackendError> {
        if self.read_only {
            return Err(BackendError::Unsupported);
        }
        self.check_range(slba, (buf.len() as u64) >> BLOCK_SHIFT)?;
        let obj = u64::from(self.object_size);
        let mut off = slba << BLOCK_SHIFT;
        let mut done = 0usize;
        while done < buf.len() {
            let idx = off / obj;
            let in_obj = off % obj;
            let chunk = (buf.len() - done).min((obj - in_obj) as usize);
            self.write_object(idx, in_obj, &buf[done..done + chunk])
                .await?;
            off += chunk as u64;
            done += chunk;
        }
        Ok(())
    }

    async fn flush(&self) -> Result<(), BackendError> {
        // Writes use SD_FLAG_CMD_DIRECT (object cache bypassed) — already durable.
        Ok(())
    }

    async fn write_zeroes(&self, range: LbaRange) -> Result<(), BackendError> {
        if self.read_only {
            return Err(BackendError::Unsupported);
        }
        self.check_range(range.slba, u64::from(range.nlb))?;
        let total = u64::from(range.nlb) << BLOCK_SHIFT;
        let chunk_len = total.min(u64::from(self.object_size)) as usize;
        let zeros = AlignedBuf::zeroed(chunk_len.max(1));
        let mut slba = range.slba;
        let mut remaining = total;
        while remaining > 0 {
            let n = remaining.min(zeros.len() as u64) as usize;
            self.write(slba, &zeros[..n]).await?;
            slba += (n as u64) >> BLOCK_SHIFT;
            remaining -= n as u64;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Cluster VDI / ACL enumeration
// ---------------------------------------------------------------------------

/// One VDI found on a cluster by [`list_vdis`] or [`list_acls`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VdiInfo {
    /// VDI name — the `dog vdi list` NAME column, unique among a cluster's
    /// writable heads *within one ACL*.
    pub name: String,
    /// Snapshot tag; empty for the writable head.
    pub tag: String,
    /// The 24-bit VDI id, stable for the life of the VDI.
    pub vid: u32,
    /// Volume size in bytes.
    pub size: u64,
    /// True for a snapshot: a frozen VDI, servable only read-only.
    pub snapshot: bool,
    /// The VDI's cluster-assigned UUID (inode `uuid[16]`) — the identity to
    /// export as the namespace UUID. `None` for an inode carrying none.
    pub uuid: Option<[u8; SD_UUID_LEN]>,
    /// The vid of the ACL object this VDI belongs to (inode `acl_id`), `0`
    /// for a VDI in no ACL. Every lookup of this VDI's name must carry it.
    pub acl: u32,
}

/// One ACL object found on a cluster by [`list_acls`]: a VDI carrying
/// `SD_VDI_FLAG_ACL`, together with the volumes its `data_vdi_id[]` lists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclInfo {
    /// ACL object name — the `dog acl list` NAME column.
    pub name: String,
    /// The ACL object's own 24-bit vid: the `acl_id` its members carry and
    /// the value every lookup and lock of a member passes.
    pub vid: u32,
    /// Slots of the ACL inode's member array in use (`max_data_id_nr`), holes
    /// included: the cluster's own count of the volumes in this ACL, for a
    /// target to report as Identify Controller `nn`. Never below `vdis.len()`.
    pub max_data_id_nr: u32,
    /// The ACL's member VDIs, sorted by (name, tag), snapshots included.
    pub vdis: Vec<VdiInfo>,
}

/// One live entry of the cluster's VDI table, as [`scan_vdi_table`] reads it.
struct TableEntry {
    info: VdiInfo,
    /// The entry is an ACL object (`SD_VDI_FLAG_ACL`), not a volume.
    is_acl: bool,
    /// Slots of `data_vdi_id[]` in use (`max_data_id_nr`): the object-map
    /// extent of a volume, the member count of an ACL object.
    max_data_id_nr: u32,
}

/// Enumerate every volume on the cluster at `addr`, sorted by (name, tag).
///
/// ACL objects are VDIs too, but not volumes: they are left out here and
/// enumerated by [`list_acls`] instead. Reported volumes carry the ACL they
/// belong to in [`VdiInfo::acl`], which [`SheepdogBackend::open`] needs to
/// reach them.
pub fn list_vdis(addr: SocketAddr) -> io::Result<Vec<VdiInfo>> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_nodelay(true).ok();

    let mut vdis: Vec<VdiInfo> = scan_vdi_table(&mut stream)?
        .into_iter()
        .filter(|entry| !entry.is_acl)
        .map(|entry| entry.info)
        .collect();
    vdis.sort_by(|a, b| (&a.name, &a.tag).cmp(&(&b.name, &b.tag)));
    Ok(vdis)
}

/// Enumerate the cluster's ACL objects, each with its member VDIs, sorted by
/// ACL name.
///
/// Membership comes from the ACL object's own inode: the vids in
/// `data_vdi_id[0..max_data_id_nr]`, which is the list `dog acl add`/`remove`
/// maintain and `dog acl info` prints. Zero entries are holes left by a
/// removal, not the end of the list. Each vid is then resolved against the
/// VDI table this scan already read, for the member's name, size and identity.
///
/// A listed vid that is not a volume on the cluster, or whose inode names some
/// other ACL, is dropped with a warning — `dog acl add` writes the array entry
/// before the member's `acl_id`, so a half-completed add leaves exactly that,
/// and the cluster would refuse to resolve the name under this ACL anyway. A
/// volume whose `acl_id` names this ACL but that the array does not list is
/// dropped the same way: the ACL's list is what `dog` shows and what an
/// administrator edits.
///
/// An empty ACL is still reported: it is a subsystem with no namespaces yet,
/// not an error. Volumes belonging to no ACL appear in no entry; use
/// [`list_vdis`] for those.
pub fn list_acls(addr: SocketAddr) -> io::Result<Vec<AclInfo>> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_nodelay(true).ok();
    let entries = scan_vdi_table(&mut stream)?;

    let mut acls = Vec::new();
    for entry in entries.iter().filter(|entry| entry.is_acl) {
        let members = sync_read_acl_members(&mut stream, entry.info.vid, entry.max_data_id_nr)?;
        let mut vdis: Vec<VdiInfo> = Vec::new();
        for vid in members.iter().copied().filter(|&vid| vid != 0) {
            match entries.iter().find(|e| e.info.vid == vid && !e.is_acl) {
                Some(member) if member.info.acl == entry.info.vid => {
                    vdis.push(member.info.clone());
                }
                // The member's own inode disagrees (or is not there at all):
                // the cluster will refuse every lookup of it under this ACL,
                // so there is nothing to export — say so rather than
                // silently dropping it.
                Some(member) => tracing::warn!(
                    acl = %entry.info.name,
                    vdi = %member.info.name,
                    vid = format_args!("{vid:x}"),
                    acl_id = format_args!("{:x}", member.info.acl),
                    "sheepdog: ACL lists a VDI whose inode names another ACL"
                ),
                None => tracing::warn!(
                    acl = %entry.info.name,
                    vid = format_args!("{vid:x}"),
                    "sheepdog: ACL lists a vid that is not a volume on the cluster"
                ),
            }
        }
        for orphan in entries
            .iter()
            .filter(|e| !e.is_acl && e.info.acl == entry.info.vid && !members.contains(&e.info.vid))
        {
            tracing::warn!(
                acl = %entry.info.name,
                vdi = %orphan.info.name,
                vid = format_args!("{:x}", orphan.info.vid),
                "sheepdog: VDI names an ACL that does not list it"
            );
        }
        vdis.sort_by(|a, b| (&a.name, &a.tag).cmp(&(&b.name, &b.tag)));
        acls.push(AclInfo {
            name: entry.info.name.clone(),
            vid: entry.info.vid,
            max_data_id_nr: entry.max_data_id_nr,
            vdis,
        });
    }
    for entry in entries.iter().filter(|e| {
        !e.is_acl && e.info.acl != SD_ACL_NONE && !acls.iter().any(|acl| acl.vid == e.info.acl)
    }) {
        // The named ACL object is gone (or was never one): the cluster will
        // refuse every lookup of this VDI, so nothing can export it.
        tracing::warn!(
            vdi = %entry.info.name,
            acl = format_args!("{:x}", entry.info.acl),
            "sheepdog: VDI names an ACL that is not on the cluster"
        );
    }
    acls.sort_by(|a, b| (&a.name, a.vid).cmp(&(&b.name, b.vid)));
    Ok(acls)
}

/// Read an ACL object's member list: `data_vdi_id[0..max_data_id_nr]`, which
/// starts at [`SD_INODE_HEADER_SIZE`] in the inode object. Zero entries are
/// holes and stay in the returned vector, so its length is the `nn` an ACL
/// reports. A count past the array's end is clamped rather than trusted.
fn sync_read_acl_members(
    stream: &mut TcpStream,
    vid: u32,
    max_data_id_nr: u32,
) -> io::Result<Vec<u32>> {
    let count = u64::from(max_data_id_nr).min(SD_INODE_DATA_INDEX) as usize;
    if count == 0 {
        return Ok(Vec::new());
    }
    let mut bytes = vec![0u8; count * 4];
    sync_read_obj(
        stream,
        vid_to_vdi_oid(vid),
        SD_INODE_HEADER_SIZE,
        &mut bytes,
    )?;
    Ok((0..count).map(|i| read_u32(&bytes, i * 4)).collect())
}

// ---------------------------------------------------------------------------
// VDI registration and lock holders
// ---------------------------------------------------------------------------

/// One holder of a VDI's shared cluster lock: a target serving that volume, as
/// the cluster records it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VdiHolder {
    /// The address the holder registered itself under. For an ioutgt target
    /// that is the address its fabric listens on, which is exactly what a
    /// discovery-log entry advertises (`traddr` + `trsvcid`).
    pub addr: SocketAddr,
    /// The holder's slot in the VDI's participant list: unique among the
    /// holders of *this* volume, and stable while it stays registered
    /// (`sheep` compacts the list when one leaves).
    pub index: u16,
    /// Registrations this holder currently has. One per open, so normally
    /// one: `sheep` refcounts repeats of one owner rather than listing it
    /// twice, and a holder above one has the volume open more than once.
    pub registrations: u32,
}

/// The holders of the shared cluster lock on each of `vids`, in participant
/// order — every target that has registered itself as serving that volume,
/// this one included. Returns one list per requested vid, in the order asked.
///
/// One `GET_VDI_COPIES` covers the lot: the reply is the whole cluster's VDI
/// state table however few records the caller wants out of it, so asking per
/// volume would re-fetch it per volume.
///
/// A vid's list is empty when the volume is unlocked (nobody serves it), held
/// exclusively (a QEMU guest, or a target that opened it outside any ACL), or
/// absent from the table altogether. Blocking, and called from the control
/// plane only: at startup and from the refresh that keeps a subsystem's
/// advertised paths current.
pub fn vdi_holders(cluster: SocketAddr, vids: &[u32]) -> io::Result<Vec<Vec<VdiHolder>>> {
    let mut stream = TcpStream::connect(cluster)?;
    stream.set_nodelay(true).ok();
    let states = sync_get_vdi_states(&mut stream)?;
    Ok(vids
        .iter()
        .map(|&vid| {
            states
                .chunks_exact(SD_VDI_STATE_SIZE)
                .find(|vs| read_u32(vs, VS_OFF_VID) == vid)
                .map(parse_holders)
                .unwrap_or_default()
        })
        .collect())
}

/// The participant list of one `vdi_state` record.
fn parse_holders(vs: &[u8]) -> Vec<VdiHolder> {
    if read_u32(vs, VS_OFF_LOCK_STATE) != LOCK_STATE_SHARED {
        return Vec::new();
    }
    let count = (read_u32(vs, VS_OFF_NR_PARTICIPANTS) as usize).min(SD_MAX_COPIES);
    (0..count)
        .map(|i| {
            let nid = VS_OFF_PARTICIPANTS + i * SD_NODE_ID_SIZE;
            let addr: [u8; 16] = vs[nid + NID_OFF_ADDR..nid + NID_OFF_ADDR + 16]
                .try_into()
                .expect("16 bytes");
            let port = u16::from_le_bytes(
                vs[nid + NID_OFF_PORT..nid + NID_OFF_PORT + 2]
                    .try_into()
                    .expect("2 bytes"),
            );
            // participants_state packs the shared-lock state in the low byte
            // and the owner's registration count above it; a `sheep` predating
            // the count reports zero, which is the one registration it means.
            let state = read_u32(vs, VS_OFF_PARTICIPANTS_STATE + i * 4);
            VdiHolder {
                addr: SocketAddr::new(decode_node_addr(&addr), port),
                index: u16::try_from(i).expect("at most SD_MAX_COPIES"),
                registrations: (state >> 8).max(1),
            }
        })
        .collect()
}

/// Take the cluster lock on the VDI named `vdi` within ACL `acl`, registering
/// `owner` as the holder: the lookup-by-name `LOCK_VDI` does, with the owner
/// supplied by us rather than filled in with the gateway's own node id.
///
/// `acl` is both the scope the name resolves in and the lock's kind — a real
/// ACL id joins the volume's participant list, [`SD_ACL_NONE`] claims a volume
/// that is in no ACL exclusively.
fn sync_register_vdi(
    stream: &mut TcpStream,
    vdi: &str,
    owner: SocketAddr,
    acl: u32,
) -> io::Result<()> {
    use std::io::{Read, Write};

    if vdi.is_empty() || vdi.len() >= SD_MAX_VDI_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid VDI name",
        ));
    }
    let mut payload = vec![0u8; SD_MAX_VDI_LEN + SD_MAX_VDI_TAG_LEN];
    payload[..vdi.len()].copy_from_slice(vdi.as_bytes());

    let mut hdr = [0u8; SD_HDR_SIZE];
    encode_vdi_lock_req(
        &mut hdr,
        SD_OP_REGISTER_VDI,
        SD_FLAG_CMD_WRITE,
        payload.len() as u32,
        0, // looked up by name, like LOCK_VDI
        owner,
        acl,
    );
    stream.write_all(&hdr)?;
    stream.write_all(&payload)?;

    let mut resp = [0u8; SD_HDR_SIZE];
    stream.read_exact(&mut resp)?;
    let resp_len = resp_data_length(&resp) as u64;
    if resp_len > 0 {
        io::copy(&mut stream.take(resp_len), &mut io::sink())?;
    }
    match resp_result(&resp) {
        SD_RES_SUCCESS => Ok(()),
        SD_RES_NO_VDI => Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("no VDI named '{vdi}' under ACL {acl:#x}"),
        )),
        // Held the other way round: a shared holder where we want it to
        // ourselves, or an exclusive one (a QEMU guest) where we want to join.
        SD_RES_VDI_LOCKED => Err(io::Error::new(
            io::ErrorKind::ResourceBusy,
            format!("VDI '{vdi}' is locked incompatibly by another client"),
        )),
        // Named from outside the ACL its inode records, or locked under a
        // different one: the cluster admits the name, but not to us.
        SD_RES_VDI_DENIED => Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("VDI '{vdi}' is not reachable under ACL {acl:#x}"),
        )),
        // `sheep` keeps at most SD_MAX_COPIES participants per VDI.
        SD_RES_NO_SPACE => Err(io::Error::new(
            io::ErrorKind::QuotaExceeded,
            format!("VDI '{vdi}' already has {SD_MAX_COPIES} registered holders"),
        )),
        res => Err(io::Error::other(format!(
            "REGISTER_VDI failed: SD_RES {res:#x}"
        ))),
    }
}

/// One `UNREGISTER_VDI` round trip — the [`sync_register_vdi`] inverse, naming
/// the same owner and ACL back. Reported rather than propagated: every caller
/// is on a teardown path with nowhere to return an error to.
fn release_registration(stream: &mut TcpStream, vid: u32, owner: SocketAddr, acl: u32) {
    use std::io::{Read, Write};

    let mut hdr = [0u8; SD_HDR_SIZE];
    encode_vdi_lock_req(&mut hdr, SD_OP_UNREGISTER_VDI, 0, 0, vid, owner, acl);
    let result = (|| -> io::Result<u32> {
        stream.write_all(&hdr)?;
        let mut resp = [0u8; SD_HDR_SIZE];
        stream.read_exact(&mut resp)?;
        let resp_len = resp_data_length(&resp) as u64;
        if resp_len > 0 {
            io::copy(&mut stream.take(resp_len), &mut io::sink())?;
        }
        Ok(resp_result(&resp))
    })();
    match result {
        // NOT_LOCKED: the cluster dropped this holder on its own (the node
        // left the cluster's view and came back). The postcondition holds.
        Ok(SD_RES_SUCCESS | SD_RES_VDI_NOT_LOCKED) => {}
        Ok(res) => tracing::warn!(
            vid = format_args!("{vid:x}"),
            %owner,
            "sheepdog: UNREGISTER_VDI failed: SD_RES {res:#x}"
        ),
        Err(err) => tracing::warn!(
            vid = format_args!("{vid:x}"),
            %owner,
            %err,
            "sheepdog: VDI registration not released"
        ),
    }
}

/// Fetch the cluster's whole VDI state table (`GET_VDI_COPIES`), retrying with
/// a larger buffer for as long as the server says it is too small — the same
/// grow-and-retry `dog vdi lock list` does, since the reply is sized by the
/// number of VDIs the cluster holds and there is no way to ask for the count
/// first.
///
/// The returned bytes are a whole number of `vdi_state` records; a reply that
/// is not is a protocol violation rather than something to parse half of.
fn sync_get_vdi_states(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    use std::io::Write;

    let mut records = SD_VDI_STATE_BATCH;
    loop {
        let mut buf = vec![0u8; records * SD_VDI_STATE_SIZE];
        let mut hdr = [0u8; SD_HDR_SIZE];
        encode_sheep_req(&mut hdr, SD_OP_GET_VDI_COPIES, buf.len() as u32);
        stream.write_all(&hdr)?;

        let mut resp = [0u8; SD_HDR_SIZE];
        std::io::Read::read_exact(stream, &mut resp)?;
        let len = read_payload(stream, &resp, &mut buf)?;
        match resp_result(&resp) {
            SD_RES_SUCCESS => {
                if len % SD_VDI_STATE_SIZE != 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("GET_VDI_COPIES returned {len} bytes, not whole vdi_state records"),
                    ));
                }
                buf.truncate(len);
                return Ok(buf);
            }
            SD_RES_BUFFER_SMALL if records * 2 <= SD_VDI_STATE_MAX => records *= 2,
            SD_RES_BUFFER_SMALL => {
                return Err(io::Error::other(format!(
                    "GET_VDI_COPIES wants more than {SD_VDI_STATE_MAX} records"
                )));
            }
            res => {
                return Err(io::Error::other(format!(
                    "GET_VDI_COPIES failed: SD_RES {res:#x}"
                )));
            }
        }
    }
}

/// Walk the cluster's VDI bitmap (`READ_VDIS`, one bit per vid) and read each
/// live vid's inode metadata.
///
/// Blocking and off the io_uring path, like [`SheepdogBackend::open`]: a
/// target calls it once at startup to map the cluster onto subsystems and
/// namespaces. Vids that vanish between the bitmap snapshot and the inode
/// read (a concurrent `dog vdi delete`), and unnamed or zero-sized inodes,
/// are skipped rather than failing the whole enumeration.
fn scan_vdi_table(stream: &mut TcpStream) -> io::Result<Vec<TableEntry>> {
    let bitmap = sync_read_vdi_bitmap(stream)?;

    let mut entries = Vec::new();
    let mut inode = vec![0u8; SD_INODE_META_SIZE];
    for (byte, &bits) in bitmap.iter().enumerate() {
        if bits == 0 {
            continue;
        }
        for bit in 0..8u32 {
            let vid = byte as u32 * 8 + bit;
            // vid 0 is the "no VDI" sentinel and never a real volume.
            if bits & (1 << bit) == 0 || vid == 0 {
                continue;
            }
            if sync_try_read_obj(stream, vid_to_vdi_oid(vid), 0, &mut inode)? != SD_RES_SUCCESS {
                continue;
            }
            let name = read_cstr(&inode[INO_OFF_NAME..INO_OFF_NAME + SD_MAX_VDI_LEN]);
            let size = read_u64(&inode, INO_OFF_VDI_SIZE);
            if name.is_empty() || size == 0 {
                continue;
            }
            entries.push(TableEntry {
                info: VdiInfo {
                    name,
                    tag: read_cstr(&inode[INO_OFF_TAG..INO_OFF_TAG + SD_MAX_VDI_TAG_LEN]),
                    vid,
                    size,
                    snapshot: read_u64(&inode, INO_OFF_SNAP_CTIME) != 0,
                    uuid: read_uuid(&inode),
                    acl: read_u32(&inode, INO_OFF_ACL_ID),
                },
                is_acl: read_u32(&inode, INO_OFF_VDI_FLAGS) & SD_VDI_FLAG_ACL != 0,
                max_data_id_nr: read_u32(&inode, INO_OFF_MAX_DATA_ID_NR),
            });
        }
    }
    Ok(entries)
}

// ---------------------------------------------------------------------------
// Synchronous startup helpers (blocking TcpStream, off the io_uring path)
// ---------------------------------------------------------------------------

fn read_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(buf[off..off + 4].try_into().expect("4 bytes"))
}

fn read_u64(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(buf[off..off + 8].try_into().expect("8 bytes"))
}

/// Decode the inode header's `uuid[16]`, `None` for the all-zero value —
/// `uuid_is_null()`, the same "unset" test `dog vdi list` applies before
/// reporting the field.
fn read_uuid(inode: &[u8]) -> Option<[u8; SD_UUID_LEN]> {
    let uuid: [u8; SD_UUID_LEN] = inode[INO_OFF_UUID..INO_OFF_UUID + SD_UUID_LEN]
        .try_into()
        .expect("16 bytes");
    (uuid != [0u8; SD_UUID_LEN]).then_some(uuid)
}

/// Decode a fixed-width, NUL-padded inode string field.
fn read_cstr(field: &[u8]) -> String {
    let end = field.iter().position(|&b| b == 0).unwrap_or(field.len());
    String::from_utf8_lossy(&field[..end]).into_owned()
}

/// Look up an ACL object by name → its vid, checking that it really is one.
///
/// An ACL object belongs to no ACL itself, so it is looked up unscoped; the
/// `SD_VDI_FLAG_ACL` check then keeps an ordinary VDI that happens to share
/// the name from being mistaken for an access-control scope (the same guard
/// `dog acl` applies).
fn sync_lookup_acl(stream: &mut TcpStream, acl: &str) -> io::Result<u32> {
    if acl.is_empty() || acl.len() >= SD_MAX_VDI_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid ACL name",
        ));
    }
    let vid = sync_lookup_vdi(stream, acl, None, SD_ACL_NONE)?;
    let mut meta = vec![0u8; SD_INODE_META_SIZE];
    sync_read_obj(stream, vid_to_vdi_oid(vid), 0, &mut meta)?;
    if read_u32(&meta, INO_OFF_VDI_FLAGS) & SD_VDI_FLAG_ACL == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("'{acl}' is an ordinary VDI, not an ACL object"),
        ));
    }
    Ok(vid)
}

/// Look up a VDI by name (and optional snapshot tag) within ACL `acl` → its
/// vid. `sheep` matches the name only against inodes whose `acl_id` is `acl`,
/// so the scope is part of the question (see the module docs).
fn sync_lookup_vdi(
    stream: &mut TcpStream,
    vdi: &str,
    tag: Option<&str>,
    acl: u32,
) -> io::Result<u32> {
    use std::io::{Read, Write};

    let mut payload = vec![0u8; SD_MAX_VDI_LEN + SD_MAX_VDI_TAG_LEN];
    payload[..vdi.len()].copy_from_slice(vdi.as_bytes());
    if let Some(tag) = tag {
        if tag.len() >= SD_MAX_VDI_TAG_LEN {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "tag too long"));
        }
        payload[SD_MAX_VDI_LEN..SD_MAX_VDI_LEN + tag.len()].copy_from_slice(tag.as_bytes());
    }

    let mut hdr = [0u8; SD_HDR_SIZE];
    encode_vdi_req(
        &mut hdr,
        SD_OP_GET_VDI_INFO,
        SD_FLAG_CMD_WRITE,
        payload.len() as u32,
        0,
        0,
        acl,
    );
    stream.write_all(&hdr)?;
    stream.write_all(&payload)?;

    let mut resp = [0u8; SD_HDR_SIZE];
    stream.read_exact(&mut resp)?;
    // Drain any response payload to keep the stream framed.
    let resp_len = resp_data_length(&resp) as usize;
    if resp_len > 0 {
        io::copy(&mut stream.take(resp_len as u64), &mut io::sink())?;
    }
    match resp_result(&resp) {
        SD_RES_SUCCESS => Ok(read_u32(&resp, 24)), // vdi_id at byte 24
        SD_RES_NO_VDI => Err(io::Error::new(io::ErrorKind::NotFound, "no such VDI")),
        SD_RES_NO_TAG => Err(io::Error::new(
            io::ErrorKind::NotFound,
            "no such snapshot tag",
        )),
        SD_RES_VDI_LOCKED => Err(io::Error::new(
            io::ErrorKind::ResourceBusy,
            "VDI is locked incompatibly by another client",
        )),
        // Locked under some other ACL, or named from outside the ACL it
        // belongs to: the cluster admits the name, but not to us.
        SD_RES_VDI_DENIED => Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("VDI is not reachable under ACL {acl:#x}"),
        )),
        res => Err(io::Error::other(format!(
            "VDI lookup failed: SD_RES {res:#x}"
        ))),
    }
}

/// The address to register a VDI lock under: `fabric`, the endpoint this
/// target serves on — unless it bound the wildcard, in which case the local
/// address of a route to the cluster, which is the interface this target and
/// its peers share and the best available guess at one a host can reach it on.
///
/// The UDP socket sends nothing; connecting it only asks the kernel to pick
/// the source address it would use.
fn advertised_addr(fabric: SocketAddr, cluster: SocketAddr) -> io::Result<SocketAddr> {
    if !fabric.ip().is_unspecified() {
        return Ok(fabric);
    }
    let any = if cluster.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let probe = std::net::UdpSocket::bind(any)?;
    probe.connect(cluster)?;
    Ok(SocketAddr::new(probe.local_addr()?.ip(), fabric.port()))
}

/// Synchronously read the cluster's VDI bitmap (one bit per vid, LSB-first
/// within each byte — the kernel `test_bit` layout the `sheep` gateway uses).
fn sync_read_vdi_bitmap(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    use std::io::{Read, Write};

    let mut hdr = [0u8; SD_HDR_SIZE];
    encode_vdi_req(
        &mut hdr,
        SD_OP_READ_VDIS,
        0,
        SD_VDI_BITMAP_SIZE as u32,
        0,
        0,
        0,
    );
    stream.write_all(&hdr)?;

    let mut resp = [0u8; SD_HDR_SIZE];
    stream.read_exact(&mut resp)?;
    let result = resp_result(&resp);
    let mut bitmap = vec![0u8; SD_VDI_BITMAP_SIZE];
    let resp_len = read_payload(stream, &resp, &mut bitmap)?;
    bitmap[resp_len..].fill(0);
    if result != SD_RES_SUCCESS {
        return Err(io::Error::other(format!(
            "READ_VDIS failed: SD_RES {result:#x}"
        )));
    }
    Ok(bitmap)
}

/// Synchronously read `dst.len()` bytes of object `oid` at `offset`,
/// zero-filling any trailing bytes the server trims. A non-success
/// `SD_RES_*` is an error; see [`sync_try_read_obj`] to inspect it.
fn sync_read_obj(stream: &mut TcpStream, oid: u64, offset: u64, dst: &mut [u8]) -> io::Result<()> {
    match sync_try_read_obj(stream, oid, offset, dst)? {
        SD_RES_SUCCESS => Ok(()),
        result => Err(io::Error::other(format!(
            "READ_OBJ failed: SD_RES {result:#x}"
        ))),
    }
}

/// [`sync_read_obj`] returning the raw `SD_RES_*` result, for callers that
/// tolerate a failed object (e.g. an inode deleted under an enumeration).
fn sync_try_read_obj(
    stream: &mut TcpStream,
    oid: u64,
    offset: u64,
    dst: &mut [u8],
) -> io::Result<u32> {
    use std::io::Write;

    let mut hdr = [0u8; SD_HDR_SIZE];
    encode_obj_req(
        &mut hdr,
        SD_OP_READ_OBJ,
        0,
        0,
        dst.len() as u32,
        oid,
        0,
        0,
        offset,
    );
    stream.write_all(&hdr)?;

    let mut resp = [0u8; SD_HDR_SIZE];
    std::io::Read::read_exact(stream, &mut resp)?;
    let resp_len = read_payload(stream, &resp, dst)?;
    dst[resp_len..].fill(0);
    Ok(resp_result(&resp))
}

/// Read a response's payload into `dst`, returning its length. Keeps the
/// stream framed for the next request: an over-long payload (more than was
/// asked for) is a protocol violation, not something to leave unread.
fn read_payload(
    stream: &mut TcpStream,
    resp: &[u8; SD_HDR_SIZE],
    dst: &mut [u8],
) -> io::Result<usize> {
    let resp_len = resp_data_length(resp) as usize;
    if resp_len > dst.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "response payload {resp_len} exceeds the {} requested",
                dst.len()
            ),
        ));
    }
    if resp_len > 0 {
        std::io::Read::read_exact(stream, &mut dst[..resp_len])?;
    }
    Ok(resp_len)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oid_math() {
        // Inode object has VDI_BIT set; data objects encode idx in the low bits.
        assert_eq!(vid_to_vdi_oid(0xab_cdef), VDI_BIT | (0x00ab_cdef << 32));
        assert_eq!(vid_to_data_oid(0xabcdef, 0), 0x00ab_cdef << 32);
        assert_eq!(vid_to_data_oid(0xabcdef, 7), (0x00ab_cdef << 32) | 7);
        // oid_to_vid inverse: (oid & SD_VDI_MASK) >> 32.
        let oid = vid_to_data_oid(0x123456, 42);
        assert_eq!((oid & 0x00FF_FFFF_0000_0000) >> 32, 0x123456);
    }

    #[test]
    fn uuid_is_none_only_when_the_inode_carries_none() {
        let mut inode = vec![0u8; SD_INODE_META_SIZE];
        assert_eq!(read_uuid(&inode), None, "all-zero uuid[16] is unset");
        // A neighbouring field set does not make one appear...
        inode[INO_OFF_ACL_ID..INO_OFF_ACL_ID + 4].copy_from_slice(&0x4711u32.to_le_bytes());
        inode[INO_OFF_VDI_FLAGS..INO_OFF_VDI_FLAGS + 4].copy_from_slice(&1u32.to_le_bytes());
        assert_eq!(read_uuid(&inode), None);
        // ...and a real one is read back verbatim, in inode byte order.
        let uuid: [u8; SD_UUID_LEN] = std::array::from_fn(|i| i as u8 + 1);
        inode[INO_OFF_UUID..INO_OFF_UUID + SD_UUID_LEN].copy_from_slice(&uuid);
        assert_eq!(read_uuid(&inode), Some(uuid));
    }

    #[test]
    fn obj_req_layout() {
        let mut hdr = [0u8; SD_HDR_SIZE];
        encode_obj_req(
            &mut hdr,
            SD_OP_WRITE_OBJ,
            SD_FLAG_CMD_WRITE | SD_FLAG_CMD_DIRECT,
            0x1122_3344,
            0x1000,
            0xdead_beef_0000_0000,
            0,
            3,
            0x4_0000,
        );
        assert_eq!(hdr[0], SD_PROTO_VER);
        assert_eq!(hdr[1], SD_OP_WRITE_OBJ);
        assert_eq!(u16::from_le_bytes([hdr[2], hdr[3]]), 0x09);
        assert_eq!(read_u32(&hdr, 8), 0x1122_3344); // id
        assert_eq!(read_u32(&hdr, 12), 0x1000); // data_length
        assert_eq!(read_u64(&hdr, 16), 0xdead_beef_0000_0000); // oid
        assert_eq!(hdr[32], 3); // copies
        assert_eq!(read_u64(&hdr, 40), 0x4_0000); // offset
    }

    #[test]
    fn vdi_req_layout() {
        let mut hdr = [0u8; SD_HDR_SIZE];
        encode_vdi_req(
            &mut hdr,
            SD_OP_GET_VDI_INFO,
            SD_FLAG_CMD_WRITE,
            512,
            0,
            0,
            0,
        );
        assert_eq!(hdr[0], SD_PROTO_VER);
        assert_eq!(hdr[1], SD_OP_GET_VDI_INFO);
        assert_eq!(u16::from_le_bytes([hdr[2], hdr[3]]), SD_FLAG_CMD_WRITE);
        assert_eq!(read_u32(&hdr, 12), 512);

        // A lookup names its VDI in base_vdi_id (24) and its ACL scope in acl
        // (36) — the fields sd_req's vdi union puts there.
        encode_vdi_req(&mut hdr, SD_OP_GET_VDI_INFO, 0, 0, 0, 0xab_cdef, 0x4711);
        assert_eq!(read_u32(&hdr, 24), 0xab_cdef);
        assert_eq!(read_u32(&hdr, 32), 0); // snapid
        assert_eq!(read_u32(&hdr, 36), 0x4711); // acl
    }

    #[test]
    fn vdi_lock_req_layout() {
        // REGISTER_VDI: no vid (the name in the payload resolves it), the
        // owner we choose in addr/port, the ACL id in acl.
        let mut hdr = [0u8; SD_HDR_SIZE];
        let owner: SocketAddr = "10.1.2.3:14420".parse().unwrap();
        encode_vdi_lock_req(
            &mut hdr,
            SD_OP_REGISTER_VDI,
            SD_FLAG_CMD_WRITE,
            512,
            0,
            owner,
            0x4711,
        );
        assert_eq!(hdr[0], SD_PROTO_VER);
        assert_eq!(hdr[1], SD_OP_REGISTER_VDI);
        assert_eq!(u16::from_le_bytes([hdr[2], hdr[3]]), SD_FLAG_CMD_WRITE);
        assert_eq!(read_u32(&hdr, 12), 512);
        assert_eq!(read_u32(&hdr, 16), 0); // vid
        assert_eq!(
            &hdr[24..40],
            &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 1, 2, 3]
        );
        assert_eq!(u16::from_le_bytes([hdr[40], hdr[41]]), 14420);
        assert_eq!(read_u32(&hdr, 44), 0x4711);

        // UNREGISTER_VDI names the vid instead, and carries no payload.
        encode_vdi_lock_req(
            &mut hdr,
            SD_OP_UNREGISTER_VDI,
            0,
            0,
            0xab_cdef,
            owner,
            0x4711,
        );
        assert_eq!(hdr[1], SD_OP_UNREGISTER_VDI);
        assert_eq!(read_u32(&hdr, 12), 0);
        assert_eq!(read_u32(&hdr, 16), 0xab_cdef);

        // Sheep-internal opcodes ride the other protocol version.
        encode_sheep_req(&mut hdr, SD_OP_GET_VDI_COPIES, 4096);
        assert_eq!(hdr[0], SD_SHEEP_PROTO_VER);
        assert_eq!(hdr[1], SD_OP_GET_VDI_COPIES);
        assert_eq!(read_u32(&hdr, 12), 4096);
    }

    #[test]
    fn node_addr_round_trips_both_families() {
        for ip in [
            "192.168.7.9".parse::<std::net::IpAddr>().unwrap(),
            "fd00::1".parse().unwrap(),
        ] {
            assert_eq!(decode_node_addr(&encode_node_addr(ip)), ip);
        }
        // An unset addr[16] is all zeroes, which is neither of the above and
        // must not be mistaken for 0.0.0.0.
        assert_eq!(
            decode_node_addr(&[0u8; 16]),
            "::".parse::<std::net::IpAddr>().unwrap()
        );
    }

    #[test]
    fn vdi_state_offsets_match_struct() {
        // participants_state[31] then participants[31] close out the record;
        // if the arithmetic drifts, GET_VDI_COPIES records misalign silently.
        assert_eq!(
            VS_OFF_PARTICIPANTS,
            VS_OFF_PARTICIPANTS_STATE + SD_MAX_COPIES * 4
        );
        assert_eq!(
            SD_VDI_STATE_SIZE,
            VS_OFF_PARTICIPANTS + SD_MAX_COPIES * SD_NODE_ID_SIZE
        );
    }

    /// A `vdi_state` record with `holders` participants, as `sheep` sends it.
    fn vdi_state_record(vid: u32, lock_state: u32, holders: &[(&str, u32)]) -> Vec<u8> {
        let mut vs = vec![0u8; SD_VDI_STATE_SIZE];
        vs[VS_OFF_VID..VS_OFF_VID + 4].copy_from_slice(&vid.to_le_bytes());
        vs[VS_OFF_LOCK_STATE..VS_OFF_LOCK_STATE + 4].copy_from_slice(&lock_state.to_le_bytes());
        let nr = holders.len() as u32;
        vs[VS_OFF_NR_PARTICIPANTS..VS_OFF_NR_PARTICIPANTS + 4].copy_from_slice(&nr.to_le_bytes());
        for (i, (addr, count)) in holders.iter().enumerate() {
            let addr: SocketAddr = addr.parse().unwrap();
            let nid = VS_OFF_PARTICIPANTS + i * SD_NODE_ID_SIZE;
            vs[nid + NID_OFF_ADDR..nid + NID_OFF_ADDR + 16]
                .copy_from_slice(&encode_node_addr(addr.ip()));
            vs[nid + NID_OFF_PORT..nid + NID_OFF_PORT + 2]
                .copy_from_slice(&addr.port().to_le_bytes());
            // shared state in the low byte, registration count above it.
            let state = 2u32 | (count << 8);
            let off = VS_OFF_PARTICIPANTS_STATE + i * 4;
            vs[off..off + 4].copy_from_slice(&state.to_le_bytes());
        }
        vs
    }

    #[test]
    fn holders_come_from_the_participant_list() {
        let vs = vdi_state_record(
            0x42,
            LOCK_STATE_SHARED,
            &[("10.0.0.1:14420", 4), ("[fd00::2]:4420", 8)],
        );
        let holders = parse_holders(&vs);
        assert_eq!(
            holders,
            vec![
                VdiHolder {
                    addr: "10.0.0.1:14420".parse().unwrap(),
                    index: 0,
                    registrations: 4,
                },
                VdiHolder {
                    addr: "[fd00::2]:4420".parse().unwrap(),
                    index: 1,
                    registrations: 8,
                },
            ]
        );

        // A sheep predating the count reports zero, meaning one registration.
        let mut vs = vdi_state_record(0x42, LOCK_STATE_SHARED, &[("10.0.0.1:14420", 0)]);
        assert_eq!(parse_holders(&vs)[0].registrations, 1);

        // Nobody serves an ACL that is unlocked, or one held exclusively.
        for state in [1u32, 2] {
            vs[VS_OFF_LOCK_STATE..VS_OFF_LOCK_STATE + 4].copy_from_slice(&state.to_le_bytes());
            assert!(parse_holders(&vs).is_empty());
        }

        // nr_participants is a wire value: never index past the array with it.
        let mut vs = vdi_state_record(0x42, LOCK_STATE_SHARED, &[("10.0.0.1:14420", 1)]);
        vs[VS_OFF_NR_PARTICIPANTS..VS_OFF_NR_PARTICIPANTS + 4]
            .copy_from_slice(&999u32.to_le_bytes());
        assert_eq!(parse_holders(&vs).len(), SD_MAX_COPIES);
    }

    #[test]
    fn inode_header_offsets_match_struct() {
        // The header offsets are computed from the C struct layout; guard the
        // arithmetic (name[256]+tag[256] + fixed fields ... + __unused[]).
        assert_eq!(INO_OFF_SNAP_CTIME, 520);
        assert_eq!(INO_OFF_MAX_DATA_ID_NR, 528);
        assert_eq!(INO_OFF_VDI_SIZE, 536);
        assert_eq!(INO_OFF_NR_COPIES, 554);
        assert_eq!(INO_OFF_BLOCK_SIZE_SHIFT, 555);
        // acl_id (572) + uuid[16] (576) + vdi_flags (592) were carved out of
        // the leading __unused words; the metadata read must cover them.
        assert_eq!(INO_OFF_ACL_ID, 572);
        assert_eq!(INO_OFF_UUID, INO_OFF_ACL_ID + 4);
        assert_eq!(INO_OFF_VDI_FLAGS, INO_OFF_UUID + SD_UUID_LEN);
        assert_eq!(SD_INODE_META_SIZE, INO_OFF_VDI_FLAGS + 4);
        // offsetof(sd_inode, data_vdi_id): 572 (btree_counter) + 4092 (__unused).
        assert_eq!(SD_INODE_HEADER_SIZE, 572 + 4 * 1023);
    }
}
