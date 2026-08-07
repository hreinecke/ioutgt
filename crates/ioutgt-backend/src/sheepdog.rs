//! Sheepdog cluster backend: serves a namespace from a named VDI on a
//! [Sheepdog](https://github.com/sheepdog/sheepdog) distributed-storage
//! cluster, over the plain-TCP client/gateway protocol. [`list_vdis`]
//! enumerates the cluster's VDIs so a target can export one namespace per
//! VDI.
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
use std::sync::atomic::{AtomicU32, Ordering};

use ioutgt_core::buf::AlignedBuf;
use ioutgt_core::{Backend, BackendError, LbaRange};
use ioutgt_uring::ops;

// ---------------------------------------------------------------------------
// Wire constants (include/sheepdog_proto.h)
// ---------------------------------------------------------------------------

const SD_PROTO_VER: u8 = 0x02;
/// Default `sheep` client port.
pub const SD_LISTEN_PORT: u16 = 7000;

const SD_OP_CREATE_AND_WRITE_OBJ: u8 = 0x01;
const SD_OP_READ_OBJ: u8 = 0x02;
const SD_OP_WRITE_OBJ: u8 = 0x03;
const SD_OP_GET_VDI_INFO: u8 = 0x14;
const SD_OP_READ_VDIS: u8 = 0x15;

const SD_FLAG_CMD_WRITE: u16 = 0x01;
const SD_FLAG_CMD_COW: u16 = 0x02;
const SD_FLAG_CMD_DIRECT: u16 = 0x08;

const SD_RES_SUCCESS: u32 = 0x00;
const SD_RES_NO_VDI: u32 = 0x08;
const SD_RES_NO_TAG: u32 = 0x0E;
const SD_RES_NO_SPACE: u32 = 0x15;
const SD_RES_READONLY: u32 = 0x1A;

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
const SD_INODE_META_SIZE: usize = 572;
/// `SD_INODE_DATA_INDEX` — entries in `data_vdi_id[]` (max 4 TiB at 4 MiB).
const SD_INODE_DATA_INDEX: u64 = 1 << 20;

// Inode header field offsets (little-endian).
const INO_OFF_NAME: usize = 0;
const INO_OFF_TAG: usize = SD_MAX_VDI_LEN;
const INO_OFF_SNAP_CTIME: usize = 520;
const INO_OFF_VDI_SIZE: usize = 536;
const INO_OFF_NR_COPIES: usize = 554;
const INO_OFF_BLOCK_SIZE_SHIFT: usize = 555;

/// NVMe logical block shift (512 B), matching the other backends' static path.
const BLOCK_SHIFT: u8 = 9;

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

/// Encode a 48-byte `vdi` request header (the VDI-lookup family).
fn encode_vdi_req(
    hdr: &mut [u8; SD_HDR_SIZE],
    opcode: u8,
    flags: u16,
    data_length: u32,
    snapid: u32,
) {
    hdr.fill(0);
    hdr[0] = SD_PROTO_VER;
    hdr[1] = opcode;
    hdr[2..4].copy_from_slice(&flags.to_le_bytes());
    hdr[12..16].copy_from_slice(&data_length.to_le_bytes());
    // vdi union: snapid at byte 32.
    hdr[32..36].copy_from_slice(&snapid.to_le_bytes());
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
    /// The (writable head) VDI id.
    vid: u32,
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
}

impl SheepdogBackend {
    /// Look up `vdi` (optionally at snapshot `tag`) on the cluster at `addr`,
    /// read its inode, and build the backend. Performs a small synchronous
    /// handshake (blocking `TcpStream`) once at startup — off the io_uring path.
    pub fn open(addr: SocketAddr, vdi: &str, tag: Option<&str>) -> io::Result<SheepdogBackend> {
        if vdi.is_empty() || vdi.len() >= SD_MAX_VDI_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid VDI name",
            ));
        }
        let mut stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true).ok();

        let vid = sync_lookup_vdi(&mut stream, vdi, tag)?;

        // Inode metadata, then the data_vdi_id[] slice sized to the volume.
        let inode_oid = vid_to_vdi_oid(vid);
        let mut header = vec![0u8; SD_INODE_META_SIZE];
        sync_read_obj(&mut stream, inode_oid, 0, &mut header)?;

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
            sync_read_obj(&mut stream, inode_oid, SD_INODE_HEADER_SIZE, &mut map_bytes)?;
        }
        let data_map: Box<[AtomicU32]> = (0..nr_objects as usize)
            .map(|i| AtomicU32::new(read_u32(&map_bytes, i * 4)))
            .collect();

        Ok(SheepdogBackend {
            addr,
            vid,
            object_size,
            nr_copies,
            nr_blocks: vdi_size >> BLOCK_SHIFT,
            read_only: snap_ctime != 0,
            data_map,
        })
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
// Cluster VDI enumeration
// ---------------------------------------------------------------------------

/// One VDI found on a cluster by [`list_vdis`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VdiInfo {
    /// VDI name — the `dog vdi list` NAME column, unique among a cluster's
    /// writable heads.
    pub name: String,
    /// Snapshot tag; empty for the writable head.
    pub tag: String,
    /// The 24-bit VDI id, stable for the life of the VDI.
    pub vid: u32,
    /// Volume size in bytes.
    pub size: u64,
    /// True for a snapshot: a frozen VDI, servable only read-only.
    pub snapshot: bool,
}

/// Enumerate every VDI on the cluster at `addr`, sorted by (name, tag).
///
/// Reads the cluster's VDI bitmap (`READ_VDIS`, one bit per vid), then each
/// live vid's inode metadata. Blocking and off the io_uring path, like
/// [`SheepdogBackend::open`]: a target calls it once at startup to map the
/// cluster onto namespaces. Vids that vanish between the bitmap snapshot and
/// the inode read (a concurrent `dog vdi delete`), and unnamed or zero-sized
/// inodes, are skipped rather than failing the whole enumeration.
pub fn list_vdis(addr: SocketAddr) -> io::Result<Vec<VdiInfo>> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_nodelay(true).ok();
    let bitmap = sync_read_vdi_bitmap(&mut stream)?;

    let mut vdis = Vec::new();
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
            if sync_try_read_obj(&mut stream, vid_to_vdi_oid(vid), 0, &mut inode)? != SD_RES_SUCCESS
            {
                continue;
            }
            let name = read_cstr(&inode[INO_OFF_NAME..INO_OFF_NAME + SD_MAX_VDI_LEN]);
            let size = read_u64(&inode, INO_OFF_VDI_SIZE);
            if name.is_empty() || size == 0 {
                continue;
            }
            vdis.push(VdiInfo {
                name,
                tag: read_cstr(&inode[INO_OFF_TAG..INO_OFF_TAG + SD_MAX_VDI_TAG_LEN]),
                vid,
                size,
                snapshot: read_u64(&inode, INO_OFF_SNAP_CTIME) != 0,
            });
        }
    }
    vdis.sort_by(|a, b| (&a.name, &a.tag).cmp(&(&b.name, &b.tag)));
    Ok(vdis)
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

/// Decode a fixed-width, NUL-padded inode string field.
fn read_cstr(field: &[u8]) -> String {
    let end = field.iter().position(|&b| b == 0).unwrap_or(field.len());
    String::from_utf8_lossy(&field[..end]).into_owned()
}

/// Look up a VDI by name (and optional snapshot tag) → its vid.
fn sync_lookup_vdi(stream: &mut TcpStream, vdi: &str, tag: Option<&str>) -> io::Result<u32> {
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
        res => Err(io::Error::other(format!(
            "VDI lookup failed: SD_RES {res:#x}"
        ))),
    }
}

/// Synchronously read the cluster's VDI bitmap (one bit per vid, LSB-first
/// within each byte — the kernel `test_bit` layout the `sheep` gateway uses).
fn sync_read_vdi_bitmap(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    use std::io::{Read, Write};

    let mut hdr = [0u8; SD_HDR_SIZE];
    encode_vdi_req(&mut hdr, SD_OP_READ_VDIS, 0, SD_VDI_BITMAP_SIZE as u32, 0);
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
        encode_vdi_req(&mut hdr, SD_OP_GET_VDI_INFO, SD_FLAG_CMD_WRITE, 512, 0);
        assert_eq!(hdr[0], SD_PROTO_VER);
        assert_eq!(hdr[1], SD_OP_GET_VDI_INFO);
        assert_eq!(u16::from_le_bytes([hdr[2], hdr[3]]), SD_FLAG_CMD_WRITE);
        assert_eq!(read_u32(&hdr, 12), 512);
    }

    #[test]
    fn inode_header_offsets_match_struct() {
        // The header offsets are computed from the C struct layout; guard the
        // arithmetic (name[256]+tag[256] + fixed fields ... + __unused[1023]).
        assert_eq!(INO_OFF_SNAP_CTIME, 520);
        assert_eq!(INO_OFF_VDI_SIZE, 536);
        assert_eq!(INO_OFF_NR_COPIES, 554);
        assert_eq!(INO_OFF_BLOCK_SIZE_SHIFT, 555);
        // offsetof(sd_inode, data_vdi_id): 572 (btree_counter) + 4092 (__unused).
        assert_eq!(SD_INODE_HEADER_SIZE, 572 + 4 * 1023);
    }
}
