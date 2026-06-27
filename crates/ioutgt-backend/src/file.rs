//! File and block-device backend: vectored IO through the queue
//! thread's io_uring, O_DIRECT on a backing store whose alignment our
//! ring payloads meet, buffered (`RWF_DONTCACHE`) otherwise.
//!
//! One implementation serves both regular files and block devices
//! (geometry probing differs; the IO path is identical), mirroring how
//! little actually differs in userspace — unlike kernel nvmet's
//! bio-vs-kiocb split.
//!
//! A single fd is opened `O_DIRECT`; whether O_DIRECT is *used* is decided
//! once, at open. The decision depends on whether the zero-copy recv ring is
//! enabled (see [`FileBackend::open`]'s `ring_enabled`):
//!
//! - **Ring off (the default).** The backend only ever sees page-aligned pool
//!   buffers, which satisfy any DIO alignment a store can demand, so O_DIRECT
//!   is kept whenever the kernel opened the `O_DIRECT` fd — no `statx` gate.
//! - **Ring on.** Write payloads can live in ring memory at only **dword/PDO
//!   (4-byte) alignment**, not page alignment, so O_DIRECT is usable only when
//!   the store's `statx STATX_DIOALIGN` reports a memory alignment no coarser
//!   than 4 bytes (`stx_dio_mem_align <= 4`) and an offset alignment no coarser
//!   than our logical block. NVMe-class block devices (and filesystems on them)
//!   report a 4-byte DIO memory alignment, so the dword-aligned ring payloads
//!   DMA straight from ring memory.
//!
//! When O_DIRECT is rejected — a too-coarse DIO alignment under the ring, a
//! store where the kernel can't report DIOALIGN (e.g. tmpfs), or a store that
//! refuses `O_DIRECT` at open — the backend falls back to a buffered fd with
//! `RWF_DONTCACHE` (self-correcting to plain buffered on the first
//! `EOPNOTSUPP`/`EINVAL`, for pre-6.14 kernels and filesystems without
//! DONTCACHE).

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use ioutgt_core::pool::{MAX_SEGS, Seg};
use ioutgt_core::{Backend, BackendError, LbaRange};
use ioutgt_uring::ops;

/// `errno` from the most recent failed libc call.
fn errno() -> i32 {
    io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

/// Backing kind, decided by `fstat` at open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Regular,
    Block,
}

const EMPTY_IOVEC: libc::iovec = libc::iovec {
    iov_base: std::ptr::null_mut(),
    iov_len: 0,
};

/// File/bdev backend issuing vectored IO; O_DIRECT or buffered+DONTCACHE,
/// decided at open from `statx STATX_DIOALIGN`. See module docs.
pub struct FileBackend {
    /// O_DIRECT when the store's DIO alignment fits our 4-byte ring
    /// payloads, else a plain buffered fd.
    fd: OwnedFd,
    kind: Kind,
    block_shift: u8,
    nr_blocks: u64,
    /// O_DIRECT is in effect (false ⇒ the store's DIO alignment was too
    /// coarse or unavailable, IO is buffered with `RWF_DONTCACHE`).
    direct: bool,
    /// `RWF_DONTCACHE` usable on the buffered fd. Optimistically true;
    /// self-corrects to false on the first `EOPNOTSUPP`/`EINVAL`.
    dontcache: AtomicBool,
}

/// Query DIO alignment via `statx(STATX_DIOALIGN)`. Returns
/// `(stx_dio_mem_align, stx_dio_offset_align)`, or `(0, 0)` when the kernel
/// can't report it (pre-6.1, or a backing store without DIO support).
fn query_dioalign(fd: RawFd) -> (u32, u32) {
    // SAFETY: statx writes the struct on success; zeroed is a valid init.
    let mut stx: libc::statx = unsafe { std::mem::zeroed() };
    // SAFETY: empty path + AT_EMPTY_PATH targets `fd`; out-pointer valid.
    let r = unsafe {
        libc::statx(
            fd,
            c"".as_ptr(),
            libc::AT_EMPTY_PATH,
            libc::STATX_DIOALIGN,
            &raw mut stx,
        )
    };
    if r < 0 || stx.stx_mask & libc::STATX_DIOALIGN == 0 {
        return (0, 0);
    }
    (stx.stx_dio_mem_align, stx.stx_dio_offset_align)
}

/// Whether to keep the opened `O_DIRECT` fd, given the store's `statx
/// STATX_DIOALIGN` and whether the recv ring is enabled (see module docs for
/// the ring-off vs ring-on rule).
fn direct_usable(ring_enabled: bool, dio_mem: u32, dio_off: u32, block_shift: u8) -> bool {
    if !ring_enabled {
        return true;
    }
    dio_mem != 0 && dio_mem <= 4 && dio_off != 0 && u64::from(dio_off) <= (1u64 << block_shift)
}

/// Fill `iovs` from `segs`, clamping the total to `total` bytes; returns
/// the number of iovec entries used.
fn fill_iovecs(iovs: &mut [libc::iovec], segs: &[Seg], total: usize) -> usize {
    let mut remaining = total;
    let mut n = 0;
    for seg in segs {
        if remaining == 0 {
            break;
        }
        let take = remaining.min(seg.len);
        iovs[n] = libc::iovec {
            iov_base: seg.ptr.cast(),
            iov_len: take,
        };
        n += 1;
        remaining -= take;
    }
    n
}

/// Advance `iovs[idx..]` past `n` transferred bytes (short-IO resume).
fn advance_iovecs(iovs: &mut [libc::iovec], idx: &mut usize, mut n: usize) {
    while n > 0 {
        let v = &mut iovs[*idx];
        if v.iov_len <= n {
            n -= v.iov_len;
            *idx += 1;
        } else {
            // SAFETY: advancing within the current iovec's own buffer.
            v.iov_base = unsafe { v.iov_base.cast::<u8>().add(n).cast() };
            v.iov_len -= n;
            n = 0;
        }
    }
}

impl FileBackend {
    /// Open `path` (regular file or block device). Tries `O_DIRECT`, keeping it
    /// or falling back to a buffered (`RWF_DONTCACHE`) fd per the ring-gated
    /// rule in the module docs.
    pub fn open(path: &Path, block_shift: u8, ring_enabled: bool) -> io::Result<FileBackend> {
        use std::os::unix::ffi::OsStrExt;
        let cpath = std::ffi::CString::new(path.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL in path"))?;

        // Try O_DIRECT; keep it only if direct_usable() (see module docs), else
        // close and reopen buffered.
        // SAFETY: valid NUL-terminated path; flags are plain constants.
        let dfd = unsafe { libc::open(cpath.as_ptr(), libc::O_RDWR | libc::O_DIRECT) };
        let (fd, direct) = if dfd >= 0 {
            // SAFETY: fresh fd, exclusively owned.
            let dfd = unsafe { OwnedFd::from_raw_fd(dfd) };
            let (dio_mem, dio_off) = query_dioalign(dfd.as_raw_fd());
            let usable = direct_usable(ring_enabled, dio_mem, dio_off, block_shift);
            if usable {
                (dfd, true)
            } else {
                drop(dfd);
                // SAFETY: valid NUL-terminated path; flags are plain constants.
                let bfd = unsafe { libc::open(cpath.as_ptr(), libc::O_RDWR) };
                if bfd < 0 {
                    return Err(io::Error::last_os_error());
                }
                // SAFETY: fresh fd, exclusively owned.
                (unsafe { OwnedFd::from_raw_fd(bfd) }, false)
            }
        } else if matches!(errno(), libc::EINVAL | libc::EOPNOTSUPP) {
            // Store refuses O_DIRECT outright (e.g. tmpfs): buffered fd.
            // SAFETY: valid NUL-terminated path; flags are plain constants.
            let bfd = unsafe { libc::open(cpath.as_ptr(), libc::O_RDWR) };
            if bfd < 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: fresh fd, exclusively owned.
            (unsafe { OwnedFd::from_raw_fd(bfd) }, false)
        } else {
            return Err(io::Error::last_os_error());
        };

        // SAFETY: stat is written by the kernel on success.
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        // SAFETY: valid fd and out-pointer.
        if unsafe { libc::fstat(fd.as_raw_fd(), &raw mut stat) } < 0 {
            return Err(io::Error::last_os_error());
        }
        let (kind, size_bytes) = if stat.st_mode & libc::S_IFMT == libc::S_IFBLK {
            let mut size: u64 = 0;
            // BLKGETSIZE64 = _IOR(0x12, 114, size_t)
            const BLKGETSIZE64: libc::c_ulong = 0x8008_1272;
            // SAFETY: valid fd; the ioctl writes a u64.
            if unsafe { libc::ioctl(fd.as_raw_fd(), BLKGETSIZE64, &raw mut size) } < 0 {
                return Err(io::Error::last_os_error());
            }
            (Kind::Block, size)
        } else if stat.st_mode & libc::S_IFMT == libc::S_IFREG {
            #[allow(clippy::cast_sign_loss)]
            (Kind::Regular, stat.st_size.max(0) as u64)
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "not a file or block device",
            ));
        };

        let nr_blocks = size_bytes >> block_shift;
        if nr_blocks == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "backing store too small",
            ));
        }

        Ok(FileBackend {
            fd,
            kind,
            block_shift,
            nr_blocks,
            direct,
            dontcache: AtomicBool::new(true),
        })
    }

    /// O_DIRECT is in effect for this backend's IO (decided at open).
    pub fn is_direct(&self) -> bool {
        self.direct
    }

    fn offset(&self, slba: u64) -> u64 {
        slba << self.block_shift
    }

    /// The op target for this backend's fd: a registered fixed-file index when
    /// the reactor accepted one (skips the kernel's per-IO fd lookup), else the
    /// raw fd. Best-effort and per-thread, mirroring the fixed-buffer fallback.
    fn backend_fd(&self) -> ops::BackendFd {
        let fd = self.fd.as_raw_fd();
        match ioutgt_uring::fixed_file_index(fd) {
            Some(idx) => ops::BackendFd::Fixed(idx),
            None => ops::BackendFd::Raw(fd),
        }
    }

    /// Issue one vectored read/write on the backend fd (O_DIRECT or
    /// buffered+DONTCACHE, fixed at open), resuming across short transfers. When
    /// `buf_index` is `Some`, the iovecs point into that registered pool
    /// buffer and the op is `READV_FIXED`/`WRITEV_FIXED` — the kernel reuses
    /// the pre-pinned mapping instead of mapping the pages every IO.
    async fn rwv(
        &self,
        write: bool,
        slba: u64,
        iovs: &mut [libc::iovec],
        total: usize,
        buf_index: Option<u16>,
    ) -> Result<(), BackendError> {
        if total == 0 {
            return Ok(());
        }
        self.check_range(slba, (total as u64) >> self.block_shift)?;
        let base_off = self.offset(slba);
        let file = self.backend_fd();

        let mut done = 0usize;
        let mut idx = 0usize;
        while done < total {
            let off = base_off + done as u64;
            // Buffered IO drops the page cache via RWF_DONTCACHE (close to
            // DIO without alignment constraints); O_DIRECT needs no flag.
            // The flag self-corrects off on the first EOPNOTSUPP/EINVAL.
            let use_dc = !self.direct && self.dontcache.load(Ordering::Relaxed);
            let flags = if use_dc { libc::RWF_DONTCACHE } else { 0 };
            let ptr = iovs[idx..].as_ptr();
            #[allow(clippy::cast_possible_truncation)]
            let cnt = (iovs.len() - idx) as u32;
            // SAFETY: `iovs` and every buffer they point at outlive this
            // awaited op — `iovs` lives in the caller's frame (held across
            // the await) and the segment buffers are the caller's slot
            // memory, valid while the slot is Executing. The reactor's
            // orphan protocol holds the op entry to its terminal CQE on
            // whole-future drop, the same envelope as the other raw ops.
            // For the fixed variants the buffers additionally fall within the
            // registered arena `buf_index`, which is unregistered only after
            // the op drain at teardown.
            let res = unsafe {
                match (write, buf_index) {
                    (true, Some(bi)) => ops::writev_fixed_at_raw(file, ptr, cnt, off, bi, flags),
                    (false, Some(bi)) => ops::readv_fixed_at_raw(file, ptr, cnt, off, bi, flags),
                    (true, None) => ops::writev_at_raw(file, ptr, cnt, off, flags),
                    (false, None) => ops::readv_at_raw(file, ptr, cnt, off, flags),
                }
            };
            let op = res.map_err(|e| BackendError::Io(e.raw_os_error().unwrap_or(libc::EIO)))?;
            match op.await {
                Ok(0) => return Err(BackendError::Io(libc::EIO)),
                Ok(n) => {
                    let n = n as usize;
                    advance_iovecs(iovs, &mut idx, n);
                    done += n;
                }
                Err(e) => {
                    let code = e.raw_os_error().unwrap_or(libc::EIO);
                    // RWF_DONTCACHE unsupported (pre-6.14 or this fs): drop
                    // the flag for good and retry the same offset buffered.
                    if use_dc && matches!(code, libc::EOPNOTSUPP | libc::EINVAL) {
                        self.dontcache.store(false, Ordering::Relaxed);
                        continue;
                    }
                    return Err(map_errno(code));
                }
            }
        }
        Ok(())
    }
}

fn map_errno(err: i32) -> BackendError {
    match err {
        libc::ENOSPC => BackendError::NoSpace,
        libc::EOPNOTSUPP | libc::EINVAL => BackendError::Unsupported,
        other => BackendError::Io(other),
    }
}

impl Backend for FileBackend {
    fn block_shift(&self) -> u8 {
        self.block_shift
    }

    fn nr_blocks(&self) -> u64 {
        self.nr_blocks
    }

    async fn read(&self, slba: u64, buf: &mut [u8]) -> Result<(), BackendError> {
        let mut iovs = [libc::iovec {
            iov_base: buf.as_mut_ptr().cast(),
            iov_len: buf.len(),
        }];
        self.rwv(false, slba, &mut iovs, buf.len(), None).await
    }

    async fn write(&self, slba: u64, buf: &[u8]) -> Result<(), BackendError> {
        let mut iovs = [libc::iovec {
            iov_base: buf.as_ptr().cast_mut().cast(),
            iov_len: buf.len(),
        }];
        self.rwv(true, slba, &mut iovs, buf.len(), None).await
    }

    async fn read_segs(
        &self,
        slba: u64,
        segs: &[Seg],
        total: usize,
        buf_index: Option<u16>,
    ) -> Result<(), BackendError> {
        let mut iovs = [EMPTY_IOVEC; MAX_SEGS];
        let n = fill_iovecs(&mut iovs, segs, total);
        self.rwv(false, slba, &mut iovs[..n], total, buf_index)
            .await
    }

    async fn write_segs(
        &self,
        slba: u64,
        segs: &[Seg],
        total: usize,
        buf_index: Option<u16>,
    ) -> Result<(), BackendError> {
        let mut iovs = [EMPTY_IOVEC; MAX_SEGS];
        let n = fill_iovecs(&mut iovs, segs, total);
        self.rwv(true, slba, &mut iovs[..n], total, buf_index).await
    }

    async fn flush(&self) -> Result<(), BackendError> {
        ops::fsync(self.backend_fd(), true)
            .map_err(|e| BackendError::Io(e.raw_os_error().unwrap_or(libc::EIO)))?
            .await
            .map(|_| ())
            .map_err(|e| map_errno(e.raw_os_error().unwrap_or(libc::EIO)))
    }

    async fn discard(&self, ranges: &[LbaRange]) -> Result<(), BackendError> {
        // Deallocate is a hint: punch holes where supported, succeed
        // regardless (block devices need BLKDISCARD/uring-cmd — roadmap).
        if self.kind != Kind::Regular {
            return Ok(());
        }
        for range in ranges {
            if self.check_range(range.slba, u64::from(range.nlb)).is_err() {
                return Err(BackendError::OutOfRange);
            }
            let mode = libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE;
            let len = u64::from(range.nlb) << self.block_shift;
            if let Ok(op) = ops::fallocate(self.backend_fd(), mode, self.offset(range.slba), len) {
                let _ = op.await;
            }
        }
        Ok(())
    }

    async fn write_zeroes(&self, range: LbaRange) -> Result<(), BackendError> {
        self.check_range(range.slba, u64::from(range.nlb))?;
        let len = u64::from(range.nlb) << self.block_shift;
        if self.kind == Kind::Regular {
            // ZERO_RANGE, then PUNCH_HOLE (reads back zeroes on files).
            for mode in [
                libc::FALLOC_FL_ZERO_RANGE,
                libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            ] {
                if let Ok(op) =
                    ops::fallocate(self.backend_fd(), mode, self.offset(range.slba), len)
                {
                    if op.await.is_ok() {
                        return Ok(());
                    }
                }
            }
        }
        // Fallback (and the block-device path until uring-cmd discard
        // lands): write zero chunks through the buffered fd.
        let chunk = ioutgt_core::buf::AlignedBuf::zeroed(64 * 1024);
        let mut remaining = len;
        let mut off = self.offset(range.slba);
        while remaining > 0 {
            let want = u32::try_from(remaining.min(chunk.len() as u64)).expect("chunk-bounded");
            // SAFETY: chunk is alive across the await; read-only for the
            // kernel.
            let n = unsafe { ops::write_at_raw(self.backend_fd(), chunk.as_ptr(), want, off) }
                .map_err(|e| BackendError::Io(e.raw_os_error().unwrap_or(libc::EIO)))?
                .await
                .map_err(|e| map_errno(e.raw_os_error().unwrap_or(libc::EIO)))?;
            if n == 0 {
                return Err(BackendError::Io(libc::EIO));
            }
            remaining -= u64::from(n);
            off += u64::from(n);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::direct_usable;

    #[test]
    fn ring_off_keeps_direct_regardless_of_dio_alignment() {
        // The default (ring off) path only writes from page-aligned pool
        // buffers, so O_DIRECT is kept whatever the store reports — including a
        // `dio_mem > 4` device (the case the statx gate used to pessimize into
        // buffered+RWF_DONTCACHE) and a store that can't report DIOALIGN (0, 0).
        for (dio_mem, dio_off) in [(512u32, 512u32), (4096, 4096), (8, 8), (0, 0)] {
            assert!(
                direct_usable(false, dio_mem, dio_off, 9),
                "ring off must keep O_DIRECT (dio_mem={dio_mem}, dio_off={dio_off})"
            );
        }
    }

    #[test]
    fn ring_on_gates_direct_on_dword_alignment() {
        // Ring on: payloads may be 4-byte-aligned ring memory. Keep O_DIRECT
        // only for NVMe-class stores (mem align <= 4, offset align <= block).
        assert!(
            direct_usable(true, 4, 512, 9),
            "NVMe-class: 4-byte mem, 512 off"
        );
        assert!(direct_usable(true, 1, 1, 9), "byte-aligned store");
        assert!(
            !direct_usable(true, 512, 512, 9),
            "512-byte mem align too coarse"
        );
        assert!(
            !direct_usable(true, 4096, 4096, 12),
            "4K mem align too coarse"
        );
        assert!(
            !direct_usable(true, 0, 0, 9),
            "unreportable DIOALIGN -> buffered"
        );
        // Offset alignment coarser than the logical block is unusable.
        assert!(
            !direct_usable(true, 4, 8192, 9),
            "8K offset align > 512 block"
        );
        assert!(
            direct_usable(true, 4, 4096, 12),
            "4K offset align == 4K block ok"
        );
    }
}
