//! File and block-device backend: vectored IO through the queue
//! thread's io_uring, O_DIRECT on a backing store that supports it,
//! buffered otherwise (e.g. tmpfs).
//!
//! One implementation serves both regular files and block devices
//! (geometry probing differs; the IO path is identical), mirroring how
//! little actually differs in userspace — unlike kernel nvmet's
//! bio-vs-kiocb split.
//!
//! A single fd is opened `O_DIRECT`, falling back to a plain buffered fd
//! when the store refuses it. The choice is fixed at open and needs no
//! alignment probing: our data buffers come from the page-granular slot
//! pool and every NVMe transfer is a logical-block multiple, so once an
//! O_DIRECT fd opens it serves *every* IO. (Sub-page-aligned buffers —
//! which would need the per-store `statx STATX_DIOALIGN` check and a
//! buffered fallback — only arise with a zero-copy recv ring, which this
//! backend does not yet receive into.)

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::Path;

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

/// File/bdev backend issuing vectored IO; O_DIRECT or buffered, fixed at
/// open. See module docs.
pub struct FileBackend {
    /// O_DIRECT when the store allowed it, else a plain buffered fd.
    fd: OwnedFd,
    kind: Kind,
    block_shift: u8,
    nr_blocks: u64,
    /// O_DIRECT is in effect (false ⇒ the store refused it, IO is buffered).
    direct: bool,
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
    /// Open `path` (regular file or block device) `O_DIRECT`, falling back
    /// to buffered IO where the store refuses direct.
    pub fn open(path: &Path, block_shift: u8) -> io::Result<FileBackend> {
        use std::os::unix::ffi::OsStrExt;
        let cpath = std::ffi::CString::new(path.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL in path"))?;

        let mut direct = true;
        // SAFETY: valid NUL-terminated path; flags are plain constants.
        let mut rfd = unsafe { libc::open(cpath.as_ptr(), libc::O_RDWR | libc::O_DIRECT) };
        if rfd < 0 && matches!(errno(), libc::EINVAL | libc::EOPNOTSUPP) {
            direct = false;
            // SAFETY: as above.
            rfd = unsafe { libc::open(cpath.as_ptr(), libc::O_RDWR) };
        }
        if rfd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: fresh fd, exclusively owned.
        let fd = unsafe { OwnedFd::from_raw_fd(rfd) };

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
        })
    }

    /// O_DIRECT is in effect for this backend's IO (decided at open).
    pub fn is_direct(&self) -> bool {
        self.direct
    }

    fn offset(&self, slba: u64) -> u64 {
        slba << self.block_shift
    }

    /// Issue one vectored read/write on the backend fd (O_DIRECT or
    /// buffered, fixed at open), resuming across short transfers.
    async fn rwv(
        &self,
        write: bool,
        slba: u64,
        iovs: &mut [libc::iovec],
        total: usize,
    ) -> Result<(), BackendError> {
        if total == 0 {
            return Ok(());
        }
        self.check_range(slba, (total as u64) >> self.block_shift)?;
        let base_off = self.offset(slba);
        let fd = self.fd.as_raw_fd();

        let mut done = 0usize;
        let mut idx = 0usize;
        while done < total {
            let off = base_off + done as u64;
            let ptr = iovs[idx..].as_ptr();
            #[allow(clippy::cast_possible_truncation)]
            let cnt = (iovs.len() - idx) as u32;
            // SAFETY: `iovs` and every buffer they point at outlive this
            // awaited op — `iovs` lives in the caller's frame (held across
            // the await) and the segment buffers are the caller's slot
            // memory, valid while the slot is Executing. The reactor's
            // orphan protocol holds the op entry to its terminal CQE on
            // whole-future drop, the same envelope as the other raw ops.
            let res = unsafe {
                if write {
                    ops::writev_at_raw(fd, ptr, cnt, off, 0)
                } else {
                    ops::readv_at_raw(fd, ptr, cnt, off, 0)
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
                Err(e) => return Err(map_errno(e.raw_os_error().unwrap_or(libc::EIO))),
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
        self.rwv(false, slba, &mut iovs, buf.len()).await
    }

    async fn write(&self, slba: u64, buf: &[u8]) -> Result<(), BackendError> {
        let mut iovs = [libc::iovec {
            iov_base: buf.as_ptr().cast_mut().cast(),
            iov_len: buf.len(),
        }];
        self.rwv(true, slba, &mut iovs, buf.len()).await
    }

    async fn read_segs(&self, slba: u64, segs: &[Seg], total: usize) -> Result<(), BackendError> {
        let mut iovs = [EMPTY_IOVEC; MAX_SEGS];
        let n = fill_iovecs(&mut iovs, segs, total);
        self.rwv(false, slba, &mut iovs[..n], total).await
    }

    async fn write_segs(&self, slba: u64, segs: &[Seg], total: usize) -> Result<(), BackendError> {
        let mut iovs = [EMPTY_IOVEC; MAX_SEGS];
        let n = fill_iovecs(&mut iovs, segs, total);
        self.rwv(true, slba, &mut iovs[..n], total).await
    }

    async fn flush(&self) -> Result<(), BackendError> {
        ops::fsync(self.fd.as_raw_fd(), true)
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
            if let Ok(op) = ops::fallocate(
                self.fd.as_raw_fd(),
                mode,
                self.offset(range.slba),
                len,
            ) {
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
                if let Ok(op) = ops::fallocate(
                    self.fd.as_raw_fd(),
                    mode,
                    self.offset(range.slba),
                    len,
                ) {
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
            let n = unsafe {
                ops::write_at_raw(self.fd.as_raw_fd(), chunk.as_ptr(), want, off)
            }
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
