//! File and block-device backend: O_DIRECT IO through the queue
//! thread's io_uring.
//!
//! One implementation serves both regular files and block devices
//! (geometry probing differs; the IO path is identical), mirroring how
//! little actually differs in userspace — unlike kernel nvmet's
//! bio-vs-kiocb split.

use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::Path;

use ioutgt_core::{Backend, BackendError, LbaRange};
use ioutgt_uring::ops;

/// Backing kind, decided by `fstat` at open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Regular,
    Block,
}

/// O_DIRECT file/bdev backend. See module docs.
pub struct FileBackend {
    fd: OwnedFd,
    kind: Kind,
    block_shift: u8,
    nr_blocks: u64,
    /// O_DIRECT in effect (false: buffered fallback, e.g. tmpfs).
    direct: bool,
}

fn errno() -> i32 {
    io::Error::last_os_error()
        .raw_os_error()
        .unwrap_or(libc::EIO)
}

impl FileBackend {
    /// Open `path` (regular file or block device) with O_DIRECT,
    /// falling back to buffered IO where direct is unsupported.
    pub fn open(path: &Path, block_shift: u8) -> io::Result<FileBackend> {
        use std::os::unix::ffi::OsStrExt;
        let cpath = std::ffi::CString::new(path.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL in path"))?;

        let mut direct = true;
        // SAFETY: valid NUL-terminated path; flags are plain constants.
        let mut fd = unsafe { libc::open(cpath.as_ptr(), libc::O_RDWR | libc::O_DIRECT) };
        if fd < 0 && matches!(errno(), libc::EINVAL | libc::EOPNOTSUPP) {
            direct = false;
            // SAFETY: as above.
            fd = unsafe { libc::open(cpath.as_ptr(), libc::O_RDWR) };
        }
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: fresh fd, exclusively owned.
        let fd = unsafe { <OwnedFd as std::os::fd::FromRawFd>::from_raw_fd(fd) };

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

    /// O_DIRECT active (false means the filesystem refused it and IO is
    /// buffered — correct, just not the performance path).
    pub fn is_direct(&self) -> bool {
        self.direct
    }

    fn offset(&self, slba: u64) -> u64 {
        slba << self.block_shift
    }

    async fn rw(
        &self,
        write: bool,
        slba: u64,
        ptr: *mut u8,
        len: usize,
    ) -> Result<(), BackendError> {
        self.check_range(slba, (len as u64) >> self.block_shift)?;
        let mut done = 0usize;
        while done < len {
            let off = self.offset(slba) + done as u64;
            let want = u32::try_from(len - done).map_err(|_| BackendError::Io(libc::EINVAL))?;
            // SAFETY: ptr..ptr+len is the caller's slot buffer, valid and
            // exclusively borrowed for the duration of this future; queue
            // teardown drains executing slots before freeing it.
            let res = unsafe {
                if write {
                    ops::write_at_raw(self.fd.as_raw_fd(), ptr.add(done), want, off)
                } else {
                    ops::read_at_raw(self.fd.as_raw_fd(), ptr.add(done), want, off)
                }
            };
            let n = res
                .map_err(|e| BackendError::Io(e.raw_os_error().unwrap_or(libc::EIO)))?
                .await
                .map_err(|e| map_errno(e.raw_os_error().unwrap_or(libc::EIO)))?;
            if n == 0 {
                return Err(BackendError::Io(libc::EIO));
            }
            done += n as usize;
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
        self.rw(false, slba, buf.as_mut_ptr(), buf.len()).await
    }

    async fn write(&self, slba: u64, buf: &[u8]) -> Result<(), BackendError> {
        self.rw(true, slba, buf.as_ptr().cast_mut(), buf.len())
            .await
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
            if let Ok(op) = ops::fallocate(self.fd.as_raw_fd(), mode, self.offset(range.slba), len)
            {
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
                    ops::fallocate(self.fd.as_raw_fd(), mode, self.offset(range.slba), len)
                {
                    if op.await.is_ok() {
                        return Ok(());
                    }
                }
            }
        }
        // Fallback (and the block-device path until uring-cmd discard
        // lands): write zero chunks.
        let chunk = ioutgt_core::buf::AlignedBuf::zeroed(64 * 1024);
        let mut remaining = len;
        let mut off = self.offset(range.slba);
        while remaining > 0 {
            let want = u32::try_from(remaining.min(chunk.len() as u64)).expect("chunk-bounded");
            // SAFETY: chunk is alive across the await; read-only for the
            // kernel.
            let n = unsafe {
                ops::write_at_raw(self.fd.as_raw_fd(), chunk.as_ptr().cast_mut(), want, off)
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
