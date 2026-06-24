//! The storage backend abstraction.
//!
//! Backends know nothing about NVMe: they expose block-addressed
//! read/write/flush/discard/write-zeroes on a fixed-geometry device.
//! `ioutgt-core` is generic over one `Backend` implementation; the binary
//! instantiates it with `ioutgt-backend`'s `AnyBackend` enum, keeping
//! dispatch monomorphized (no per-IO boxing) while allowing heterogeneous
//! namespaces.

use crate::pool::Seg;

/// A contiguous LBA range (discard / write-zeroes).
#[derive(Debug, Clone, Copy)]
#[allow(missing_docs)]
pub struct LbaRange {
    pub slba: u64,
    /// Number of logical blocks (1-based count).
    pub nlb: u32,
}

/// Backend failure, mapped to NVMe status by the dispatcher.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendError {
    /// Access beyond end of device.
    OutOfRange,
    /// Out of space (thin backing store).
    NoSpace,
    /// Operation not supported by this backend.
    Unsupported,
    /// IO error (errno).
    Io(i32),
}

/// Block storage provider for one namespace.
///
/// All methods take `&self`: backends are shared by every queue thread's
/// namespace map (each thread holds its own handle; implementations must
/// be `Send + Sync` and internally either lock-free or thread-local).
/// `async fn` here is not dyn-compatible by design — see module docs.
pub trait Backend: Send + Sync + 'static {
    /// log2 of the logical block size (9 = 512B, 12 = 4K).
    fn block_shift(&self) -> u8;

    /// Device capacity in logical blocks.
    fn nr_blocks(&self) -> u64;

    /// Read `buf.len()` bytes starting at logical block `slba`.
    fn read(&self, slba: u64, buf: &mut [u8]) -> impl Future<Output = Result<(), BackendError>>;

    /// Write `buf.len()` bytes starting at logical block `slba`.
    fn write(&self, slba: u64, buf: &[u8]) -> impl Future<Output = Result<(), BackendError>>;

    /// Read `total` bytes starting at logical block `slba` into `segs`,
    /// filling segments in order. Default: one [`Self::read`] per segment;
    /// backends that support vectored IO override with a single op.
    ///
    /// # Safety contract
    /// Each [`Seg`] must describe a buffer valid and exclusively borrowed
    /// for the duration of the returned future.
    fn read_segs(
        &self,
        slba: u64,
        segs: &[Seg],
        total: usize,
    ) -> impl Future<Output = Result<(), BackendError>> {
        async move {
            let mut remaining = total;
            let mut cur = slba;
            for seg in segs {
                if remaining == 0 {
                    break;
                }
                let take = remaining.min(seg.len);
                // SAFETY: per the method's safety contract, `seg` is a valid
                // exclusively-borrowed buffer of at least `seg.len` bytes.
                let s = unsafe { std::slice::from_raw_parts_mut(seg.ptr, take) };
                self.read(cur, s).await?;
                cur += (take as u64) >> self.block_shift();
                remaining -= take;
            }
            Ok(())
        }
    }

    /// Write `total` bytes drawn from `segs` (in order) starting at logical
    /// block `slba`. Default: one [`Self::write`] per segment; backends that
    /// support vectored IO override with a single op.
    ///
    /// # Safety contract
    /// As [`Self::read_segs`] (the buffers are read only).
    fn write_segs(
        &self,
        slba: u64,
        segs: &[Seg],
        total: usize,
    ) -> impl Future<Output = Result<(), BackendError>> {
        async move {
            let mut remaining = total;
            let mut cur = slba;
            for seg in segs {
                if remaining == 0 {
                    break;
                }
                let take = remaining.min(seg.len);
                // SAFETY: per the method's safety contract, `seg` is a valid
                // exclusively-borrowed buffer of at least `seg.len` bytes.
                let s = unsafe { std::slice::from_raw_parts(seg.ptr, take) };
                self.write(cur, s).await?;
                cur += (take as u64) >> self.block_shift();
                remaining -= take;
            }
            Ok(())
        }
    }

    /// Persist completed writes.
    fn flush(&self) -> impl Future<Output = Result<(), BackendError>>;

    /// Deallocate ranges (DSM AD). Default: accepted no-op, per spec
    /// (deallocate is a hint).
    fn discard(&self, ranges: &[LbaRange]) -> impl Future<Output = Result<(), BackendError>> {
        let _ = ranges;
        async { Ok(()) }
    }

    /// Write zeroes without data transfer.
    fn write_zeroes(&self, range: LbaRange) -> impl Future<Output = Result<(), BackendError>>;

    /// Bounds-check an LBA range against the device.
    fn check_range(&self, slba: u64, nlb: u64) -> Result<(), BackendError> {
        if slba
            .checked_add(nlb)
            .is_none_or(|end| end > self.nr_blocks())
        {
            return Err(BackendError::OutOfRange);
        }
        Ok(())
    }
}
