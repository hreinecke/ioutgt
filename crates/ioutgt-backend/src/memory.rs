//! RAM-backed backend for tests and protocol bring-up.
//!
//! The store is sharded into fixed 2 MiB chunks behind per-chunk RwLocks:
//! queue threads access disjoint hot ranges mostly without contention,
//! and allocation happens lazily on first write (reads of untouched
//! chunks return zeroes).

use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ioutgt_core::{Backend, BackendError, LbaRange};

const CHUNK_SHIFT: u32 = 21; // 2 MiB
const CHUNK_SIZE: usize = 1 << CHUNK_SHIFT;

/// See module docs.
pub struct MemoryBackend {
    block_shift: u8,
    nr_blocks: u64,
    chunks: Vec<RwLock<Option<Box<[u8]>>>>,
    /// Test-only artificial per-write completion delay (microseconds). A real
    /// SSD makes writes take time, so the data buffer feeding the write stays
    /// referenced for the op's duration; the instant memory backend hides any
    /// bug that depends on a buffer being held across a slow write. `0`
    /// (default) keeps writes synchronous. Set via
    /// [`MemoryBackend::set_write_delay_us`].
    write_delay_us: AtomicU64,
}

impl MemoryBackend {
    /// A zero-filled store of `size_bytes` rounded down to whole blocks.
    pub fn new(size_bytes: u64, block_shift: u8) -> Self {
        let nr_blocks = size_bytes >> block_shift;
        let bytes = nr_blocks << block_shift;
        let nr_chunks = usize::try_from(bytes.div_ceil(CHUNK_SIZE as u64)).expect("size fits");
        MemoryBackend {
            block_shift,
            nr_blocks,
            chunks: (0..nr_chunks).map(|_| RwLock::new(None)).collect(),
            write_delay_us: AtomicU64::new(0),
        }
    }

    /// Set the test-only artificial per-write delay in microseconds (see
    /// [`MemoryBackend::write_delay_us`]). Used to emulate a slow real disk so
    /// recv-side buffers stay referenced across the write, exposing
    /// hold-across-write bugs the instant default cannot.
    pub fn set_write_delay_us(&self, us: u64) {
        self.write_delay_us.store(us, Ordering::Relaxed);
    }

    /// Park the current op on the reactor timer for the configured write delay,
    /// if any. A no-op when the delay is 0 or no reactor timer is available.
    async fn write_delay(&self) {
        let us = self.write_delay_us.load(Ordering::Relaxed);
        if us == 0 {
            return;
        }
        if let Ok(sleep) = ioutgt_uring::ops::sleep(Duration::from_micros(us)) {
            let _ = sleep.await;
        }
    }

    /// Apply `f` to each (chunk, in-chunk range) overlapping
    /// `offset..offset+len`.
    fn for_each_chunk<F>(&self, offset: u64, len: usize, mut f: F) -> Result<(), BackendError>
    where
        F: FnMut(&RwLock<Option<Box<[u8]>>>, usize, usize, usize) -> Result<(), BackendError>,
    {
        let mut remaining = len;
        let mut pos = offset;
        let mut buf_off = 0usize;
        while remaining > 0 {
            let chunk_idx = usize::try_from(pos >> CHUNK_SHIFT).expect("bounded by size");
            let in_chunk = usize::try_from(pos & (CHUNK_SIZE as u64 - 1)).expect("< 2MiB");
            let take = remaining.min(CHUNK_SIZE - in_chunk);
            f(&self.chunks[chunk_idx], in_chunk, take, buf_off)?;
            pos += take as u64;
            buf_off += take;
            remaining -= take;
        }
        Ok(())
    }
}

impl Backend for MemoryBackend {
    fn block_shift(&self) -> u8 {
        self.block_shift
    }

    fn nr_blocks(&self) -> u64 {
        self.nr_blocks
    }

    async fn read(&self, slba: u64, buf: &mut [u8]) -> Result<(), BackendError> {
        self.check_range(slba, (buf.len() as u64) >> self.block_shift)?;
        let offset = slba << self.block_shift;
        self.for_each_chunk(offset, buf.len(), |chunk, in_chunk, take, buf_off| {
            let guard = chunk.read().expect("chunk poisoned");
            match guard.as_ref() {
                Some(data) => {
                    buf[buf_off..buf_off + take].copy_from_slice(&data[in_chunk..in_chunk + take]);
                }
                None => buf[buf_off..buf_off + take].fill(0),
            }
            Ok(())
        })
    }

    async fn write(&self, slba: u64, buf: &[u8]) -> Result<(), BackendError> {
        self.check_range(slba, (buf.len() as u64) >> self.block_shift)?;
        self.write_delay().await;
        let offset = slba << self.block_shift;
        self.for_each_chunk(offset, buf.len(), |chunk, in_chunk, take, buf_off| {
            let mut guard = chunk.write().expect("chunk poisoned");
            let data = guard.get_or_insert_with(|| vec![0u8; CHUNK_SIZE].into_boxed_slice());
            data[in_chunk..in_chunk + take].copy_from_slice(&buf[buf_off..buf_off + take]);
            Ok(())
        })
    }

    async fn flush(&self) -> Result<(), BackendError> {
        Ok(())
    }

    async fn write_zeroes(&self, range: LbaRange) -> Result<(), BackendError> {
        self.check_range(range.slba, u64::from(range.nlb))?;
        let offset = range.slba << self.block_shift;
        let len = usize::try_from(u64::from(range.nlb) << self.block_shift)
            .map_err(|_| BackendError::OutOfRange)?;
        self.for_each_chunk(offset, len, |chunk, in_chunk, take, _| {
            let mut guard = chunk.write().expect("chunk poisoned");
            if let Some(data) = guard.as_mut() {
                data[in_chunk..in_chunk + take].fill(0);
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block_on<F: Future>(fut: F) -> F::Output {
        // Backends are runtime-agnostic; a trivial poll loop suffices in
        // unit tests because Memory/Null never return Pending.
        let mut fut = std::pin::pin!(fut);
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        match fut.as_mut().poll(&mut cx) {
            std::task::Poll::Ready(v) => v,
            std::task::Poll::Pending => unreachable!("memory backend never pends"),
        }
    }

    #[test]
    fn write_read_roundtrip_across_chunks() {
        let be = MemoryBackend::new(8 << 20, 12); // 8 MiB, 4K blocks
        // Span the 2 MiB chunk boundary.
        let slba = (CHUNK_SIZE as u64 - 8192) >> 12;
        let data = vec![0xCDu8; 16384];
        block_on(be.write(slba, &data)).unwrap();
        let mut out = vec![0u8; 16384];
        block_on(be.read(slba, &mut out)).unwrap();
        assert_eq!(out, data);

        // Untouched region reads zero.
        let mut zeroes = vec![0xFFu8; 4096];
        block_on(be.read(0, &mut zeroes)).unwrap();
        assert!(zeroes.iter().all(|&b| b == 0));

        // Write zeroes erases.
        block_on(be.write_zeroes(LbaRange { slba, nlb: 4 })).unwrap();
        block_on(be.read(slba, &mut out)).unwrap();
        assert!(out.iter().all(|&b| b == 0));
    }

    #[test]
    fn out_of_range_rejected() {
        let be = MemoryBackend::new(1 << 20, 12); // 256 blocks
        let mut buf = vec![0u8; 4096];
        assert_eq!(
            block_on(be.read(256, &mut buf)).unwrap_err(),
            BackendError::OutOfRange
        );
        assert_eq!(block_on(be.read(255, &mut buf)), Ok(()));
        assert_eq!(
            block_on(be.write(u64::MAX, &buf)).unwrap_err(),
            BackendError::OutOfRange
        );
    }
}
