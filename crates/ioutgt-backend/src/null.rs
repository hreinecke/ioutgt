//! Null backend: reads return zeroes, writes are discarded. The
//! protocol-overhead measurement backend.

use ioutgt_core::{Backend, BackendError, LbaRange};

/// See module docs.
pub struct NullBackend {
    block_shift: u8,
    nr_blocks: u64,
}

impl NullBackend {
    /// `size_bytes` rounded down to a whole number of blocks.
    pub fn new(size_bytes: u64, block_shift: u8) -> Self {
        NullBackend {
            block_shift,
            nr_blocks: size_bytes >> block_shift,
        }
    }
}

impl Backend for NullBackend {
    fn block_shift(&self) -> u8 {
        self.block_shift
    }

    fn nr_blocks(&self) -> u64 {
        self.nr_blocks
    }

    async fn read(&self, slba: u64, buf: &mut [u8]) -> Result<(), BackendError> {
        self.check_range(slba, (buf.len() as u64) >> self.block_shift)?;
        buf.fill(0);
        Ok(())
    }

    async fn write(&self, slba: u64, buf: &[u8]) -> Result<(), BackendError> {
        self.check_range(slba, (buf.len() as u64) >> self.block_shift)?;
        Ok(())
    }

    async fn flush(&self) -> Result<(), BackendError> {
        Ok(())
    }

    async fn write_zeroes(&self, range: LbaRange) -> Result<(), BackendError> {
        self.check_range(range.slba, u64::from(range.nlb))
    }
}
