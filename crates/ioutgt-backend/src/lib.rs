//! Storage backends implementing [`ioutgt_core::Backend`].
//!
//! `NullBackend` (discard writes, zero reads), `MemoryBackend`
//! (RAM-backed, for tests and protocol bring-up); `FileBackend`
//! (O_DIRECT) and `BlockBackend` arrive with the backend milestone.
//! [`AnyBackend`] is the closed enum the binary instantiates
//! `ioutgt-core` with: dispatch stays monomorphized (no per-IO boxing)
//! while namespaces stay heterogeneous.

mod file;
mod memory;
mod null;
mod sheepdog;

pub use file::FileBackend;
pub use memory::MemoryBackend;
pub use null::NullBackend;
pub use sheepdog::{AclInfo, SD_LISTEN_PORT, SheepdogBackend, VdiInfo, list_acls, list_vdis};

use ioutgt_core::pool::Seg;
use ioutgt_core::{Backend, BackendError, LbaRange};

/// All compiled-in backends, for heterogeneous namespace maps.
pub enum AnyBackend {
    /// See [`NullBackend`].
    Null(NullBackend),
    /// See [`MemoryBackend`].
    Memory(MemoryBackend),
    /// See [`FileBackend`] (regular files and block devices).
    File(FileBackend),
    /// See [`SheepdogBackend`] (a VDI on a Sheepdog cluster).
    Sheepdog(SheepdogBackend),
}

impl Backend for AnyBackend {
    fn block_shift(&self) -> u8 {
        match self {
            AnyBackend::Null(b) => b.block_shift(),
            AnyBackend::Memory(b) => b.block_shift(),
            AnyBackend::File(b) => b.block_shift(),
            AnyBackend::Sheepdog(b) => b.block_shift(),
        }
    }

    fn nr_blocks(&self) -> u64 {
        match self {
            AnyBackend::Null(b) => b.nr_blocks(),
            AnyBackend::Memory(b) => b.nr_blocks(),
            AnyBackend::File(b) => b.nr_blocks(),
            AnyBackend::Sheepdog(b) => b.nr_blocks(),
        }
    }

    async fn read(&self, slba: u64, buf: &mut [u8]) -> Result<(), BackendError> {
        match self {
            AnyBackend::Null(b) => b.read(slba, buf).await,
            AnyBackend::Memory(b) => b.read(slba, buf).await,
            AnyBackend::File(b) => b.read(slba, buf).await,
            AnyBackend::Sheepdog(b) => b.read(slba, buf).await,
        }
    }

    async fn write(&self, slba: u64, buf: &[u8]) -> Result<(), BackendError> {
        match self {
            AnyBackend::Null(b) => b.write(slba, buf).await,
            AnyBackend::Memory(b) => b.write(slba, buf).await,
            AnyBackend::File(b) => b.write(slba, buf).await,
            AnyBackend::Sheepdog(b) => b.write(slba, buf).await,
        }
    }

    async fn read_segs(
        &self,
        slba: u64,
        segs: &[Seg],
        total: usize,
        buf_index: Option<u16>,
    ) -> Result<(), BackendError> {
        match self {
            AnyBackend::Null(b) => b.read_segs(slba, segs, total, buf_index).await,
            AnyBackend::Memory(b) => b.read_segs(slba, segs, total, buf_index).await,
            AnyBackend::File(b) => b.read_segs(slba, segs, total, buf_index).await,
            AnyBackend::Sheepdog(b) => b.read_segs(slba, segs, total, buf_index).await,
        }
    }

    async fn write_segs(
        &self,
        slba: u64,
        segs: &[Seg],
        total: usize,
        buf_index: Option<u16>,
    ) -> Result<(), BackendError> {
        match self {
            AnyBackend::Null(b) => b.write_segs(slba, segs, total, buf_index).await,
            AnyBackend::Memory(b) => b.write_segs(slba, segs, total, buf_index).await,
            AnyBackend::File(b) => b.write_segs(slba, segs, total, buf_index).await,
            AnyBackend::Sheepdog(b) => b.write_segs(slba, segs, total, buf_index).await,
        }
    }

    async fn flush(&self) -> Result<(), BackendError> {
        match self {
            AnyBackend::Null(b) => b.flush().await,
            AnyBackend::Memory(b) => b.flush().await,
            AnyBackend::File(b) => b.flush().await,
            AnyBackend::Sheepdog(b) => b.flush().await,
        }
    }

    async fn discard(&self, ranges: &[LbaRange]) -> Result<(), BackendError> {
        match self {
            AnyBackend::Null(b) => b.discard(ranges).await,
            AnyBackend::Memory(b) => b.discard(ranges).await,
            AnyBackend::File(b) => b.discard(ranges).await,
            AnyBackend::Sheepdog(b) => b.discard(ranges).await,
        }
    }

    async fn write_zeroes(&self, range: LbaRange) -> Result<(), BackendError> {
        match self {
            AnyBackend::Null(b) => b.write_zeroes(range).await,
            AnyBackend::Memory(b) => b.write_zeroes(range).await,
            AnyBackend::File(b) => b.write_zeroes(range).await,
            AnyBackend::Sheepdog(b) => b.write_zeroes(range).await,
        }
    }
}
