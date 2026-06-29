//! NVMe/RDMA transport for the ioutgt target (RoCE / InfiniBand via ibverbs).
//!
//! Work in progress. v1 target: discovery + connect + read/write over RC queue
//! pairs, with the host's keyed SGL driving target-issued RDMA READ (write
//! data) / RDMA WRITE (read data). This crate owns the RDMA-specific pieces
//! (memory registration, completion-queue draining via the reactor, RDMA-CM
//! connection acceptance, the recv/send loops) and reuses everything else
//! through [`ioutgt_harness`] and `ioutgt-core`.

mod verbs;

pub use verbs::rdma_devices;
