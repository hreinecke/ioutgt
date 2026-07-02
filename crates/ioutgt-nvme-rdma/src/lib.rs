//! NVMe/RDMA transport for the ioutgt target (RoCE / InfiniBand via ibverbs):
//! discovery + connect + read/write over RC queue pairs, with the host's keyed
//! SGL driving target-issued RDMA READ (write data) / RDMA WRITE (read data),
//! running on the shared [`ioutgt_harness`] queue-thread pool. This crate owns
//! the RDMA-specific pieces (memory registration, completion-queue reaping via
//! the reactor, RDMA-CM connection acceptance, the per-queue reap loop) and
//! reuses everything else through the harness and `ioutgt-core`.

pub mod cm;
pub mod cmproto;
pub mod cq;
pub mod target;
pub mod transport;
// Test-only scaffolding: RC-loopback resource helpers for the rxe gates. The
// production path builds its resources in `target`/`cm` directly on sideway.
#[cfg(test)]
mod verbs;
#[cfg(test)]
pub use verbs::{RcDest, Rdma, rdma_devices};
