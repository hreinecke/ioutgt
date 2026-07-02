//! NVMe/RDMA transport for the ioutgt target (RoCE / InfiniBand via ibverbs):
//! discovery + connect + read/write over RC queue pairs, with the host's keyed
//! SGL driving target-issued RDMA READ (write data) / RDMA WRITE (read data),
//! running on the shared [`ioutgt_harness`] queue-thread pool. This crate owns
//! the RDMA-specific pieces (memory registration, completion-queue reaping via
//! the reactor, RDMA-CM connection acceptance, the per-queue reap loop) and
//! reuses everything else through the harness and `ioutgt-core`.

// Render via `Debug`, not `Display`: most sideway errors are `thiserror`
// structs whose `Display` is a fixed string with the real errno hidden in a
// `#[error(transparent)]` source — `Debug` keeps the kind/errno chain, which is
// what makes an RDMA bring-up failure (EINVAL/ENOMEM/EPERM) diagnosable.
pub(crate) fn oerr<E: std::error::Error>(e: E) -> std::io::Error {
    std::io::Error::other(format!("{e:?}"))
}

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
