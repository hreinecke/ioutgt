//! Protocol-neutral queue engine: the per-queue command-slot array with
//! one persistent async task per tag ([`slotq`]), the shared buffer pool
//! ([`pool`]), connection permits ([`permit`]), aligned buffers
//! ([`buf`]), the [`Backend`] trait that storage backends implement, and
//! the generic per-connection context [`queue::QueueCore`].
//!
//! Nothing here knows any protocol: the NVMe model lives in
//! `ioutgt-nvme`, which layers on top of this crate. A future NBD
//! frontend would instantiate the same engine with its own command type
//! (`QueueCore<NbdReq>`).
//!
//! Everything is single-threaded per queue (`Rc`/`Cell`, no atomics).

pub mod backend;
pub mod buf;
pub mod permit;
pub mod pool;
pub mod queue;
pub mod slotq;

pub use backend::{Backend, BackendError, LbaRange};
