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

/// Largest queue we accept (for NVMe, CAP.MQES advertises this minus one).
///
/// Slots no longer pin a per-slot data buffer (they lease on demand from
/// a shared per-queue [`pool::BufPool`]), so per-queue memory is bounded
/// by the pool size, not by `entries × MDTS`. The host sizes its queues
/// to `min(desired, MQES + 1)`; Connect requests beyond this are rejected
/// (a hostile host ignores the advertised MQES, so the limit is enforced,
/// not just advertised).
pub const MAX_QUEUE_ENTRIES: u16 = 256;

/// Maximum single-command transfer (for NVMe: MDTS, 2^5 × 4 KiB pages =
/// 128 KiB, matching the `mdts = 5` we advertise). Read/write transfers
/// are validated against this; a slot leases exactly the transfer size.
pub const MDTS_BYTES: u32 = 128 * 1024;
