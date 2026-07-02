//! Transport-independent NVMe target core.
//!
//! Owns the subsystem/controller/namespace model, the per-queue command-slot
//! array with one persistent async task per tag, admin and IO command
//! dispatch, and the `Backend` trait that storage backends implement.
//! Mirrors the role of `core.c`/`nvmet.h` in the Linux kernel nvmet target:
//! no transport or backend specifics live here.
//!
//! Everything is single-threaded per queue (`Rc`/`Cell`, no atomics); the
//! only cross-thread types are the configuration snapshots handed to queue
//! threads at startup and the controller registry.

pub mod admin;
pub mod backend;
pub mod buf;
pub mod controller;
pub mod dispatch;
pub mod fabrics_exec;
pub mod io;
pub mod permit;
pub mod pool;
pub mod queue;
pub mod slotq;
pub mod subsystem;

pub use backend::{Backend, BackendError, LbaRange};

/// Largest queue we accept (CAP.MQES advertises this minus one).
///
/// Slots no longer pin a per-slot data buffer (they lease on demand from
/// a shared per-queue [`pool::BufPool`]), so per-queue memory is bounded
/// by the pool size, not by `entries × MDTS`. The host sizes its queues
/// to `min(desired, MQES + 1)`; Connect requests beyond this are rejected
/// (a hostile host ignores the advertised MQES, so the limit is enforced,
/// not just advertised).
pub const MAX_QUEUE_ENTRIES: u16 = 256;

/// Maximum single-command transfer (MDTS): 2^5 × 4 KiB pages = 128 KiB,
/// matching the `mdts = 5` we advertise. Read/write transfers are
/// validated against this; a slot leases exactly the transfer size.
pub const MDTS_BYTES: u32 = 128 * 1024;

/// Cap on admin-command response data staged in a slot (identify/log
/// pages). The admin pool is sized `depth × ADMIN_DATA_MAX` so admin
/// leases never block.
pub const ADMIN_DATA_MAX: usize = 8 * 1024;

/// In-capsule data the RDMA transport advertises via IOCCSZ (one page,
/// matching nvmet-rdma's default `inline_data_size`): the host then embeds
/// write payloads up to this size in the command capsule itself, sparing the
/// target a per-write RDMA READ round trip. Must match the RDMA transport's
/// RECV capsule sizing.
pub const RDMA_INLINE_DATA_SIZE: u32 = 4096;

/// In-capsule data we advertise via IOCCSZ (16 KiB, nvmet's default).
pub const INLINE_DATA_SIZE: u32 = 16 * 1024;

/// AEC bit: namespace-attribute-changed notices.
pub const AEN_CFG_NS_ATTR: u32 = 1 << 8;
