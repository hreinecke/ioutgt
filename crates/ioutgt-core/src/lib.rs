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
pub mod queue;
pub mod subsystem;

pub use backend::{Backend, BackendError, LbaRange};

/// Largest queue we accept (CAP.MQES advertises this minus one).
pub const MAX_QUEUE_ENTRIES: u16 = 1024;

/// In-capsule data we advertise via IOCCSZ (16 KiB, nvmet's default).
pub const INLINE_DATA_SIZE: u32 = 16 * 1024;

/// AEC bit: namespace-attribute-changed notices.
pub const AEN_CFG_NS_ATTR: u32 = 1 << 8;
