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
//! threads at startup.

pub mod backend;
pub mod controller;
pub mod dispatch;
pub mod queue;
pub mod subsystem;

pub use backend::{Backend, BackendError, LbaRange};
