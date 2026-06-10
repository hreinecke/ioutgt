//! Per-thread io_uring reactor and operation futures.
//!
//! One ring per queue thread, created with `SINGLE_ISSUER | DEFER_TASKRUN`.
//! Operations are futures whose state lives in a per-thread slab; the slab
//! key is the io_uring `user_data`. The reactor integrates with a Tokio
//! current-thread runtime via the `on_thread_park` hook: `io_uring_enter`
//! is the park primitive, so submission is batched and the steady-state
//! syscall count approaches zero under load.
//!
//! # Threading model
//!
//! Everything in this crate is deliberately thread-local and `!Send`:
//! a [`QueueRuntime`] owns one reactor and one Tokio current-thread runtime
//! on the thread that created it. The only cross-thread entry point is the
//! [`mailbox`] doorbell.
//!
//! # Cancellation contract
//!
//! Owned-buffer ops ([`ops::read_at`], [`ops::recv`], ...) are safe to drop
//! at any time: the buffer lives in the reactor slab until the kernel's
//! terminal CQE arrives. Raw-pointer ops ([`ops::recv_raw`], ...) require
//! the caller to keep the memory valid until the op completes or the
//! reactor is drained — see the per-function safety docs.
//!
//! See `docs/architecture.md` ("Reactor") for the full design.

mod cqe;
pub mod mailbox;
mod op;
pub mod ops;
mod probe;
mod reactor;
mod runtime;

pub use cqe::CqeResult;
pub use probe::{Features, probe};
pub use reactor::{Reactor, RingConfig};
pub use runtime::QueueRuntime;
