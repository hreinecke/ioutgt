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

pub mod bufring;
mod cqe;
pub mod mailbox;
mod op;
pub mod ops;
mod probe;
mod reactor;
mod runtime;
pub mod sendbatch;

pub use bufring::{BufRing, RecvChunk};
pub use cqe::CqeResult;
pub use ops::BackendFd;
pub use probe::{Features, probe};
pub use reactor::{Reactor, ReactorStats, RingConfig};
pub use runtime::QueueRuntime;

/// Lifetime ring counters of the current thread's reactor.
///
/// Errors if the thread has no live [`QueueRuntime`].
pub fn reactor_stats() -> std::io::Result<ReactorStats> {
    Ok(reactor::Reactor::current()?.stats())
}

/// Zero the current thread's ring counters (the stats-clear path).
///
/// Errors if the thread has no live [`QueueRuntime`].
pub fn reset_reactor_stats() -> std::io::Result<()> {
    reactor::Reactor::current()?.reset_stats();
    Ok(())
}

/// Register a park-probe on the current thread's reactor: a callback run by
/// the park hook before each sleep, letting a transport drain a foreign
/// completion source (e.g. an RDMA CQ) with no fd round-trip while the thread
/// is busy. Return `true` if the probe produced work (park then skips the
/// sleep); returning `false` promises the probe armed its own wakeup first.
/// Returns a handle id for [`remove_park_probe`]; the probe MUST be removed
/// before the resources it polls are torn down. Errors if the thread has no
/// live reactor.
pub fn add_park_probe(probe: Box<dyn Fn() -> bool>) -> std::io::Result<u64> {
    Ok(reactor::Reactor::current()?.add_park_probe(probe))
}

/// Remove a probe registered by [`add_park_probe`] (no-op without a reactor).
pub fn remove_park_probe(id: u64) {
    if let Ok(r) = reactor::Reactor::current() {
        r.remove_park_probe(id);
    }
}

/// Pin `ptr..ptr+len` as one fixed buffer on the current thread's reactor,
/// returning its index — `Some` means `READV_FIXED`/`WRITEV_FIXED` are usable
/// against it, `None` means fall back to plain readv/writev. Used to register
/// a connection's whole data-buffer pool arena at install. See
/// [`Reactor::register_buffer`].
///
/// The returned index is valid only on the reactor that minted it (the
/// calling thread's). That is sound because a pool lease carrying the index
/// is produced and consumed entirely on its owning queue thread (task-per-tag
/// on the same current-thread runtime), and reactor handles are `!Send`, so an
/// index can never reach another thread's ring.
pub fn register_pool_buffer(ptr: *const u8, len: usize) -> Option<u16> {
    reactor::Reactor::current().ok()?.register_buffer(ptr, len)
}

/// Whether the current thread's reactor supports the fixed-buffer table —
/// lets a `None` from [`register_pool_buffer`] be reported as "no kernel
/// support" vs "table full". `false` if there is no live reactor.
pub fn fixed_buffers_supported() -> bool {
    reactor::Reactor::current()
        .map(|r| r.fixed_buffers_supported())
        .unwrap_or(false)
}

/// Release a fixed-buffer index from [`register_pool_buffer`] at teardown.
pub fn unregister_pool_buffer(idx: u16) {
    if let Ok(r) = reactor::Reactor::current() {
        r.unregister_buffer(idx);
    }
}

/// Fixed-file table index for `fd` on the current thread's reactor, lazily
/// registering it on first use — `Some(idx)` means disk ops may address `fd`
/// via [`BackendFd::Fixed`], `None` means use [`BackendFd::Raw`]. `None` when
/// there is no live reactor. See [`Reactor::fixed_file_index`].
///
/// As with [`register_pool_buffer`], the returned index is valid only on the
/// reactor that minted it (the calling thread's); reactor handles are `!Send`,
/// so an index can never reach another thread's ring.
pub fn fixed_file_index(fd: std::os::fd::RawFd) -> Option<u16> {
    reactor::Reactor::current()
        .ok()
        .and_then(|r| r.fixed_file_index(fd))
}

/// Whether the current thread's reactor supports the fixed-file table — lets a
/// `None` from [`fixed_file_index`] be reported as "no kernel support" vs
/// "table full". `false` if there is no live reactor.
pub fn fixed_files_supported() -> bool {
    reactor::Reactor::current()
        .map(|r| r.fixed_files_supported())
        .unwrap_or(false)
}
