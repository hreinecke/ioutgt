//! Tokio current-thread runtime glued to the per-thread reactor.

use std::future::Future;
use std::io;
use std::rc::Rc;

use crate::reactor::{Reactor, RingConfig};

/// One queue thread's runtime: a Tokio current-thread scheduler whose park
/// primitive is the thread's io_uring.
///
/// Create it on the thread that will run it, then call [`block_on`]
/// (tasks may use `tokio::task::spawn_local`). Tokio's IO driver and time
/// driver are intentionally absent: all IO and all timers on this thread
/// go through the ring ([`crate::ops`]).
///
/// [`block_on`]: QueueRuntime::block_on
pub struct QueueRuntime {
    // Dropped first: cancels all tasks (orphaning their ops) before the
    // reactor drains and the ring closes.
    rt: tokio::runtime::Runtime,
    reactor: Rc<Reactor>,
}

impl QueueRuntime {
    /// Build the ring and the runtime on the current thread.
    pub fn new(config: RingConfig) -> io::Result<Self> {
        let reactor = Reactor::init(config)?;
        let rt = tokio::runtime::Builder::new_current_thread()
            .on_thread_park(Reactor::park_current)
            .build()
            .inspect_err(|_| Reactor::clear_current())?;
        Ok(QueueRuntime { rt, reactor })
    }

    /// Run a future to completion inside a fresh `LocalSet`.
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        let local = tokio::task::LocalSet::new();
        self.rt.block_on(local.run_until(future))
    }

    /// The thread's reactor (for [`Reactor::pending_ops`],
    /// [`Reactor::drain`], file registration).
    pub fn reactor(&self) -> &Rc<Reactor> {
        &self.reactor
    }
}

impl Drop for QueueRuntime {
    fn drop(&mut self) {
        // Release the thread-local now so a subsequent QueueRuntime can be
        // created on this thread; in-flight op drops during `rt` teardown
        // hold their own Rc<Reactor> and do not go through the
        // thread-local. Per-connection recv rings are owned by their
        // connection readers and already dropped (unregistering their
        // buf_ring + fixed buffers) by the time the connection tasks end.
        Reactor::clear_current();
    }
}
