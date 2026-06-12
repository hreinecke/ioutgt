//! The per-thread reactor: ring ownership, op slab, park/reap loop.

use std::cell::{Cell, Ref, RefCell, RefMut};
use std::io;
use std::rc::Rc;

use io_uring::{IoUring, opcode, squeue, types};
use slab::Slab;

use crate::cqe::CqeResult;
use crate::op::{IGNORE_USER_DATA, OpEntry, Resources};

thread_local! {
    static CURRENT: RefCell<Option<Rc<Reactor>>> = const { RefCell::new(None) };
}

/// Backstop wait inside the park loop: bounds the damage of any missed
/// wakeup to 100 ms without ever being the *intended* wake mechanism.
const PARK_SAFETY_NS: u32 = 100_000_000;

/// Lifetime ring counters for one queue thread. A snapshot of the
/// owning thread's `Cell` counters; obtainable only on that thread
/// (via [`crate::reactor_stats`] or [`Reactor::stats`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReactorStats {
    /// `io_uring_enter` syscalls (parks + SQ-full flushes + teardown
    /// waits).
    pub enters: u64,
    /// Park-hook waits (`submit_and_wait` from `on_thread_park`); a
    /// subset of `enters`.
    pub parks: u64,
    /// SQEs pushed to the submission ring.
    pub sqes: u64,
    /// CQEs reaped from the completion ring.
    pub cqes: u64,
}

/// The live counters behind [`ReactorStats`]: plain `Cell`s, written
/// only by the owning thread — no atomics on the IO path.
#[derive(Default)]
struct StatCells {
    enters: Cell<u64>,
    parks: Cell<u64>,
    sqes: Cell<u64>,
    cqes: Cell<u64>,
}

impl StatCells {
    #[inline]
    fn bump(cell: &Cell<u64>) {
        cell.set(cell.get() + 1);
    }
}

/// Ring geometry for one queue thread.
#[derive(Debug, Clone, Copy)]
pub struct RingConfig {
    /// SQ ring entries (power of two).
    pub sq_entries: u32,
    /// CQ ring entries; sized larger than the SQ for multishot headroom.
    pub cq_entries: u32,
}

impl Default for RingConfig {
    fn default() -> Self {
        RingConfig {
            sq_entries: 256,
            cq_entries: 1024,
        }
    }
}

/// Thread-local io_uring reactor.
///
/// Created via [`crate::QueueRuntime`]; ops reach it through the
/// thread-local handle. All methods are single-threaded by construction
/// (`Reactor` is neither `Send` nor `Sync`).
pub struct Reactor {
    // Field order is load-bearing: the ring must drop (and the kernel must
    // finish or cancel every in-flight op, see `Drop`) before the slab
    // frees the buffers those ops reference.
    ring: RefCell<IoUring>,
    slab: RefCell<Slab<OpEntry>>,
    stats: StatCells,
}

impl Reactor {
    /// Build the ring and install this reactor as the thread's current one.
    ///
    /// Fails if the thread already has a live reactor.
    pub(crate) fn init(config: RingConfig) -> io::Result<Rc<Reactor>> {
        CURRENT.with(|current| {
            let mut current = current.borrow_mut();
            if current.is_some() {
                return Err(io::Error::other(
                    "thread already has a ioutgt-uring reactor",
                ));
            }
            let ring = IoUring::builder()
                .setup_single_issuer()
                .setup_defer_taskrun()
                .setup_cqsize(config.cq_entries)
                .build(config.sq_entries)?;
            let reactor = Rc::new(Reactor {
                ring: RefCell::new(ring),
                slab: RefCell::new(Slab::with_capacity(config.cq_entries as usize)),
                stats: StatCells::default(),
            });
            *current = Some(Rc::clone(&reactor));
            Ok(reactor)
        })
    }

    pub(crate) fn clear_current() {
        CURRENT.with(|current| current.borrow_mut().take());
    }

    /// The current thread's reactor, if a [`crate::QueueRuntime`] is live.
    pub(crate) fn current() -> io::Result<Rc<Reactor>> {
        CURRENT
            .with(|current| current.borrow().clone())
            .ok_or_else(|| io::Error::other("no ioutgt-uring reactor on this thread"))
    }

    /// `on_thread_park` hook: park the thread inside `io_uring_enter`.
    pub(crate) fn park_current() {
        let reactor = CURRENT.with(|current| current.borrow().clone());
        if let Some(reactor) = reactor {
            reactor.park();
        }
    }

    pub(crate) fn slab_mut(&self) -> RefMut<'_, Slab<OpEntry>> {
        self.slab.borrow_mut()
    }

    fn slab_ref(&self) -> Ref<'_, Slab<OpEntry>> {
        self.slab.borrow()
    }

    /// Number of in-flight (not yet reaped-and-consumed) operations.
    /// Primarily for tests and teardown assertions.
    pub fn pending_ops(&self) -> usize {
        self.slab_ref().len()
    }

    /// Snapshot the lifetime ring counters (owning thread only).
    pub fn stats(&self) -> ReactorStats {
        ReactorStats {
            enters: self.stats.enters.get(),
            parks: self.stats.parks.get(),
            sqes: self.stats.sqes.get(),
            cqes: self.stats.cqes.get(),
        }
    }

    /// Zero the ring counters (owning thread only) — the stats-clear
    /// path; in-flight ops keep counting from zero.
    pub fn reset_stats(&self) {
        self.stats.enters.set(0);
        self.stats.parks.set(0);
        self.stats.sqes.set(0);
        self.stats.cqes.set(0);
    }

    /// Reserve a slab entry, build the SQE with its key as `user_data`,
    /// and push it to the SQ ring (flushing with a submit syscall only if
    /// the ring is full).
    pub(crate) fn submit_op(
        &self,
        build: impl FnOnce(u64) -> squeue::Entry,
        resources: Resources,
    ) -> io::Result<usize> {
        let key = {
            let mut slab = self.slab.borrow_mut();
            let entry = slab.vacant_entry();
            let key = entry.key();
            entry.insert(OpEntry::new(resources));
            key
        };
        let sqe = build(key as u64);
        if let Err(err) = self.push_sqe(&sqe) {
            self.slab.borrow_mut().remove(key);
            return Err(err);
        }
        Ok(key)
    }

    fn push_sqe(&self, sqe: &squeue::Entry) -> io::Result<()> {
        let mut ring = self.ring.borrow_mut();
        // SAFETY: every pointer carried by the SQE refers to memory owned
        // by the corresponding slab entry (or by caller-guaranteed slot
        // memory for raw ops), which outlives the op by construction.
        unsafe {
            if ring.submission().push(sqe).is_ok() {
                StatCells::bump(&self.stats.sqes);
                return Ok(());
            }
        }
        // SQ full: flush to the kernel and retry once.
        StatCells::bump(&self.stats.enters);
        ring.submit()?;
        // SAFETY: as above.
        unsafe {
            ring.submission()
                .push(sqe)
                .map_err(|_| io::Error::other("SQ ring full after flush"))?;
        }
        StatCells::bump(&self.stats.sqes);
        Ok(())
    }

    /// Mark an op whose future was dropped. The entry (and its resources)
    /// stays alive until the terminal CQE; a best-effort ASYNC_CANCEL
    /// nudges the kernel to produce that CQE soon.
    pub(crate) fn orphan(&self, key: usize) {
        {
            let mut slab = self.slab.borrow_mut();
            let Some(entry) = slab.get_mut(key) else {
                return;
            };
            if entry.terminated {
                slab.remove(key);
                return;
            }
            entry.orphaned = true;
            entry.waker = None;
        }
        let cancel = opcode::AsyncCancel::new(key as u64)
            .build()
            .user_data(IGNORE_USER_DATA);
        // Best effort: if the SQ is wedged the 100 ms park backstop and
        // eventual completion still reclaim the entry.
        let _ = self.push_sqe(&cancel);
    }

    /// Park the thread: submit pending SQEs and wait for at least one CQE,
    /// looping until some waker has been woken (or no ops remain).
    ///
    /// Called from Tokio's `on_thread_park`, i.e. only when no task is
    /// runnable. Every live op has registered a waker by then, so any
    /// reaped CQE translates into a wake and Tokio's own park returns
    /// immediately.
    pub(crate) fn park(&self) {
        loop {
            // CQEs may already be sitting in the ring (inline completions
            // posted during an SQ-full flush): consume before sleeping.
            if self.reap() > 0 {
                return;
            }
            if self.slab_ref().is_empty() {
                // Nothing in flight: nothing a CQE wait could wake.
                return;
            }
            let timeout = types::Timespec::new().nsec(PARK_SAFETY_NS);
            let args = types::SubmitArgs::new().timespec(&timeout);
            StatCells::bump(&self.stats.parks);
            StatCells::bump(&self.stats.enters);
            let res = self
                .ring
                .borrow_mut()
                .submitter()
                .submit_with_args(1, &args);
            match res {
                Ok(_) => {}
                Err(ref err)
                    if matches!(
                        err.raw_os_error(),
                        Some(libc::ETIME | libc::EINTR | libc::EBUSY)
                    ) => {}
                Err(err) => panic!("io_uring_enter failed: {err}"),
            }
            if self.reap() > 0 {
                return;
            }
        }
    }

    /// Drain the completion ring, routing each CQE to its slab entry.
    /// Returns the number of wakers woken.
    fn reap(&self) -> usize {
        let mut ring = self.ring.borrow_mut();
        let mut slab = self.slab.borrow_mut();
        let mut completion = ring.completion();
        completion.sync();
        let mut woken = 0;
        for cqe in &mut completion {
            StatCells::bump(&self.stats.cqes);
            let key = cqe.user_data();
            if key == IGNORE_USER_DATA {
                continue;
            }
            let Ok(key) = usize::try_from(key) else {
                debug_assert!(false, "CQE user_data out of range: {key}");
                continue;
            };
            let Some(entry) = slab.get_mut(key) else {
                debug_assert!(false, "CQE for unknown op {key}");
                continue;
            };
            let result = CqeResult {
                result: cqe.result(),
                flags: cqe.flags(),
            };
            if !result.more() {
                entry.terminated = true;
            }
            if entry.orphaned {
                if entry.terminated {
                    slab.remove(key);
                }
                continue;
            }
            entry.push_result(result);
            if let Some(waker) = entry.waker.take() {
                waker.wake();
                woken += 1;
            }
        }
        woken
    }

    /// Register files with the ring (fixed-file table). Phase-2 ops will
    /// address them by index.
    pub fn register_files(&self, fds: &[std::os::fd::RawFd]) -> io::Result<()> {
        self.ring.borrow_mut().submitter().register_files(fds)
    }

    /// Wait until every in-flight op has reached its terminal CQE.
    ///
    /// Use before tearing down memory referenced by raw ops (queue
    /// teardown). Sleeps are themselves ops, so the check excludes the op
    /// issued by the current iteration by sampling before sleeping.
    pub async fn drain(&self) {
        loop {
            if self.pending_ops() == 0 {
                return;
            }
            if let Ok(sleep) = crate::ops::sleep(std::time::Duration::from_micros(500)) {
                let _ = sleep.await;
            } else {
                return;
            }
        }
    }
}

impl Drop for Reactor {
    /// Closing the ring fd does not synchronously wait for in-flight ops
    /// (`io_ring_exit_work` is asynchronous), so reap until the slab is
    /// empty — all futures are gone by now, hence every entry is orphaned
    /// and already has a cancel queued or a completion coming.
    fn drop(&mut self) {
        for _ in 0..500 {
            if self.slab.borrow().is_empty() {
                return;
            }
            let timeout = types::Timespec::new().nsec(10_000_000);
            let args = types::SubmitArgs::new().timespec(&timeout);
            StatCells::bump(&self.stats.enters);
            let _ = self
                .ring
                .borrow_mut()
                .submitter()
                .submit_with_args(1, &args);
            self.reap();
        }
        // Leak rather than free memory the kernel may still write to.
        if !self.slab.borrow().is_empty() {
            let entries = std::mem::take(&mut *self.slab.borrow_mut());
            std::mem::forget(entries);
        }
    }
}
