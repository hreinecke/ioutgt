//! The per-thread reactor: ring ownership, op slab, park/reap loop.

use std::cell::{Cell, Ref, RefCell, RefMut};
use std::io;
use std::os::fd::RawFd;
use std::rc::Rc;

use io_uring::{IoUring, Probe, opcode, squeue, types};

/// Fixed-buffer table slots: the cap on data-buffer pools (one per
/// connection) live on one queue thread at once. Sized well above the
/// realistic connections-per-thread (≈ `ceil(io-queues / io-threads)`);
/// past it, further connections fall back to plain `readv`/`writev`.
/// Registering empty slots is cheap, so erring large costs little.
const MAX_REG_BUFS: u16 = 64;
/// Fixed-file table slots: the cap on distinct backend fds registered on one
/// queue thread at once. A handful in practice (one per backing store); sized
/// well above that. Past it, further fds fall back to plain (non-registered)
/// IO. Registered sparsely on the same ring as the buffer table (separate
/// tables).
const MAX_REG_FILES: u16 = 64;
use slab::Slab;

use crate::cqe::CqeResult;
use crate::op::{IGNORE_USER_DATA, OpEntry, Resources};

thread_local! {
    static CURRENT: RefCell<Option<Rc<Reactor>>> = const { RefCell::new(None) };
}

/// Backstop wait inside the park loop: bounds the damage of any missed
/// wakeup to 1 s without ever being the *intended* wake mechanism (CQEs
/// wake it long before this). Kept coarse so an idle thread re-parks at
/// 1 Hz rather than 10 Hz.
const PARK_SAFETY_SECS: u64 = 1;

/// Classifies a submitted SQE for the per-type counters. Most ops are
/// `Other`; network send/recv and backend storage read/write are split
/// out so the stats expose the op mix.
#[derive(Clone, Copy)]
pub(crate) enum SqeClass {
    Other,
    Send,
    Recv,
    Read,
    Write,
}

/// Lifetime ring counters for one queue thread. A snapshot of the
/// owning thread's `Cell` counters; obtainable only on that thread
/// (via [`crate::reactor_stats`] or [`Reactor::stats`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReactorStats {
    /// Park-hook waits (`submit_and_wait` from `on_thread_park`) — the
    /// thread's idle `io_uring_enter` count, and the syscall-batching
    /// denominator (`sqes / parks` ≈ ops per syscall).
    pub parks: u64,
    /// SQEs pushed to the submission ring (all op types).
    pub sqes: u64,
    /// Network send SQEs (Send/SendMsg/SendMsgZc) — a subset of `sqes`.
    pub send_sqes: u64,
    /// Network recv SQEs (Recv) — a subset of `sqes`.
    pub recv_sqes: u64,
    /// Backend storage read SQEs (file/bdev positional Read) — a subset
    /// of `sqes`. Zero for the memory/null backends (served in-CPU, no
    /// ring op).
    pub read_sqes: u64,
    /// Backend storage write SQEs (file/bdev positional Write) — a subset
    /// of `sqes`. Zero for the memory/null backends.
    pub write_sqes: u64,
    /// CQEs reaped from the completion ring.
    pub cqes: u64,
    /// Histogram of backend storage read+write SQEs carried by each ring
    /// submission (`io_uring_enter`), log2 buckets for 1 / 2 / 3-4 / 5-8 /
    /// 9-16 / 17+. Submissions carrying no backend IO (pure waits, network
    /// or poll traffic) are not recorded.
    pub rw_submit_hist: [u64; 6],
}

/// The live counters behind [`ReactorStats`]: plain `Cell`s, written
/// only by the owning thread — no atomics on the IO path.
#[derive(Default)]
struct StatCells {
    parks: Cell<u64>,
    sqes: Cell<u64>,
    send_sqes: Cell<u64>,
    recv_sqes: Cell<u64>,
    read_sqes: Cell<u64>,
    write_sqes: Cell<u64>,
    cqes: Cell<u64>,    /// Backend read+write SQEs per submission, log2-bucketed (see
    /// [`ReactorStats::rw_submit_hist`]).
    rw_submit_hist: [Cell<u64>; 6],
    /// `read_sqes + write_sqes` at the previous submission — the delta at
    /// each submit is that submission's backend-IO batch.
    last_rw: Cell<u64>,
}

impl StatCells {
    #[inline]
    /// Record the backend read/write SQEs this ring submission carries
    /// (the delta of the typed counters since the previous submission).
    fn note_submit(&self) {
        let rw = self.read_sqes.get() + self.write_sqes.get();
        let batch = rw - self.last_rw.get();
        self.last_rw.set(rw);
        let idx = match batch {
            0 => return,
            1 => 0,
            2 => 1,
            3..=4 => 2,
            5..=8 => 3,
            9..=16 => 4,
            _ => 5,
        };
        Self::bump(&self.rw_submit_hist[idx as usize]);
    }

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

/// A transport park-probe: drains a foreign completion source before the
/// thread sleeps (see [`Reactor::add_park_probe`]).
type ParkProbe = Box<dyn Fn() -> bool>;

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
    /// Transport park-probes (`add_park_probe`), run by [`Self::park`] before
    /// each sleep. A probe drains its own completion source (e.g. an RDMA CQ)
    /// and wakes tasks; returning `true` means it produced work and the park
    /// must not sleep. Returning `false` promises the probe armed its own
    /// wakeup (an event on an fd with a registered ring op) first.
    park_probes: RefCell<Vec<(u64, ParkProbe)>>,
    next_probe_id: Cell<u64>,
    /// Free indices into the ring's fixed-buffer table. Empty when the
    /// kernel lacks `READV_FIXED`/`WRITEV_FIXED` or sparse buffer
    /// registration — then [`Self::register_buffer`] returns `None` and the
    /// backend falls back to plain `readv`/`writev`. A non-empty pop
    /// therefore *implies* the fixed ops are usable.
    free_bufs: RefCell<Vec<u16>>,
    /// Whether the kernel supports the fixed-buffer table at all (distinct
    /// from `free_bufs` being momentarily empty because every slot is
    /// claimed). Lets callers tell "no kernel support" from "table full".
    fixed_supported: bool,
    /// Free indices into the ring's fixed-file table. Empty when the kernel
    /// lacks sparse file-table registration — then [`Self::register_file`]
    /// returns `None` and disk ops stay on the plain (non-registered) fd.
    free_files: RefCell<Vec<u16>>,
    /// Whether the kernel supports the fixed-file table at all (vs the free
    /// list being momentarily empty).
    files_supported: bool,
    /// Per-fd memo of fixed-file registration results: each backend fd is
    /// registered at most once per reactor, and a cached `None` (failure or
    /// no support) suppresses retries. A linear scan over a few entries beats
    /// a HashMap. Lives on the Reactor so it resets when the ring is rebuilt.
    file_index_cache: RefCell<Vec<(RawFd, Option<u16>)>>,
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
            // Enable fixed-buffer (`READV_FIXED`/`WRITEV_FIXED`) registration
            // when the kernel supports those ops AND a sparse buffer table.
            // The whole table is reserved once here; each connection pool
            // claims one slot for its arena. On any gap, leave the table
            // empty so the backend stays on plain readv/writev.
            let mut probe = Probe::new();
            let fixed_supported = ring.submitter().register_probe(&mut probe).is_ok()
                && probe.is_supported(opcode::ReadvFixed::CODE)
                && probe.is_supported(opcode::WritevFixed::CODE)
                && ring
                    .submitter()
                    .register_buffers_sparse(u32::from(MAX_REG_BUFS))
                    .is_ok();
            // Pop from the back so indices hand out 0, 1, 2, …
            let free_bufs = if fixed_supported {
                (0..MAX_REG_BUFS).rev().collect()
            } else {
                Vec::new()
            };
            // Reserve a sparse fixed-file table too (separate from the buffer
            // table). Backend fds register lazily into it; on any gap, leave it
            // empty so disk ops stay on plain (non-registered) fds.
            let files_supported = ring
                .submitter()
                .register_files_sparse(u32::from(MAX_REG_FILES))
                .is_ok();
            let free_files = if files_supported {
                (0..MAX_REG_FILES).rev().collect()
            } else {
                Vec::new()
            };
            let reactor = Rc::new(Reactor {
                ring: RefCell::new(ring),
                slab: RefCell::new(Slab::with_capacity(config.cq_entries as usize)),
                stats: StatCells::default(),
                park_probes: RefCell::new(Vec::new()),
                next_probe_id: Cell::new(0),
                free_bufs: RefCell::new(free_bufs),
                fixed_supported,
                free_files: RefCell::new(free_files),
                files_supported,
                file_index_cache: RefCell::new(Vec::new()),
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
    /// Register a transport park-probe (see the field doc on `park_probes`);
    /// returns an id for [`Self::remove_park_probe`]. The probe MUST be
    /// removed before the resources it polls are torn down.
    pub fn add_park_probe(&self, probe: ParkProbe) -> u64 {
        let id = self.next_probe_id.get();
        self.next_probe_id.set(id + 1);
        self.park_probes.borrow_mut().push((id, probe));
        id
    }

    /// Remove a probe registered by [`Self::add_park_probe`].
    pub fn remove_park_probe(&self, id: u64) {
        self.park_probes.borrow_mut().retain(|(pid, _)| *pid != id);
    }

    /// Run every park-probe; `true` if any produced work (the park must not
    /// sleep). All probes run even after one reports work, so every
    /// connection's completion source is drained per park cycle.
    fn run_park_probes(&self) -> bool {
        let probes = self.park_probes.borrow();
        let mut work = false;
        for (_, probe) in probes.iter() {
            work |= probe();
        }
        work
    }

    /// In-flight op count in the slab (kernel-visible ops).
    pub fn pending_ops(&self) -> usize {
        self.slab_ref().len()
    }

    /// Snapshot the lifetime ring counters (owning thread only).
    pub fn stats(&self) -> ReactorStats {
        ReactorStats {
            parks: self.stats.parks.get(),
            sqes: self.stats.sqes.get(),
            send_sqes: self.stats.send_sqes.get(),
            recv_sqes: self.stats.recv_sqes.get(),
            read_sqes: self.stats.read_sqes.get(),
            write_sqes: self.stats.write_sqes.get(),
            cqes: self.stats.cqes.get(),
            rw_submit_hist: std::array::from_fn(|i| self.stats.rw_submit_hist[i].get()),
        }
    }

    /// Zero the ring counters (owning thread only) — the stats-clear
    /// path; in-flight ops keep counting from zero.
    pub fn reset_stats(&self) {
        self.stats.parks.set(0);
        self.stats.sqes.set(0);
        self.stats.send_sqes.set(0);
        self.stats.recv_sqes.set(0);
        self.stats.read_sqes.set(0);
        self.stats.write_sqes.set(0);
        self.stats.cqes.set(0);
        for cell in &self.stats.rw_submit_hist {
            cell.set(0);
        }
        self.stats.last_rw.set(0);
    }

    /// Count a successfully pushed SQE: the total plus, for network
    /// send/recv and backend read/write, the per-type counter.
    #[inline]
    fn count_sqe(&self, class: SqeClass) {
        StatCells::bump(&self.stats.sqes);
        match class {
            SqeClass::Send => StatCells::bump(&self.stats.send_sqes),
            SqeClass::Recv => StatCells::bump(&self.stats.recv_sqes),
            SqeClass::Read => StatCells::bump(&self.stats.read_sqes),
            SqeClass::Write => StatCells::bump(&self.stats.write_sqes),
            SqeClass::Other => {}
        }
    }

    /// Reserve a slab entry, build the SQE with its key as `user_data`,
    /// and push it to the SQ ring (flushing with a submit syscall only if
    /// the ring is full).
    pub(crate) fn submit_op(
        &self,
        build: impl FnOnce(u64) -> squeue::Entry,
        resources: Resources,
        class: SqeClass,
    ) -> io::Result<usize> {
        let key = {
            let mut slab = self.slab.borrow_mut();
            let entry = slab.vacant_entry();
            let key = entry.key();
            entry.insert(OpEntry::new(resources));
            key
        };
        let sqe = build(key as u64);
        if let Err(err) = self.push_sqe(&sqe, class) {
            self.slab.borrow_mut().remove(key);
            return Err(err);
        }
        Ok(key)
    }

    fn push_sqe(&self, sqe: &squeue::Entry, class: SqeClass) -> io::Result<()> {
        let mut ring = self.ring.borrow_mut();
        // SAFETY: every pointer carried by the SQE refers to memory owned
        // by the corresponding slab entry (or by caller-guaranteed slot
        // memory for raw ops), which outlives the op by construction.
        unsafe {
            if ring.submission().push(sqe).is_ok() {
                self.count_sqe(class);
                return Ok(());
            }
        }
        // SQ full: flush to the kernel and retry once.
        self.stats.note_submit();
        ring.submit()?;
        // SAFETY: as above.
        unsafe {
            ring.submission()
                .push(sqe)
                .map_err(|_| io::Error::other("SQ ring full after flush"))?;
        }
        self.count_sqe(class);
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
        // Best effort: if the SQ is wedged the 1 s park backstop and
        // eventual completion still reclaim the entry.
        let _ = self.push_sqe(&cancel, SqeClass::Other);
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
            // Transport park-probes: drain foreign completion sources (RDMA
            // CQs) right here, at the only place this thread sleeps. A probe
            // that produced work woke its task, so return to the scheduler
            // instead of sleeping; a probe returning false has armed its own
            // wakeup, making the submit_and_wait below safe.
            if self.run_park_probes() {
                return;
            }
            if self.slab_ref().is_empty() {
                // Nothing in flight: nothing a CQE wait could wake.
                return;
            }
            let timeout = types::Timespec::new().sec(PARK_SAFETY_SECS);
            let args = types::SubmitArgs::new().timespec(&timeout);
            StatCells::bump(&self.stats.parks);
            self.stats.note_submit();
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

    /// Pin `ptr..ptr+len` as one fixed buffer and return its table index, or
    /// `None` when the fixed-buffer table is unavailable (kernel gap) or
    /// full. A returned index implies `READV_FIXED`/`WRITEV_FIXED` work, so
    /// callers treat `Some` as "use the fixed ops". The caller must
    /// [`unregister_buffer`](Self::unregister_buffer) before freeing the
    /// memory (queue teardown, after the op drain).
    pub fn register_buffer(&self, ptr: *const u8, len: usize) -> Option<u16> {
        let idx = self.free_bufs.borrow_mut().pop()?;
        let iov = libc::iovec {
            iov_base: ptr.cast_mut().cast(),
            iov_len: len,
        };
        // SAFETY: `ptr..ptr+len` is the pool arena, kept alive and unmoved
        // for the pool's lifetime; the slot is cleared in `unregister_buffer`
        // before the arena is freed, after the reactor has drained in-flight
        // ops that may reference it.
        let r = unsafe {
            self.ring
                .borrow_mut()
                .submitter()
                .register_buffers_update(u32::from(idx), &[iov], None)
        };
        if r.is_err() {
            self.free_bufs.borrow_mut().push(idx);
            return None;
        }
        Some(idx)
    }

    /// Whether the kernel supports the fixed-buffer table at all. A `None`
    /// from [`register_buffer`](Self::register_buffer) means "no support"
    /// when this is false, or "table full" when true.
    pub fn fixed_buffers_supported(&self) -> bool {
        self.fixed_supported
    }

    /// Release a fixed-buffer slot taken by [`register_buffer`](Self::register_buffer),
    /// clearing the kernel's pin and returning the index to the free list.
    pub fn unregister_buffer(&self, idx: u16) {
        let empty = libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 0,
        };
        // SAFETY: clearing slot `idx` with an empty iovec drops the prior pin.
        let _ = unsafe {
            self.ring.borrow_mut().submitter().register_buffers_update(
                u32::from(idx),
                &[empty],
                None,
            )
        };
        self.free_bufs.borrow_mut().push(idx);
    }

    /// Claim a fixed-file slot for `fd` and return its table index, or `None`
    /// when the table is unavailable (kernel gap) or full. Mirrors
    /// [`register_buffer`](Self::register_buffer).
    fn register_file(&self, fd: RawFd) -> Option<u16> {
        let idx = self.free_files.borrow_mut().pop()?;
        let r = self
            .ring
            .borrow_mut()
            .submitter()
            .register_files_update(u32::from(idx), &[fd]);
        if r.is_err() {
            self.free_files.borrow_mut().push(idx);
            return None;
        }
        Some(idx)
    }

    /// Fixed-file index for `fd`, lazily registering it on first use and
    /// memoizing the result (including failure, so we never retry per-IO).
    /// `Some(idx)` means disk ops may address `fd` via `types::Fixed`; `None`
    /// means fall back to the raw fd.
    ///
    /// No explicit unregister is needed: backend fds (held by the
    /// `Arc<AnyBackend>`) outlive every reactor, and the fixed-file
    /// registrations die when the ring drops at thread teardown — before the
    /// fd is ever closed. The cache lives on the Reactor, so it resets when the
    /// reactor is rebuilt.
    pub fn fixed_file_index(&self, fd: RawFd) -> Option<u16> {
        if let Some((_, r)) = self
            .file_index_cache
            .borrow()
            .iter()
            .find(|(cached, _)| *cached == fd)
        {
            return *r;
        }
        let r = self.register_file(fd);
        self.file_index_cache.borrow_mut().push((fd, r));
        r
    }

    /// Whether the kernel supports the fixed-file table at all — lets a `None`
    /// from [`fixed_file_index`](Self::fixed_file_index) be told apart from
    /// "table full".
    pub fn fixed_files_supported(&self) -> bool {
        self.files_supported
    }

    /// Register a provided-buffer ring (`bgid`) for incremental multishot
    /// recv. `ring_addr`/`entries` describe a page-aligned `io_uring_buf`
    /// array that must stay alive until `unregister_buf_ring`.
    ///
    /// # Safety
    /// Caller upholds the ring-memory lifetime contract.
    pub(crate) unsafe fn register_buf_ring(
        &self,
        ring_addr: u64,
        entries: u16,
        bgid: u16,
    ) -> io::Result<()> {
        // IOU_PBUF_RING_INC (== 2): the kernel keeps filling ONE buffer across
        // successive recvs (advancing its internal offset), only advancing
        // `head` once the buffer is full. Essential here: with only 2 buffers
        // and zero-copy write borrows pinning them, per-recv whole-buffer
        // consumption would exhaust the ring and deadlock the recv loop.
        // io-uring 0.7.12 re-exports IOU_PBUF_RING_INC as a c_uint, but
        // register_buf_ring_with_flags takes a u16 — pass the value directly.
        const INC_FLAG: u16 = 2;
        const _: () = assert!(io_uring::types::IOU_PBUF_RING_INC == INC_FLAG as _);
        // SAFETY: caller upholds the ring-memory lifetime contract.
        unsafe {
            self.ring
                .borrow()
                .submitter()
                .register_buf_ring_with_flags(ring_addr, entries, bgid, INC_FLAG)
        }
    }

    /// Unregister the provided-buffer ring `bgid` registered by
    /// [`register_buf_ring`](Self::register_buf_ring).
    pub(crate) fn unregister_buf_ring(&self, bgid: u16) -> io::Result<()> {
        self.ring.borrow().submitter().unregister_buf_ring(bgid)
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
