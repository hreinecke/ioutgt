//! Provided-buffer ring for multishot receive.
//!
//! A `BufRing` owns a kernel-registered ring of fixed-size receive
//! buffers (the `bgid` group). A multishot `RECV` ([`crate::ops::recv_multi`])
//! draws one buffer per completion from this group instead of taking a
//! caller buffer per op, so a single submitted op keeps receiving as data
//! arrives. After consuming a buffer's bytes the caller hands it back with
//! [`BufRing::recv_done`]; running the group dry terminates the multishot
//! with `-ENOBUFS`, after which the caller replenishes and re-arms.
//!
//! One ring serves exactly ONE connection: each connection that enables the
//! ring creates and owns its own `BufRing` (its own `bgid`, its own 2
//! sub-buffers, its own offset/waker state). A recv CQE reports only
//! `(bid, len)`, not the per-buffer consume offset; with a single consumer
//! per ring the running offset can be tracked locally and can never desync.
//! (Sharing one ring across connections is unsafe: their tasks do not drain
//! CQEs in completion order, so a shared offset would cross connections'
//! bytes.) `bgid`s are handed out by a thread-local allocator
//! (`alloc_bgid`) so the connections multiplexed onto one io_uring get
//! distinct group ids; the id is freed back on [`BufRing`] drop.
//!
//! Layout follows the kernel ABI: an array of `io_uring_buf` entries whose
//! first entry's `resv` field doubles as the shared tail (see
//! [`BufRingEntry::tail`]). We publish provided buffers by writing the entry
//! then storing the advanced tail with release ordering.

use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::cell::{Cell, RefCell};
use std::io;
use std::rc::Rc;
use std::sync::atomic::{AtomicU16, Ordering};

use io_uring::types::BufRingEntry;

use crate::reactor::Reactor;

const PAGE: usize = 4096;

/// Sub-buffers per ring — exactly 2, the minimum that double-buffers (one
/// filled by the kernel while the other is consumed). Fixed, not configurable:
/// the per-buffer state arrays are `[_; 2]` and `BUF_MASK` (= `NBUFS - 1`)
/// drives `bid & BUF_MASK` indexing.
const NBUFS: u16 = 2;
const BUF_MASK: u16 = NBUFS - 1;

/// A page-aligned zeroed heap allocation.
struct AlignedMem {
    ptr: *mut u8,
    layout: Layout,
}

impl AlignedMem {
    fn zeroed(len: usize) -> AlignedMem {
        let layout = Layout::from_size_align(len.max(1), PAGE).expect("valid layout");
        // SAFETY: non-zero size; null checked below.
        let ptr = unsafe { alloc_zeroed(layout) };
        assert!(!ptr.is_null(), "buf_ring allocation failed");
        AlignedMem { ptr, layout }
    }
}

impl Drop for AlignedMem {
    fn drop(&mut self) {
        // SAFETY: allocated with this exact layout.
        unsafe { dealloc(self.ptr, self.layout) };
    }
}

/// One received chunk: which ring buffer the kernel filled, how many
/// bytes, and whether the multishot is still armed.
#[derive(Debug, Clone, Copy)]
pub struct RecvChunk {
    /// Buffer id (index into the ring's data arena).
    pub bid: u16,
    /// Bytes received into the buffer.
    pub len: u32,
    /// `IORING_CQE_F_MORE`: the multishot recv remains armed.
    pub more: bool,
    /// `IORING_CQE_F_BUF_MORE`: with an incremental ring, this buffer was
    /// only partially consumed and will receive MORE completions (same bid,
    /// next offset). When clear, the kernel advanced `head` — the buffer is
    /// fully consumed and the app must re-provide it.
    pub buf_more: bool,
}

/// A registered provided-buffer ring. Single-threaded (`Cell`, no
/// atomics on our side beyond the kernel-shared tail).
pub struct BufRing {
    reactor: Rc<Reactor>,
    bgid: u16,
    /// Whether `bgid` came from the thread-local [`alloc_bgid`] pool and must
    /// be returned to it on drop. Connection rings own their id; test rings
    /// built with an explicit `bgid` via [`BufRing::new`] do not.
    bgid_owned: bool,
    /// Entry array (`NBUFS × size_of::<BufRingEntry>()`), page-aligned.
    ring: AlignedMem,
    /// Buffer data arena (`NBUFS × buf_size`), page-aligned.
    data: AlignedMem,
    buf_size: u32,
    /// Our cached copy of the ring tail.
    tail: Cell<u16>,
    /// Number of in-flight write borrows per buffer (indexed by bid & BUF_MASK).
    pending: [Cell<u32>; 2],
    /// Whether the recv loop has finished with this buffer and is waiting
    /// for all borrows to drain before returning it to the kernel.
    awaiting: [Cell<bool>; 2],
    /// Running per-buffer consume offset under the incremental ring
    /// (`IOU_PBUF_RING_INC`): a recv CQE reports only `(bid, len)` and data
    /// lands at `buf(bid) + recv_off[bid]`. Reset to 0 on re-provide. (Single
    /// consumer ⇒ authoritative; see module docs.)
    recv_off: [Cell<usize>; 2],
    /// Bumped every time a buffer is (re-)provided to the kernel. The recv
    /// loop parks on this when a multishot recv drains the ring (`-ENOBUFS`)
    /// and wakes to retry once a buffer returns — instead of busy re-arming.
    provide_gen: Cell<u64>,
    /// Waker for the recv loop parked in [`BufRing::wait_for_provide`]. One
    /// connection owns this ring, so there is exactly one recv loop that can
    /// park on ENOBUFS — a single slot suffices. Lives only on the
    /// back-pressure (park/unpark) edge, never the steady-state IO path.
    provide_waker: RefCell<Option<std::task::Waker>>,
    /// io_uring fixed-buffer index for each sub-buffer's data region, so the
    /// backend can `WRITE_FIXED` received write payloads straight from ring
    /// memory. `None` when the kernel lacks fixed buffers or the table is full
    /// (the backend then falls back to plain writev from the ring pointer).
    buf_index: [Option<u16>; 2],
    #[cfg(any(test, feature = "test-helpers"))]
    provided_count: [Cell<u32>; 2],
}

impl BufRing {
    /// Create and register a buffer ring on the current reactor with an
    /// explicit group id `bgid`, using exactly 2 buffers of `ring_bytes / 2`
    /// each (page-rounded down, minimum 1 page). All buffers are provided to
    /// the kernel up front. The `bgid` is caller-chosen and not freed to the
    /// thread-local pool on drop — for tests and callers that manage ids
    /// themselves. Fixed-buffer registration is best-effort here (`buf_index`
    /// entries may be `None`); production connections use
    /// [`for_connection`](Self::for_connection), which requires it.
    pub fn new(bgid: u16, ring_bytes: usize) -> io::Result<Rc<BufRing>> {
        Self::build(bgid, false, false, ring_bytes)
    }

    /// Create a ring for a single connection, allocating its own `bgid` from
    /// the thread-local pool. Returns `None` — so the caller falls back to the
    /// classic recv path — when the bgid pool is exhausted, a sub-buffer can't
    /// be registered as a fixed buffer (table full / unsupported), or the
    /// kernel can't register the buf_ring. Any partial setup is fully unwound
    /// (fixed buffers unregistered, bgid returned) before returning `None`.
    pub fn for_connection(ring_bytes: usize) -> Option<Rc<BufRing>> {
        let bgid = alloc_bgid()?;
        match Self::build(bgid, true, true, ring_bytes) {
            Ok(ring) => Some(ring),
            Err(_) => {
                free_bgid(bgid);
                None
            }
        }
    }

    /// Shared constructor. `owned` records whether `bgid` must be freed to the
    /// thread-local pool on drop. When `require_fixed`, a failed sub-buffer
    /// fixed-buffer registration aborts construction (unwinding any earlier
    /// registration); otherwise it is tolerated and the slot's `buf_index` is
    /// left `None`.
    fn build(
        bgid: u16,
        owned: bool,
        require_fixed: bool,
        ring_bytes: usize,
    ) -> io::Result<Rc<BufRing>> {
        let reactor = Reactor::current()?;
        let half = (ring_bytes / 2) & !(PAGE - 1);
        let buf_size = half.max(PAGE);
        let ring = AlignedMem::zeroed(NBUFS as usize * size_of::<BufRingEntry>());
        let data = AlignedMem::zeroed(NBUFS as usize * buf_size);

        // Register each sub-buffer's data region as a fixed buffer so the
        // backend can WRITE_FIXED received write payloads straight from ring
        // memory (no per-write mapping). On failure, `require_fixed` callers
        // unwind every slot registered so far and abort; the BufRing struct is
        // not built, so its Drop never runs — we must unregister by hand here.
        let mut buf_index: [Option<u16>; 2] = [None, None];
        for (i, slot) in buf_index.iter_mut().enumerate() {
            // SAFETY: `data` covers entries × buf_size; sub-buffer i is within it.
            let ptr = unsafe { data.ptr.add(i * buf_size) };
            match reactor.register_buffer(ptr, buf_size) {
                Some(idx) => *slot = Some(idx),
                None if require_fixed => {
                    for idx in buf_index.into_iter().flatten() {
                        reactor.unregister_buffer(idx);
                    }
                    return Err(io::Error::other("fixed-buffer table full"));
                }
                None => {}
            }
        }

        // SAFETY: `ring` stays alive until this BufRing drops, which
        // unregisters the bgid first (see Drop). On failure, unwind the
        // fixed buffers registered above before returning (Drop won't run).
        if let Err(e) = unsafe { reactor.register_buf_ring(ring.ptr as u64, NBUFS, bgid) } {
            for idx in buf_index.into_iter().flatten() {
                reactor.unregister_buffer(idx);
            }
            return Err(e);
        }

        let br = Rc::new(BufRing {
            reactor,
            bgid,
            bgid_owned: owned,
            ring,
            data,
            #[allow(clippy::cast_possible_truncation)] // buf_size is page-aligned; max practical value fits u32
            buf_size: buf_size as u32,
            tail: Cell::new(0),
            pending: [Cell::new(0), Cell::new(0)],
            awaiting: [Cell::new(false), Cell::new(false)],
            recv_off: [Cell::new(0), Cell::new(0)],
            provide_gen: Cell::new(0),
            provide_waker: RefCell::new(None),
            buf_index,
            #[cfg(any(test, feature = "test-helpers"))]
            provided_count: [Cell::new(0), Cell::new(0)],
        });
        for bid in 0..NBUFS {
            br.kernel_provide(bid);
        }
        // Reset test counters so they reflect only post-construction provides.
        #[cfg(any(test, feature = "test-helpers"))]
        {
            for i in 0..NBUFS as usize {
                br.provided_count[i].set(0);
            }
        }
        Ok(br)
    }

    /// The buffer group id to pass to `recv_multi`.
    pub fn bgid(&self) -> u16 {
        self.bgid
    }

    /// Number of buffers (== ring entries).
    pub fn nbufs(&self) -> u16 {
        NBUFS
    }

    /// Per-buffer capacity in bytes.
    pub fn buf_size(&self) -> u32 {
        self.buf_size
    }

    /// The io_uring fixed-buffer index for sub-buffer `bid`, if it was
    /// registered. `Some` ⇒ the backend can `WRITE_FIXED` received data from
    /// this sub-buffer; `None` ⇒ fall back to plain writev from `buf(bid)`.
    pub fn buf_index(&self, bid: u16) -> Option<u16> {
        self.buf_index[(bid & BUF_MASK) as usize]
    }

    /// Start of buffer `bid`'s data.
    pub fn buf(&self, bid: u16) -> *mut u8 {
        // SAFETY: bid < entries, so the offset stays within the arena.
        unsafe { self.data.ptr.add(bid as usize * self.buf_size as usize) }
    }

    /// Hand buffer `bid` (back) to the kernel: write its ring entry, then
    /// publish the advanced tail with release ordering.
    fn kernel_provide(&self, bid: u16) {
        // A buffer must never go back to the kernel while a zero-copy write
        // still borrows a region of it — the kernel would refill bytes an
        // in-flight WRITE_FIXED is still reading. Every provide site (init,
        // recv_done, release) is supposed to gate on `pending == 0`.
        debug_assert_eq!(
            self.pending[(bid & BUF_MASK) as usize].get(),
            0,
            "re-provide of borrowed sub-buffer bid={bid}"
        );
        let tail = self.tail.get();
        let idx = (tail & BUF_MASK) as usize;
        let base = self.ring.ptr.cast::<BufRingEntry>();
        // SAFETY: idx < entries; `base` is our registered ring memory, and
        // the kernel has not been shown slot `idx` yet (tail not advanced),
        // so this &mut is exclusive for its lifetime.
        let entry = unsafe { &mut *base.add(idx) };
        entry.set_addr(self.buf(bid) as u64);
        entry.set_len(self.buf_size);
        entry.set_bid(bid);
        let new_tail = tail.wrapping_add(1);
        self.tail.set(new_tail);
        // SAFETY: the tail lives in entry[0].resv (2-byte aligned); it is
        // shared with the kernel, so publish with a release store.
        let tail_ptr = unsafe { BufRingEntry::tail(base.cast_const()) }.cast_mut();
        // SAFETY: `tail_ptr` is a valid, aligned u16 inside our ring memory.
        unsafe { AtomicU16::from_ptr(tail_ptr).store(new_tail, Ordering::Release) };
        let i = (bid & BUF_MASK) as usize;
        self.awaiting[i].set(false);
        // Fresh buffer: the kernel restarts its per-buffer offset at 0, so the
        // shared running offset must reset in lockstep.
        self.recv_off[i].set(0);
        #[cfg(any(test, feature = "test-helpers"))]
        self.provided_count[i].set(self.provided_count[i].get() + 1);
        // A buffer is available again: wake the recv loop if it parked on
        // ENOBUFS. Take the waker (dropping the borrow) BEFORE waking, so a
        // woken task re-entering `wait_for_provide` cannot deadlock on the
        // RefCell. The waker re-checks its `since` snapshot on re-poll and
        // re-parks if its generation is stale, so a spurious wakeup is harmless.
        self.provide_gen.set(self.provide_gen.get().wrapping_add(1));
        let waker = self.provide_waker.borrow_mut().take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Current provide generation (bumped on every re-provide); snapshot at
    /// recv-arm time and pass to [`wait_for_provide`](Self::wait_for_provide).
    pub fn provide_gen(&self) -> u64 {
        self.provide_gen.get()
    }

    /// Park until a buffer is (re-)provided, relative to the `since` generation
    /// the caller snapshotted when it armed the recv that has now drained the
    /// ring (`-ENOBUFS`). Resolves immediately if any provide has happened
    /// since `since` — the buffers are already back and the caller should just
    /// re-arm; otherwise it parks until `kernel_provide`
    /// signals one came back.
    ///
    /// Snapshotting at arm time (not at park time) is essential: a multishot
    /// recv runs ahead in the kernel and can fill and exhaust every provided
    /// buffer — posting the terminal ENOBUFS CQE — before userspace drains the
    /// buffer CQEs that precede it. Userspace re-provides those buffers (bumping
    /// the generation) as it drains, then finally observes the already-queued
    /// ENOBUFS. Parking against a generation snapshotted *here* would block for
    /// a further provide that never comes (every buffer is already back),
    /// deadlocking recv. Comparing against the arm-time snapshot detects those
    /// re-provides and re-arms instead.
    pub async fn wait_for_provide(&self, since: u64) {
        std::future::poll_fn(|cx| {
            if self.provide_gen.get() != since {
                std::task::Poll::Ready(())
            } else {
                *self.provide_waker.borrow_mut() = Some(cx.waker().clone());
                std::task::Poll::Pending
            }
        })
        .await;
    }

    /// A write begins borrowing buffer `bid`; it will not return to the
    /// kernel until released.
    pub fn borrow(&self, bid: u16) {
        let i = (bid & BUF_MASK) as usize;
        self.pending[i].set(self.pending[i].get() + 1);
    }

    /// A borrowing write finished. Returns the buffer to the kernel once the
    /// recv loop is also done with it and no borrows remain.
    pub fn release(&self, bid: u16) {
        let i = (bid & BUF_MASK) as usize;
        debug_assert!(
            self.pending[i].get() > 0,
            "release of unborrowed sub-buffer bid={bid} (borrow/release imbalance)"
        );
        let n = self.pending[i].get() - 1;
        self.pending[i].set(n);
        if n == 0 && self.awaiting[i].get() {
            self.kernel_provide(bid);
        }
    }

    /// The current running consume offset within buffer `bid` (incremental
    /// ring). A recv CQE for `bid` carrying `len` bytes places that data at
    /// `buf(bid) + recv_off(bid)`; after consuming it the reader calls
    /// [`recv_advance`](Self::recv_advance).
    pub fn recv_off(&self, bid: u16) -> usize {
        self.recv_off[(bid & BUF_MASK) as usize].get()
    }

    /// Advance buffer `bid`'s running consume offset by `len` after a recv CQE
    /// that did NOT fully consume the buffer (`IORING_CQE_F_BUF_MORE` set).
    pub fn recv_advance(&self, bid: u16, len: usize) {
        let i = (bid & BUF_MASK) as usize;
        self.recv_off[i].set(self.recv_off[i].get() + len);
    }

    /// The recv loop finished reading buffer `bid`. Returns the buffer to the
    /// kernel immediately if no writes are currently borrowing it; otherwise
    /// defers until all borrows are released.
    pub fn recv_done(&self, bid: u16) {
        let i = (bid & BUF_MASK) as usize;
        if self.pending[i].get() == 0 {
            self.kernel_provide(bid);
        } else {
            self.awaiting[i].set(true);
        }
    }

    /// Number of times buffer `bid` has been returned to the kernel.
    ///
    /// Available in crate-internal tests and with `feature = "test-helpers"`
    /// (for integration tests in sibling crates). Never use in production.
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn kernel_provided_count(&self, bid: u16) -> u32 {
        let i = (bid & BUF_MASK) as usize;
        self.provided_count[i].get()
    }
}

impl Drop for BufRing {
    fn drop(&mut self) {
        // Best-effort: unregister before the ring memory frees. One connection
        // owns this ring and outlives all its recv ops, so by here nothing is
        // drawing from it.
        let _ = self.reactor.unregister_buf_ring(self.bgid);
        // Release the sub-buffer fixed-buffer slots before `data` frees.
        for idx in self.buf_index.into_iter().flatten() {
            self.reactor.unregister_buffer(idx);
        }
        // Return an allocator-issued group id to the thread-local pool.
        if self.bgid_owned {
            free_bgid(self.bgid);
        }
    }
}

/// Number of distinct buffer-group ids each io-thread can hand out. Connections
/// on one reactor thread share its io_uring, so each per-connection ring needs
/// a distinct `bgid` within that ring; ids may repeat across threads (separate
/// io_urings) — the thread-local pool gives that for free. The fixed-buffer
/// budget (`MAX_REG_BUFS = 64`, ~3 slots per ring) caps live rings well below
/// this, so the pool is never the limiting resource.
const BGID_RANGE: u16 = 64;

thread_local! {
    /// Free list of buffer-group ids for this io-thread's rings. `None` until
    /// first use, then a stack of `0..BGID_RANGE` (`Vec::pop` hands out
    /// 0, 1, 2, …). Persists across reactor respawns on the thread — every id
    /// is returned on [`BufRing`] drop, so the list refills itself.
    static BGID_POOL: RefCell<Option<Vec<u16>>> = const { RefCell::new(None) };
}

/// Take a free buffer-group id for a new per-connection ring, or `None` when
/// the thread's pool is exhausted (caller falls back to classic recv).
fn alloc_bgid() -> Option<u16> {
    BGID_POOL.with(|pool| {
        pool.borrow_mut()
            .get_or_insert_with(|| (0..BGID_RANGE).rev().collect())
            .pop()
    })
}

/// Return a group id taken by [`alloc_bgid`] to the thread's pool.
fn free_bgid(bgid: u16) {
    BGID_POOL.with(|pool| {
        if let Some(list) = pool.borrow_mut().as_mut() {
            list.push(bgid);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{QueueRuntime, RingConfig, ops};
    use std::os::fd::AsRawFd;

    /// A representative per-buffer payload size for the drain test below.
    const RECV_BUF_SIZE: usize = 16 * 1024;

    fn socketpair() -> (std::os::fd::OwnedFd, std::os::fd::OwnedFd) {
        pair(libc::SOCK_STREAM)
    }

    fn pair(ty: libc::c_int) -> (std::os::fd::OwnedFd, std::os::fd::OwnedFd) {
        let mut fds = [0i32; 2];
        // SAFETY: writes two fds into `fds` on success.
        let r = unsafe { libc::socketpair(libc::AF_UNIX, ty, 0, fds.as_mut_ptr()) };
        assert_eq!(r, 0, "socketpair failed");
        // SAFETY: fresh fds, exclusively owned.
        unsafe {
            (
                std::os::fd::FromRawFd::from_raw_fd(fds[0]),
                std::os::fd::FromRawFd::from_raw_fd(fds[1]),
            )
        }
    }

    fn send_all(fd: &std::os::fd::OwnedFd, data: &[u8]) {
        // SAFETY: plain blocking write of `data`.
        let n = unsafe { libc::write(fd.as_raw_fd(), data.as_ptr().cast(), data.len()) };
        assert_eq!(n, data.len() as isize, "short write");
    }

    #[test]
    fn register_provide_consume() {
        let (a, b) = socketpair();
        let rt = QueueRuntime::new(RingConfig::default()).unwrap();
        rt.block_on(async move {
            let ring = BufRing::new(7, 256 * 1024).unwrap();
            assert_eq!(ring.nbufs(), 2);
            send_all(&b, b"hello buf_ring");

            let mut op = ops::recv_multi(a.as_raw_fd(), ring.bgid()).unwrap();
            let chunk = op.next().await.unwrap().unwrap();
            assert_eq!(chunk.len, 14);
            assert!(chunk.more, "multishot should stay armed");
            // SAFETY: the kernel filled `chunk.len` bytes of buffer `bid`.
            let got =
                unsafe { std::slice::from_raw_parts(ring.buf(chunk.bid), chunk.len as usize) };
            assert_eq!(got, b"hello buf_ring");
            ring.recv_done(chunk.bid); // recycle

            // A second message lands in another (or the recycled) buffer.
            send_all(&b, b"again");
            let chunk2 = op.next().await.unwrap().unwrap();
            assert_eq!(chunk2.len, 5);
            // SAFETY: the kernel filled `chunk2.len` bytes of buffer `bid`.
            let got2 =
                unsafe { std::slice::from_raw_parts(ring.buf(chunk2.bid), chunk2.len as usize) };
            assert_eq!(got2, b"again");
        });
    }

    #[test]
    fn enobufs_when_ring_drained_then_rearm() {
        // Datagram pair: each message is sized to FULLY consume one buffer, so
        // under the incremental ring the kernel marks it done (buf_more clear)
        // and advances to the next buffer. (A small datagram would only
        // partially consume a buffer — buf_more set — and never drain the
        // group.) Interleave send/recv so the socket queue holds one datagram
        // at a time (no qlen / blocking issues).
        let (a, b) = pair(libc::SOCK_DGRAM);
        let rt = QueueRuntime::new(RingConfig::default()).unwrap();
        rt.block_on(async move {
            let ring = BufRing::new(3, 8 * RECV_BUF_SIZE).unwrap();
            let n = ring.nbufs();
            assert_eq!(n, 2);
            let full = vec![0xCDu8; ring.buf_size() as usize];

            let mut op = ops::recv_multi(a.as_raw_fd(), ring.bgid()).unwrap();
            // Consume one buffer per datagram without recycling, draining
            // the group exactly.
            let mut held = Vec::new();
            for _ in 0..n {
                send_all(&b, &full);
                match op.next().await {
                    Some(Ok(chunk)) => {
                        assert_eq!(chunk.len as usize, full.len());
                        assert!(!chunk.buf_more, "a buffer-filling datagram retires it");
                        held.push(chunk.bid);
                    }
                    other => panic!("expected a chunk, got {other:?}"),
                }
            }
            // Group empty: the next datagram has no buffer → -ENOBUFS,
            // terminating the multishot.
            send_all(&b, &full);
            match op.next().await {
                Some(Err(e)) => assert_eq!(e.raw_os_error(), Some(libc::ENOBUFS)),
                other => panic!("expected ENOBUFS, got {other:?}"),
            }

            // Replenish and re-arm: a fresh recv_multi works again. The
            // datagram that hit ENOBUFS was not consumed (no buffer), so it
            // is still queued and arrives first — drain until "rearmed" (a
            // full-size payload whose last byte tags it).
            for bid in held {
                ring.recv_done(bid);
            }
            let mut rearmed = full.clone();
            *rearmed.last_mut().unwrap() = 0x42;
            send_all(&b, &rearmed);
            let mut op2 = ops::recv_multi(a.as_raw_fd(), ring.bgid()).unwrap();
            loop {
                let chunk = op2.next().await.unwrap().unwrap();
                // SAFETY: the kernel filled `chunk.len` bytes of buffer `bid`.
                let got =
                    unsafe { std::slice::from_raw_parts(ring.buf(chunk.bid), chunk.len as usize) };
                let is_rearmed = got.last() == Some(&0x42);
                ring.recv_done(chunk.bid); // recycle so the group never re-drains
                if is_rearmed {
                    break;
                }
            }
        });
    }

    #[test]
    fn eof_yields_none() {
        let (a, b) = socketpair();
        let rt = QueueRuntime::new(RingConfig::default()).unwrap();
        rt.block_on(async move {
            let ring = BufRing::new(5, 256 * 1024).unwrap();
            let mut op = ops::recv_multi(a.as_raw_fd(), ring.bgid()).unwrap();
            drop(b); // peer closes → EOF
            assert!(op.next().await.is_none(), "EOF ends the stream");
        });
    }

    #[test]
    fn two_buffers_of_half_size() {
        let rt = QueueRuntime::new(RingConfig::default()).unwrap();
        rt.block_on(async {
            let ring = BufRing::new(11, 4 * 1024 * 1024).unwrap();
            assert_eq!(ring.nbufs(), 2);
            assert_eq!(ring.buf_size(), 2 * 1024 * 1024);
        });
    }

    #[test]
    fn buffer_held_until_recv_done_and_all_borrows_released() {
        let rt = QueueRuntime::new(RingConfig::default()).unwrap();
        rt.block_on(async {
            let ring = BufRing::new(12, 256 * 1024).unwrap(); // 2 x 128 KiB
            // Two writes borrow buffer 0; recv finishes it.
            ring.borrow(0);
            ring.borrow(0);
            ring.recv_done(0); // recv done, but 2 borrows pending
            assert_eq!(
                ring.kernel_provided_count(0),
                0,
                "must not be re-provided yet"
            );
            ring.release(0); // 1 borrow left
            assert_eq!(ring.kernel_provided_count(0), 0);
            ring.release(0); // last borrow -> provide now
            assert_eq!(ring.kernel_provided_count(0), 1);
        });
    }

    // Regression: a buffer re-provided AFTER the kernel queued ENOBUFS but
    // BEFORE the recv loop parks must not be lost. The recv loop snapshots the
    // provide generation when it arms the multishot recv; `wait_for_provide`
    // resolves immediately if any provide happened since that snapshot (the
    // buffers are already back — re-arm), and only parks when none has. Getting
    // this wrong deadlocks recv: it blocks for a further provide that never
    // comes because every buffer is already in the kernel.
    #[test]
    fn wait_for_provide_does_not_miss_a_reprovide_since_arm() {
        fn poll_once<T>(fut: std::pin::Pin<&mut impl Future<Output = T>>) -> std::task::Poll<T> {
            let waker = std::task::Waker::noop();
            let mut cx = std::task::Context::from_waker(waker);
            fut.poll(&mut cx)
        }
        use std::future::Future;

        let rt = QueueRuntime::new(RingConfig::default()).unwrap();
        rt.block_on(async {
            let ring = BufRing::new(31, 256 * 1024).unwrap();

            // Recv arms a multishot here.
            let armed = ring.provide_gen();
            // While it runs ahead, a borrowed buffer is recv-done'd then
            // released — i.e. it cycles back to the kernel (one provide, gen++).
            ring.borrow(0);
            ring.recv_done(0); // deferred (borrow held): no provide yet
            ring.release(0); // last borrow drops -> kernel_provide, gen++

            // Recv only NOW observes the already-queued ENOBUFS. The buffer is
            // already back, so the park must resolve immediately.
            let fut = ring.wait_for_provide(armed);
            let mut fut = std::pin::pin!(fut);
            assert!(
                poll_once(fut.as_mut()).is_ready(),
                "a re-provide since arm must be seen (else recv deadlocks)"
            );

            // Conversely, with no provide since the arm snapshot, the wait must
            // park (the ring really is drained).
            let armed2 = ring.provide_gen();
            let fut2 = ring.wait_for_provide(armed2);
            let mut fut2 = std::pin::pin!(fut2);
            assert!(
                poll_once(fut2.as_mut()).is_pending(),
                "no provide since arm -> must park until one returns"
            );
        });
    }

    #[test]
    fn recv_done_with_no_borrows_provides_immediately() {
        let rt = QueueRuntime::new(RingConfig::default()).unwrap();
        rt.block_on(async {
            let ring = BufRing::new(13, 256 * 1024).unwrap();
            ring.recv_done(1);
            assert_eq!(ring.kernel_provided_count(1), 1);
        });
    }

    // Each sub-buffer is registered as an io_uring fixed buffer: a WRITE_FIXED
    // from ring memory (the zero-copy disk-write path) lands the bytes on disk.
    // Skips on kernels without fixed-buffer support.
    #[test]
    fn write_fixed_from_ring_sub_buffer_roundtrips() {
        use std::io::Read;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ringwrite");
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        let fd = file.as_raw_fd();

        let rt = QueueRuntime::new(RingConfig::default()).unwrap();
        rt.block_on(async move {
            let ring = BufRing::new(21, 256 * 1024).unwrap();
            let Some(idx) = ring.buf_index(0) else {
                eprintln!("kernel lacks fixed buffers; skipping");
                return;
            };
            const N: usize = 8192;
            let base = ring.buf(0);
            // SAFETY: base..base+N is within sub-buffer 0 (buf_size ≥ 1 page).
            unsafe { std::slice::from_raw_parts_mut(base, N).fill(0x5A) };
            let iov = [libc::iovec {
                iov_base: base.cast(),
                iov_len: N,
            }];
            // SAFETY: iov base is within registered buffer `idx`; the ring
            // outlives the awaited op.
            let got = unsafe {
                ops::writev_fixed_at_raw(ops::BackendFd::Raw(fd), iov.as_ptr(), 1, 0, idx, 0)
            }
            .unwrap()
            .await
            .unwrap();
            assert_eq!(got as usize, N, "WRITE_FIXED short write");
            ops::fsync(ops::BackendFd::Raw(fd), false)
                .unwrap()
                .await
                .unwrap();
        });

        let mut back = Vec::new();
        std::fs::File::open(&path)
            .unwrap()
            .read_to_end(&mut back)
            .unwrap();
        assert_eq!(back.len(), 8192);
        assert!(
            back.iter().all(|&b| b == 0x5A),
            "WRITE_FIXED from ring sub-buffer corrupted the data"
        );
    }
}
