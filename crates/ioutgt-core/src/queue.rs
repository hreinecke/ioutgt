//! Per-queue command slots: the bounded-concurrency core.
//!
//! A queue owns exactly `sqsize` preallocated [`CmdSlot`]s. The transport's
//! receive path claims a free tag, fills the slot's SQE (and write payload)
//! in place, and rings the slot's doorbell; the persistent task parked on
//! that slot wakes, dispatches, and pushes a [`Completion`] onto the
//! queue-local completion list for the transport's send path. The tag
//! returns to the freelist only after the response (and any C2H data read
//! from the slot buffer) is fully on the wire.
//!
//! Everything here is single-threaded (`Cell`/`RefCell`, no atomics); the
//! transfer tag on the wire *is* the slot index, so no CID lookup
//! structure exists.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;
use std::task::{Poll, Waker};

use ioutgt_nvme::spec::{Cqe, Sqe};

use crate::buf::AlignedBuf;

/// Per-queue lifetime IO counters. All writers run on the owning queue
/// thread (`Cell`, hence `!Sync` — a cross-thread read cannot compile);
/// GET_STATS snapshots them *on that thread* via the mailbox. Shared as
/// `Rc` so the thread's stats list can outlive the connection without
/// pinning slot memory.
#[derive(Debug)]
pub struct QueueStats {
    /// Queue id (immutable; reporting identity together with `cntlid`).
    pub qid: u16,
    /// Owning controller, set when Connect executes (0 until then).
    pub cntlid: Cell<u16>,
    /// NVM Read commands dispatched.
    pub read_cmds: Cell<u64>,
    /// NVM Write commands dispatched.
    pub write_cmds: Cell<u64>,
    /// NVM Flush commands dispatched.
    pub flush_cmds: Cell<u64>,
    /// Admin, fabrics, and non-Read/Write/Flush IO commands.
    pub other_cmds: Cell<u64>,
    /// Payload bytes of successful backend reads.
    pub read_bytes: Cell<u64>,
    /// Payload bytes of successful backend writes.
    pub write_bytes: Cell<u64>,
    /// IO-path commands completed with non-success status (validation
    /// and backend failures). Admin/fabrics failures are not counted,
    /// and a pre-dispatch rejection (unknown namespace, bad opcode)
    /// bumps this without a cmd-class counter — so the class counters
    /// do not necessarily sum to commands received.
    pub errors: Cell<u64>,
}

/// Plain-`u64` copy of [`QueueStats`]; doubles as the fold accumulator
/// for torn-down queues ([`QueueStatsSnapshot::absorb`]).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[allow(missing_docs)]
pub struct QueueStatsSnapshot {
    pub qid: u16,
    pub cntlid: u16,
    pub read_cmds: u64,
    pub write_cmds: u64,
    pub flush_cmds: u64,
    pub other_cmds: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
    pub errors: u64,
}

/// Counter increment used on the IO path: a plain `Cell` add — no
/// atomics, no locks.
#[inline]
pub fn stat_add(cell: &Cell<u64>, n: u64) {
    cell.set(cell.get() + n);
}

impl QueueStats {
    /// Fresh zeroed counters for queue `qid`.
    pub fn new(qid: u16) -> QueueStats {
        QueueStats {
            qid,
            cntlid: Cell::new(0),
            read_cmds: Cell::new(0),
            write_cmds: Cell::new(0),
            flush_cmds: Cell::new(0),
            other_cmds: Cell::new(0),
            read_bytes: Cell::new(0),
            write_bytes: Cell::new(0),
            errors: Cell::new(0),
        }
    }

    /// Copy out the current values (owning thread only).
    pub fn snapshot(&self) -> QueueStatsSnapshot {
        QueueStatsSnapshot {
            qid: self.qid,
            cntlid: self.cntlid.get(),
            read_cmds: self.read_cmds.get(),
            write_cmds: self.write_cmds.get(),
            flush_cmds: self.flush_cmds.get(),
            other_cmds: self.other_cmds.get(),
            read_bytes: self.read_bytes.get(),
            write_bytes: self.write_bytes.get(),
            errors: self.errors.get(),
        }
    }
}

impl QueueStatsSnapshot {
    /// Accumulate `other`'s counters; identity fields stay untouched
    /// (the accumulator represents "all retired queues").
    pub fn absorb(&mut self, other: &QueueStatsSnapshot) {
        self.read_cmds += other.read_cmds;
        self.write_cmds += other.write_cmds;
        self.flush_cmds += other.flush_cmds;
        self.other_cmds += other.other_cmds;
        self.read_bytes += other.read_bytes;
        self.write_bytes += other.write_bytes;
        self.errors += other.errors;
    }
}

/// Slot lifecycle. Transitions are all same-thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    /// On the freelist.
    Free,
    /// Claimed by the receive path; SQE/payload being filled.
    Receiving,
    /// Command complete on the wire side; slot task wakeable.
    Ready,
    /// Slot task is dispatching / awaiting the backend.
    Executing,
    /// Completion queued for / being written by the send path.
    Responding,
}

/// One preallocated command slot.
pub struct CmdSlot {
    state: Cell<SlotState>,
    /// The received command (placed by the recv path before `Ready`).
    sqe: Cell<Sqe>,
    /// Slot task doorbell.
    waker: Cell<Option<Waker>>,
    /// Data buffer: write payload in, read payload out. Sized at queue
    /// creation (admin: 8 KiB; IO: MDTS); 4K-aligned for O_DIRECT
    /// backends.
    data: RefCell<AlignedBuf>,
    /// Valid bytes in `data` (received payload or response data).
    data_len: Cell<u32>,
    /// Reassembly cursor for multi-PDU H2C transfers.
    recv_offset: Cell<u32>,
}

#[allow(missing_docs)] // accessor naming mirrors the field semantics above
impl CmdSlot {
    fn new(buf_size: usize) -> Self {
        CmdSlot {
            state: Cell::new(SlotState::Free),
            sqe: Cell::new(Sqe::zeroed()),
            waker: Cell::new(None),
            data: RefCell::new(AlignedBuf::zeroed(buf_size)),
            data_len: Cell::new(0),
            recv_offset: Cell::new(0),
        }
    }

    pub fn state(&self) -> SlotState {
        self.state.get()
    }

    pub fn sqe(&self) -> Sqe {
        self.sqe.get()
    }

    /// Borrow the slot data buffer (short-lived: one copy in/out, or
    /// held across a backend await while the slot is `Executing`).
    pub fn data(&self) -> std::cell::RefMut<'_, AlignedBuf> {
        self.data.borrow_mut()
    }

    pub fn data_len(&self) -> u32 {
        self.data_len.get()
    }

    pub fn set_data_len(&self, len: u32) {
        self.data_len.set(len);
    }

    pub fn recv_offset(&self) -> u32 {
        self.recv_offset.get()
    }

    pub fn set_recv_offset(&self, off: u32) {
        self.recv_offset.set(off);
    }

    /// Park the SQE while its in-capsule payload is still arriving
    /// (state stays `Receiving`; [`QueueCore::submit`] delivers it).
    pub fn stash_sqe(&self, sqe: Sqe) {
        debug_assert_eq!(self.state.get(), SlotState::Receiving);
        self.sqe.set(sqe);
    }

    /// The SQE parked by [`CmdSlot::stash_sqe`].
    pub fn stashed_sqe(&self) -> Sqe {
        self.sqe.get()
    }
}

/// A completed command waiting for the send path.
#[derive(Debug, Clone, Copy)]
#[allow(missing_docs)]
pub struct Completion {
    pub tag: u16,
    pub cqe: Cqe,
    /// Bytes of read data in the slot buffer to send as C2HData before
    /// (or instead of, with the success flag) the response capsule.
    pub data_len: u32,
}

/// One unit of work for the transport's send path. R2Ts originate from
/// the receive path (solicit write data) but must serialize with
/// responses on the wire, so they travel through the same queue.
#[derive(Debug, Clone, Copy)]
#[allow(missing_docs)]
pub enum SendWork {
    Response(Completion),
    R2t {
        tag: u16,
        cid: u16,
        offset: u32,
        length: u32,
    },
}

/// The per-queue state shared by recv path, slot tasks, and send path
/// (single thread; shared via `Rc`).
#[allow(missing_docs)]
pub struct QueueCore {
    pub qid: u16,
    /// Queue depth in entries; slot count.
    pub sqsize: u16,
    slots: Box<[CmdSlot]>,
    free_tags: RefCell<Vec<u16>>,
    sqhd: Cell<u16>,
    /// Host requested SQ flow control disabled (Connect cattr bit).
    pub sqhd_disabled: bool,
    send_work: RefCell<VecDeque<SendWork>>,
    send_waker: Cell<Option<Waker>>,
    /// Teardown: `next_send_work` yields `None` once set (and the
    /// pending list is drained), letting the send task exit before
    /// the queue's slot memory is freed.
    send_closed: Cell<bool>,
    /// Slots currently inside dispatch (possibly awaiting a backend op
    /// that references slot memory). Teardown drains this to zero
    /// before freeing the slots.
    executing: Cell<u16>,
    /// Recv-path doorbell for [`Self::await_tag`] (ZC mode): woken by
    /// `release_tag` so a parked claim retries. Single waiter — the
    /// recv loop is the only claimer.
    tag_waiter: Cell<Option<Waker>>,
    /// Lifetime IO counters, shared with the owning thread's stats list.
    pub stats: Rc<QueueStats>,
}

impl QueueCore {
    /// Allocate a queue: `sqsize` slots each with a `slot_buf_size` data
    /// buffer.
    pub fn new(qid: u16, sqsize: u16, slot_buf_size: usize, sqhd_disabled: bool) -> Rc<QueueCore> {
        let slots: Vec<CmdSlot> = (0..sqsize).map(|_| CmdSlot::new(slot_buf_size)).collect();
        // LIFO freelist: hot slots stay cache-warm.
        let free_tags: Vec<u16> = (0..sqsize).rev().collect();
        Rc::new(QueueCore {
            qid,
            sqsize,
            slots: slots.into_boxed_slice(),
            free_tags: RefCell::new(free_tags),
            sqhd: Cell::new(0),
            sqhd_disabled,
            send_work: RefCell::new(VecDeque::with_capacity(usize::from(sqsize) * 2)),
            send_waker: Cell::new(None),
            send_closed: Cell::new(false),
            executing: Cell::new(0),
            tag_waiter: Cell::new(None),
            stats: Rc::new(QueueStats::new(qid)),
        })
    }

    /// The slot for `tag` (the wire TTAG).
    pub fn slot(&self, tag: u16) -> &CmdSlot {
        &self.slots[usize::from(tag)]
    }

    /// Claim a free tag for an arriving command (recv path). `None`
    /// means the host exceeded the negotiated queue depth — a fatal
    /// protocol error.
    pub fn claim_tag(&self) -> Option<u16> {
        let tag = self.free_tags.borrow_mut().pop()?;
        let slot = self.slot(tag);
        debug_assert_eq!(slot.state.get(), SlotState::Free);
        slot.state.set(SlotState::Receiving);
        slot.data_len.set(0);
        slot.recv_offset.set(0);
        Some(tag)
    }

    /// Claim a free tag, parking until one frees if the list is empty.
    ///
    /// ZC mode only: payload tags release on the send *notification*
    /// (≈ the peer's TCP ACK), which races the host's next command —
    /// an empty freelist is an expected transient there, not a
    /// depth violation. Parking the recv path is deliberate TCP
    /// backpressure; release never depends on the recv path, so this
    /// cannot deadlock.
    pub async fn await_tag(self: &Rc<QueueCore>) -> u16 {
        std::future::poll_fn(|cx| match self.claim_tag() {
            Some(tag) => Poll::Ready(tag),
            None => {
                self.tag_waiter.set(Some(cx.waker().clone()));
                Poll::Pending
            }
        })
        .await
    }

    /// Deliver a fully received command to the slot task (recv path).
    pub fn submit(&self, tag: u16, sqe: Sqe) {
        let slot = self.slot(tag);
        debug_assert_eq!(slot.state.get(), SlotState::Receiving);
        slot.sqe.set(sqe);
        slot.state.set(SlotState::Ready);
        if let Some(waker) = slot.waker.take() {
            waker.wake();
        }
    }

    /// Await the next command for `tag` (slot task side).
    pub async fn await_command(self: &Rc<QueueCore>, tag: u16) -> Sqe {
        std::future::poll_fn(|cx| {
            let slot = self.slot(tag);
            match slot.state.get() {
                SlotState::Ready => {
                    slot.state.set(SlotState::Executing);
                    self.executing.set(self.executing.get() + 1);
                    Poll::Ready(slot.sqe.get())
                }
                _ => {
                    slot.waker.set(Some(cx.waker().clone()));
                    Poll::Pending
                }
            }
        })
        .await
    }

    /// Current sqhd, advancing it (call once per completion). 16-bit
    /// circular per the negotiated queue size, as in nvmet.
    pub fn advance_sqhd(&self) -> u16 {
        if self.sqhd_disabled {
            return 0;
        }
        let next = (self.sqhd.get() + 1) % self.sqsize;
        self.sqhd.set(next);
        next
    }

    /// Queue a completion for the send path (slot task side).
    pub fn complete(&self, tag: u16, cqe: Cqe, data_len: u32) {
        let slot = self.slot(tag);
        debug_assert_eq!(slot.state.get(), SlotState::Executing);
        slot.state.set(SlotState::Responding);
        self.executing.set(self.executing.get() - 1);
        self.push_send_work(SendWork::Response(Completion { tag, cqe, data_len }));
    }

    /// Fail a command still in the receive phase (payload/digest) without
    /// dispatching it — e.g. a data-digest mismatch, where executing the
    /// write would persist corrupt data. The slot task never saw this
    /// command (it is still parked in `await_command`), so `executing`
    /// is deliberately untouched: the same task will serve the next
    /// command that claims this tag.
    pub fn complete_receiving(&self, tag: u16, cqe: Cqe) {
        let slot = self.slot(tag);
        debug_assert_eq!(slot.state.get(), SlotState::Receiving);
        slot.state.set(SlotState::Responding);
        self.push_send_work(SendWork::Response(Completion {
            tag,
            cqe,
            data_len: 0,
        }));
    }

    /// Queue an R2T soliciting write data for `tag` (recv path side;
    /// the slot stays `Receiving`).
    pub fn solicit(&self, tag: u16, cid: u16, offset: u32, length: u32) {
        debug_assert_eq!(self.slot(tag).state.get(), SlotState::Receiving);
        self.push_send_work(SendWork::R2t {
            tag,
            cid,
            offset,
            length,
        });
    }

    fn push_send_work(&self, work: SendWork) {
        self.send_work.borrow_mut().push_back(work);
        if let Some(waker) = self.send_waker.take() {
            waker.wake();
        }
    }

    /// Non-blocking pop of send work (batching: drain without parking).
    pub fn try_next_send_work(&self) -> Option<SendWork> {
        self.send_work.borrow_mut().pop_front()
    }

    /// Poll-shaped core of [`Self::next_send_work`]: pending work
    /// first, `None` once closed, else park the send waker. Public so
    /// the transport's send loop can combine it with ZC notification
    /// polling in a single hand-rolled future.
    pub fn poll_send_work(&self, cx: &mut std::task::Context<'_>) -> Poll<Option<SendWork>> {
        if let Some(work) = self.send_work.borrow_mut().pop_front() {
            return Poll::Ready(Some(work));
        }
        if self.send_closed.get() {
            return Poll::Ready(None);
        }
        self.send_waker.set(Some(cx.waker().clone()));
        Poll::Pending
    }

    /// Await the next send-path work item; `None` after [`close_send`]
    /// (pending work is delivered first).
    pub async fn next_send_work(self: &Rc<QueueCore>) -> Option<SendWork> {
        std::future::poll_fn(|cx| self.poll_send_work(cx)).await
    }

    /// Wake the send loop into orderly exit: `next_send_work` yields
    /// queued work first, then `None`. Called at connection teardown
    /// so the gather send's slot references drain before the queue is
    /// freed.
    pub fn close_send(&self) {
        self.send_closed.set(true);
        if let Some(waker) = self.send_waker.take() {
            waker.wake();
        }
    }

    /// Return a tag to the freelist once its response is fully sent
    /// (send path side).
    pub fn release_tag(&self, tag: u16) {
        let slot = self.slot(tag);
        debug_assert_eq!(slot.state.get(), SlotState::Responding);
        slot.state.set(SlotState::Free);
        self.free_tags.borrow_mut().push(tag);
        if let Some(waker) = self.tag_waiter.take() {
            waker.wake();
        }
    }

    /// Slots currently executing a command (teardown gate: their
    /// backend ops may reference slot memory).
    pub fn executing(&self) -> u16 {
        self.executing.get()
    }

    /// All slots free — used by teardown drains and leak assertions.
    pub fn idle(&self) -> bool {
        self.free_tags.borrow().len() == usize::from(self.sqsize)
    }

    /// Number of free tags (== sqsize when idle).
    pub fn free_tags(&self) -> usize {
        self.free_tags.borrow().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tag_lifecycle_and_sqhd_wrap() {
        let q = QueueCore::new(1, 4, 4096, false);
        // sqhd wraps modulo sqsize.
        assert_eq!(q.advance_sqhd(), 1);
        assert_eq!(q.advance_sqhd(), 2);
        assert_eq!(q.advance_sqhd(), 3);
        assert_eq!(q.advance_sqhd(), 0);
        assert_eq!(q.advance_sqhd(), 1);

        // Claim all four tags; the fifth claim fails.
        let tags: Vec<u16> = (0..4).map(|_| q.claim_tag().unwrap()).collect();
        assert!(q.claim_tag().is_none());
        assert!(!q.idle());

        // Walk one slot through the full lifecycle, including the
        // await_command transition (polled manually; it is Ready).
        let tag = tags[0];
        q.submit(tag, Sqe::zeroed());
        assert_eq!(q.slot(tag).state(), SlotState::Ready);
        {
            let fut = q.await_command(tag);
            let mut fut = std::pin::pin!(fut);
            let waker = std::task::Waker::noop();
            let mut cx = std::task::Context::from_waker(waker);
            assert!(fut.as_mut().poll(&mut cx).is_ready());
        }
        assert_eq!(q.executing(), 1);
        q.complete(tag, Cqe::new(0, 0, 1, 7, 0), 0);
        assert_eq!(q.executing(), 0);
        assert_eq!(q.slot(tag).state(), SlotState::Responding);
        q.release_tag(tag);
        assert_eq!(q.slot(tag).state(), SlotState::Free);
        assert_eq!(q.free_tags(), 1);
    }

    #[test]
    fn sqhd_disabled_reports_zero() {
        let q = QueueCore::new(1, 8, 64, true);
        assert_eq!(q.advance_sqhd(), 0);
        assert_eq!(q.advance_sqhd(), 0);
    }

    #[test]
    fn await_tag_immediate_when_free() {
        let q = QueueCore::new(1, 2, 64, false);
        let fut = q.await_tag();
        let mut fut = std::pin::pin!(fut);
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let Poll::Ready(tag) = std::future::Future::poll(fut.as_mut(), &mut cx) else {
            panic!("free tags must claim immediately");
        };
        assert_eq!(q.slot(tag).state(), SlotState::Receiving);
    }

    #[test]
    fn await_tag_parks_until_release() {
        let q = QueueCore::new(1, 2, 64, false);
        let t0 = q.claim_tag().unwrap();
        let _t1 = q.claim_tag().unwrap();

        let fut = q.await_tag();
        let mut fut = std::pin::pin!(fut);
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        assert!(std::future::Future::poll(fut.as_mut(), &mut cx).is_pending());

        // Walk t0 to Responding so release_tag's debug_assert holds.
        q.submit(t0, Sqe::zeroed());
        {
            let cmd = q.await_command(t0);
            let mut cmd = std::pin::pin!(cmd);
            assert!(std::future::Future::poll(cmd.as_mut(), &mut cx).is_ready());
        }
        q.complete(t0, Cqe::new(0, 0, 1, 7, 0), 0);
        q.release_tag(t0);

        assert!(matches!(
            std::future::Future::poll(fut.as_mut(), &mut cx),
            Poll::Ready(tag) if tag == t0
        ));
    }

    #[test]
    fn close_send_drains_then_ends() {
        let queue = QueueCore::new(0, 4, 512, false);
        // solicit() debug-asserts Receiving state: claim properly.
        let tag = queue.claim_tag().unwrap();
        queue.solicit(tag, 42, 0, 512);
        queue.close_send();
        // Pending work is still delivered after close...
        assert!(matches!(
            queue.try_next_send_work(),
            Some(SendWork::R2t { .. })
        ));
        // ...then the closed flag surfaces as Ready(None), not Pending.
        let mut fut = std::pin::pin!(queue.next_send_work());
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        assert!(matches!(
            std::future::Future::poll(fut.as_mut(), &mut cx),
            Poll::Ready(None)
        ));
    }

    #[test]
    fn queue_stats_snapshot_and_absorb() {
        let stats = QueueStats::new(3);
        stats.cntlid.set(7);
        stat_add(&stats.read_cmds, 2);
        stat_add(&stats.read_bytes, 8192);
        stat_add(&stats.errors, 1);
        let snap = stats.snapshot();
        assert_eq!((snap.qid, snap.cntlid), (3, 7));
        assert_eq!((snap.read_cmds, snap.read_bytes, snap.errors), (2, 8192, 1));

        let mut retired = QueueStatsSnapshot::default();
        retired.absorb(&snap);
        retired.absorb(&snap);
        assert_eq!(retired.read_cmds, 4);
        assert_eq!(retired.read_bytes, 16384);
        assert_eq!(retired.errors, 2);
        // Identity does not aggregate: the accumulator is "all retired
        // queues", not any one of them.
        assert_eq!((retired.qid, retired.cntlid), (0, 0));
    }

    #[test]
    fn queue_core_owns_zeroed_stats() {
        let queue = QueueCore::new(1, 4, 4096, false);
        assert_eq!(
            queue.stats.snapshot(),
            QueueStatsSnapshot {
                qid: 1,
                ..QueueStatsSnapshot::default()
            }
        );
    }
}
