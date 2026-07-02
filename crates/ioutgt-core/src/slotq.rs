//! Protocol-neutral command-slot engine: the bounded-concurrency core,
//! split into two independent pieces.
//!
//! [`SlotArray<C>`] owns the preallocated slots, the tag freelist, and
//! the executing-counter teardown gate; `C` is the per-slot command
//! stash (`Sqe` for NVMe, the NBD request header for NBD).
//! [`SendList<W>`] is the ordered work queue a transport's send loop
//! drains; `W` is the transport's own work type — core never pushes
//! to it and never names it. Everything protocol-flavored (SQ flow
//! control, completion records, R2T solicitation) lives in the
//! instantiating transport. Single-threaded by construction
//! (`Cell`/`RefCell`, no atomics); the wire transfer tag *is* the
//! slot index, so no lookup structure exists.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;
use std::task::{Poll, Waker};

use crate::pool::{BufPool, SlotData};

/// Slot lifecycle. Transitions are all same-thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    /// On the freelist.
    Free,
    /// Claimed by the receive path; command/payload being filled.
    Receiving,
    /// Command complete on the wire side; slot task wakeable.
    Ready,
    /// Slot task is dispatching / awaiting the backend.
    Executing,
    /// Completion queued for / being written by the send path.
    Responding,
}

/// One preallocated command slot.
pub struct Slot<C: Copy> {
    state: Cell<SlotState>,
    /// The received command (placed by the recv path before `Ready`).
    cmd: Cell<C>,
    /// Slot task doorbell.
    waker: Cell<Option<Waker>>,
    /// Data buffer: write payload in, read payload out, viewed as one
    /// or more page-aligned segments. Leased from the queue pool when the
    /// transfer length is known; `empty` between commands.
    data: RefCell<SlotData>,
    /// Valid bytes in `data` (received payload or response data).
    data_len: Cell<u32>,
    /// Reassembly cursor for multi-step payload receives.
    recv_offset: Cell<u32>,
}

#[allow(missing_docs)] // accessor naming mirrors the field semantics above
impl<C: Copy> Slot<C> {
    fn new(init: C) -> Self {
        Slot {
            state: Cell::new(SlotState::Free),
            cmd: Cell::new(init),
            waker: Cell::new(None),
            data: RefCell::new(SlotData::empty()),
            data_len: Cell::new(0),
            recv_offset: Cell::new(0),
        }
    }

    pub fn state(&self) -> SlotState {
        self.state.get()
    }

    pub fn cmd(&self) -> C {
        self.cmd.get()
    }

    /// Borrow the slot data buffer (short-lived: one copy in/out, or
    /// held across a backend await while the slot is `Executing`).
    pub fn data(&self) -> std::cell::RefMut<'_, SlotData> {
        self.data.borrow_mut()
    }

    /// Install a leased data buffer for the command about to use it.
    pub fn set_data(&self, data: SlotData) {
        *self.data.borrow_mut() = data;
    }

    /// Drop the data buffer (returning a pool lease to its pool). Called
    /// when the tag is released back to the freelist.
    pub fn release_data(&self) {
        *self.data.borrow_mut() = SlotData::empty();
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

    /// Park the command while its payload is still arriving (state
    /// stays `Receiving`; [`SlotArray::submit`] delivers it).
    pub fn stash_cmd(&self, cmd: C) {
        debug_assert_eq!(self.state.get(), SlotState::Receiving);
        self.cmd.set(cmd);
    }
}

/// The preallocated slot set shared by a connection's recv path, slot
/// tasks, and send path (single thread; shared via the transport's
/// `Rc` composite).
pub struct SlotArray<C: Copy> {
    /// Slot count == maximum concurrent commands.
    pub nslots: u16,
    slots: Box<[Slot<C>]>,
    free_tags: RefCell<Vec<u16>>,
    /// Slots currently inside dispatch (possibly awaiting a backend op
    /// that references slot memory). Teardown drains this to zero
    /// before freeing the slots.
    executing: Cell<u16>,
    /// Recv-path doorbell for [`Self::await_tag`]: woken by
    /// `release_tag` so a parked claim retries. Single waiter — the
    /// recv loop is the only claimer.
    tag_waiter: Cell<Option<Waker>>,
    /// Shared data-buffer arena. Slots lease from it on demand and the
    /// lease is returned at `release_tag`. Declared last so it outlives
    /// the slots during drop (their leases reference it).
    pool: Rc<BufPool>,
}

impl<C: Copy> SlotArray<C> {
    /// Allocate `nslots` slots (each with no data buffer until it leases
    /// one) plus a shared `pool_bytes` data-buffer pool.
    pub fn new(nslots: u16, pool_bytes: usize, init: C) -> SlotArray<C> {
        let slots: Vec<Slot<C>> = (0..nslots).map(|_| Slot::new(init)).collect();
        // LIFO freelist: hot slots stay cache-warm.
        let free_tags: Vec<u16> = (0..nslots).rev().collect();
        SlotArray {
            nslots,
            slots: slots.into_boxed_slice(),
            free_tags: RefCell::new(free_tags),
            executing: Cell::new(0),
            tag_waiter: Cell::new(None),
            pool: BufPool::new(pool_bytes),
        }
    }

    /// The shared data-buffer pool.
    pub fn pool(&self) -> &Rc<BufPool> {
        &self.pool
    }

    /// Lease a `len`-byte data buffer into `tag`'s slot, parking until the
    /// pool can satisfy it (backpressure for an oversubscribed pool).
    ///
    /// Park-safe only for an *independent* leaser (a read slot task): the
    /// serial recv loop must not park here, or pipelined writes deadlock —
    /// it uses [`Self::lease_or_owned`] instead.
    pub async fn lease_await(&self, tag: u16, len: usize) {
        let data = self.pool.alloc_await(len).await;
        self.slot(tag).set_data(data);
    }

    /// Lease into `tag` from the pool only; `false` when the pool cannot
    /// satisfy `len` right now. For transports that must land the data in
    /// the registered pool arena (RDMA), where `lease_or_owned`'s private
    /// heap fallback is unusable — the caller defers the command and
    /// retries as completions release leases, instead of failing it.
    pub fn try_lease(&self, tag: u16, len: usize) -> bool {
        match self.pool.alloc(len) {
            Some(data) => {
                self.slot(tag).set_data(data);
                true
            }
            None => false,
        }
    }

    /// Lease into `tag`, falling back to a private heap buffer when the
    /// pool is momentarily exhausted. Never blocks — the deadlock-free
    /// path for the serial recv loop (write payloads) and admin data. The
    /// fallback allocation is a degraded-mode transient; the steady state
    /// (pool not exhausted) allocates nothing.
    pub fn lease_or_owned(&self, tag: u16, len: usize) {
        let data = self
            .pool
            .alloc(len)
            .unwrap_or_else(|| SlotData::owned(len.max(1)));
        self.slot(tag).set_data(data);
    }

    /// The slot for `tag` (the wire transfer tag).
    pub fn slot(&self, tag: u16) -> &Slot<C> {
        &self.slots[usize::from(tag)]
    }

    /// Claim a free tag for an arriving command (recv path). `None`
    /// means every slot is busy — a protocol error for transports
    /// with a negotiated depth, an expected transient otherwise.
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
    /// Parking the recv path is deliberate backpressure; release never
    /// depends on the recv path, so this cannot deadlock.
    pub async fn await_tag(&self) -> u16 {
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
    pub fn submit(&self, tag: u16, cmd: C) {
        let slot = self.slot(tag);
        debug_assert_eq!(slot.state.get(), SlotState::Receiving);
        slot.cmd.set(cmd);
        slot.state.set(SlotState::Ready);
        if let Some(waker) = slot.waker.take() {
            waker.wake();
        }
    }

    /// Await the next command for `tag` (slot task side).
    pub async fn await_command(&self, tag: u16) -> C {
        std::future::poll_fn(|cx| {
            let slot = self.slot(tag);
            match slot.state.get() {
                SlotState::Ready => {
                    slot.state.set(SlotState::Executing);
                    self.executing.set(self.executing.get() + 1);
                    Poll::Ready(slot.cmd.get())
                }
                _ => {
                    slot.waker.set(Some(cx.waker().clone()));
                    Poll::Pending
                }
            }
        })
        .await
    }

    /// Step an `Executing` slot to `Responding` (slot task side, once
    /// dispatch produced a result). The caller queues its own send
    /// work — the engine does not know its shape.
    pub fn begin_respond(&self, tag: u16) {
        let slot = self.slot(tag);
        debug_assert_eq!(slot.state.get(), SlotState::Executing);
        slot.state.set(SlotState::Responding);
        self.executing.set(self.executing.get() - 1);
    }

    /// Step a `Receiving` slot straight to `Responding` — fail a
    /// command without dispatching it (payload/validation error). The
    /// slot task never saw it (still parked in `await_command`), so
    /// `executing` is deliberately untouched.
    pub fn respond_receiving(&self, tag: u16) {
        let slot = self.slot(tag);
        debug_assert_eq!(slot.state.get(), SlotState::Receiving);
        slot.state.set(SlotState::Responding);
    }

    /// Return a tag to the freelist once its response is fully sent
    /// (send path side).
    pub fn release_tag(&self, tag: u16) {
        let slot = self.slot(tag);
        debug_assert_eq!(slot.state.get(), SlotState::Responding);
        // Return the data buffer to the pool before the tag is reusable;
        // this wakes any task parked on pool exhaustion.
        slot.release_data();
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
        self.free_tags.borrow().len() == usize::from(self.nslots)
    }

    /// Number of free tags (== nslots when idle).
    pub fn free_tags(&self) -> usize {
        self.free_tags.borrow().len()
    }
}

/// The ordered send-work queue a transport's send loop drains. `W` is
/// the transport's own type; core neither pushes nor names it.
pub struct SendList<W> {
    work: RefCell<VecDeque<W>>,
    waker: Cell<Option<Waker>>,
    /// Teardown: `next` yields `None` once set (and the pending list
    /// is drained), letting the send task exit before slot memory is
    /// freed.
    closed: Cell<bool>,
}

impl<W> SendList<W> {
    /// Preallocate for roughly `nslots` in-flight items (responses
    /// plus solicitations).
    pub fn new(nslots: u16) -> SendList<W> {
        SendList {
            work: RefCell::new(VecDeque::with_capacity(usize::from(nslots) * 2)),
            waker: Cell::new(None),
            closed: Cell::new(false),
        }
    }

    /// Queue work and ring the send loop's doorbell.
    pub fn push(&self, work: W) {
        self.work.borrow_mut().push_back(work);
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    /// Non-blocking pop (batching: drain without parking).
    pub fn try_next(&self) -> Option<W> {
        self.work.borrow_mut().pop_front()
    }

    /// Poll-shaped core of [`Self::next`]: pending work first, `None`
    /// once closed, else park the waker. Public so a transport's send
    /// loop can combine it with other polling in one hand-rolled
    /// future (the ZC notification reaper does).
    pub fn poll_next(&self, cx: &mut std::task::Context<'_>) -> Poll<Option<W>> {
        if let Some(work) = self.work.borrow_mut().pop_front() {
            return Poll::Ready(Some(work));
        }
        if self.closed.get() {
            return Poll::Ready(None);
        }
        self.waker.set(Some(cx.waker().clone()));
        Poll::Pending
    }

    /// Await the next work item; `None` after [`Self::close`]
    /// (pending work is delivered first).
    pub async fn next(&self) -> Option<W> {
        std::future::poll_fn(|cx| self.poll_next(cx)).await
    }

    /// Wake the send loop into orderly exit: `next` yields queued work
    /// first, then `None`. Called at connection teardown so in-flight
    /// slot references drain before the queue is freed.
    pub fn close(&self) {
        self.closed.set(true);
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::PAGE;

    #[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
    struct TestCmd(u64);

    #[test]
    fn tag_lifecycle() {
        let a: SlotArray<TestCmd> = SlotArray::new(4, 4096, TestCmd::default());
        let tags: Vec<u16> = (0..4).map(|_| a.claim_tag().unwrap()).collect();
        assert!(a.claim_tag().is_none());
        assert!(!a.idle());

        let tag = tags[0];
        a.submit(tag, TestCmd(7));
        assert_eq!(a.slot(tag).state(), SlotState::Ready);
        {
            let fut = a.await_command(tag);
            let mut fut = std::pin::pin!(fut);
            let waker = std::task::Waker::noop();
            let mut cx = std::task::Context::from_waker(waker);
            let Poll::Ready(cmd) = fut.as_mut().poll(&mut cx) else {
                panic!("ready slot must dispatch immediately");
            };
            assert_eq!(cmd, TestCmd(7));
        }
        assert_eq!(a.executing(), 1);
        a.begin_respond(tag);
        assert_eq!(a.executing(), 0);
        assert_eq!(a.slot(tag).state(), SlotState::Responding);
        a.release_tag(tag);
        assert_eq!(a.slot(tag).state(), SlotState::Free);
        assert_eq!(a.free_tags(), 1);
    }

    #[test]
    fn lease_returns_to_pool_on_release() {
        // One-page pool, two slots: leasing the page then releasing the
        // tag must hand the page back.
        let a: SlotArray<TestCmd> = SlotArray::new(2, PAGE, TestCmd::default());
        let full = a.pool().free_pages();
        assert_eq!(full, 1);

        let tag = a.claim_tag().unwrap();
        a.lease_or_owned(tag, PAGE);
        assert_eq!(a.pool().free_pages(), 0, "page leased");

        a.respond_receiving(tag);
        a.release_tag(tag);
        assert_eq!(a.pool().free_pages(), full, "page returned on release");
    }

    #[test]
    fn over_subscribed_lease_falls_back_to_owned() {
        // Pool holds one page; a second concurrent lease can't come from
        // the pool, so it falls back to a private buffer (no panic, no
        // block) and the pool stays at zero.
        let a: SlotArray<TestCmd> = SlotArray::new(2, PAGE, TestCmd::default());
        let t0 = a.claim_tag().unwrap();
        let t1 = a.claim_tag().unwrap();
        a.lease_or_owned(t0, PAGE);
        a.lease_or_owned(t1, PAGE); // pool empty → owned fallback
        assert_eq!(a.pool().free_pages(), 0);
        assert_eq!(a.slot(t1).data().len(), PAGE);
    }

    #[test]
    fn respond_receiving_skips_executing() {
        let a: SlotArray<TestCmd> = SlotArray::new(2, 64, TestCmd::default());
        let tag = a.claim_tag().unwrap();
        a.respond_receiving(tag);
        assert_eq!(a.executing(), 0);
        a.release_tag(tag);
        assert!(a.idle());
    }

    #[test]
    fn await_tag_parks_until_release() {
        let a: SlotArray<TestCmd> = SlotArray::new(1, 64, TestCmd::default());
        let t0 = a.claim_tag().unwrap();

        let fut = a.await_tag();
        let mut fut = std::pin::pin!(fut);
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        assert!(std::future::Future::poll(fut.as_mut(), &mut cx).is_pending());

        a.submit(t0, TestCmd(1));
        {
            let cmd = a.await_command(t0);
            let mut cmd = std::pin::pin!(cmd);
            assert!(std::future::Future::poll(cmd.as_mut(), &mut cx).is_ready());
        }
        a.begin_respond(t0);
        a.release_tag(t0);

        assert!(matches!(
            std::future::Future::poll(fut.as_mut(), &mut cx),
            Poll::Ready(tag) if tag == t0
        ));
    }

    #[test]
    fn send_list_close_drains_then_ends() {
        let s: SendList<u16> = SendList::new(4);
        s.push(42);
        s.close();
        assert_eq!(s.try_next(), Some(42));
        let mut fut = std::pin::pin!(s.next());
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        assert!(matches!(
            std::future::Future::poll(fut.as_mut(), &mut cx),
            Poll::Ready(None)
        ));
    }
}
