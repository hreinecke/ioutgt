//! One multiplexed `sheep` connection per thread — the object-IO data path.
//!
//! A queue thread opens exactly **one** TCP connection to the cluster and runs
//! every namespace's object IO over it, pipelined. Requests carry a client
//! `id` the server echoes into the response (`rsp.id = req->rq.id` in
//! `sheep`'s `tx_work`), and `sheep` hands each request straight to a worker
//! and answers in completion order — so responses arrive out of order and are
//! routed back to their caller by that id. This is the shape QEMU's sheepdog
//! driver uses, and the reason the wire has an id field at all.
//!
//! Three pieces:
//!
//! * A [`Session`] per (thread, cluster address): the fd, a slot table of
//!   in-flight requests, and a send gate. Sessions live in a thread-local map
//!   and are `!Send` — their io_uring ops bind to `Reactor::current()`.
//! * A **pump** task per session, spawned on the queue thread's `LocalSet`,
//!   the connection's only reader: it reads a 48-byte response header, routes
//!   the payload straight into the waiting caller's buffer (no staging copy),
//!   and wakes it. With nothing outstanding the pump parks on a waker rather
//!   than on the socket, so an idle thread arms no op.
//! * A **send gate**: a request's header and its payload must reach the wire
//!   back to back, so a sender holds the gate across its writes (FIFO, one
//!   waiter woken per release). Nothing else about the connection is
//!   serialized; the wire is the only thing being taken turns on.
//!
//! Any IO error, EOF, or cancellation mid-send **poisons** the session: it is
//! marked dead, the socket is shut down so the pump wakes, every waiter fails
//! with `EIO`, and the next request dials a fresh session. The blast radius is
//! wider than the connection-per-request pool this replaces — one dead
//! connection now fails every request in flight on the thread rather than one
//! — but the host retries those commands, and the target holds one fd per
//! thread instead of one per concurrent command.

use std::cell::{Cell, RefCell, UnsafeCell};
use std::collections::{HashMap, VecDeque};
use std::future::poll_fn;
use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use ioutgt_uring::ops;

use super::{SD_HDR_SIZE, new_stream_socket, resp_data_length, resp_id, resp_result, set_req_id};

/// Bytes drained per round trip when a response outruns the caller's buffer
/// (a server bug or a truncated read): the stream has to be resynchronized to
/// the next header, and this is the buffer that swallows the difference.
const SCRATCH_LEN: usize = 4096;

/// Ceiling on concurrent requests per connection: the request id is
/// `slot | (seq << 16)`, so the slot table cannot outgrow `u16`. A thread
/// would need 64 Ki commands in flight at once to reach it.
const MAX_SLOTS: usize = u16::MAX as usize + 1;

thread_local! {
    /// The live session per cluster address on this thread. Entries are
    /// inserted by [`session`] and removed by the owning pump when it exits.
    static SESSIONS: RefCell<HashMap<SocketAddr, Rc<Session>>> =
        RefCell::new(HashMap::new());
}

/// Where a request has got to, so that dropping one mid-flight (a cancelled
/// slot task) leaves the connection in a state the next request can use.
#[derive(Clone, Copy)]
enum Phase {
    /// Waiting for the send gate: nothing has reached the wire.
    Queued,
    /// Writing the header/payload: the stream's framing is at stake.
    Sending,
    /// Fully sent, waiting for the response.
    Sent,
    /// Response in hand (or a failure already accounted for).
    Done,
}

/// The lifecycle of one entry in the slot table.
#[derive(Clone, Copy)]
enum SlotState {
    /// Unused, on the free list.
    Free,
    /// A request is queued, sending, or waiting for its response.
    Pending,
    /// The caller is gone; the pump discards the response and frees the slot.
    Abandoned,
    /// The response arrived; `result` is its `SD_RES_*`.
    Done { result: u32 },
    /// The connection died under this request; `errno` is what to report.
    Failed { errno: i32 },
}

/// One in-flight request: where its response payload goes, how far it has
/// got, and who to wake when it lands.
struct Slot {
    state: SlotState,
    /// Bumped on every reuse and carried in the high half of the request id,
    /// so a reply for a previous occupant can never be mistaken for this one.
    seq: u16,
    /// Destination for the response payload (null = discard it).
    dst: *mut u8,
    dst_len: usize,
    /// Set while this slot sits in [`Session::send_queue`].
    queued: bool,
    waker: Option<Waker>,
}

impl Slot {
    fn new() -> Slot {
        Slot {
            state: SlotState::Free,
            seq: 0,
            dst: std::ptr::null_mut(),
            dst_len: 0,
            queued: false,
            waker: None,
        }
    }
}

/// One thread's connection to one cluster, and everything in flight on it.
struct Session {
    fd: OwnedFd,
    /// In-flight requests, indexed by the low half of the request id.
    slots: RefCell<Vec<Slot>>,
    /// Free slot indices (reused before the table grows).
    free: RefCell<Vec<u16>>,
    /// Slots not yet reaped by their caller — what tells the pump whether a
    /// response is still owed, and so whether to read or to park.
    inflight: Cell<usize>,
    /// Set once, for good: no further use of this fd.
    poisoned: Cell<bool>,
    /// The send gate, and the senders queued for it in arrival order.
    sending: Cell<bool>,
    send_queue: RefCell<VecDeque<u16>>,
    /// The pump's waker while it is parked with nothing outstanding.
    pump_waker: RefCell<Option<Waker>>,
    /// Set while the pump has a recv in flight into [`Session::hdr`] or
    /// [`Session::scratch`] — see the leak note in [`PumpGuard::drop`].
    recv_armed: Cell<bool>,
    /// Response header, written by the kernel. Pump-only.
    hdr: UnsafeCell<[u8; SD_HDR_SIZE]>,
    /// Resynchronization sink for payload the caller has no room for.
    /// Pump-only.
    scratch: UnsafeCell<[u8; SCRATCH_LEN]>,
}

impl Session {
    fn new(fd: OwnedFd) -> Session {
        Session {
            fd,
            slots: RefCell::new(Vec::new()),
            free: RefCell::new(Vec::new()),
            inflight: Cell::new(0),
            poisoned: Cell::new(false),
            sending: Cell::new(false),
            send_queue: RefCell::new(VecDeque::new()),
            pump_waker: RefCell::new(None),
            recv_armed: Cell::new(false),
            hdr: UnsafeCell::new([0u8; SD_HDR_SIZE]),
            scratch: UnsafeCell::new([0u8; SCRATCH_LEN]),
        }
    }

    /// Claim a slot for a request whose response payload goes to `dst`,
    /// returning the request id to stamp into the header and the slot index.
    fn alloc(&self, dst: Option<&mut [u8]>) -> io::Result<(u32, u16)> {
        let (ptr, len) = match dst {
            Some(d) => (d.as_mut_ptr(), d.len()),
            None => (std::ptr::null_mut(), 0),
        };
        let mut slots = self.slots.borrow_mut();
        let idx = match self.free.borrow_mut().pop() {
            Some(idx) => idx,
            None => {
                if slots.len() >= MAX_SLOTS {
                    return Err(io::Error::from_raw_os_error(libc::EBUSY));
                }
                slots.push(Slot::new());
                u16::try_from(slots.len() - 1).expect("bounded by MAX_SLOTS")
            }
        };
        let slot = &mut slots[usize::from(idx)];
        slot.seq = slot.seq.wrapping_add(1);
        slot.state = SlotState::Pending;
        slot.dst = ptr;
        slot.dst_len = len;
        slot.waker = None;
        let id = u32::from(idx) | (u32::from(slot.seq) << 16);
        drop(slots);
        self.inflight.set(self.inflight.get() + 1);
        Ok((id, idx))
    }

    /// The state of a slot, for deciding what to do about it without holding
    /// the table borrowed.
    fn state(&self, idx: u16) -> SlotState {
        self.slots.borrow()[usize::from(idx)].state
    }

    /// Return a slot to the free list. The caller has its outcome (or never
    /// sent anything), so nothing is owed on it any more.
    fn free_slot(&self, idx: u16) {
        if matches!(self.state(idx), SlotState::Free) {
            return;
        }
        {
            let mut slots = self.slots.borrow_mut();
            let slot = &mut slots[usize::from(idx)];
            slot.state = SlotState::Free;
            slot.dst = std::ptr::null_mut();
            slot.dst_len = 0;
            slot.waker = None;
        }
        self.free.borrow_mut().push(idx);
        self.inflight.set(self.inflight.get() - 1);
    }

    /// The caller of a sent request is gone: the response is still coming, so
    /// the slot stays claimed until the pump discards it.
    fn abandon(&self, idx: u16) {
        match self.state(idx) {
            SlotState::Pending => {
                let mut slots = self.slots.borrow_mut();
                let slot = &mut slots[usize::from(idx)];
                slot.state = SlotState::Abandoned;
                slot.dst = std::ptr::null_mut();
                slot.dst_len = 0;
                slot.waker = None;
            }
            // Already resolved: nothing is coming, so hand the slot back.
            SlotState::Done { .. } | SlotState::Failed { .. } => self.free_slot(idx),
            SlotState::Free | SlotState::Abandoned => {}
        }
    }

    /// End this connection: fail everything on it and break the pump out of
    /// its recv. Idempotent; the pump does the removal from [`SESSIONS`].
    fn poison(&self) {
        if self.poisoned.replace(true) {
            return;
        }
        // SAFETY: `self.fd` is open for as long as the session lives; a
        // shutdown on a live fd has no preconditions beyond that.
        unsafe {
            libc::shutdown(self.fd.as_raw_fd(), libc::SHUT_RDWR);
        }
        self.fail_all();
        // Senders parked on the gate re-check `poisoned` when polled.
        let queued: Vec<u16> = self.send_queue.borrow_mut().drain(..).collect();
        for idx in queued {
            self.wake_slot(idx);
        }
        self.wake_pump();
    }

    /// Fail every unresolved request with `EIO` and wake its caller.
    fn fail_all(&self) {
        let len = self.slots.borrow().len();
        for idx in 0..len {
            let idx = u16::try_from(idx).expect("bounded by MAX_SLOTS");
            match self.state(idx) {
                SlotState::Pending => {
                    let waker = {
                        let mut slots = self.slots.borrow_mut();
                        let slot = &mut slots[usize::from(idx)];
                        slot.state = SlotState::Failed { errno: libc::EIO };
                        slot.queued = false;
                        slot.waker.take()
                    };
                    if let Some(w) = waker {
                        w.wake();
                    }
                }
                // No caller left to tell.
                SlotState::Abandoned => self.free_slot(idx),
                SlotState::Free | SlotState::Done { .. } | SlotState::Failed { .. } => {}
            }
        }
    }

    fn wake_slot(&self, idx: u16) {
        let waker = self.slots.borrow_mut()[usize::from(idx)].waker.take();
        if let Some(w) = waker {
            w.wake();
        }
    }

    fn wake_pump(&self) {
        let waker = self.pump_waker.borrow_mut().take();
        if let Some(w) = waker {
            w.wake();
        }
    }

    // --- send gate ---------------------------------------------------------

    /// Take the gate, in arrival order, so this request's header and payload
    /// are written back to back.
    fn poll_acquire_send(&self, idx: u16, cx: &Context<'_>) -> Poll<io::Result<()>> {
        if self.poisoned.get() {
            return Poll::Ready(Err(io::Error::from_raw_os_error(libc::EIO)));
        }
        let mut queue = self.send_queue.borrow_mut();
        let our_turn = queue.front().is_none_or(|&front| front == idx);
        if !self.sending.get() && our_turn {
            if queue.front() == Some(&idx) {
                queue.pop_front();
                self.slots.borrow_mut()[usize::from(idx)].queued = false;
            }
            self.sending.set(true);
            return Poll::Ready(Ok(()));
        }
        let mut slots = self.slots.borrow_mut();
        let slot = &mut slots[usize::from(idx)];
        slot.waker = Some(cx.waker().clone());
        if !slot.queued {
            slot.queued = true;
            queue.push_back(idx);
        }
        Poll::Pending
    }

    /// Hand the gate to the next sender in line.
    fn release_send(&self) {
        self.sending.set(false);
        let next = self.send_queue.borrow().front().copied();
        if let Some(idx) = next {
            self.wake_slot(idx);
        }
    }

    /// Drop a cancelled sender out of the queue, and pass the gate on if it
    /// was the one holding everyone else up.
    fn unqueue_send(&self, idx: u16) {
        let was_queued = {
            let mut slots = self.slots.borrow_mut();
            std::mem::replace(&mut slots[usize::from(idx)].queued, false)
        };
        if !was_queued {
            return;
        }
        self.send_queue.borrow_mut().retain(|&q| q != idx);
        if !self.sending.get() {
            let next = self.send_queue.borrow().front().copied();
            if let Some(next) = next {
                self.wake_slot(next);
            }
        }
    }

    // --- response side (pump only) ----------------------------------------

    /// Read one response and hand it to its caller. Errors here are fatal to
    /// the connection: the stream's framing is no longer known.
    async fn read_one(&self) -> io::Result<()> {
        let hdr = self.hdr.get().cast::<u8>();
        // SAFETY: the header buffer belongs to the session, which outlives
        // this task (it is kept alive deliberately if a recv is still in
        // flight, see `PumpGuard::drop`), and only the pump touches it.
        let got = unsafe { self.recv_exact(hdr, SD_HDR_SIZE) }.await?;
        if got < SD_HDR_SIZE {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }
        // SAFETY: the 48 bytes were just filled and nothing else aliases them.
        let hdr = unsafe { &*self.hdr.get() };
        let (id, result, len) = (
            resp_id(hdr),
            resp_result(hdr),
            resp_data_length(hdr) as usize,
        );

        let target = self.claim(id);
        let mut copied = 0usize;
        if let Some((_, dst, dst_len)) = target.filter(|&(_, dst, _)| !dst.is_null()) {
            let want = len.min(dst_len);
            if want > 0 {
                // SAFETY: `dst` is the caller's response buffer — a queue-slot
                // buffer that outlives the reactor drain — and no one else
                // reads or writes it while the request is in flight.
                copied = unsafe { self.recv_exact(dst, want) }.await?;
            }
            if copied < dst_len {
                // The server may trim trailing zeroes; the caller expects its
                // whole buffer written.
                // SAFETY: `dst[copied..dst_len]` is the untouched tail of that
                // same buffer.
                unsafe { std::ptr::write_bytes(dst.add(copied), 0, dst_len - copied) };
            }
        }
        // Whatever the caller had no room for still has to come off the wire.
        self.drain(len - copied.min(len)).await?;

        if let Some((idx, _, _)) = target {
            self.complete(idx, result);
        }
        Ok(())
    }

    /// Find the slot a response id belongs to, if it is still a live one.
    /// Returns the destination as it stood at that moment.
    fn claim(&self, id: u32) -> Option<(u16, *mut u8, usize)> {
        let idx = u16::try_from(id & 0xFFFF).expect("masked to 16 bits");
        let seq = u16::try_from(id >> 16).expect("masked to 16 bits");
        let slots = self.slots.borrow();
        let slot = slots.get(usize::from(idx))?;
        if slot.seq != seq {
            return None;
        }
        match slot.state {
            SlotState::Pending | SlotState::Abandoned => Some((idx, slot.dst, slot.dst_len)),
            SlotState::Free | SlotState::Done { .. } | SlotState::Failed { .. } => None,
        }
    }

    /// Publish a response to its caller (or reclaim an abandoned slot).
    fn complete(&self, idx: u16, result: u32) {
        if !matches!(self.state(idx), SlotState::Pending) {
            self.free_slot(idx);
            return;
        }
        let waker = {
            let mut slots = self.slots.borrow_mut();
            let slot = &mut slots[usize::from(idx)];
            slot.state = SlotState::Done { result };
            slot.waker.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
    }

    /// Swallow `len` bytes of payload nobody wants, so the next header is
    /// where the stream says it is.
    async fn drain(&self, mut len: usize) -> io::Result<()> {
        while len > 0 {
            let want = len.min(SCRATCH_LEN);
            // SAFETY: the scratch buffer belongs to the session (see
            // `read_one`) and only the pump touches it.
            let got = unsafe { self.recv_exact(self.scratch.get().cast::<u8>(), want) }.await?;
            if got < want {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
            len -= got;
        }
        Ok(())
    }

    /// [`recv_all`] with the in-flight bookkeeping the teardown path reads.
    ///
    /// # Safety
    /// Same contract as [`recv_all`]: `ptr..ptr+len` stays valid and unaliased
    /// for writes until the op's terminal CQE.
    async unsafe fn recv_exact(&self, ptr: *mut u8, len: usize) -> io::Result<usize> {
        self.recv_armed.set(true);
        // SAFETY: the caller upholds the buffer contract.
        let got = unsafe { recv_all(self.fd.as_raw_fd(), ptr, len) }.await;
        self.recv_armed.set(false);
        got
    }
}

/// Poll one request's outcome.
fn poll_slot(sess: &Session, idx: u16, cx: &Context<'_>) -> Poll<io::Result<u32>> {
    let mut slots = sess.slots.borrow_mut();
    let slot = &mut slots[usize::from(idx)];
    match slot.state {
        SlotState::Done { result } => Poll::Ready(Ok(result)),
        SlotState::Failed { errno } => Poll::Ready(Err(io::Error::from_raw_os_error(errno))),
        _ => {
            slot.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

// ---------------------------------------------------------------------------
// The pump
// ---------------------------------------------------------------------------

/// Unregisters a dead session, whether the pump returned or was cancelled
/// with the runtime.
struct PumpGuard {
    addr: SocketAddr,
    sess: Rc<Session>,
}

impl Drop for PumpGuard {
    fn drop(&mut self) {
        SESSIONS.with(|map| {
            let mut map = map.borrow_mut();
            if map
                .get(&self.addr)
                .is_some_and(|s| Rc::ptr_eq(s, &self.sess))
            {
                map.remove(&self.addr);
            }
        });
        self.sess.poisoned.set(true);
        self.sess.fail_all();
        if self.sess.recv_armed.get() {
            // Cancelled with a recv in flight (runtime teardown): the kernel
            // may still write into the session's buffers, so keep them —
            // leak, deliberately, exactly as the reactor does with an
            // undrained slab.
            std::mem::forget(Rc::clone(&self.sess));
        }
    }
}

/// The session's only reader: route responses until the connection dies.
async fn pump(addr: SocketAddr, sess: Rc<Session>) {
    let _guard = PumpGuard {
        addr,
        sess: Rc::clone(&sess),
    };
    loop {
        if sess.poisoned.get() {
            return;
        }
        if sess.inflight.get() == 0 {
            // Nothing owed: park on a waker rather than on the socket, so an
            // idle thread has no op armed.
            poll_fn(|cx| {
                if sess.poisoned.get() || sess.inflight.get() > 0 {
                    Poll::Ready(())
                } else {
                    *sess.pump_waker.borrow_mut() = Some(cx.waker().clone());
                    Poll::Pending
                }
            })
            .await;
            continue;
        }
        if sess.read_one().await.is_err() {
            sess.poison();
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// Sessions
// ---------------------------------------------------------------------------

/// This thread's live session for `addr`, dialing (and starting a pump) if
/// there is none.
async fn session(addr: SocketAddr) -> io::Result<Rc<Session>> {
    if let Some(sess) = live(addr) {
        return Ok(sess);
    }
    let fd = new_stream_socket(&addr)?;
    ops::connect(fd.as_raw_fd(), &addr)?.await?;
    // Another task may have dialed while we were: one connection per thread
    // means the loser's fd is dropped, not kept as a spare.
    if let Some(sess) = live(addr) {
        return Ok(sess);
    }
    let sess = Rc::new(Session::new(fd));
    SESSIONS.with(|map| map.borrow_mut().insert(addr, Rc::clone(&sess)));
    tokio::task::spawn_local(pump(addr, Rc::clone(&sess)));
    Ok(sess)
}

/// The mapped session for `addr` if it is still usable.
fn live(addr: SocketAddr) -> Option<Rc<Session>> {
    SESSIONS.with(|map| {
        map.borrow()
            .get(&addr)
            .filter(|s| !s.poisoned.get())
            .map(Rc::clone)
    })
}

// ---------------------------------------------------------------------------
// The request path
// ---------------------------------------------------------------------------

/// Runs one request to completion on this thread's connection to `addr`,
/// returning the response's `SD_RES_*` result.
///
/// `hdr` is a fully encoded 48-byte request header except for its id, which
/// this call stamps (it is the routing key for the response). `write` is the
/// payload that follows the header; `read` is where the response payload
/// lands, written by the pump and zero-filled past what the server sent.
///
/// Cancellation: dropping the returned future before the header has reached
/// the wire costs nothing; dropping it mid-send poisons the connection (its
/// framing would be unknowable); dropping it while waiting for the response
/// leaves the response to be discarded by the pump.
pub(super) async fn request(
    addr: SocketAddr,
    hdr: &mut [u8; SD_HDR_SIZE],
    write: Option<&[u8]>,
    read: Option<&mut [u8]>,
) -> io::Result<u32> {
    let sess = session(addr).await?;
    let (id, idx) = sess.alloc(read)?;
    set_req_id(hdr, id);
    let mut guard = RequestGuard {
        sess: &sess,
        idx,
        phase: Phase::Queued,
    };

    poll_fn(|cx| sess.poll_acquire_send(idx, cx)).await?;
    guard.phase = Phase::Sending;
    // A request is a header and, for writes, a payload — back to back on the
    // wire. Both live in the caller's frame (the slot task's), valid until the
    // ops' terminal CQEs, the same envelope `FileBackend`'s vectored IO uses.
    let sent = async {
        // SAFETY: `hdr` is the caller's live buffer, held across the await.
        unsafe { send_all(sess.fd.as_raw_fd(), hdr.as_ptr(), SD_HDR_SIZE) }.await?;
        if let Some(w) = write {
            // SAFETY: `w` is the caller's buffer, valid across the await.
            unsafe { send_all(sess.fd.as_raw_fd(), w.as_ptr(), w.len()) }.await?;
        }
        Ok::<(), io::Error>(())
    }
    .await;
    guard.phase = Phase::Sent;
    sess.release_send();
    // The pump parks while nothing is owed; this request is now owed.
    sess.wake_pump();
    if let Err(e) = sent {
        sess.poison();
        guard.phase = Phase::Done;
        return Err(e);
    }

    let result = poll_fn(|cx| poll_slot(&sess, idx, cx)).await;
    guard.phase = Phase::Done;
    result
}

/// Keeps the session consistent when a request future is dropped before it
/// completes — see the cancellation note on [`request`].
struct RequestGuard<'a> {
    sess: &'a Rc<Session>,
    idx: u16,
    phase: Phase,
}

impl Drop for RequestGuard<'_> {
    fn drop(&mut self) {
        match self.phase {
            Phase::Queued => {
                self.sess.unqueue_send(self.idx);
                self.sess.free_slot(self.idx);
            }
            Phase::Sending => {
                // A send op may still be writing into the socket: what the
                // peer will have seen is unknowable, so the connection goes.
                self.sess.poison();
                self.sess.free_slot(self.idx);
            }
            Phase::Sent => self.sess.abandon(self.idx),
            Phase::Done => self.sess.free_slot(self.idx),
        }
    }
}

// ---------------------------------------------------------------------------
// Socket helpers
// ---------------------------------------------------------------------------

/// Send all of `ptr..ptr+len` on `fd`, resuming across short sends.
///
/// # Safety
/// `ptr..ptr+len` must stay valid (reads) until the returned future completes
/// or the reactor drains — the raw-op contract of [`ops::send_raw`].
async unsafe fn send_all(fd: RawFd, ptr: *const u8, len: usize) -> io::Result<()> {
    let mut off = 0usize;
    while off < len {
        let want = u32::try_from(len - off).unwrap_or(u32::MAX);
        // SAFETY: `ptr.add(off)` stays within the caller's buffer; the buffer
        // is kept valid per this function's safety contract.
        let n = unsafe { ops::send_raw(fd, ptr.add(off), want) }?.await?;
        if n == 0 {
            return Err(io::Error::from(io::ErrorKind::WriteZero));
        }
        off += n as usize;
    }
    Ok(())
}

/// Receive up to `len` bytes into `ptr`, resuming across short receives.
/// Returns the number of bytes read; fewer than `len` only on EOF.
///
/// # Safety
/// `ptr..ptr+len` must stay valid and unaliased for writes until the returned
/// future completes or the reactor drains — the contract of [`ops::recv_raw`].
async unsafe fn recv_all(fd: RawFd, ptr: *mut u8, len: usize) -> io::Result<usize> {
    let mut off = 0usize;
    while off < len {
        let want = u32::try_from(len - off).unwrap_or(u32::MAX);
        // SAFETY: `ptr.add(off)` stays within the caller's buffer, kept valid
        // per this function's safety contract.
        let n = unsafe { ops::recv_raw(fd, ptr.add(off), want) }?.await? as usize;
        if n == 0 {
            break; // EOF
        }
        off += n;
    }
    Ok(off)
}
