# StreamSender — the gather-send harness for stream transports

> You may have heard it called `SenderStream`; the type is named
> **`StreamSender`** and lives in the `ioutgt-stream` crate
> (`crates/ioutgt-stream/src/lib.rs`). NVMe/TCP is its first user; NBD
> is the planned second.

`StreamSender` is the **send path** of a stream transport: it drains a
queue of completed work, packs each item onto the wire as one vectored
`sendmsg`, and — under zero-copy mode — keeps the kernel's page
references straight so slot memory is never reused too early. It is
**protocol-neutral**: everything NVMe-specific is supplied by the
transport as a one-line staging closure.

This document goes top to bottom: motivation → where it sits → the data
structures → the send loop → staging → shipping → the zero-copy
lifecycle → teardown → invariants → how NVMe/TCP wires it up.

---

## 1. Motivation: why a separate harness

A naive send path writes one response at a time:

```text
  for each completed command:
      write(socket, header)
      write(socket, payload)      ← one syscall per PDU, one copy per payload
```

That is three problems at once on a saturated 100G link:

1. **Syscall + park per PDU.** Every small R2T or status capsule costs a
   round trip through the reactor.
2. **A payload copy** from the slot buffer into the kernel.
3. **Wire ordering is fragile** the moment you try to overlap sends —
   independent `sendmsg` SQEs on one socket have *no* ordering
   guarantee, so you cannot just pipeline two of them.

The send path that solves all three is intricate: a header **arena** +
**iovec gather** (one syscall, zero payload copy), `SENDMSG_ZC` for true
zero-copy with a **pin-budget fallback**, **short-send resume**,
**notification reaping**, and a **double-buffer** so the kernel's
zero-copy notification (≈ one RTT) overlaps the next batch's staging
without ever putting two ops on the wire.

None of that is NVMe-specific. `StreamSender` factors the whole machine
into a reusable crate so a second transport (NBD) gets it for free. The
crate doc says it plainly:

> All of that machinery … is protocol-agnostic and lives here in
> `StreamSender`. The one transport-specific concern is *staging one
> work item*.

---

## 2. Where it sits

`ioutgt-stream` is a mid-layer crate that bridges the two opposite
leaves without making them depend on each other:

```text
        ioutgt-nvme-tcp        ← transport: PDU codec + staging closure
              │  uses
              ▼
        ┌───────────────┐
        │ ioutgt-stream │      ← StreamSender: the send-path state machine
        │  StreamSender │
        └───────────────┘
           │           │
   uses    │           │   uses
           ▼           ▼
  ioutgt-core      ioutgt-uring
  · SendList<W>    · GatherBatch   (arena + iovec mechanics)
  · SlotArray<C>   · ops::sendmsg* (io_uring send ops)
  (slot engine)    · ZcNotif       (reactor)
```

- **`ioutgt-core`** owns the slots and the work queue (no I/O).
- **`ioutgt-uring`** owns the gather *mechanics* and the io_uring ops
  (no protocol).
- **`ioutgt-stream`** owns the *policy* that drives them — batching,
  ordering, ZC lifecycle — but knows nothing about NVMe.

### The three actors per connection

`StreamSender` is one of three tasks that never call each other; their
only rendezvous is the shared queue state.

```text
  recv loop ──┐                              ┌── slot tasks
              │  claim_tag / submit          │   dispatch::execute
              ▼                              ▼
        ┌────────────────────────────────────────────┐
        │  shared queue  (single thread, no locks)    │
        │   · SlotArray<C>   command slots + freelist  │
        │   · SendList<W>    ordered work queue        │
        └────────────────────────────────────────────┘
                      │  work.next()
                      ▼
                 send loop  ── StreamSender ──► socket
```

The slot task finishes a command and **pushes** `SendWork` onto the
`SendList`; the send loop **pulls** it. That hand-off is the only
coupling.

---

## 3. The data structures, bottom up

Every type the send path touches, grouped by the crate that owns it.
The ownership/reference map:

```text
  ioutgt-stream  ─────────────────────────────────────────────┐
    StreamSender                                               │
      ├─ batches: [GatherSendBatch; 2]                         │
      │             ├─ gather: GatherBatch ───────────┐        │
      │             ├─ tags_at_cqe / tags_at_notif     │ owns   │
      │             └─ pending_notifs: Vec<ZcNotif> ─┐ │        │
      ├─ inflight: VecDeque<usize>                   │ │        │
      └─ zc_* counters  → ZcStats                    │ │        │
                                                     │ │        │
  ioutgt-uring  (pure IO) ◄───────────────────────── │ ┘        │
    GatherBatch   arena + iovec + msghdr             │          │
    SendZcOp ──into_notif──► ZcNotif  ◄──────────────┘          │
    RawOp     (plain sendmsg)                                   │
                                                                │
  ioutgt-core  (slot engine) ◄──── run() borrows ──────────────┤
    QueueCore<C> ─┬─ slots: SlotArray<C> ─┬─ Slot<C> · SlotState│
                  │                       └─ free_tags          │
                  └─ stats: QueueStats                          │
    SendList<W>   ordered work queue                            │
                                                                │
  ioutgt-nvme-tcp  (transport) ◄──── composes core + list ──────┘
    NvmeTcpQueue ─┬─ nvme: Rc<QueueCore<Sqe>>
                  └─ send: SendList<SendWork>
    SendWork · Completion   (the W and its payload)
```

`StreamSender` *owns* its batches (and through them the `GatherBatch`es
and `ZcNotif`s); it only *borrows* the slots and work list for the
duration of `run()`. Walking bottom up:

### 3.1 `GatherBatch` — protocol-free gather mechanics (`ioutgt-uring`)

The lowest layer: a header **arena** and an **iovec list**, plus a
short-send resume cursor. It knows nothing about tags, work, or async —
pure byte/pointer plumbing.

```rust
pub struct GatherBatch {
    arena: Box<[u8]>,        // headers/digests packed linearly
    arena_used: usize,
    iovs: Vec<libc::iovec>,  // gather list: arena chunks + payload refs
    iov_cap: usize,          // hard cap (≤ UIO_MAXIOV = 1024)
    live: usize,             // first not-yet-fully-sent iovec
    msghdr: Box<libc::msghdr>,
}
```

Key methods (all `#[inline]`, hot path):

| Method | Does |
|--------|------|
| `arena_tail()` | mutable slice to encode the next header into |
| `push_arena(n)` | publish `n` bytes at the tail + append an iovec |
| `push_raw(ptr, n)` | append a payload ref **in place**; merges if byte-contiguous with the previous entry |
| `fits(a, i)` | headroom for one more worst-case item? |
| `msghdr()` | build the `msghdr` over the unsent suffix `iovs[live..]` |
| `advance(n)` | consume `n` sent bytes; `true` when the batch is fully sent |
| `reset()` | recycle for the next round |

The merge in `push_raw` is what makes a header-only batch collapse to a
**single** iovec:

```text
 push_arena("R2T#1")   ┐ byte-contiguous in the arena
 push_arena("R2T#2")   ┘ → ONE iovec entry, iov_len grows
```

A read's C2HData payload rides in place from the slot's pooled data
buffer. That buffer may be one contiguous run or a scatter list of pool
pages, so the transport pushes one `push_raw` per segment; non-adjacent
segments stay as separate iovec entries. The per-item `fits(a, i)` headroom
therefore reserves `MAX_SEGS + 3` iovecs (header + up to `MAX_SEGS` payload
segments + DDGST + capsule) so staging a scattered payload can never
overrun the iovec cap (which itself clamps to `UIO_MAXIOV`).

The two kernel structs it manages are raw libc types:

- **`libc::iovec`** `{ iov_base: *mut c_void, iov_len: usize }` — one
  gather entry (a pointer + length into the arena or a slot buffer).
- **`libc::msghdr`** — the `sendmsg(2)` descriptor; `msghdr()` points its
  `msg_iov`/`msg_iovlen` at the unsent suffix `iovs[live..]` before each
  submit.

### 3.1.1 The io_uring send ops (`ioutgt-uring::ops`)

`ship_batch` issues one of two op futures over the `msghdr`:

```rust
// Plain vectored send (IORING_OP_SENDMSG). One CQE = bytes accepted.
pub struct RawOp { /* slab-entry handle */ }     // .await -> io::Result<u32>

// Zero-copy vectored send (IORING_OP_SENDMSG_ZC). TWO CQEs:
pub struct SendZcOp { op: MultiOp }
impl SendZcOp {
    pub async fn sent(&mut self) -> io::Result<u32>; // CQE 1: bytes accepted
    pub fn into_notif(self) -> ZcNotif;              // CQE 2 handle (take always)
}

// The notification future gating slot reuse. Yields `true` if the
// kernel copied after all (no pages pinned — REPORT_USAGE).
pub struct ZcNotif { op: MultiOp }
impl Future for ZcNotif { type Output = bool; /* ... */ }
```

```text
  sendmsg_zc_raw(fd, msghdr)
        │
        ├─ CQE 1  ── SendZcOp::sent()  ─► bytes accepted (like sendmsg)
        │
        └─ CQE 2  ── ZcNotif (await)   ─► kernel dropped its last page ref
                                          (≈ peer ACK). Until this fires,
                                          the slot buffer MUST NOT be reused.
```

`into_notif()` must be taken (and eventually awaited or deliberately
orphaned) even on the error path — a failed ZC send can still pin pages
(`F_MORE`) and post a notification.

### 3.2 `GatherSendBatch` — one batch + its tag/ZC accounting (`ioutgt-stream`, private)

`StreamSender` wraps each `GatherBatch` with the bookkeeping needed to
release tags at the right time:

```rust
struct GatherSendBatch {
    gather: GatherBatch,
    arena_per_item: usize,      // worst-case sizings, for fits()
    iovs_per_item: usize,
    tags_at_cqe: Vec<u16>,      // release at the send CQE (no slot ref)
    tags_at_notif: Vec<u16>,    // release only after ZC notifs reaped
    pending_notifs: Vec<ZcNotif>, // one per ZC op issued for this batch
}
```

### 3.3 `Staged` — the transport's release verdict (`ioutgt-stream`, public)

The staging closure returns one of these for each work item. It is the
*entire* protocol-specific contract:

```rust
pub enum Staged {
    NoRelease,     // e.g. an NVMe R2T — slot stays Receiving, no tag freed
    AtCqe(u16),    // op references no slot memory (header/capsule only)
    AtNotif(u16),  // op references slot buffers in place → ZC-gated
}
```

```text
   work item ──► stage() ──► Staged
                              ├─ NoRelease   → no tag bookkeeping
                              ├─ AtCqe(t)    → tags_at_cqe.push(t)
                              └─ AtNotif(t)  → tags_at_notif.push(t)
```

### 3.4 `StreamSender` — the driver (`ioutgt-stream`, public)

```rust
pub struct StreamSender {
    batches: [GatherSendBatch; 2],  // double-buffered: stage one, reap one
    inflight: VecDeque<usize>,      // batch indices awaiting ZC notifs (≤ 2)
    zc_batches: u64,                // telemetry
    zc_copied: u64,
    zc_fallbacks: u64,
}
```

Two batches is the whole trick: while batch A's zero-copy notification
is outstanding (≈ one RTT), batch B stages and ships. At most **one send
op is ever in flight** on the socket; only the *waits* overlap.

### 3.5 `ZcStats` — the telemetry snapshot (`ioutgt-stream`, public)

`stats()` returns this after `run()`; the transport logs it:

```rust
pub struct ZcStats {
    pub zc_batches: u64,    // batches shipped with ≥1 outstanding ZC notif
    pub zc_copied: u64,     // notifs that reported the kernel copied anyway
    pub zc_fallbacks: u64,  // ZC sends refused by the pin budget → copied
}
```

A high `zc_copied` or `zc_fallbacks` is the signal that zero-copy isn't
paying off (loopback always copies; the pin budget is shared and small).

### 3.6 The slot engine (`ioutgt-core`)

The work queue the sender drains, and the slots it releases tags into.

**`SendList<W>`** — the ordered work queue (FIFO). The transport `push`es;
the sender `next`/`try_next`/`poll_next`s; `close()` ends the loop:

```rust
pub struct SendList<W> {
    work: RefCell<VecDeque<W>>, // transport pushes, sender drains (FIFO)
    waker: Cell<Option<Waker>>, // send-loop doorbell
    closed: Cell<bool>,         // close() → next() yields None = teardown
}
```

**`SlotArray<C>`** — the preallocated command slots + the tag freelist.
`release_tag` is what the sender calls to return a slot:

```rust
pub struct SlotArray<C: Copy> {
    pub nslots: u16,
    slots: Box<[Slot<C>]>,
    free_tags: RefCell<Vec<u16>>,   // LIFO: hot slots stay cache-warm
    executing: Cell<u16>,           // slots inside dispatch (teardown gate)
    tag_waiter: Cell<Option<Waker>>,// recv-path doorbell, woken by release_tag
}
// release_tag(tag): return tag to free_tags, wake a recv path parked on
// tag exhaustion. The send path's only write into the slot engine.
```

**`Slot<C>`** — one command slot; `data` is the buffer the sender
references in place (read payload out) or the recv path fills (write
payload in):

```rust
pub struct Slot<C: Copy> {
    state: Cell<SlotState>,
    cmd: Cell<C>,                 // the received command (C = Sqe for NVMe)
    waker: Cell<Option<Waker>>,   // slot-task doorbell
    data: RefCell<AlignedBuf>,    // payload buffer (4K-aligned for O_DIRECT)
    data_len: Cell<u32>,
    recv_offset: Cell<u32>,       // multi-PDU reassembly cursor
}
```

**`SlotState`** — the lifecycle a tag walks; the sender acts at the tail:

```text
  Free ─claim_tag─► Receiving ─submit─► Ready ─dispatch─► Executing
   ▲                   ╷                                      │
   │      R2T: stays ──╯ (no tag release;                     ▼ complete
   │      Receiving       slot still filling)             Responding
   │                                                          │
   └──────────────────── release_tag ◄───────────────────────┘
        the send path's ONLY slot transition: Responding → Free
```

```rust
pub enum SlotState { Free, Receiving, Ready, Executing, Responding }
```

**`QueueCore<C>`** — the transport-neutral per-queue context: the slots
plus SQ-head flow control and stats. Generic over the command type (`Sqe`
for NVMe, an NBD request header next). It deliberately does **not** hold
the send list — that `W` belongs to the transport:

```rust
pub struct QueueCore<C: Copy> {
    pub slots: SlotArray<C>,    // also reachable via Deref
    pub sqsize: u16,
    pub qid: u16,               // 0 = admin
    sqhd: Cell<u16>,            // NVMe SQ-head flow control
    pub sqhd_disabled: bool,
    pub stats: Rc<QueueStats>,  // lifetime IO counters
}
```

(`QueueStats` is a bag of `Cell<u64>` lifetime counters — commands,
bytes, errors — shared with the owning thread's stats list. Not on the
send hot path.)

### 3.7 The NVMe/TCP composite (`ioutgt-nvme-tcp`)

The connection-shared state, and the concrete `W` the sender carries.

**`NvmeTcpQueue`** — joins the core context with this transport's send
list; `Deref`s to `QueueCore<Sqe>` for convenience:

```rust
pub struct NvmeTcpQueue {
    pub nvme: Rc<QueueCore<Sqe>>,  // slots, sqhd, stats
    pub send: SendList<SendWork>,  // the work queue StreamSender drains
}
```

**`SendWork`** — the `W` for NVMe/TCP; one unit of send work. Both
variants serialize through the one `SendList` to keep wire order:

```rust
pub enum SendWork {
    Response(Completion),                 // C2HData payload + status capsule
    R2t { tag: u16, cid: u16,             // solicit host write data
          offset: u32, length: u32 },     //   (slot stays Receiving)
}
pub struct Completion {
    pub tag: u16,        // slot index
    pub cqe: Cqe,        // NVMe completion entry (status, cid, …)
    pub data_len: u32,   // bytes of read data in the slot to send as C2HData
}
```

`Sqe`/`Cqe` are the 64-byte NVMe submission/completion entries from the
sans-IO `ioutgt-nvme` codec — `Sqe` is the stashed command (`C`), `Cqe`
the status the capsule carries back.

---

## 4. The send loop, top to bottom

### 4.1 Entry point: `run()`

The transport calls `run()` once per connection. It runs the loop, then
**always drains** before returning:

```rust
pub async fn run<C, W, F>(&mut self, fd, send_zc, slots, work, stage) -> io::Result<()>
where F: FnMut(&mut GatherBatch, &W) -> Staged
{
    let result = self.send_batches(fd, send_zc, slots, work, &mut stage).await;
    self.drain(slots).await;   // kernel may still hold page refs
    result
}
```

> **Why drain on every exit?** Orderly close, send error, or `WriteZero`
> all land here. The kernel may still hold page references to the arena
> and slot buffers; the caller frees that memory right after joining the
> send task, so we must wait out every ZC notification first.

### 4.2 The core: `send_batches()`

Five phases per turn — get work, acquire a batch, stage, ship, retire:

```rust
let mut carry: Option<W> = None;
loop {
    let first = match carry.take() {
        Some(item) => item,                              // overflow from last round
        None => match self.next_work_reaping(slots, work).await {
            Some(item) => item,
            None => return Ok(()),                       // close(): teardown
        },
    };
    let idx   = self.acquire(slots).await;               // free batch (reap if both busy)
    let batch = &mut self.batches[idx];
    carry     = stage_batch(batch, first, work, stage);  // greedily pack until full
    ship_batch(fd, batch, send_zc, &mut self.zc_fallbacks).await?;  // one op + short-send loop
    self.retire(slots, idx);                             // release tags, classify
}
```

```text
  ┌─────────────────────────────────────────────────────────────┐
  │ 1. first item   carry, else next_work_reaping (anti-deadlock)│
  │ 2. acquire      a free batch  (reap oldest if both in flight) │
  │ 3. stage_batch  pack first + every ready item until headroom  │
  │ 4. ship_batch   ONE sendmsg / sendmsg_zc; loop on short send   │
  │ 5. retire       free AtCqe tags now; queue ZC batch for reaping│
  └─────────────────────────────────────────────────────────────┘
            ▲                                              │
            └──────────────────  loop  ────────────────────┘
```

`carry` is a zero-cost handoff: when an item doesn't fit the current
batch, `stage_batch` hands it back and it *seeds the next round* — never
re-queued, never re-awaited.

---

## 5. Staging: filling one batch

`stage_batch` greedily packs work until the batch runs out of headroom:

```rust
fn stage_batch<W, F>(batch, first, work, stage) -> Option<W> {
    let mut item = Some(first);
    while let Some(w) = item {
        if !batch.fits() { return Some(w); }   // full → flush, w leads next round
        match stage(&mut batch.gather, &w) {    // ← transport closure
            Staged::NoRelease => {}
            Staged::AtCqe(t)  => batch.tags_at_cqe.push(t),
            Staged::AtNotif(t)=> batch.tags_at_notif.push(t),
        }
        item = work.try_next();                 // non-blocking pop
    }
    None
}
```

The closure encodes headers into the arena and references payloads in
place. Here is what one read response with data looks like in the batch:

```text
  arena (headers/digests, packed)        iovec gather list (what the kernel sends)
  ┌──────────────────────────────┐       ┌───────────────────────────────┐
  │ C2HData hdr │ DDGST │ capsule │       │ 1: → C2HData hdr (in arena)    │
  └─────┬───────┴───┬───┴────┬────┘       │ 2: → slot buffer payload  ◄────┼─ IN PLACE,
        │           │        │            │ 3: → DDGST (in arena)          │   zero copy
        ▼           │        ▼            │ 4: → capsule (in arena)        │
   slot buffer ─────┘   (CRC32C)          └───────────────────────────────┘
   (read data,                            entries 1,3,4 are arena chunks;
    referenced not copied)                only 2 points at the slot
```

Only headers and digests are copied (into the arena). **Payload bytes
never move** — `push_raw` adds an iovec pointing straight at the slot
buffer.

---

## 6. Shipping: one op, short-send loop, ZC fallback

`ship_batch` issues exactly one op and loops *only* to finish a partial
send — nothing else may interleave on the wire:

```rust
let mut use_zc = send_zc;
loop {
    let n = if use_zc {
        let mut op = ops::sendmsg_zc_raw(fd, batch.gather.msghdr())?;
        let res = op.sent().await;
        batch.pending_notifs.push(op.into_notif());   // stash notif BEFORE checking res
        match res {
            Ok(n) => n,
            Err(e) if e is ENOMEM|ENOBUFS => { *zc_fallbacks += 1; use_zc = false; continue; }
            Err(e) => return Err(e),
        }
    } else {
        ops::sendmsg_raw(fd, batch.gather.msghdr())?.await?   // copy path
    };
    if n == 0 { return Err(WriteZero); }
    if batch.gather.advance(n) { return Ok(()); }   // fully sent → done
    // else: short send — re-issue the unsent suffix on the SAME batch
}
```

```text
  ┌── ship ──────────────────────────────────────────────────────┐
  │  use_zc?                                                       │
  │   ├─ yes → sendmsg_zc ─ stash notif ─ ENOMEM/ENOBUFS? ─┐       │
  │   │                                       │ no         │ yes   │
  │   │                                       ▼            ▼       │
  │   └─ no  → sendmsg (copy) ──────────────► sent n   use_zc=false│
  │                                            │        (retry by  │
  │                                            ▼         copy)     │
  │                              advance(n) fully sent? ──► done    │
  │                                    │ no                         │
  │                                    └── re-issue unsent suffix ──┘
  └───────────────────────────────────────────────────────────────┘
```

Two subtleties worth calling out:

- **The notification is stashed before the result is examined.** A
  failed ZC send can *still* have pinned pages (the `F_MORE` partial
  case); only the stashed handle makes the teardown drain wait for them.
- **`ENOMEM`/`ENOBUFS` is not an error.** Zero-copy pins pages against
  the per-user `RLIMIT_MEMLOCK` budget, shared across connections — two
  full batches can exceed the default 8 MiB. The code transparently
  falls back to a copying `sendmsg` for the rest of the batch and counts
  it in `zc_fallbacks`. **ZC is an optimization, never a correctness
  dependency.**
- **Short-send resume is in place.** `advance(n)` skips fully-sent
  iovecs and bumps the partial one's `iov_base`/`iov_len` — no memmove,
  and the re-issue is the *same batch*, preserving wire order. Under ZC
  the re-issue **stays ZC** (the batch's pages are already pinned), so it
  stashes a second notification — the only path that makes
  `pending_notifs.len() > 1`, and the reason that vector is sized for 8.

---

## 7. The zero-copy lifecycle — the heart of the design

This is what justifies the whole harness. With `SENDMSG_ZC` the kernel
holds references to the slot pages until the data is ACKed; reusing a
slot before that is a use-after-free on the wire. So tag release is
**gated on a notification**, and the loop must never deadlock waiting
for one.

### 7.1 Tag release classes — decided at stage, acted on at retire

```text
  Staged::AtCqe(t)   ──► tags_at_cqe   ──► release in retire()  (send CQE)
  Staged::AtNotif(t) ──► tags_at_notif ──► release in recycle() (after notifs)
  Staged::NoRelease  ──► (nothing)
```

`retire()` runs the instant a batch hits the socket:

```rust
fn retire(&mut self, slots, idx) {
    for tag in batch.tags_at_cqe.drain(..) { slots.release_tag(tag); }  // safe now
    if batch.pending_notifs.is_empty() {
        batch.recycle(slots);          // copy-only batch → reuse immediately
    } else {
        self.zc_batches += 1;
        self.inflight.push_back(idx);  // ZC batch → reap later
    }
}
```

### 7.2 Double-buffer timeline

```text
  time ─────────────────────────────────────────────────────────►

  batch A:  [stage][ship]·····wait notif (≈RTT)·····[reap → recycle]
  batch B:              [stage][ship]·····wait notif·····[reap]
                         ▲
                         └ A's notification overlaps B's work;
                           never two ships at once (wire order kept)
```

`acquire()` returns a free batch; if **both** are awaiting
notifications, it `reap_oldest()` first — bounded to two in flight.

### 7.3 Anti-deadlock: `next_work_reaping`

The trap: in ZC mode, suppose every tag is notif-gated and the host is
idle. No new `SendWork` arrives — but the *only* thing that frees a tag
is a notification. Parking on "new work" alone would hang forever.

The fix: park on work and the oldest batch's notifications **in
parallel**.

```rust
let outcome = poll_fn(|cx| {
    if let Poll::Ready(item) = work.poll_next(cx) { return Ready(Wait::Work(item)); }
    if poll_notifs(&mut batch.pending_notifs, zc_copied, cx) { return Ready(Wait::Reaped); }
    Poll::Pending
}).await;
```

```text
        ┌──────────────────────────────────────┐
        │  park, watching BOTH:                 │
        │    · work.poll_next()  ──► new item   │──► return item, ship it
        │    · oldest batch notifs ──► reaped   │──► recycle, frees a tag,
        └──────────────────────────────────────┘     host can send again → loop
```

Whichever fires first wins; reaping a batch recycles it and releases its
notif-gated tags, which unblocks the host.

---

## 8. Teardown

```text
  recv loop sees EOF / error
        │  SendList::close()
        ▼
  work.next() → None  ──► send_batches returns Ok
        │
        ▼
  run() calls drain(slots):
        · reap every batch on the inflight list
        · reap both batch slots (covers a batch a send error abandoned
          mid-ship — not on the list, but may hold notifs)
        │
        ▼
  send task joins ──► queue memory freed  (now safe: no kernel page refs)
```

A `Drop` tripwire (`debug_assert`) fires if a `StreamSender` is ever
dropped with `inflight` non-empty or notifications pending — that would
mean a path shipped ZC ops without draining, i.e. a use-after-free risk.

---

## 9. Invariants

1. **Zero steady-state allocation.** Both batches' arenas, iovec lists,
   and tag vectors are preallocated in `new()`; the loop only `reset()`s
   and recycles. (The notif vector is sized for the common ≤1-notif case
   and would only grow under repeated short sends — never in steady
   state.)
2. **No locks, no atomics.** Single thread; `SendList`/`SlotArray` use
   `Cell`/`RefCell` and waker doorbells.
3. **One send op in flight per connection.** Double-buffering overlaps
   *waits*, never *sends*; short sends loop on the same batch. (This is a
   per-connection rule — a queue thread's single `io_uring_enter` still
   batches this connection's lone send SQE with every other
   connection's; see `architecture.md` §4.2.2.)
4. **ZC notification gating.** A payload-carrying tag is released only
   after every notification for its batch is reaped.
5. **Cancellation/teardown safety.** `run()` drains on every exit before
   the caller frees queue memory; the `Drop` tripwire enforces it.
6. **Anti-deadlock.** Tag release never depends on new work arriving.

---

## 10. How NVMe/TCP wires it up

The transport supplies just two things: a staging closure and the
worst-case sizings. From `crates/ioutgt-nvme-tcp/src/connection.rs`:

```rust
const ARENA_PER_ITEM: usize = 64;  // C2HData hdr 24+4 + DDGST 4 + capsule 24+4
const IOVS_PER_ITEM:  usize = 4;   // header, payload, digest, capsule (adjacent merge)

let mut sender = StreamSender::new(queue.sqsize, ARENA_PER_ITEM, IOVS_PER_ITEM);
sender.run(fd, send_zc, &queue.nvme.slots, &queue.send,
    |gather, work: &SendWork| {
        stage_send_work(gather, queue, work, hdr_digest, data_digest);  // encode PDUs
        release_class(work)                                             // → Staged
    },
).await
```

The closure is the entire NVMe-specific surface:

```rust
fn release_class(work: &SendWork) -> Staged {
    match *work {
        SendWork::Response(c) if c.data_len > 0 => Staged::AtNotif(c.tag), // payload in iovecs
        SendWork::Response(c)                   => Staged::AtCqe(c.tag),   // capsule only
        SendWork::R2t { .. }                    => Staged::NoRelease,      // slot still Receiving
    }
}
```

`stage_send_work` encodes R2T / C2HData / response-capsule PDUs (with
optional header & data digests, and the SUCCESS-elision optimization
when SQ flow control is off) via the sans-IO `ioutgt-nvme` codec,
calling `gather.push_arena()` for headers and `gather.push_raw()` to
reference the slot payload in place.

**That is the full extent of NVMe knowledge in the send path.** Swap the
closure and the sizings, and the same `StreamSender` drives NBD.

---

## In one sentence

`StreamSender` is a serial, double-buffered send-path state machine that
gathers headers into an arena and payloads in place, ships one
`sendmsg`/`SENDMSG_ZC` at a time, and gates slot reuse on zero-copy
notifications — with all protocol specifics delegated to a one-line
staging closure.
