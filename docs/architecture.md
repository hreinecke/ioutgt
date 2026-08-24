# ioutgt Architecture Specification

Status: as-built specification through M17. The milestone table at the
end records what shipped; `docs/roadmap.md` holds what's next. This doc
covers the transport-neutral engine; the per-transport detail lives in
[`docs/nvme-tcp.md`](nvme-tcp.md) and [`docs/nvme-rdma.md`](nvme-rdma.md).

## 1. The core idea: bounded concurrency

Most async servers have unbounded concurrency and pay for it with dynamic
allocation, task spawning, and buffer churn on every request. NVMe does not:
a queue pair has a fixed depth negotiated at Connect time, and every command
is identified by a CID drawn from that bounded space.

ioutgt treats this bound as the central scheduling primitive, the way SPDK's
request tracker does, but expressed as async Rust:

- At queue install, a `Box<[Slot<Sqe>]>` of exactly `sqsize` slots is
  allocated, plus one **persistent async task per slot** ("task per tag").
- Each task loops forever: await command arrival in my slot → dispatch →
  await backend completion → queue the response → return my tag.
- The transport's transfer identifier (TCP TTAG, the low bits of the RDMA
  wr_id) *is* the slot index. The host CID is opaque — stored in the slot
  and echoed in the CQE — so no CID→slot hash map exists anywhere.
- Slot wakeups are same-thread `Cell<Option<Waker>>` doorbells: no atomics,
  no channels, no allocation.

Steady state on the IO path: **zero allocations, zero atomic RMW, zero
locks**.

## 2. Process and thread model

One process serves one port (N subsystems). Three kinds of thread:

```text
Controller Process
│
├── Control Thread            plain Tokio (current-thread, enable_all)
│     ├── transport listener + accept
│     ├── transport handshake → routing key (qid)
│     ├── UDS control plane (JSON): namespace mgmt, stats
│     └── routes accepted queues:  qid 0 → Admin thread
│                                  qid n → IO thread[(n-1) % N]
│
├── Admin Queue Thread         own ring; admin queues of all controllers
│
└── IO Queue Threads 0..N-1    pinned to one CPU from spread_cpus group i
                               (§10); own ring, own memory, own command
                               slots, own send/recv machines
```

Why the handshake runs on the control thread:

- The routing key (qid) is only knowable after transport-specific setup
  (§5.1 item 1) — blind round-robin of raw connections would land admin
  queues on IO threads.
- Handshake traffic is control-plane rate; plain Tokio sockets cost
  nothing there and keep queue threads free of accept/handshake states.
- After the handshake, the connection is packed into a transport `Conn`
  value and mailed to its queue thread, which owns it exclusively for the
  connection's lifetime.

Cross-thread communication into a queue thread happens **only** through its
mailbox (MPSC queue + eventfd doorbell, watched by a persistent multishot
read on the ring). Queue-thread handles are deliberately not `Send`; the
mailbox sender is the only exported handle.

One transport-specific variance: NVMe/RDMA adds a dedicated CM reactor
thread for the `rdma_cm` event channel (its fd parks on io_uring
`POLL_ADD`, which the plain-Tokio control thread cannot provide); accepted
queues reach the same admin/IO threads through the shared harness.

## 3. Crate map and cross-crate call flow

The workspace is ten crates forming a strict dependency DAG — every
crate depends only on layers below it. The two foundation leaves are
deliberately opposite in character:

- `ioutgt-core` is the **protocol-neutral queue engine** — slot array,
  buffer pool, permits, the `Backend` trait — plus the structural
  target model (subsystem/namespace tables, controller registry);
  zero dependencies.
- `ioutgt-uring` is **pure IO**: op futures and the reactor, zero
  protocol knowledge.

`ioutgt-nvme` layers the NVMe protocol on `ioutgt-core`: its codec
modules (`spec`/`pdu`/`identify`/`fabrics`/`status`/`digest`) stay
**sans-IO** — pure bytes ↔ structs, no sockets, no async, fuzzable in
isolation — while the rest of the crate executes commands (dispatch,
admin/IO handlers, fabrics Connect, CC/CSTS register state). The
structural target model — subsystem/namespace tables and the
controller registry — lives in `ioutgt-core`, so the harness and
control plane hold the served model without an NVMe dependency.

A third small leaf, `ioutgt-cpus`, groups CPUs evenly per NUMA / cluster /
SMT locality (`spread_cpus`); the algorithm is pure (driven by a
`CpuTopology` value, synthetic in tests), with sysfs reading confined to
`CpuTopology::from_sysfs()`.

Above the frontends, `ioutgt-harness` is the shared binary harness —
config loading, `spawn()`, the queue-thread pool, control server and the
`ctl`/`list`/`stat` clients — parameterized over a `Transport` trait so
the NVMe/TCP and NVMe/RDMA binaries are thin wrappers around the same
machinery.

**Crate map — the dependency DAG**

```text
  binaries  ┌─────────────────────────┐  ┌─────────────────────────┐
            │ ioutgt-nvme-tcp         │  │ ioutgt-nvme-rdma        │
            │ (bin: ioutgt-nvme-tcp)  │  │ (bin: ioutgt-nvme-rdma) │
            └─────────────────────────┘  └─────────────────────────┘
  harness   ┌──────────────────────────────────────────────────────┐
            │ ioutgt-harness — config, spawn(), queue-thread pool, │
            │ control server + ctl/list/stat clients (Transport-   │
            │ generic; both binaries are thin wrappers)            │
            └──────────────────────────────────────────────────────┘
  frontends ┌───────────────┐ ┌───────────────┐ ┌──────────────────┐
            │ ioutgt-control│ │ ioutgt-nvme-  │ │ ioutgt-nvme-rdma │
            │ JSON schema,  │ │ tcp: ICReq,   │ │ lib: CM, verbs   │
            │ UDS control   │ │ recv/send     │ │ QP/CQ, reap loop │
            │ server        │ │ loops, slots  │ │ (nvme-rdma.md)   │
            └───────────────┘ └───────────────┘ └──────────────────┘
  shared    ┌─────────────────────────┐  ┌─────────────────────────┐
            │ ioutgt-backend          │  │ ioutgt-stream           │
            │ AnyBackend:             │  │ ZC gather-send harness  │
            │ Null / Memory / File    │  │ + recv byte-source      │
            │                         │  │ (StreamSender/Reader)   │
            └─────────────────────────┘  └─────────────────────────┘
  protocol  ┌──────────────────────────────────────────────────────┐
            │ ioutgt-nvme — NVMe command execution (dispatch,      │
            │ admin/IO, fabrics, CC/CSTS), plus the sans-IO        │
            │ NVMe(-oF) codec: Sqe/Cqe, PDUs, CRC32C               │
            └──────────────────────────────────────────────────────┘
  leaves    ┌─────────────────────────┐  ┌─────────────────────────┐
            │ ioutgt-core             │  │ ioutgt-uring            │
            │ protocol-neutral slot   │  │ io_uring reactor, op    │
            │ engine (`slotq`),       │  │ futures, mailbox,       │
            │ Backend trait, model:   │  │ QueueRuntime,           │
            │ Subsystem/Ns, Registry  │  │ sendbatch (GatherBatch) │
            └─────────────────────────┘  └─────────────────────────┘
```

| Crate | Role | Depends on (workspace) |
|-------|------|------------------------|
| [`ioutgt-nvme-tcp`](../crates/ioutgt-nvme-tcp) | NVMe/TCP transport + binary | harness, core, backend, control, stream, nvme, uring |
| [`ioutgt-nvme-rdma`](../crates/ioutgt-nvme-rdma) | NVMe/RDMA transport + binary | harness, core, backend, control, nvme, uring |
| [`ioutgt-harness`](../crates/ioutgt-harness) | shared binary harness (spawn, queue-thread pool, control server, stat client) | core, backend, control, cpus, uring |
| [`ioutgt-control`](../crates/ioutgt-control) | config + UDS control plane | core, backend, cpus |
| [`ioutgt-backend`](../crates/ioutgt-backend) | storage backends | core, uring |
| [`ioutgt-stream`](../crates/ioutgt-stream) | protocol-neutral stream mechanics: ZC gather-send (`StreamSender`) + buffered recv byte-source (`StreamReader`) | core, uring |
| [`ioutgt-core`](../crates/ioutgt-core) | protocol-neutral `slotq` engine, `Backend` trait, subsystem/registry model | — |
| [`ioutgt-nvme`](../crates/ioutgt-nvme) | sans-IO codec + NVMe command execution | core, cpus |
| [`ioutgt-uring`](../crates/ioutgt-uring) | reactor + op futures + `sendbatch` | — |
| [`ioutgt-cpus`](../crates/ioutgt-cpus) | locality-aware even CPU grouping | — |

### 3.1 Assembly: what the harness `spawn()` wires up

`main()` parses the config and calls the binary's thin entry point —
`spawn_target()` ([`crates/ioutgt-nvme-tcp/src/lib.rs`](../crates/ioutgt-nvme-tcp/src/lib.rs))
is a kernel-feature probe plus `ioutgt_harness::spawn::<TcpTransport>()`;
the RDMA binary passes `RdmaTransport` through the same seam.

`main()` brackets that call with the harness's shutdown pair:
`install_shutdown_handler()` before it (the backends take their Sheepdog
VDI locks inside `spawn`, so the window has to be covered) and
`wait_for_shutdown()` after, which parks the main thread until SIGINT or
SIGTERM. The handler itself only writes the signal number to a self-pipe;
the waiting thread does the work, in `shutdown()`, in two phases:

1. **Stop IO** — the teardown handshake. Each control thread registered a
   quiesce channel in `control_loop`; `shutdown()` sends every one of them a
   reply channel and then waits. A control thread that gets the request
   leaves its accept loop for good, `quiesce_pool()`s (a `Shutdown` message
   carrying a oneshot ack to every queue thread, all in flight at once), and
   answers only once they have all acked. A queue thread acks from inside its
   mailbox loop, after firing each connection's `stop` hook (`ConnHandles`:
   `shutdown(2)` on the socket for TCP, the CM stop `Notify` for RDMA) and
   waiting for the `run_queue` tasks to return through their normal teardown
   — so nothing is executing a command or holding a backend op in flight.
2. **Release** — the walk over every port `build_port()` registered, calling
   `AnyBackend::shutdown()` on each namespace, so external state (that VDI
   lock) goes back.

The order is the point: releasing first would let an in-flight write land on
a VDI this process no longer holds the lock for. Three nested budgets keep
any of it from hanging — 5 s for a queue thread's connections, 8 s for a
control thread's pool, 12 s for the whole set of targets — each layer
reporting stragglers and carrying on rather than waiting on the one below.
A host sees the connection drop it would see on any restart, just before the
release instead of racing it. `SA_RESETHAND` leaves a second signal to the
default action: a shutdown wedged on an unresponsive cluster stays killable.

**`spawn::<T>()` — the control thread**

```text
spawn::<T>(config)                                     [ioutgt-harness]
  └─ "ioutgt-control" thread (plain Tokio) → control_loop::<T>():
       Registry::new()                                 [ioutgt-core]
       T::bind() → listener + bound address
       build_port(): Subsystem / Namespace → AnyBackend [core, backend]
       spawn_control_api(): UDS server                 [ioutgt-control]
       loop select!
         ├─ T::accept()  ──► handle_accept()
         ├─ idle tick    ──► teardown pool after the grace window
         └─ quiesce req  ──► quiesce_pool(), ack, leave the loop (§3.1 above)
```

**`handle_accept()` — pool bring-up, handshake, routing**

```text
handle_accept(raw)
  ensure_pool_up(): pool down? build admin + N IO threads, each:
      mailbox (MPSC + eventfd) ⇄ pinned OS thread + QueueRuntime (own ring)
      loop mailbox.recv():
        Conn(conn) → spawn T::run_queue(conn), track task + stop hook
        Stats      → snapshot own counters, reply
        Shutdown   → stop each connection, await its task, ack, return
                     (ring drops)
  ConnPermit: count connection, reject past the limit
  spawn T::handshake(raw) → (qid, conn)
    qid 0 → admin mailbox          qid n → io[(n-1) % N] mailbox
```

The mailbox ([`ioutgt-uring`](../crates/ioutgt-uring)`::mailbox`) is the
only cross-thread channel; handing off a connection never touches a queue
thread's hot path.

**Lazy pool spawn + idle teardown.** The pool exists only while
connections do; `senders: Mutex<Option<PoolSenders>>` is the single
source of truth (`None` = down):

```text
        first accept (or first after teardown): ensure_pool_up()
  None ────────────────────────────────────────────────────────► Some(pool)
   ▲                                                                │
   └──── active == 0 for the whole grace window: teardown_pool() ◄──┘
         (Shutdown to every thread → rings drop; senders → None)
```

- Grace window: 30 s default (`--idle-teardown-secs`, `0` disables) —
  long enough to survive nvme reconnect / kill-recovery (~10 s), so only
  a genuinely idle target reclaims its threads.
- `active` is the connection count; `ConnPermit` decrements it when
  `run_queue` ends.
- A fresh or idle-reclaimed target holds only the control thread; the
  pool threads are up before the triggering connection is routed to them.
- While the pool is down: a stats query answers with a zeroed snapshot
  (never blocks, never mis-reports), and a namespace-change nudge no-ops —
  the edit still lands in the port model for the next connect.
- Idle teardown sends the same `Shutdown` message the handshake does, but
  with no ack and (by construction) no connections to stop, and does not
  wait for the threads to die: the next connect respawns the pool, and the
  two sets briefly overlapping is harmless.

### 3.2 Queue thread: the per-connection task set

Every transport's `run_queue()` builds the same shape on the queue
thread: the generic slot engine (`QueueCore<C>`, core) joined with a
transport-owned send list, plus one persistent task per slot ("task
per tag"), a send-side task, and a recv/reap loop running as the task
body. The tasks never call each other — their only rendezvous is the
transport's queue type:

```text
  recv loop ──claim_tag / submit──►┐
                                   │ QueueCore<C> + send list
  slot tasks × sqsize              │ (NvmeTcpQueue / RdmaQueue)
    await_command → dispatch ─────►│
    → begin_respond → push work ──►│
                                   │
  send loop ◄── drain send list ───┘
    ship batch → release_tag once the kernel/NIC
    is provably done with the slot memory
```

The transport-specific halves — task set, wire state machines, data
movement, copy budget — live in their own docs:

- **NVMe/TCP**: [`docs/nvme-tcp.md`](nvme-tcp.md) — PDU recv phase
  machine, gather/zero-copy send, the copy budget.
- **NVMe/RDMA**: [`docs/nvme-rdma.md`](nvme-rdma.md) — CM acceptance,
  keyed-SGL data movement, the reap loop.

Crate seams, with NVMe/TCP as the example:

| Seam | What crosses it |
|------|-----------------|
| bin → transport | the handshake calls + `run_queue()` (queue-thread entry point, with the `on_ctx` hook registering per-connection stats) |
| bin/transport → uring | op futures + mailbox |
| transport → core | `QueueCore<Sqe>`/`SlotArray` slot API; the send list and work type (`SendWork`) are transport-owned |
| tcp → stream | `StreamSender`/`StreamReader`, driven by transport closures |
| nvme → backend | the `Backend` trait behind `Arc<Namespace>` |
| control → core | `Registry` + `Subsystem` add/remove + the NS-changed nudge; GET_STATS reaches queue threads via binary-injected `StatsSource` closures over the same mailboxes |
| transport → nvme | `dispatch::execute` plus codec types for encode/decode — the codec never does IO, the reactor never sees a PDU |

## 4. Reactor: io_uring under Tokio current-thread

Each queue thread runs `tokio::runtime::Builder::new_current_thread()` with
a `LocalSet`, with **no** Tokio IO driver or timer enabled. A thread-local
reactor owns the ring:

- **Ring setup**: `IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN |
  IORING_SETUP_CQSIZE`, CQ sized ≥ 2× SQ (multishot headroom).
- **Op lifecycle**: an op future on first poll claims a slab entry
  (`user_data` = slab key), writes the SQE into the SQ ring (no syscall),
  stores its waker, returns `Pending`. CQE reaping looks up the slab entry,
  stores `(res, flags)`, and wakes.
- **Parking**: while tasks are runnable, nobody calls `io_uring_enter`.
  When the runtime goes idle it invokes `on_thread_park`; the reactor then
  calls `submit_and_wait(1)` with an EXT_ARG timeout equal to the nearest
  reactor timer (capped at 1 s as a missed-wakeup backstop), reaps all
  CQEs, and wakes wakers — which makes Tokio's own park return immediately.
  Result: one syscall per idle→busy transition, zero syscalls while
  saturated.
- **Timers**: queue threads use `IORING_OP_TIMEOUT` futures (keep-alive,
  retries). One wait primitive, one clock source. The control thread uses
  ordinary Tokio time.
- **Cancellation safety** (the most bug-prone invariant): the *slab entry*,
  not the future, owns kernel-visible resources (buffers, fd slots). A
  future dropped mid-flight flips its entry to `Orphaned`; the reactor
  issues an opportunistic `ASYNC_CANCEL` and frees the entry only when the
  terminal CQE arrives. This is stress-tested (drop-at-random-poll, ASAN
  soak) before anything is built on top.

**Off the scheduler** (`BlockingRing`): a synchronous caller with no runtime
under it — the Sheepdog backend's control plane, whose signatures are
synchronous and whose threads may be a plain `std::thread` or someone else's
Tokio worker — still reaches the same ops. `BlockingRing` polls one future
with a flag-setting waker and, between polls, parks the whole calling thread
in the same `Reactor::park` the runtime's hook uses. A thread that already
has a reactor has it *adopted* (config ignored, ring outlives the handle);
a thread with none gets a private ring, uninstalled at drop. It panics rather
than spinning if the future is pending with nothing in flight on the reactor.

Rejected alternatives: **tokio-uring** (no multishot recv / provided-buffer
rings / SEND_ZC notification control; owned-buffer model conflicts with
preallocated slots; maintenance mode) and a **fully custom executor**
(Tokio's current-thread scheduler is cheap, battle-tested, and brings
`select!`/`JoinHandle`/ecosystem for free — only the wait primitive needs
replacing).

## 5. Transports

Two production transports share the engine; each has an as-built doc.
The obligations any transport must meet are the contract in §5.1.

- **NVMe/TCP** ([`docs/nvme-tcp.md`](nvme-tcp.md)) — PDU phase machine
  mirroring nvmet's, in-capsule ≤ 16 KiB + single-R2T writes, CRC32C
  digests, batch-drained gather send with opt-in `SENDMSG_ZC`, opt-in
  per-connection provided-buffer recv ring (`--recv-buf-mb`).
- **NVMe/RDMA** ([`docs/nvme-rdma.md`](nvme-rdma.md)) — keyed-SGL
  one-sided data movement (target-posted RDMA READ/WRITE), rdma_cm
  acceptance on a dedicated CM reactor thread, batched WR doorbells,
  adaptive `--poll`.

## 5.1 Transport contract

The engine split (§3.2) makes the obligations of any transport explicit.
A transport supplies six pieces; this section is the authoritative
as-built statement.

1. **Setup** (control thread, plain Tokio): authenticate or handshake
   enough to determine the routing key, then send a queue-install message
   to the appropriate queue thread's mailbox. The routing key differs by
   transport: NVMe/TCP parses the first Connect capsule for qid and routes
   qid 0 → admin thread, qid n → io thread `(n-1) % N`; NVMe/RDMA reads qid
   from the CM CONNECT_REQUEST private data (available before any capsule);
   NBD has no qid concept and routes round-robin. Admission control uses
   `ConnPermit` (`ioutgt-core::permit`).

2. **Install** (queue thread; reached only via its mailbox):
   instantiate `SlotArray<C>` + `SendList<W>` (from `ioutgt-core::slotq`) and
   any protocol context, then spawn one persistent task per slot. All slot and
   buffer memory is allocated at this point, once, on the owning thread
   (first-touch NUMA locality).

3. **Recv path**: obtain a tag, treating exhaustion as backpressure,
   never as a protocol error — a conforming host at full depth can
   deliver command N+1 before the target's own send completion frees
   tag N (both fabrics; nvmet never terms on depth either). Where the
   intake can block, park it in `await_tag` (NVMe/TCP recv loop, NBD);
   where it must not, `claim_tag` and park the command in a transport
   queue instead (NVMe/RDMA's `parked`). Then fill the slot's command
   and payload and `submit`. Failures are graded: a per-command
   error calls `respond_receiving` and pushes an error work item; a protocol
   violation produces a transport-specific termination signal (C2HTermReq /
   close), never a panic or silent drop.

4. **Slot task**: `await_command` → protocol dispatch → `begin_respond` →
   push `W` onto the send list. The slot task is the only path that calls
   `begin_respond`; it decrements the `executing` counter that gates teardown.

5. **Send path**: drain the send list (batch where the medium rewards it),
   ship the batch, then **`release_tag` only when the kernel or NIC
   provably no longer references slot memory** — at the send CQE for copying
   sends, at the ZC notification for `SENDMSG_ZC`, at the RDMA SEND
   completion for verbs. This placement is the memory-safety line, not
   bookkeeping.

6. **Teardown**: stop intake → `SendList::close` → join the send task
   (draining queued work and any pending ZC notifications) → quiesce the
   `executing` counter to zero (backend ops may still be writing into slots)
   → free. If a backend never returns, the design leaks rather than
   use-after-frees.

The standing invariants from §1/§2 apply unchanged across all transports:
zero steady-state allocation, no locks, no atomic RMW on the IO path;
mailbox-only entry into queue threads; codec modules sans-IO; reactor cancellation
safety (the slab entry, not the op future, owns kernel-visible resources).

### 5.1.1 NBD on the refactored base

NBD (`ioutgt-nbd`, follow-up plan) maps cleanly onto the contract with no
NVMe machinery at all: `C = NbdCmd` (flags, type, cookie, offset, length —
24 bytes), `W = NbdReply` (tag, error, data_len), cookie stored in the slot
and echoed in the reply (no lookup map, the same trick as TTAG = slot index).
Depth is server-chosen, so `await_tag` parks the recv loop as backpressure.
Write payload always follows the 28-byte request header inline (no R2T);
large tails use the direct-to-slot `MSG_WAITALL` path shared with NVMe/TCP.
Read responses use `GatherBatch` (`ioutgt-uring::sendbatch`) — the same
arena/iovec/short-send logic — with a 16-byte simple-reply header. Setup is
fixed-newstyle option haggling on the control thread, routed round-robin.

### 5.1.2 NVMe/RDMA on the refactored base

NVMe/RDMA (`ioutgt-nvme-rdma`, built — see `docs/nvme-rdma.md` for the
as-built detail) reuses `C = Sqe` with its own response work type (no R2T
variant: data movement is transport-posted). The wr_id encodes
`kind << 40 | tag/recv-idx` — the same TTAG trick plus a WR-class byte. Host
writes arrive as keyed SGL commands; the transport posts an RDMA READ from
host memory into the slot's pool lease and calls `submit` on READ completion
(parking the command when tags or the pool are transiently exhausted — see
the backpressure notes in `docs/nvme-rdma.md`). Host reads have dispatch fill
the slot, then the reap loop posts an RDMA WRITE from the slot followed by an
RDMA SEND carrying the CQE; QP ordering makes WRITE-before-SEND free.
`release_tag` fires when both signaled response completions are reaped — when
the NIC is provably done with slot pages, matching obligation 5. Slot/pool
buffers are registered as MRs at queue install (the registered-buffers theme
from §8, mandatory here). Setup uses an rdma_cm event channel on a dedicated
CM reactor thread (its fd parks on io_uring `POLL_ADD`, which the plain-tokio
control thread cannot provide); qid is read from CONNECT_REQUEST private data
and routed `(qid-1) % N` as today. The verbs completion-channel fd is a
persistent multishot poll on the queue thread — the same mailbox-doorbell
pattern — so one wait primitive still rules the thread. `QueueCore<Sqe>`,
dispatch, controller model, and discovery are all reused unchanged;
`PortConfig.trtype = TransportType::Rdma` makes discovery advertise the
correct TRTYPE.

## 6. NVMe model (`ioutgt-nvme`)

**Object model — Port / Subsystem / Namespace, and the per-Connect Controller**

```text
Port ──┬── Subsystem (NQN) ──┬── Namespace (nsid → Backend)
       │                     └── allowed hosts
       └── Discovery subsystem (nqn.2014-08.org.nvmexpress.discovery)

Controller (cntlid) ── created by fabrics Connect on the admin queue
  ├── CC/CSTS register state machine (enable → ready, shutdown)
  ├── Keep-alive timer (KAS granularity 10 s; teardown on expiry).
  │   Traffic-based (CTRATT.TBKAS on TCP): every queue publishes its
  │   command traffic into one shared flag, so IO alone keeps the
  │   controller alive and a busy host sends no Keep Alive commands
  ├── AER pool (4 outstanding; NS_CHANGED on namespace add/remove,
  │   ANA_CHANGE when a cluster namespace changes path locality)
  └── queues: admin (qid 0) + up to N IO queues (clamped to thread count
      via Set Features NUM_QUEUES)
```

Queue teardown — the userspace analogue of nvmet's `percpu_ref`:

- Fail parked AERs first (`ConnCtx::close`, the analog of
  `nvmet_async_events_failall`; its omission was a measurable
  per-disconnect leak).
- Drain the executing-slot counter to zero before freeing slot memory
  (backend ops may still be DMAing into it).
- If a backend never returns: leak, deliberately, rather than
  use-after-free.

Namespace table — versioned for runtime add/remove:

- An `Arc` snapshot behind a generation counter; IO queues revalidate
  with one atomic load per command and refresh only when the control
  plane changed something.
- Changes fire the NS_ATTR async event (note: Identify must advertise
  OAES.NS_ATTR or Linux hosts never enable the notice). OAES is split by
  controller type as nvmet's is: an NVM controller advertises NS_ATTR (plus
  ANA Change where the subsystem reports ANA), a discovery controller
  advertises only DISC_CHANGE (bit 31) — the two sets are disjoint.

- Capacity in bytes comes off the same snapshot: `Namespace::capacity`
  (Identify Namespace `NVMCAP`) and its sum `Subsystem::total_capacity`
  (Identify Controller `TNVMCAP`), both recomputed per Identify, so an add
  or remove moves them. `UNVMCAP` is 0 — there is no spare pool behind the
  namespaces, every byte is allocated. The control plane reports the same
  two numbers as `capacity` on its namespace and subsystem bodies.

Admin command surface (interop-minimal, values per nvmet): Identify CNS
0x00/0x01/0x02/0x03, Get/Set Features (NUM_QUEUES, KATO, async event
config), Keep Alive, AER, Get Log Page (error/SMART/firmware/discovery/ANA),
Property Get/Set (CAP/VS/CC/CSTS), fabrics Connect. IO commands: Read,
Write, Flush, then Write Zeroes and DSM-deallocate advertised via ONCS once
backend support lands.

## 7. Backend trait

```rust
trait Backend {
    async fn read(&self, lba: u64, buf: &mut [u8]) -> Result<(), BackendError>;
    async fn write(&self, lba: u64, buf: &[u8]) -> Result<(), BackendError>;
    // Vectored variants over a command's data segments (default: one
    // read/write per segment; the file backend overrides with one op).
    async fn read_segs(&self, lba: u64, segs: &[Seg], total: usize) -> Result<(), BackendError>;
    async fn write_segs(&self, lba: u64, segs: &[Seg], total: usize) -> Result<(), BackendError>;
    async fn flush(&self) -> Result<(), BackendError>;
    async fn discard(&self, ranges: &[LbaRange]) -> Result<(), BackendError>;
    async fn write_zeroes(&self, range: LbaRange) -> Result<(), BackendError>;
    // size / block_size / topology probes
}
```

(Signature sketch.) Backends: `Null`, `Memory` (bring-up + tests), `File`
(regular file or block device), `Sheepdog` (a VDI on a Sheepdog cluster).
Disk ops run on the owning queue thread's own ring. The file backend issues vectored
`READV`/`WRITEV` over a command's data segments (one iovec per pool
segment — contiguous or scattered). It opens a single fd `O_DIRECT`,
falling back to buffered only when the store refuses direct (e.g. tmpfs);
the choice is fixed at open and needs no per-store alignment probing,
because the slot pool's buffers are page-granular and every transfer is a
block multiple, so once O_DIRECT opens it serves every IO. (Sub-page
buffers — which would require a `statx STATX_DIOALIGN` check — only arise
with a zero-copy recv ring, deferred.) `FSYNC` flush, `FALLOCATE`
punch-hole/zero-range as before. IOPOLL is not used: a polled ring cannot
carry socket ops, and a second per-thread IOPOLL ring is a measured-later
roadmap item.

The **Sheepdog** backend is a *network* backend: instead of a local fd it
talks the plain-TCP Sheepdog gateway protocol to a cluster. It holds only
`Send + Sync` state (cluster address, geometry learned from the VDI inode
at open, and the mutable `data_vdi_id[]` object map as an atomic array);
the actual TCP connection is `!Send` (its io_uring ops bind to
`Reactor::current()`), so it lives in a `thread_local`, dialed lazily with
the client-side `IORING_OP_CONNECT` op (`ops::connect`, the outbound
counterpart to `accept`). A logical read/write
splits into per-object requests; holes read as zeroes, first writes allocate
objects (persisting the map entry back into the inode) and snapshot parents
copy-on-write. Requests/responses use raw io_uring send/recv with the header
held in the awaiting slot-task frame — the same cancellation envelope as the
file backend's vectored IO.

**One connection per queue thread** (`sheepdog::mux`), shared by every
namespace on that cluster and pipelined rather than one connection per
concurrent command: `sheep` hands each request to a worker and answers in
completion order, echoing the client's request `id`, so responses come back
out of order and are routed by that id. Each thread's connection has a slot
table of in-flight requests, a **send gate** (a request's header and payload
must reach the wire back to back; nothing else is serialized) and a **pump**
task — the connection's only reader, which reads the 48-byte response header,
lands the payload straight in the waiting caller's buffer and wakes it. With
nothing outstanding the pump parks on a waker, not on the socket, so an idle
thread arms no op. Any IO error, EOF, or cancellation mid-send poisons the
connection: all its waiters fail with `EIO` (the host retries; a shared
connection means a wider blast radius than the per-command connections this
replaced) and the next request dials a fresh one.

The backend's **control plane** — the lookups and inode read at open, the VDI
registration and its release, and the cluster enumerations (`list_vdis`,
`list_acls`, `vdi_holders`, `cluster_ana_state`) — runs on the ring too, but
over its own one-shot connections (`sheepdog::ctl`), not the multiplexed one.
Its callers are synchronous and hold no scheduler (the CLI, the ACL refresh
thread) or sit inside the control server's runtime, so each connection carries
a `BlockingRing` (§4): one request in flight at a time, `id` left zero, owned
buffers so an abandoned request (the 2 s lock-release deadline) leaves nothing
the kernel could still write into, and the calling thread parked in
`io_uring_enter` until the answer lands. No socket in this backend is touched
any other way.

Access control on the cluster side is the **ACL object**: a VDI carrying
`SD_VDI_FLAG_ACL`, named back by the volumes it grants access to (their
inode `acl_id`). Every lookup and lock carries an ACL id, and `sheep`
resolves a name only within it, so the backend's `open` takes the ACL as
well as the VDI name. A writable VDI is opened under the cluster's VDI lock,
taken with `REGISTER_VDI` — the lock op whose *owner* the client supplies
(`LOCK_VDI` records the relaying `sheep` gateway, which is no use as a
`traddr`), so the holder the cluster records is the address this target's
fabric listens on. The lock is held on the connection that took it and
released with `UNREGISTER_VDI` from the backend's `Drop` — or, on the way
out of a target whose namespaces the queue threads still own, from the
shutdown walk (`ioutgt-harness`'s `install_shutdown_handler` /
`wait_for_shutdown`: SIGINT/SIGTERM → stop serving IO, then
`AnyBackend::shutdown()` over every port this process built — §3.1).
The ACL id doubles as the lock type, so an open under an
ACL takes the *shared* lock (a second target serving the same ACL may
export the same volume, and both appear in the volume's participant list)
while one outside any ACL takes `LOCK_TYPE_NORMAL` and stands alone. Either
way a volume a client holds incompatibly (a QEMU
guest, or a different ACL) is refused at startup rather than raced;
`?nolock` / `"lock": false` waives it. Sharing assumes non-overlapping
writers: the cached object map is never invalidated, so two targets
allocating the same object lose one of the writes.

A cluster can also be exported wholesale, and the ACL is what the target
model hangs off: `list_acls` reads the cluster VDI bitmap (`READ_VDIS`)
plus each vid's inode at startup, then takes each ACL object's membership
from its *own* inode — the vids in `data_vdi_id[0..max_data_id_nr]`, the
list `dog acl add vdi`/`remove vdi` maintain, zeroes being holes — and
`ioutgt_control::cli` turns that into **one subsystem per ACL object — NQN
= the ACL's name verbatim — holding one namespace per writable member**. A
listed vid the members' side contradicts (no such volume, or an inode
naming another ACL, as a half-completed `dog acl add vdi` leaves) is
skipped with a warning, as is a volume naming an ACL that does not list it:
the cluster resolves neither under that ACL. The same inode's `metadata[]`
holds the ACL's other list — its *member names* (`dog acl add member`),
fixed-width slots with zeroed holes, read by `read_acl_state` into
`AclInfo::hosts` — and those become the subsystem's `allowed_hosts` with
`allow_any_host` off: the cluster's ACL is the host ACL, and a Connect from
a hostnqn it does not list is refused (`CONNECT_INVALID_HOST`,
`Subsystem::admits`). An ACL with no members names nobody to keep out, so
that subsystem keeps `allow_any_host`. Membership — of hosts *and* of
volumes — is the administrator's to change while the target runs:
`SubsystemConfig::sheepdog_acl` records which ACL object (cluster address +
vid, plus its `lock` setting) a subsystem came from, and the 10 s refresh
thread re-reads its member names (`acl_state` → `Subsystem::set_host_acl`,
alongside the holder lists and object locality it already re-reads), so a `dog
acl add member` takes effect within a tick. It decides the *next* Connect: a
host dropped from the ACL keeps the controllers it already has, as unlinking a
host in nvmet's configfs does. Only cluster mode tracks this — under `%ACL` the
ACL is a lookup scope, not the subsystem, and the host list stays the config
file's.

The same refresh re-reads the ACL's *volume* membership too
(`ioutgt_backend::acl_members`, `refresh_cluster_namespaces`'s scoped,
one-ACL form of what `list_acls` does for every ACL at startup): `dog acl add
vdi`/`remove vdi` on a running cluster adds or removes a namespace on this
target, not only a discovery-log path entry or a `vdi_epoch` bump. A vid the
ACL now lists that this subsystem does not yet export is opened exactly as
`acl_subsystem` opens one at startup (`build_backend`, the same fabric address
and `--recv-buf-mb` setting, the ACL's own `lock`) and added
(`Subsystem::add_namespace`), logging `"sheepdog VDI exported"` as the
startup path does; one the ACL no longer lists is removed
(`Subsystem::remove_namespace`, `"sheepdog VDI unexported"`) — only ever an
nsid this same ACL's cluster could have added, so a namespace `ADD_NAMESPACE`
put there by hand is not this refresh's to take back. Either edge posts the
NS_ATTR async event to the live controllers, the same notice `ADD_NAMESPACE`/
`REMOVE_NAMESPACE` raise over the control socket — a host sees the new or
missing nsid without reconnecting. A namespace hot-added this way also joins
the subsystem's path-list and ANA tracking alongside the ones opened at
startup (`track_cluster_backend`), with one gap: a subsystem that had zero
cluster namespaces at startup has nowhere to attach ANA tracking to when its
first one arrives later (the notifier closure needs the queue-thread pool,
which by then no longer has a construction site to hand it one from), so that
namespace's ANA state stays at its optimized placeholder until the target
restarts — logged, not silent. Each namespace takes its VDI's bitmap position (its vid) as its NSID
so the map is a pure function of the cluster — sparse, large NSIDs in
exchange for a numbering no other VDI's creation can disturb — and reports
the VDI's inode `uuid[16]`, the cluster's own identity for the volume, as
the namespace UUID (`AnyBackend::uuid`, consulted wherever a namespace is
built: config file, CLI spec, `ADD_NAMESPACE`, ahead of the derivation from
NQN + NSID). Identify Controller `NN` stays the highest valid NSID
(`Subsystem::max_nsid`, which also bounds the invalid-vs-inactive NSID
decision); the ACL inode's `max_data_id_nr` — the cluster's own count of
the group's volumes, which sparse NSIDs put out of NN's reach — is reported
as `MNAN` (`Subsystem::with_mnan`, from `SubsystemConfig::mnan`; 0
elsewhere). Volumes in no ACL
are exported by no subsystem; the cluster would refuse an ACL-scoped lookup
of their names anyway. That is the `--backend sheepdog:HOST` form both
binaries share (`--subsys-nqn` is unused in it).

**Who serves a subsystem — the discovery log's port list.** In cluster mode
several targets front the same ACL, so a subsystem has as many paths as
there are targets registered on it, and the discovery log says so: one
record per `(subsystem, path)` rather than the one-per-subsystem nvmet
emits. The path list comes from the cluster itself, and needs no extra
bookkeeping: the `REGISTER_VDI` each locked namespace already issues at open
(above) names this target's fabric address as the volume's owner, so the
volume's participant list *is* the list of targets serving it. There is no
registration on the ACL object and no per-IO-thread registration —
one per namespace open, which is what a holder's count normally reads.
Reading the list back is `GET_VDI_COPIES` — the `dog vdi lock list` query —
whose `vdi_state` record for a vid carries `participants[]`. A subsystem's
paths are the *union* of the holders of its Sheepdog volumes (a target that
serves any of them is reachable for the subsystem), deduplicated and sorted
by address so every target in the cluster computes the same order: each
entry becomes a `SubsystemPort` (`ioutgt-core`) on `Subsystem`, and each of
those a discovery entry with the holder's address, its index in that sorted
list as PORTID, and ADRFAM from the address family. Namespaces on a second
cluster contribute no paths (a warning; the first backend's cluster wins).
A background thread (`ioutgt-harness`, 10 s) re-reads the holders so a
target joining or leaving turns into a path appearing or disappearing, and
re-registers a namespace whose holder list no longer names us; `shutdown()`
raises a shutting-down flag as its first act, which ends that thread and
makes a refresh already in flight give up before it re-registers anything,
so a refresh cannot re-take a lock behind a release. The same flag decides
how a cluster that stops answering is reported: a warning while the target
serves (the hosts' view is going stale), a debug line during shutdown (the
connection going down is the teardown, not a fault). Every step is
best-effort:
an unreadable or empty holder list leaves the previous paths in place rather
than flapping, and a cluster that will not take the registration costs the
target its extra entries — it falls back to advertising itself, exactly as a
non-cluster subsystem does (empty port list) — not its ability to serve IO.
All of it is blocking TCP on the control plane, never on a queue thread.

**When the log changed — GENCTR and the discovery AEN.** A path list that
moves under a connected host is worth nothing unless the host is told, and
the discovery log's own two mechanisms are the generation counter in its
header and the Discovery Log Page Change notice. Both come off one number
per subsystem, `Subsystem::disc_genctr` (`ioutgt-core`), and the log's
GENCTR is the sum over the subsystems it reports — which, for the usual
one-subsystem discovery port, *is* that subsystem's counter. Two things
move it, because neither alone sees every change. The cluster's own version
of the ACL is the inode header's **`vdi_epoch`**, read alongside the member
names by the same `READ_OBJ` (`AclState { hosts, epoch }`), and every
refresh feeds it to `observe_disc_genctr` — a `fetch_max`, so an epoch that
went backwards (a rebuilt cluster) cannot walk a host's view back. But
`sheep` bumps `vdi_epoch` only on `dog acl add`/`remove vdi` and at VDI
creation, *not* when a target registers or unregisters as a holder — so the
change most visible in the discovery log, a peer target appearing, would
never move it. A path list that actually changed therefore also calls
`bump_disc_genctr` locally (`Subsystem::set_ports` reports whether it did),
and the counter is the monotonic maximum of the two. Seeding the list at
startup is not a change: `refresh_cluster_paths` takes an `announce` flag
that is false on the initial read, so the first GENCTR a host ever sees is
exactly the ACL's `vdi_epoch`.

Either move also raises the notice. The refresh thread is a plain thread and
cannot touch a queue thread's controllers, so it goes the same way the ANA
change does — through the admin thread's mailbox — but discovery
controllers live on the admin thread of whichever target the host reached,
not on a per-subsystem list, so the hop is a process-wide notifier
(`DISC_NOTIFY`, installed by `track_discovery_changes` at control-loop
start and cleared by `stop_cluster_refresh`) sending `AdminMsg::DiscChanged`
to the admin pool. Each live connection's `ChangeNudge::disc_changed`
upgrades to its `ConnCtx` and calls `fire_disc_changed`, which is a no-op on
anything but a discovery controller and otherwise posts a parked AER with
DW0 `0x0070_F002` — Notice (type 2), Discovery Log Page Changed (F0h), log
page 70h — masked as always against the host's AEC and the controller's
OAES.

**Whose cntlids are whose — partitioning by holder slot.** Several targets
fronting one subsystem raises a second collision, the same one multi-port
configs have: a cntlid is unique *per subsystem*, so two targets minting
cntlid 1 for the same NQN hand a multipath host a duplicate it rejects
(`nvme_validate_cntlid`). The cluster has already answered the question the
targets would otherwise have to negotiate — the participant array of a VDI
lock is a fixed 31 slots (`SD_MAX_COPIES`), and `REGISTER_VDI` puts this
target in one of them, stably: `sheep` leaves a departed participant's slot
as a hole rather than compacting (`del_participant`), so a slot index is a
cluster-assigned small integer no other live target holds. The port's cntlid
range is therefore cut into 31 equal partitions and this target's slot picks
one (`holder_cntlid_slice`, `ioutgt-harness`), on top of whatever
per-port slicing already narrowed it. The slot comes from the
*lowest-vid* registered namespace on the port, so all targets fronting the
same volume agree on who is who; a port with no cluster registration (no
Sheepdog namespace, `?nolock`, or a cluster that refused) keeps its full
range unpartitioned, and so does one whose range is too small to cut into 31
(a warning). Note the asymmetry with the discovery log's PORTID, which is an
index into the *sorted, hole-free* path list and so is not the slot: PORTID
names a path in a list all targets compute identically, the slot names a
seat in the cluster's array.

**Which path is the good one — ANA.** Those paths are not equal. Sheepdog
places every object on the nodes its consistent-hash ring assigns it to, so
a target whose own gateway is one of them serves it without a hop, and any
other gateway adds one — and NVMe has exactly the vocabulary for saying so:
**Asymmetric Namespace Access**. A subsystem with any Sheepdog namespace
reports ANA — CMIC bit 3, the ANA fields of Identify Controller, `ANAGRPID`
in Identify Namespace, Get Log Page 0Ch, and the ANA Change notice in OAES —
and every other subsystem leaves the bit clear, so a local-storage target is
unchanged.

A namespace's ANA group is not a state, and not something this target picks:
it is the **zone** of the cluster node whose vnode owns the volume's inode
object on the hash ring (`oid_to_first_vnode`, reproduced bit-for-bit in
`ioutgt-backend::sheepdog` and checked against the C implementation's own
values) — a fact about the object and the cluster's topology, identical
however many targets ask and through whichever gateway. That identity is
exactly what the old design (one namespace's group flips between two fixed
sentinels, "optimized" and "non-optimized", depending on which gateway asked)
got backwards: two targets fronting the same volume reported it under two
different `ANAGRPID`s, which is not a namespace two paths can agree is the
same one. `NANAGRPID` is the cluster's zone count and `ANAGRPMAX` the largest
group id in use — both real cluster facts now, not a constant 2. A Sheepdog
zone id is not directly an `ANAGRPID`, though: `0` is an everyday zone (the
default for a cluster's first node, zoned by index — `sheep`'s own
`docker/gen_sheep_cluster_yaml.sh` does this) but an NVMe host rejects it
outright (`nvme_parse_ana_log`'s `WARN_ON_ONCE(desc->grpid == 0)`), so every
zone id is shifted by one (`zone_to_grpid`) at the one place Sheepdog zones
become `ANAGRPID`s. A group absent from a subsystem's own namespaces is
still reported, empty, so the shape `NANAGRPID` promised at Identify time
never drifts; NSIDs within one
ascend, as `nvme_update_ana_state` assumes.

What *is* per path is a group's **state**: optimized if the gateway this
target's connection reaches is itself in that zone, non-optimized otherwise.
Both facts — the zone topology (`GET_NODE_LIST`, a **local** op answered out
of the connected node's own membership view) and, from it, the ring each vid
resolves against — come from one refresh, `ioutgt_backend::cluster_ana_state`;
`own_zone` is found by matching the connected address in the node list, the
per-vid group by walking the ring the node list built. It rides the same 10 s
refresh thread as the path list, per (subsystem, cluster) — unlike paths,
namespaces on a second cluster are not dropped but asked of *their* gateway,
since both the ring and this path's place on it are per-cluster facts. A
group set that grows (a zone appearing) or a namespace's group or state
changing bumps the subsystem's ANA change count and posts an ANA Change AER
to the live controllers through the admin thread's mailbox, so hosts re-read
0Ch instead of polling it. Unlike the discovery paths this does not depend on
the VDI lock: a namespace opened `?nolock` reports ANA like any other, since
placement is a fact about the object, not about a registration. A cluster
that will not answer leaves the states as they were — the same best-effort
rule as the paths, and for the same reason: flapping a host's path choice is
worse than a stale preference. The group *set* only ever grows across such
refreshes (`Subsystem::merge_ana_zones`) rather than being replaced, since a
multi-cluster subsystem's refreshes are independent and none may erase what
another contributed; a zone that later vanishes from a cluster is reported
the same way any group without current members is — empty, not removed.

## 8. Buffer strategy: staged, measured

| Concern | As built |
|---------|----------|
| Slot data buffers | Leased on demand from a per-queue `BufPool` ([`ioutgt-core/src/pool.rs`](../crates/ioutgt-core/src/pool.rs)): a contiguous arena (default 8 MiB, `--queue-buf-mb`, 4 KiB grain) with a coalescing free-run allocator handing out a contiguous run when one fits, else a scatter list of ≤ `MAX_SEGS`. Each command leases exactly its transfer size (reads/admin via `lease_await` with pool-exhaustion backpressure; write/admin via `lease_or_owned`, a private-buffer fallback that never blocks the serial recv loop), freed at `release_tag`. The pool is deliberately smaller than depth × MDTS. The arena is registered as an io_uring fixed buffer (and, on RDMA, as an MR). |
| Recv (TCP) | Classic single-shot RECV → 64 KiB scratch → copy into the slot; H2C write tails ≥ 16 KiB land **straight into the slot's pooled segments** (one scatter `recvmsg`/`MSG_WAITALL`). Opt-in `--recv-buf-mb`: per-connection provided-buffer ring. Details: `docs/nvme-tcp.md`. |
| Send (TCP) | Batch-drain into one gather `SENDMSG`; opt-in `--send-zc` (`SENDMSG_ZC`, slot reuse gated on the notification CQE, size-gated, copy fallback). Details: `docs/nvme-tcp.md`. |
| Data movement (RDMA) | Target-posted RDMA READ/WRITE against the registered pool arena; in-capsule write data ≤ 4 KiB. Details: `docs/nvme-rdma.md`. |
| Disk | Vectored `READV`/`WRITEV` (`_FIXED` when the lease is pooled) over the slot's segments; single fd, `O_DIRECT` with a buffered fallback when the store refuses it (see §7). |
| Deferred | RECV_ZC (zcrx) — needs real-NIC header-data split; bundles; second IOPOLL ring. |

MDTS is 128 KiB on IO queues; the admin queue sizes its pool so its
synchronous data leases never block.

## 9. Control plane and configuration

- Unix domain socket, newline-delimited JSON: `ADD_NAMESPACE`,
  `REMOVE_NAMESPACE`, `LIST_NAMESPACE`, `LIST_CONTROLLER`, `GET_STATS`.
- Stats are aggregated by querying each queue thread's mailbox — no
  shared counters:
  - Per-queue IO counters (`QueueStats`) and per-thread ring counters
    (`ReactorStats`: `io_uring_enter`/parks/SQEs/CQEs) are plain `Cell`s
    written only by the owning thread.
  - GET_STATS sends a oneshot-reply message through the mailbox; each
    thread snapshots its own cells (500 ms timeout per thread, so a
    wedged backend can't hang the control API), and on `clear` zeros
    them after the snapshot.
  - `ioutgt stat` renders them under a controller-identity header,
    `-i N` for iostat-style rates computed client-side, `--clear` to
    reset.
- The config-file schema is kernel nvmet's (the `nvmetcli save`
  format, `/etc/nvmet/config.json`), loaded by `ioutgt-control`'s
  `nvmet` module: each port matching the binary's fabric supplies a
  listen address — one process per port, forked in `main()` before any
  thread exists (foreground = lowest portid; children carry
  `PDEATHSIG`); its exported subsystems arrive with host ACLs
  (`allow_any_host` + `allowed_hosts`, enforced at Connect),
  serial/model, and file-backed namespaces (`device.path`,
  `device.uuid` pinning host-visible identity). The file owns the
  target model; engine tuning (threads, buffers, digests) stays with
  the CLI flags — the configfs/module-param split. Validation runs
  before any thread spawns.
- Runtime namespace changes propagate via mailboxes and fire AER
  NS_CHANGED so connected hosts rescan without reconnect.

## 10. CPU affinity and NUMA

Default on (`pin_threads`; opt out with `--no-pin` or
`"pin_threads": false`):

- [`ioutgt-cpus`](../crates/ioutgt-cpus)' `spread_cpus` (an
  ioutgt-original algorithm) groups all possible CPUs evenly per NUMA /
  cluster / SMT locality: group seats apportioned to nodes
  largest-remainder by present-CPU weight, nodes packed in cluster-major
  SMT-atom order with present CPUs spread first — the same locality
  properties managed IRQs (and therefore host-side nvme queues) get,
  though not bit-identical to the kernel's grouping.
- One group per IO thread; each thread pins to its group's first online
  CPU. A group with no online CPU (or sysfs failure) leaves that thread
  unpinned with a warning. The admin thread is never pinned.
- Combined with the deterministic qid→thread routing `(n-1) % N`, this
  lines the host's per-CPU queues up with topology-aware target cores.
- Slot arrays and buffers are allocated on the owning thread
  (first-touch locality); the allocation hooks take a NUMA node hint so
  multi-node placement needs no API change (development machine is
  single-node).

## 11. Testing strategy

1. **Unit**: per crate; PDU codec tested against byte fixtures captured
   from a real kernel-host ↔ kernel-nvmet loopback session (tcpdump), and
   re-fed at every fragmentation granularity down to 1 byte.
2. **Host-only integration**: a Rust test client built on the same
   `ioutgt-nvme` codec drives the target on localhost, including malformed
   frames (term-request paths) and mid-R2T disconnects.
3. **VM interop (primary acceptance)**: `testing/run_interop.sh` starts the
   target on the host; a vmtest VM (`https://github.com/ublk-org/vmtest -c
   vmtest.conf`) runs `nvme discover`, `nvme connect`,
   `nvme list/id-ctrl/id-ns`, fio `--verify=crc32c`, `nvme disconnect`
   against `10.0.2.2:14420` (the harness avoids 4420, which is often
   owned by other targets on a dev box; the port is published to the
   guest via the 9p marker), across the digest × queue-count matrix.
4. **Fuzzing**: in-crate deterministic decoder torture test
   (`crates/ioutgt-nvme/tests/decoder_fuzz.rs`) on the PDU decoder.
5. **Benchmarks**: fio (4K rand R/W, 128K seq R/W, 70/30 mix; QD 1/32/128)
   against ioutgt and an identically-configured kernel nvmet, with perf
   flamegraphs both sides. See `docs/benchmark-plan.md`.

## 12. Milestones

| # | Deliverable | Status |
|---|-------------|--------|
| M0 | workspace + this document | done |
| M1 | `ioutgt-uring` reactor | done — 11 tests, ASAN-clean, echo 0.065 syscalls/op |
| M2 | `ioutgt-nvme` codec | done — 1-byte fragmentation torture green |
| M3 | core model + handshake | done — end-to-end pipeline test |
| M4 | fabrics + admin | done — **nvme discover/connect from VM**, digest matrix |
| M5 | IO path (R2T, digests) | done — VM fio --verify clean, both digest modes |
| M6 | file/block backend | done — O_DIRECT on ext4 VM-verified (loop dev needs root: deferred) |
| M7 | control plane + JSON config | done — hot-add visible to connected host via AEN |
| M8 | hardening + fuzz | done — abuse suite, kill-recovery, RSS-gated soak, workspace ASAN |
| M9 | performance pass | part 1 done — batched send 4.2×; post-M10: gather send (+22% 128K read BW), direct-to-slot recv (−44% c/IOP 128K write); rest in roadmap |
| M10 | docs | comparison/usage/roadmap done; **nvmet benchmark deferred** (`benchmark-plan.md`) |
| M11 | transport-abstraction refactor | done — engine split (`slotq`), generic `QueueCore<C>`, transport-owned send work (`NvmeTcpQueue`), contract documented (§5.1) |
| M12 | shared send harness | done — ZC gather-send machinery extracted to `ioutgt-stream::StreamSender` behind a per-transport staging closure; NVMe/TCP keeps only PDU encoding |
| M13 | shared recv byte-source | done — buffered scratch + `ops::recv` (`fill`/`consume`) and the direct-into-slot `MSG_WAITALL` tail (`read_direct`) extracted to `ioutgt-stream::StreamReader`; NVMe/TCP keeps the PDU phase machine |
| M14 | multi-transport harness | done — spawn, queue-thread pool, control server and clients extracted to `ioutgt-harness` behind a `Transport` trait; both binaries share them |
| M15 | NVMe/RDMA transport | done — `ioutgt-nvme-rdma` (`sideway` verbs, `rdma-mummy-sys` CM); kernel-host interop on rxe (VM gates) and mlx5 (box); crc32c data-integrity gates green |
| M16 | NVMe/RDMA performance | done — pool arena as io_uring fixed buffer, reactor park-probe (CQ polled at the sleep point), in-capsule write data (IOCCSZ + SGLS SAOS, nvmet parity); matches or beats nvmet-rdma on every single-job fio_perf phase on the test box (64k +38-44%, 4k within ±3%) |
| M17 | adaptive `--poll` (RDMA binary only) | done — busy-poll while commands are in flight (+200 µs grace), event-driven when idle; qd1 latency −20-30%, admin queue exempt |

## 13. Risks

| Risk | Mitigation |
|------|-----------|
| Reactor orphan/missed-wakeup bugs | M1 first; drop-mid-flight stress; ASAN soak; 1 s park backstop |
| Fabrics/enable sequencing vs real host | real nvme-cli connect at M4; pcap fixtures at M2 |
| R2T flow corruption | fragmentation torture; fio --verify; mid-R2T kill tests |
| io-uring crate API gaps | M1 feature probe; raw-registration fallback confined to one module |
| DEFER_TASKRUN park subtleties | strace-asserted echo test; mailbox-only cross-thread rule enforced by non-Send types |
