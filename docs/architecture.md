# ioutgt Architecture Specification

Status: as-built specification (M0–M10, plus the post-M10 perf work —
gather send and direct-to-slot recv — and the M11–M13 transport
refactors). The milestone table at the end records what shipped;
`docs/roadmap.md` holds what's next.

## 1. Mission and goals

ioutgt is a userspace storage target framework built on io_uring. The first
production transport is NVMe/TCP. Goals, in priority order:

1. **Correctness / protocol compliance** — interoperate with the Linux
   kernel NVMe/TCP host driver and nvme-cli, validated continuously.
2. **Throughput and latency** — saturate 100G-class networks and modern
   NVMe SSDs at 4K and 128K block sizes with low p99 latency.
3. **Minimal allocations and queue-local execution** — zero steady-state
   allocation, no cross-queue locks, explicit CPU placement, NUMA awareness.
4. **Readable async/await** — performance must not come at the cost of an
   unmaintainable callback soup; the data path reads as straight-line
   async Rust.

Non-goals (for now): multipath/ANA beyond a single optimized group,
metadata/PI formats, fused commands, NVMe reservations.

## 2. The core idea: bounded concurrency

Most async servers have unbounded concurrency and pay for it with dynamic
allocation, task spawning, and buffer churn on every request. NVMe does not:
a queue pair has a fixed depth negotiated at Connect time, and every command
is identified by a CID drawn from that bounded space.

ioutgt treats this bound as the central scheduling primitive, the way SPDK's
request tracker does, but expressed as async Rust:

- At queue install, a `Box<[CmdSlot]>` of exactly `sqsize` slots is
  allocated, plus one **persistent async task per slot** ("task per tag").
- Each task loops forever: await command arrival in my slot → dispatch →
  await backend completion → queue the response → return my tag.
- The TCP transfer tag (TTAG) for R2T/H2CData *is* the slot index. The host
  CID is opaque to us — it is stored in the slot and echoed in the CQE, so
  no CID→slot hash map exists anywhere.
- Slot wakeups are same-thread `Cell<Option<Waker>>` doorbells: no atomics,
  no channels, no allocation.

Steady state on the IO path: **zero allocations, zero atomic RMW, zero
locks**.

## 3. Process and thread model

One process manages one NVMe controller set (one port, N subsystems).

**Process layout — control thread, admin thread, N pinned IO threads**

```text
Controller Process
│
├── Control Thread            tokio current-thread (enable_all)
│     ├── TCP listener (port 4420 + discovery)
│     ├── ICReq/ICResp handshake + first Connect capsule parse
│     ├── UDS control plane (JSON): namespace mgmt, stats
│     └── routes accepted queues:  qid 0 → Admin thread
│                                  qid n → IO thread[(n-1) % N]
│
├── Admin Queue Thread         pinned; own ring; admin queues of all ctrls
│
└── IO Queue Threads 0..N-1    pinned, one CPU from group_cpus_evenly
                               group i (§11); own ring; own memory;
                               own command slots; own send/recv machines
```

Why the control thread does the handshake: the queue ID is only knowable
from the fabrics Connect command (the first capsule), so blind round-robin
of raw connections would put admin queues on IO threads. Handshake traffic
is control-plane rate; doing it on plain Tokio sockets costs nothing where
it matters and keeps queue threads free of accept/handshake states. After
parsing Connect, the control thread packs the socket, the parsed Connect
capsule, and the negotiated digest flags into a `QueueConn` and sends it
to the target thread's mailbox.
The queue thread then owns the socket exclusively for the connection's
lifetime.

Cross-thread communication into a queue thread happens **only** through its
mailbox (MPSC queue + eventfd doorbell, watched by a persistent multishot
read on the ring). Queue-thread handles are deliberately not `Send`; the
mailbox sender is the only exported handle.

The NVMe/RDMA binary has the same shape with two differences: the
listener is an `rdma_cm` event channel driven by a dedicated CM reactor
thread (its fd parks on io_uring `POLL_ADD`, which the plain-Tokio
control thread cannot provide), and accepted queues reach the same
admin/IO threads through the shared `ioutgt-harness` pool.

## 4. Crate map and cross-crate call flow

The workspace is eleven crates forming a strict dependency DAG — every
crate depends only on layers below it. The two main leaves are
deliberately opposite in character: `ioutgt-nvme` is **sans-IO** (pure
bytes ↔ structs, no sockets, no async, fuzzable in isolation) and
`ioutgt-uring` is **pure IO** (op futures and the reactor, zero protocol
knowledge). A third small leaf, `ioutgt-cpus`, is a userspace port of
the kernel's `group_cpus_evenly()` (`lib/group_cpus.c`): the grouping
algorithm is pure (driven by a `CpuTopology` value, synthetic in tests),
with sysfs reading confined to `CpuTopology::from_sysfs()`.

Two crates sit above the frontends: `ioutgt-harness` is the shared
binary harness — config loading, `spawn()`, the queue-thread pool,
control server and the `ctl`/`list`/`stat` clients — parameterized over
a `Transport` trait so the NVMe/TCP and NVMe/RDMA binaries are thin
wrappers around the same machinery. `ioutgt-nvme-rdma` is both the
RDMA transport library and its binary (verbs via `sideway`, connection
management via `rdma-mummy-sys`; see `docs/nvme-rdma.md`).

**Crate map — the dependency DAG**

```text
  binaries  ┌─────────────────────────┐  ┌─────────────────────────┐
            │ ioutgt (NVMe/TCP)       │  │ ioutgt-nvme-rdma        │
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
  model     ┌──────────────────────────────────────────────────────┐
            │ ioutgt-core — Port/Subsystem/Namespace, Registry,    │
            │ NVMe model + dispatch + the protocol-neutral slot   │
            │ engine (`slotq`), Backend trait definition          │
            └──────────────────────────────────────────────────────┘
  leaves    ┌─────────────────────────┐  ┌─────────────────────────┐
            │ ioutgt-nvme             │  │ ioutgt-uring            │
            │ sans-IO NVMe(-oF) codec │  │ io_uring reactor, op    │
            │ Sqe/Cqe, PDUs, CRC32C   │  │ futures, mailbox,       │
            │                         │  │ QueueRuntime,           │
            │                         │  │ sendbatch (GatherBatch) │
            └─────────────────────────┘  └─────────────────────────┘
```

| Crate | Role | Depends on (workspace) |
|-------|------|------------------------|
| `ioutgt` | NVMe/TCP binary + assembly | all others |
| `ioutgt-nvme-rdma` | NVMe/RDMA transport + binary | harness, core, backend, control, nvme, uring |
| `ioutgt-harness` | shared binary harness (spawn, queue-thread pool, control server, stat client) | core, backend, control, cpus, uring |
| `ioutgt-control` | config + UDS control plane | core, backend |
| `ioutgt-nvme-tcp` | NVMe/TCP transport | core, stream, nvme, uring |
| `ioutgt-backend` | storage backends | core, uring |
| `ioutgt-stream` | protocol-neutral stream-transport mechanics: ZC gather-send (`StreamSender`) + buffered recv byte-source (`StreamReader`) | core, uring |
| `ioutgt-core` | NVMe model + dispatch + `slotq` engine | nvme |
| `ioutgt-nvme` | sans-IO codec | — |
| `ioutgt-uring` | reactor + op futures + `sendbatch` | — |
| `ioutgt-cpus` | userspace `group_cpus_evenly()` | — |

### 4.1 Assembly: what `spawn_target()` wires up

`main()` parses the config and hands everything to `spawn_target()`
(`crates/ioutgt/src/lib.rs`), which is the only place all eight crates
meet:

**`spawn_target()` — what gets spawned and wired**

```text
spawn_target(config)                                   [ioutgt]
  └─ control thread (plain Tokio): control_loop()
       ├─ Registry::new()                              [ioutgt-core]
       ├─ build_port(): per namespace
       │    build_backend() → AnyBackend               [ioutgt-control → -backend]
       │    Namespace / Subsystem::new() / PortConfig  [ioutgt-core]
       ├─ senders: Mutex<Option<PoolSenders>> = None   (pool down)
       ├─ server::serve(UnixListener, CtlState)        [ioutgt-control]
       │    └─ stats/nudge read `senders`: Some → admin/io mailbox,
       │       None → zeroed stats / no-op nudge        (pool down)
       └─ select! loop:
            ├─ accept → ensure_pool_up(senders) → setup_connection()
            │    ensure_pool_up (if down): build_pool() →
            │      make_admin/make_io: mailbox() + pending spawn closure,
            │        each → QueueRuntime::new()          [ioutgt-uring]
            │          block_on: loop { msg = mailbox.recv()
            │            Conn → spawn run_queue(conn)     [ioutgt-nvme-tcp]
            │            Shutdown → return (thread exits, ring drops) }
            │    accept_handshake/read_connect → MailboxSender::send(QueueConn)
            │      qid 0 → admin thread, qid n → io thread[(n-1) % N]
            └─ idle tick → active==0 for grace? → teardown_pool(senders)
                 (Shutdown to every thread; senders → None)
```

The mailbox (`ioutgt-uring::mailbox`) is the only cross-thread channel: an
MPSC queue plus eventfd doorbell that the queue thread watches with a
persistent ring read, so handing off a connection never touches the queue
thread's hot path.

**Lazy pool spawn + idle teardown.** The queue-thread pool (admin + N IO
threads) is spawned on the first accepted connection and reclaimed after an
idle grace period — `control_loop` owns its whole lifecycle through one
`Arc<Mutex<Option<PoolSenders>>>` (`None` = pool down). On the first accept
(or the first after a teardown), `ensure_pool_up` runs `build_pool` →
`make_admin_thread`/`make_io_thread`, which create each thread's mailbox +
eventfd and a *pending* closure that builds the OS-thread/io_uring/runtime;
the closures run before the connection's `QueueConn` is routed, so a
freshly-started or idle-reclaimed target holds only the control thread. An
`idle_teardown` grace window (default 30s, `--idle-teardown-secs`, `0`
disables) is polled on a coarse tick: once `active` (the connection count,
decremented by `ConnPermit` on `run_queue` end) has been zero for the whole
window, `teardown_pool` sends each thread a `Shutdown` message — they
return from `block_on`, dropping their rings (the op-slab is empty at idle,
so no drain) — and clears the senders to `None`. The grace window keeps the
pool alive across nvme-tcp reconnect / kill-recovery (which re-establishes
queues within ~10s); only a genuinely idle target reclaims the threads. The
control socket's stats/nudge closures read the shared cell, so they track
the live senders across teardown/respawn; while the pool is down a stats
query is answered with a zeroed snapshot (it neither blocks nor mis-reports
threads as unresponsive) and a namespace-change nudge no-ops (no live
controllers — the edit still lands in the port model for the next connect).

### 4.2 Queue thread: who calls whom inside `run_queue()`

`run_queue()` (`ioutgt-nvme-tcp/src/connection.rs`) is the per-connection
orchestrator. It builds the queue state — `QueueCore<Sqe>` (core, the
generic `QueueCore<C>` instantiated for NVMe) wrapped in an
`NvmeTcpQueue` (transport-side composite of `Rc<QueueCore<Sqe>>` +
`SendList<SendWork>`) — then spawns the task set whose **only rendezvous
is `NvmeTcpQueue`** — the recv loop, slot tasks, and send loop never call
each other directly:

**`run_queue()` — the per-connection task set**

```text
run_queue(QueueConn)                                   [ioutgt-nvme-tcp]
  ├─ QueueCore::new(qid, sqsize, …, Sqe::zeroed())     [ioutgt-core]
  ├─ NvmeTcpQueue::new(…)    (QueueCore<Sqe> +         [ioutgt-nvme-tcp]
  │                          SendList<SendWork>)
  ├─ ConnCtx::new_admin() / new_io()                   [ioutgt-core]
  ├─ spawn_local × sqsize  ── slot tasks ("task per tag"):
  │     loop { sqe = queue.slots.await_command(tag)    [core slotq]
  │            out = dispatch::execute(ctx, tag, &sqe) [core → backend]
  │            queue.slots.begin_respond(tag)          [core slotq]
  │            queue.send.push(SendWork::Response(…)) }[tcp]
  ├─ spawn_local send_loop(queue, fd)                  [tcp]
  ├─ spawn_local keep-alive watchdog (admin only)      [tcp → uring ops::sleep]
  └─ recv_loop(queue, fd)        (runs as the task body)
```

**`NvmeTcpQueue` — the tasks' only rendezvous, command lifecycle left to right**

```text
            recv_loop               NvmeTcpQueue             send_loop
            (ioutgt-nvme-tcp)     (ioutgt-nvme-tcp)      (ioutgt-nvme-tcp)
                │            QueueCore<Sqe> │ SendList          │
  ops::recv ──► │  PduDecoder [nvme]     │                    │
                │  claim_tag() ────────► │                    │
                │  solicit() R2T ──────► │ ─ SendWork::R2t ──►│ encode_r2t [nvme]
                │  submit(tag, sqe) ───► │                    │
                │                        │ wakes slot task `tag`
                │                        │  dispatch::execute()
                │                        │   └ Backend::read/
                │                        │     write [backend
                │                        │     → uring read_at/
                │                        │       write_at]
                │           begin_respond│
                │      SendWork::Response│──────────────────► │ SendList::next()
                │                        │                    │ encode_c2h_data /
                │                        │                    │ response [nvme]
                │                        │ ◄─ release_tag() ──│ ops::sendmsg_raw
                │                        │                    │   [uring]
```

#### 4.2.1 `recv_loop`: a resumable PDU state machine

One task drives the protocol-neutral `StreamReader` (`ioutgt-stream`):
it owns a reused 64 KiB scratch buffer and the `ops::recv` issuing
(`fill`/`consume`), plus the large-payload direct-into-slot path
(`read_direct`, below). The NVMe phase machine (three `RecvPhase`
states) stays here in `ioutgt-nvme-tcp`, decoding PDUs out of the
reader's window; the reader has no protocol or slot knowledge — the
same split NBD/RDMA reuse. Each completed recv steps the machine over
the bytes it brought; any phase can pause mid-PDU and resume on the next
recv:

**`RecvPhase` machine — Header / Data / Ddgst and every transition**

```text
 ops::recv ─► reused 64 KiB buffer ─► step the phase machine:

 ┌────────┐  PduDecoder [nvme] assembles one PDU header
 │ Header │  (headers can straddle recvs), then routes it:
 └────────┘
   ├─ CapsuleCmd, no data       claim_tag, submit(tag)        ─► Header
   ├─ CapsuleCmd, host write    claim_tag, solicit() ONE R2T
   │    (transport SGL)         (TTAG = slot index); payload
   │                            arrives later as H2CData      ─► Header
   ├─ CapsuleCmd, in-capsule    claim_tag; payload is next
   │                            on the stream                 ─► Data
   ├─ H2CData for a live TTAG   validate offset/length        ─► Data
   ├─ H2CTermReq from the host  close WITHOUT replying
   └─ anything else             C2HTermReq, close

 ┌────────┐  memcpy recv buffer → slot at (PDU offset + reassembly
 │  Data  │  progress), CRC32C fused into the copy; resumes
 └────────┘  across recvs
   ├─ buffer drained, H2CData tail ≥ 16 KiB (H2C_DIRECT_MIN;
   │    never in-capsule): receive the tail straight into the slot
   │    via MSG_WAITALL raw recv (best-effort — a short recv resumes
   │    in place), no buffer→slot copy; DDGST re-reads the warm tail
   ├─ payload done, no DDGST    finish(tag)                   ─► Header
   └─ payload done, DDGST       4 digest bytes trail          ─► Ddgst

 ┌────────┐  collect the 4 trailing bytes, compare to the fused CRC
 │ Ddgst  │
 └────────┘
   ├─ match                     finish(tag)                   ─► Header
   └─ mismatch                  fail THIS command only
        (DATA_XFER_ERROR|DNR, as nvmet; connection lives)     ─► Header

 finish(tag) = submit(tag) — wakes the slot task — once the full
   transfer is present (in-capsule: always; R2T: reassembly offset
   reached the SGL length). A mid-transfer H2CData just returns to
   Header to await the next one; one marked `last` with bytes still
   missing is a protocol violation (DATA_OUT_OF_RANGE term).
```

Three rules keep this simple and safe:

- **One outstanding recv, ever.** The direct-tail path works because
  the tail is by definition the next bytes on the TCP stream — the
  buffer recv simply isn't re-armed until the tail lands, so nothing
  is reordered and nothing that could have proceeded is delayed.
  Measured: −44% target cycles/IOP on 128 KiB writes
  (`docs/perf-notes.md`).
- **Failures are graded like nvmet's** (§6): a digest mismatch fails
  just that command; a malformed or out-of-place PDU is a protocol
  violation → C2HTermReq and close.
- **The decoder never sees payload.** Only header bytes pass through
  `PduDecoder` [nvme]; payload bytes go recv-buffer → slot (or
  kernel → slot), keeping the codec sans-IO and the whole copy budget
  visible in one place (§4.2.3).

The byte plumbing under this machine — the scratch buffer + `ops::recv`
(`fill`/`consume`) and the direct-into-slot `MSG_WAITALL` tail
(`read_direct`) — is the protocol-neutral `StreamReader` (`ioutgt-stream`),
reused by every stream transport; only the phase machine above stays in
NVMe/TCP. The full walkthrough — the window model, the digest seam, and
the split raw-pointer safety argument, with diagrams — is in
[`docs/stream-reader.md`](stream-reader.md).

#### 4.2.2 `send_loop`: drain everything, ship one op

The send path is the protocol-neutral `StreamSender` (`ioutgt-stream`),
reused by every stream transport. Each turn it blocks for one work item,
greedily drains the rest of the `SendList`, and ships the whole batch as
a single gather `sendmsg` (`--send-zc`: `SENDMSG_ZC`): PDU headers and
digests packed into a per-batch arena, read payloads referenced in place
from the slot buffers (zero copy), byte-contiguous chunks merged so a
payload-free batch collapses to one iovec. The NVMe/TCP transport
supplies only a *staging closure* (`stage_send_work` + `release_class`,
`connection.rs`): given one `SendWork` item, encode its PDUs into the
arena and return its tag-release class
(`Staged::{NoRelease, AtCqe, AtNotif}`). The harness drives the loop,
owns the batches and the ZC-notification lifetime, and never inspects
the work type — so a future NBD transport reuses it unchanged.

The full walkthrough — every related data structure, staging, shipping,
short-send resume, and the zero-copy lifecycle, with diagrams — is in
[`docs/stream-sender.md`](stream-sender.md). Two facts are
architecturally load-bearing:

- **One send op in flight per connection.** Independent send SQEs on one
  socket carry no ordering guarantee, so the wire is never pipelined.
  `StreamSender` double-buffers so a batch's ZC notification (≈ one RTT)
  overlaps the *next* batch's staging — only the waits overlap, never
  the sends. The recv side then parks on tag exhaustion (`await_tag`)
  rather than terminating, and the idle park reaps the oldest batch's
  notifications (`next_work_reaping`), so tag release never depends on
  new send work arriving (the anti-deadlock invariant).
- **`release_tag` timing is the memory-safety line**, not bookkeeping.
  The kernel reads slot pages for the whole send, so a tag — and with it
  the slot buffer — is released only once the kernel provably no longer
  references it: at the send CQE for capsule-only responses, at the ZC
  notification (≈ the peer's ACK) for payload-carrying ones. Teardown
  joins the send task, draining pending ZC notifications, before the
  queue is freed.

"One send op in flight" is a **per-connection** ordering rule, not a
syscall-count claim — the two batch at different levels. A connection
contributes at most one send SQE at a time (its one gather `sendmsg`). But
the queue thread shares one ring across every connection and op type
on it, and submission is deferred to the park (§ reactor): the send op
just writes its SQE into the SQ ring (no syscall) and awaits. So a
single `io_uring_enter` flushes that connection's lone send SQE
*alongside* every other connection's sends and all the recv/backend
SQEs queued since the last park — one syscall per idle→busy
transition, not one per send. Intra-op gather coalesces many PDUs into
one `SENDMSG`; submission batching coalesces many SQEs into one
`io_uring_enter`; the per-connection rule only bounds how many send
SQEs *one socket* adds to that syscall (≤ 1).

**Why `SENDMSG_ZC`, not `MSG_SPLICE_PAGES`.** The in-kernel nvmet target
sends read payloads with `MSG_SPLICE_PAGES` (`drivers/nvme/target/tcp.c`)
— true splice, no completion, no memlock charge; ioutgt cannot, for the
dual of the `release_tag` rule above. `MSG_SPLICE_PAGES` *donates* the
source pages to the skb (`skb_splice_from_iter` → `iov_iter_extract_pages`
→ frags) and reclaims them by ref/pin drop when the skb frees post-ACK —
it signals the sender *nothing*. That works only for a sender that does
not reuse its source memory and whose pages the stack can truly reclaim:
nvmet hands over fresh per-command **kernel** pages and lets the last skb
ref `put_page` them. A userspace target can only *lend* — its slot pool
is preallocated, reused, mmap'd memory the stack merely *unpins* on
skb-free (the pages stay mapped in the process, never freed), and the slot
must be refilled for the next command. Lending needs a transmit-completion
signal, exactly what `MSG_SPLICE_PAGES` omits and `MSG_ZEROCOPY` supplies
(a `ubuf_info` callback when the last referencing skb frees —
`__msg_zerocopy_callback`); `SENDMSG_ZC` is that, delivered by io_uring as
the notification CQE (`io_uring/notif.c`). So the notif-gated
`release_tag` is not avoidable overhead but the irreducible price of zero
copy from a *reusable userspace* buffer — and the choice is moot at the
ABI: io_uring strips `MSG_INTERNAL_SENDMSG_FLAGS` (⊇ `MSG_SPLICE_PAGES`)
from user `msg_flags` (`io_uring/net.c`). The *separable* cost is the
per-send page pin charged to `RLIMIT_MEMLOCK` (`mm_account_pinned_pages`),
a property of `MSG_ZEROCOPY`'s pinning, not of zero copy itself;
registered slot buffers (§9, Phase 2) would pin once and amortize it,
leaving only the notification.

#### 4.2.3 Data copies: one slot, one visible budget

The slot buffer (preallocated per tag: 8 KiB admin, 128 KiB = MDTS io)
is the single rendezvous for payload bytes; every copy on the path is
accounted against it:

**Payload byte flow — where the copies are, in both directions**

```text
 Host write (H2C)                     Host read (C2H)

 kernel ──ops::recv──► recv buffer    backend fills the slot:
                        (64 KiB)        file read_at — O_DIRECT DMAs
    ① memcpy + fused CRC │              into slot pages, zero copy
    (in-capsule data,    │                       │
     buffered prefixes)  ▼                       ▼
   ┌──────────────┐                    ┌──────────────┐
   │ slot buffer  │ ◄══ ①' H2CData     │ slot buffer  │
   └──────────────┘     tail ≥ 16 KiB: └──────────────┘
          │             MSG_WAITALL           │ ② gather iovec
          │ borrow      straight from         │ references the slot
          ▼             the kernel —          │ IN PLACE — no copy
   backend write_at     skips the             ▼
   on &slot[..len] —    buffer hop     ops::sendmsg_raw ──► kernel
   no further copy                       kernel user→skb copy;
                                         --send-zc pins pages instead:
                                         zero copy, slot reuse gated
                                         on the notification (§9)
```

The transport's userspace copy budget:

| Path                                     | Copies | Notes                         |
|------------------------------------------|--------|-------------------------------|
| H2C in-capsule / buffered prefix         | 1 (①)  | recv buffer → slot, CRC fused |
| H2CData tail ≥ `H2C_DIRECT_MIN` (16 KiB) | 0 (①') | lands directly in the slot    |
| C2H payload                              | 0 (②)  | slot referenced by the iovec  |

Backends add their own: file adds **none** when the open gets O_DIRECT
— and **O_DIRECT is the default**: `FileBackend::open` opens
`O_RDWR | O_DIRECT` unconditionally, with no buffered-mode knob (the
device DMAs against the slot pages). It falls back to buffered IO only
when the open fails with `EINVAL`/`EOPNOTSUPP` — i.e. the filesystem
refuses O_DIRECT (e.g. tmpfs) — and then the kernel copies through the
page cache; any other open error is reported, not silently degraded.
The mode that took effect is observable via `FileBackend::is_direct`.
Memory adds one copy per direction (chunk-wise across its 2 MiB
chunks); null adds none (reads memset the slot — visible when measuring
protocol overhead with it). The binary's default backend, though, is
`memory` (`--backend`, `main.rs`); O_DIRECT only matters once you select
`--backend file`.

This mirrors the kernel nvmet target, where direct IO is also the
default — but split across two backends keyed by the per-namespace
`buffered_io` configfs attribute (default `false`,
`drivers/nvme/target/core.c`). A **block-device** namespace uses
`nvmet-bdev`, which `submit_bio()`s straight to the device — direct *by
construction*, below the page cache (no O_DIRECT flag needed, and **no
buffered mode at all**). A **file** namespace uses `nvmet-file`, which
opens the backing file `O_RDWR | O_DIRECT` unless `buffered_io` is set
(`io-cmd-file.c`) — the same default-direct-with-an-opt-out as ioutgt's
`FileBackend`. On nvmet, `buffered_io=1` is really a backend *selector*:
`nvmet_bdev_ns_enable` returns `-ENOTBLK`, so a block device falls back to
the file backend opened as a buffered file (`core.c`: bdev-enable, then
file-enable on `-ENOTBLK`) — "buffered block device" quietly means "file
backend over the bdev." ioutgt collapses this to one `FileBackend` that
serves both a regular file and a block device (the geometry probe differs;
the O_DIRECT path is identical), with no buffered opt-in.

Two principles behind the budget:

- **The one write-side copy (①) is the product, not waste**: it lets a
  single flat, MDTS-sized buffer absorb arbitrarily fragmented TCP
  segments and H2CData splits, so backends never see scatter — and the
  large tails that dominate bulk writes skip it (①').
- **CRC32C runs while the bytes are cache-hot**, never as a cold pass
  later: the recv side accumulates inside the reassembly copy (digest
  negotiation gates only verification and emission), a direct tail is
  re-read right after the kernel wrote it, and the send side reads the
  slot right after the backend filled it.

Everything else on the path is bounded per PDU: header assembly in the
decoder, the 64-byte SQE stash, and header/digest encoding into the
send arena.

### 4.3 One IO command end to end

A host `Read` crosses every crate boundary exactly once per hop:

1. **Accept + handshake** (control thread): `setup_connection()` calls
   `accept_handshake()` then `read_connect()` [tcp]; the parsed
   `ConnectCommand` [nvme] yields the qid; a `QueueConn` is mailed to the
   owning queue thread [uring mailbox].
2. **Install**: `run_queue()` [tcp] builds `QueueCore<Sqe>` + `NvmeTcpQueue` + `ConnCtx`
   [core/tcp] and spawns the slot tasks; the stashed Connect SQE is the first
   `claim_tag()`/`submit()`.
3. **Receive**: `recv_loop` [tcp] awaits `ops::recv` [uring], feeds bytes
   to `PduDecoder` [nvme], claims a tag and `submit()`s the SQE [core].
   Writes larger than the inline limit first `solicit()` an R2T (TTAG =
   slot index) and reassemble H2CData into the slot buffer.
4. **Dispatch**: the woken slot task calls `dispatch::execute()` [core],
   which routes fabrics/admin/io; `io::execute` resolves the namespace via
   the generation-checked `NsCache` and awaits `Backend::read()`
   [backend], which issues `ops::read_at_raw` straight against the slot
   buffer on the same thread's ring [uring].
5. **Respond**: the slot task calls `begin_respond` [core slotq] and pushes a
   `SendWork::Response` onto `NvmeTcpQueue`'s send list [tcp]; the
   `StreamSender` send loop [stream] drains the whole list, invokes the
   transport's staging closure to encode C2HData/response headers [nvme]
   into the arena with payloads referenced from slot buffers, ships one
   gather `ops::sendmsg_raw` [uring], then `release_tag()` returns the
   slot to the freelist (under `--send-zc`, payload-carrying tags wait
   for the ZC notification instead — §4.2.2).

Boundary summary: **bin→tcp** is the two handshake calls plus
`run_queue()` itself — the queue threads' entry point, with the
`on_ctx` hook that registers each connection's stats; **bin/tcp→uring**
is op futures + mailbox; **tcp→core** is the `QueueCore<Sqe>`/`SlotArray` slot
API plus `dispatch::execute` (the send list and `SendWork` type are
transport-owned in `NvmeTcpQueue`); **tcp→stream** is the `StreamSender` send
harness, driven by the transport's staging closure; **core→backend** is the `Backend` trait behind
`Arc<Namespace>`; **control→core** is `Registry` + `Subsystem`
add/remove + the NS-changed nudge, while GET_STATS reaches the queue
threads through binary-injected `StatsSource` closures over the same
mailboxes; **core/tcp→nvme** is types and encode/decode only — the
codec never does IO, and the reactor never sees a PDU.

## 5. Reactor: io_uring under Tokio current-thread

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

Rejected alternatives: **tokio-uring** (no multishot recv / provided-buffer
rings / SEND_ZC notification control; owned-buffer model conflicts with
preallocated slots; maintenance mode) and a **fully custom executor**
(Tokio's current-thread scheduler is cheap, battle-tested, and brings
`select!`/`JoinHandle`/ecosystem for free — only the wait primitive needs
replacing).

## 6. NVMe/TCP transport

State machines mirror `drivers/nvme/target/tcp.c`:

**Wire state machines — recv phases and the ordered send list**

```text
recv:  PduHeader ──→ Data ──→ DataDigest ──→ (back to PduHeader)
                 └────────────── Error → C2HTermReq → close

send (per command, items on an ordered queue-local send list):
       C2HData hdr → C2HData payload → DataDigest
       R2T
       Response capsule
```

- **Handshake**: ICReq validated (PFV 1.0, HPDA 0), ICResp advertises
  MAXH2CDATA = 16 MiB and negotiated HDGST/DDGST (CRC32C).
- **Reads**: C2HData segmented per MDTS/SGL; optional `c2h_success`
  optimization (SUCCESS flag on final C2HData elides the response capsule)
  behind a config flag.
- **Writes**: in-capsule data up to 16 KiB inline (IOCCSZ advertises
  (64 + 16384)/16); larger transfers via R2T with TTAG = slot index. Phase
  1 allows one outstanding R2T per command (as nvmet does).
- **Digests**: incremental CRC32C (Castagnoli, hardware-accelerated).
- **Errors**: malformed PDUs produce C2HTermReq with the spec'd FES codes,
  never a panic or silent close; backend errors map via an errno→NVMe-SC
  table copied from nvmet semantics (`io::nvme_status`, a free function —
  not a `Backend`-trait method, since the trait is transport-neutral).
- **Send batching (M9) + gather**: the send task drains the entire
  completion/R2T queue into one gather SENDMSG — headers in a small
  arena, payloads referenced from slot buffers — because send SQEs on
  one socket have no ordering guarantee, so batching (not op
  pipelining) is how the per-response park cycle was removed (one
  `io_uring_enter` per batch in each direction; 4.2× on 4K reads, then
  +22% on 128K reads from dropping the staging copy, see
  `docs/perf-notes.md`).

The transport boundary is a pair of abstractions so phase-2 optimizations
and future transports slot in without touching protocol logic:

- `RecvSource` — yields borrowed byte chunks to the codec. Phase 1: plain
  single-shot `RECV` into a per-connection recv buffer, with a
  payload bypass as built: large H2C tails skip the buffer and land
  in the slot via `MSG_WAITALL` raw recv (§4.2.3). Phase 2 (as built,
  opt-in via `--recv-buf-mb`, §6.2): multishot recv with a per-connection
  provided-buffer ring — irreconcilable with the bypass on one connection
  (the kernel picks the buffer), so it is a per-connection strategy choice. (RECV_ZC requires NIC header-data
  split; deferred to real-NIC benchmarking.)
- Send side — queue tasks emit `SendWork` onto the ordered send list;
  the per-connection sender ships vectored `SENDMSG` gather batches
  (as built). With `--send-zc`, batches go out as `SENDMSG_ZC` over
  the same iovecs: double-buffered batches keep staging through the
  notification RTT, payload tags release on the notif (capsule-only
  tags still at the send CQE), the idle park polls the oldest batch's
  notifs alongside the send-list drain so tag release never depends on
  new work arriving, and pin-budget failures (per-user
  `RLIMIT_MEMLOCK`) fall back to the copying SENDMSG per batch (as
  built, opt-in).

## 6.1 Transport contract

The engine split (§4.2) makes the obligations of any transport explicit.
A transport supplies six pieces; the design spec
(`docs/superpowers/specs/2026-06-12-transport-abstraction-design.md`)
records the decision rationale; this section is the authoritative
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

3. **Recv path**: claim a tag — `claim_tag` for negotiated-depth protocols
   where overrun is a protocol error (NVMe/TCP, NVMe/RDMA), `await_tag` for
   server-chosen depth where parking is backpressure (NBD) — fill the slot's
   command and payload, then `submit`. Failures are graded: a per-command
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

The standing invariants from §2/§3 apply unchanged across all transports:
zero steady-state allocation, no locks, no atomic RMW on the IO path;
mailbox-only entry into queue threads; codecs sans-IO; reactor cancellation
safety (the slab entry, not the op future, owns kernel-visible resources).

### 6.1.1 NBD on the refactored base

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

### 6.1.2 NVMe/RDMA on the refactored base

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
from §9, mandatory here). Setup uses an rdma_cm event channel on a dedicated
CM reactor thread (its fd parks on io_uring `POLL_ADD`, which the plain-tokio
control thread cannot provide); qid is read from CONNECT_REQUEST private data
and routed `(qid-1) % N` as today. The verbs completion-channel fd is a
persistent multishot poll on the queue thread — the same mailbox-doorbell
pattern — so one wait primitive still rules the thread. `QueueCore<Sqe>`,
dispatch, controller model, and discovery are all reused unchanged;
`PortConfig.trtype = TransportType::Rdma` makes discovery advertise the
correct TRTYPE.

## 6.2 Zero-copy receive: the per-connection provided-buffer ring

`--recv-buf-mb N` (default 0 = off) turns on the Phase-2 receive path: a
provided-buffer multishot RECV ring per IO connection
(`ioutgt-uring/src/bufring.rs`). The kernel fills app-registered memory as
data arrives — no per-recv submission, no copy into a scratch buffer — and an
H2C write payload is handed to the backend `WRITE_FIXED` straight from ring
memory. Admin queues skip it (qid 0 carries no bulk write payload); without
`IOU_PBUF_RING_INC` (≈ kernel 6.12) the connection transparently falls back to
the classic scratch-recv path (§9), logging a one-line `debug!`.

**Two fixed sub-buffers, filled incrementally.** Each ring is one page-aligned
arena split into exactly two sub-buffers of `recv_buf_mb / 2`, each registered
as an io_uring fixed buffer so the backend can `WRITE_FIXED` it. The ring
carries `IOU_PBUF_RING_INC`: the kernel fills *one* sub-buffer across many recv
completions — advancing an internal offset, emitting a variable-length chunk
per CQE (`IORING_CQE_F_BUF_MORE` set while room remains), advancing `head` only
when the buffer fills. So a whole H2C payload accumulates *contiguously* in one
fixed buffer — exactly what a single-`buf_index` `WRITE_FIXED` needs. Two is the
minimum that double-buffers: one sub-buffer is the active recv target while the
other drains in-flight writes.

**Per-connection, not shared — for correctness.** A recv CQE reports only
`(bid, len)`, not the consume offset; the consumer tracks it. With one consumer
per ring that offset is authoritative; sharing one ring across the connections
multiplexed onto an io-thread would desync it — their slot tasks do not drain
CQEs in completion order, so one connection would read another's bytes. So each
ring-enabled connection owns its ring: its own `bgid` (thread-local allocator)
and its own two fixed buffers. Contention is keyed on *controller* count, not
queue count — the target offers `io_threads` IO queues per controller (a
bijection onto the io-threads), so a shared ring was only ever at risk with ≥ 2
controllers; per-connection ownership makes it moot regardless.

**Retain + borrow lifecycle.** When an H2C write payload fits the current
sub-buffer it is retained in place (via `SlotData::ring()`, a lease carrying the
fixed-buffer index) rather than copied, and the backend `WRITE_FIXED`s from that
region. A sub-buffer must not return to the kernel while a write still reads it,
so each retained payload *borrows* its sub-buffer; a sub-buffer is re-provided
only once recv is done with it *and* all borrows have drained (the
`pending`/`awaiting` refcount in `bufring.rs`). A payload that would straddle
the two sub-buffers falls back to the copy path — correctness never depends on
retention.

**Back-pressure, no stalls.** With both sub-buffers out (one filling, one
borrowed by a slow write) the multishot drains and posts `-ENOBUFS`; the recv
loop parks in `wait_for_provide` rather than busy re-arming, which would spin the
reactor and starve the very slot tasks whose write completions return buffers. It
wakes when a completing write releases its borrow. Invariant: a write's
completion never waits on recv, so progress is guaranteed. The park is gated on a
provide-generation snapshot taken at *arm* time, not park time — the kernel can
exhaust and re-provide every buffer before userspace observes the queued
`-ENOBUFS`, so snapshotting at park would block forever (see
`BufRing::wait_for_provide`).

**Cost, ceilings, and why it is off by default.** Memory is pre-pinned per
connection (the zero-allocation-on-the-IO-path invariant forbids per-command
allocation), so it scales as *connections × recv_buf_mb* — the inverse of kernel
nvmet, which allocates per-command on demand. Two ceilings bound it, both with
graceful fallback to classic recv: the per-io-thread fixed-buffer table (64
slots, ~3 per connection counting the pool arena) and the process
`RLIMIT_MEMLOCK` (all ring memory is pinned — on a tight memlock the ring
silently will not engage, so confirm the `recv ring engaged` log line). It is off
by default because the win is copy-elision and freed io-thread CPU, not headline
throughput: at depth the link and disk already bound a single queue, so on real
100GbE + NVMe the ring measured perf-neutral; its payoff shows at high connection
counts where copy bandwidth competes.

## 7. Core model

**Object model — Port / Subsystem / Namespace, and the per-Connect Controller**

```text
Port ──┬── Subsystem (NQN) ──┬── Namespace (nsid → Backend)
       │                     └── allowed hosts
       └── Discovery subsystem (nqn.2014-08.org.nvmexpress.discovery)

Controller (cntlid) ── created by fabrics Connect on the admin queue
  ├── CC/CSTS register state machine (enable → ready, shutdown)
  ├── Keep-alive timer (KAS granularity 10 s; teardown on expiry)
  ├── AER pool (4 outstanding; NS_CHANGED fired on namespace add/remove)
  └── queues: admin (qid 0) + up to N IO queues (clamped to thread count
      via Set Features NUM_QUEUES)
```

Queue teardown is the userspace analogue of nvmet's `percpu_ref`: an
executing-slot counter drained before slot memory is freed (backend ops
may still be DMAing into it), preceded by failing parked AERs
(`ConnCtx::close`, the analog of `nvmet_async_events_failall` — its
omission was a measurable per-disconnect leak), with a deliberate
leak-on-wedge instead of a use-after-free if a backend never returns.

The namespace table is versioned for runtime add/remove: an `Arc`
snapshot behind a generation counter; IO queues revalidate with one
atomic load per command and refresh only when the control plane changed
something. Changes fire the NS_ATTR async event (note: Identify must
advertise OAES.NS_ATTR or Linux hosts never enable the notice).

Admin command surface (interop-minimal, values per nvmet): Identify CNS
0x00/0x01/0x02/0x03, Get/Set Features (NUM_QUEUES, KATO, async event
config), Keep Alive, AER, Get Log Page (error/SMART/firmware/discovery),
Property Get/Set (CAP/VS/CC/CSTS), fabrics Connect. IO commands: Read,
Write, Flush, then Write Zeroes and DSM-deallocate advertised via ONCS once
backend support lands.

## 8. Backend trait

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
(regular file or block device), `Block` (raw bdev). Disk ops run on the
owning queue thread's own ring. The file backend issues vectored
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

## 9. Buffer strategy: staged, measured

| Concern | As built |
|---------|----------|
| Slot data buffers | Leased on demand from a per-queue `BufPool` (`ioutgt-core/src/pool.rs`): a contiguous arena (default 8 MiB, `--queue-buf-mb`, 4 KiB grain) with a coalescing free-run allocator handing out a contiguous run when one fits, else a scatter list of ≤ `MAX_SEGS`. Each command leases exactly its transfer size (reads/admin via `lease_await` with pool-exhaustion backpressure; write/admin via `lease_or_owned`, a private-buffer fallback that never blocks the serial recv loop), freed at `release_tag`. The pool is deliberately smaller than depth × MDTS. Separately, `--recv-buf-mb` (default 0 = off) sizes the **per-connection** provided-buffer receive ring for zero-copy receive — each ring-enabled connection owns its own ring (its own `bgid` from a thread-local pool + 2 fixed-buffer sub-buffers), so memory scales as (connections × size); left off, recv uses the classic per-recv scratch path. Per-connection (not shared) for correctness — see §6.2. |
| Recv | Classic single-shot RECV → 64 KiB scratch → copy into the slot for headers and small payloads; H2C write tails ≥ 16 KiB are received **straight into the slot's pooled segments** (`MSG_WAITALL`, per-segment — contiguous or scattered), i.e. zero-copy receive, then written by the file backend's vectored `WRITEV`. (The opt-in `--recv-buf-mb` per-connection provided-buffer ring + multishot RECV is the alternative zero-copy path — NVMe/TCP interleaves PDU headers with payload so ring chunks straddle PDU/command boundaries, making chunk-retention into the slot lower-value than receiving straight into the pooled slot.) |
| Send | Batch-drain into one gather `SENDMSG` (header arena + slot-payload iovecs, contiguous or scattered); opt-in `--send-zc`: `SENDMSG_ZC` per batch, slot reuse gated on the notification CQE, `RLIMIT_MEMLOCK` pin failures fall back to copy. |
| Disk | Vectored `READV`/`WRITEV` over the slot's segments; single fd, `O_DIRECT` with a buffered fallback when the store refuses it (see §8). |
| Deferred | RECV_ZC (zcrx) — needs real-NIC header-data split; bundles; second IOPOLL ring. |

MDTS is 128 KiB on IO queues; the admin queue sizes its pool so its
synchronous data leases never block.

## 10. Control plane and configuration

- Unix domain socket, newline-delimited JSON: `ADD_NAMESPACE`,
  `REMOVE_NAMESPACE`, `LIST_NAMESPACE`, `LIST_CONTROLLER`, `GET_STATS`.
  Stats are aggregated by querying each queue thread's mailbox — no
  shared counters: per-queue IO counters (`QueueStats`) and per-thread
  ring counters (`ReactorStats`, `io_uring_enter`/parks/SQEs/CQEs) are
  plain `Cell`s written only by the owning thread; GET_STATS sends a
  oneshot-reply message through the mailbox and each thread snapshots
  its own cells (500 ms timeout per thread, so a wedged backend can't
  hang the control API), and on `clear` zeros them after the snapshot.
  `ioutgt stat` renders them under a controller-identity header, `-i N`
  for iostat-style rates computed client-side, `--clear` to reset.
- The target is fully constructible from a JSON config file: subsystems,
  namespaces (backend type + path + nsid), listen address, thread/affinity
  map, digest policy, inline data size. Validation produces line-precise
  errors before any thread spawns.
- Runtime namespace changes propagate via mailboxes and fire AER
  NS_CHANGED so connected hosts rescan without reconnect.

## 11. CPU affinity and NUMA

By default (`pin_threads` on; opt out with `--no-pin` or
`"pin_threads": false`), IO queue thread placement uses `ioutgt-cpus`,
a userspace port of the kernel's `group_cpus_evenly()` (`lib/group_cpus.c`):
all possible CPUs are grouped evenly per NUMA / cluster / SMT locality
(present CPUs spread first, groups apportioned to nodes by CPU-count
ratio, cluster-aligned when possible, SMT-sibling-first fill — the same
spread managed IRQs and therefore host-side nvme queues get), one group
per IO thread, and each thread is pinned to its group's first online
CPU. A group with no online CPU (or sysfs failure) leaves that thread
unpinned with a warning; the admin thread is never pinned. Combined with
the deterministic qid→thread routing `(n-1) % N`, this lines the host's
per-CPU queues up with topology-aware target cores. Slot arrays and
buffers are allocated on the owning thread (first-touch locality); the
allocation hooks take a NUMA node hint so multi-node placement needs no
API change (development machine is single-node).

## 12. Testing strategy

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
4. **Fuzzing**: cargo-fuzz on the PDU decoder.
5. **Benchmarks**: fio (4K rand R/W, 128K seq R/W, 70/30 mix; QD 1/32/128)
   against ioutgt and an identically-configured kernel nvmet, with perf
   flamegraphs both sides. See `docs/benchmark-plan.md`.

## 13. Milestones

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
| M11 | transport-abstraction refactor | done — engine split (`slotq`), generic `QueueCore<C>`, transport-owned send work (`NvmeTcpQueue`), contract documented (§6.1) |
| M12 | shared send harness | done — ZC gather-send machinery extracted to `ioutgt-stream::StreamSender` behind a per-transport staging closure; NVMe/TCP keeps only PDU encoding |
| M13 | shared recv byte-source | done — buffered scratch + `ops::recv` (`fill`/`consume`) and the direct-into-slot `MSG_WAITALL` tail (`read_direct`) extracted to `ioutgt-stream::StreamReader`; NVMe/TCP keeps the PDU phase machine |
| M14 | multi-transport harness | done — spawn, queue-thread pool, control server and clients extracted to `ioutgt-harness` behind a `Transport` trait; both binaries share them |
| M15 | NVMe/RDMA transport | done — `ioutgt-nvme-rdma` (`sideway` verbs, `rdma-mummy-sys` CM); kernel-host interop on rxe (VM gates) and mlx5 (box); crc32c data-integrity gates green |
| M16 | NVMe/RDMA performance | done — pool arena as io_uring fixed buffer, reactor park-probe (CQ polled at the sleep point), in-capsule write data (IOCCSZ + SGLS SAOS, nvmet parity); matches or beats nvmet-rdma on every single-job fio_perf phase on the test box (64k +38-44%, 4k within ±3%) |
| M17 | adaptive `--poll` | done — busy-poll while commands are in flight (+200 µs grace), event-driven when idle; qd1 latency −20-30%, admin queue exempt |

## 14. Risks

| Risk | Mitigation |
|------|-----------|
| Reactor orphan/missed-wakeup bugs | M1 first; drop-mid-flight stress; ASAN soak; 1 s park backstop |
| Fabrics/enable sequencing vs real host | real nvme-cli connect at M4; pcap fixtures at M2 |
| R2T flow corruption | fragmentation torture; fio --verify; mid-R2T kill tests |
| io-uring crate API gaps | M1 feature probe; raw-registration fallback confined to one module |
| DEFER_TASKRUN park subtleties | strace-asserted echo test; mailbox-only cross-thread rule enforced by non-Send types |
