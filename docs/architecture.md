# ioutgt Architecture Specification

Status: as-built specification (M0–M9 part 1). The milestone table at
the end records what shipped; `docs/roadmap.md` holds what's next.

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

## 4. Crate map and cross-crate call flow

The workspace is eight crates forming a strict dependency DAG — every
crate depends only on layers below it, and the main two leaves are
deliberately opposite in character: `ioutgt-nvme` is **sans-IO** (pure
bytes ↔ structs, no sockets, no async, fuzzable in isolation) and
`ioutgt-uring` is **pure IO** (op futures and the reactor, zero protocol
knowledge). A third small leaf, `ioutgt-cpus`, is a userspace port of
the kernel's `group_cpus_evenly()` (`lib/group_cpus.c`): the grouping
algorithm is pure (driven by a `CpuTopology` value, synthetic in tests),
with sysfs reading confined to `CpuTopology::from_sysfs()`. Only the
`ioutgt` binary uses it (§11).

```text
  app       ┌──────────────────────────────────────────────────────┐
            │ ioutgt — binary; loads TargetConfig, spawn_target()  │
            │ spawns all threads, owns the TCP accept loop         │
            └──────────────────────────────────────────────────────┘
  frontends ┌─────────────────────────┐  ┌─────────────────────────┐
            │ ioutgt-control          │  │ ioutgt-tcp              │
            │ JSON config schema,     │  │ ICReq handshake, recv/  │
            │ UDS control server      │  │ send loops, slot tasks  │
            └─────────────────────────┘  └─────────────────────────┘
  storage   ┌──────────────────────────────────────────────────────┐
            │ ioutgt-backend — AnyBackend: Null / Memory / File    │
            └──────────────────────────────────────────────────────┘
  model     ┌──────────────────────────────────────────────────────┐
            │ ioutgt-core — Port/Subsystem/Namespace, Registry,    │
            │ QueueCore command slots, dispatch (fabrics/admin/io),│
            │ Backend trait definition                             │
            └──────────────────────────────────────────────────────┘
  leaves    ┌─────────────────────────┐  ┌─────────────────────────┐
            │ ioutgt-nvme             │  │ ioutgt-uring            │
            │ sans-IO NVMe(-oF) codec │  │ io_uring reactor, op    │
            │ Sqe/Cqe, PDUs, CRC32C   │  │ futures, mailbox,       │
            │                         │  │ QueueRuntime            │
            └─────────────────────────┘  └─────────────────────────┘
```

| Crate | Role | Depends on (workspace) |
|-------|------|------------------------|
| `ioutgt` | binary + assembly | all seven |
| `ioutgt-control` | config + UDS control plane | core, backend |
| `ioutgt-tcp` | NVMe/TCP transport | core, nvme, uring |
| `ioutgt-backend` | storage backends | core, uring |
| `ioutgt-core` | NVMe model + dispatch | nvme |
| `ioutgt-nvme` | sans-IO codec | — |
| `ioutgt-uring` | reactor + op futures | — |
| `ioutgt-cpus` | userspace `group_cpus_evenly()` | — |

### 4.1 Assembly: what `spawn_target()` wires up

`main()` parses the config and hands everything to `spawn_target()`
(`crates/ioutgt/src/lib.rs`), which is the only place all seven crates
meet:

```text
spawn_target(config)                                   [ioutgt]
  ├─ spawn_admin_thread() ─┐  each queue thread:
  ├─ spawn_io_thread() × N ┤   QueueRuntime::new()     [ioutgt-uring]
  │                        │   mailbox()               [ioutgt-uring]
  │                        └─  block_on: loop { conn = mailbox.recv()
  │                               → spawn run_queue(conn) }   [ioutgt-tcp]
  └─ control thread (plain Tokio): control_loop()
       ├─ Registry::new()                              [ioutgt-core]
       ├─ build_port(): per namespace
       │    build_backend() → AnyBackend               [ioutgt-control → -backend]
       │    Namespace / Subsystem::new() / PortConfig  [ioutgt-core]
       ├─ server::serve(UnixListener, CtlState)        [ioutgt-control]
       │    └─ notify_ns_changed ──► admin mailbox ──► ctx.fire_ns_changed()
       │       (UDS ADD/REMOVE_NAMESPACE → AER NS_CHANGED on live ctrls)
       └─ TCP accept loop → setup_connection() per socket
            ├─ accept_handshake()  ICReq/ICResp        [ioutgt-tcp]
            ├─ read_connect()      first capsule       [ioutgt-tcp]
            └─ MailboxSender::send(QueueConn)          [ioutgt-uring mailbox]
                 qid 0 → admin thread, qid n → io thread[(n-1) % N]
```

The mailbox (`ioutgt-uring::mailbox`) is the only cross-thread channel: an
MPSC queue plus eventfd doorbell that the queue thread watches with a
persistent ring read, so handing off a connection never touches the queue
thread's hot path.

### 4.2 Queue thread: who calls whom inside `run_queue()`

`run_queue()` (`ioutgt-tcp/src/connection.rs`) is the per-connection
orchestrator. It builds the core-side state, then spawns the task set whose
**only rendezvous is `QueueCore`** — the recv loop, slot tasks, and send
loop never call each other directly:

```text
run_queue(QueueConn)                                   [ioutgt-tcp]
  ├─ QueueCore::new(qid, sqsize, slot_buf, …)          [ioutgt-core]
  ├─ ConnCtx::new_admin() / new_io()                   [ioutgt-core]
  ├─ spawn_local × sqsize  ── slot tasks ("task per tag"):
  │     loop { sqe = queue.await_command(tag)          [core]
  │            out = dispatch::execute(ctx, tag, &sqe) [core → backend]
  │            queue.complete(tag, out.cqe, out.len) } [core]
  ├─ spawn_local send_loop(queue, fd)                  [tcp]
  ├─ spawn_local keep-alive watchdog (admin only)      [tcp → uring ops::sleep]
  └─ recv_loop(queue, fd)        (runs as the task body)
```

```text
            recv_loop                 QueueCore              send_loop
            (ioutgt-tcp)            (ioutgt-core)           (ioutgt-tcp)
                │                        │                       │
  ops::recv ──► │  PduDecoder [nvme]     │                       │
                │  claim_tag() ────────► │                       │
                │  solicit() R2T ──────► │ ─── SendWork::R2t ──► │ encode_r2t [nvme]
                │  submit(tag, sqe) ───► │                       │
                │                        │ wakes slot task `tag` │
                │                        │  dispatch::execute()  │
                │                        │   └ Backend::read/    │
                │                        │     write [backend    │
                │                        │     → uring read_at/  │
                │                        │       write_at]       │
                │                        │  complete(tag, cqe) ─►│ next_send_work()
                │                        │                       │ encode_c2h_data /
                │                        │                       │ response [nvme]
                │                        │ ◄── release_tag() ─── │ ops::sendmsg_raw
                │                        │                       │   [uring]
```

**recv_loop** is a resumable state machine across `ops::recv`
completions (one reused 64 KiB recv buffer, passed by value through
each op per the reactor's ownership rule). `Header` feeds bytes to
`PduDecoder` [nvme]; a decoded CapsuleCmd claims a tag and either
expects in-capsule payload next on the stream, or — for a
host-resident write (transport SGL) — `solicit()`s the whole transfer
with a single R2T (TTAG = slot index) and returns to `Header` until
the H2CData PDUs arrive. `Data` memcpys payload from the recv buffer
into the slot buffer at the PDU's data offset plus reassembly
progress, resuming across recvs; the command is `submit()`ed only
once the full transfer is present. `Ddgst` collects the trailing
4-byte digest; a mismatch fails just that command
(`DATA_XFER_ERROR|DNR`, as nvmet does) and keeps the connection,
while malformed or out-of-place PDUs produce C2HTermReq and close.

**send_loop** blocks on `next_send_work()` (`None` after
`close_send()` at teardown), then greedily drains
`try_next_send_work()`, staging R2Ts, C2HData headers, digests, and
response capsules into a small per-connection arena (sqsize × 64 B,
min 4 KiB)
while read payloads are referenced **in place** from slot buffers;
the batch ships as one gather `ops::sendmsg_raw` (byte-contiguous
arena chunks merge, so a payload-free batch is a single iovec
entry). A short send advances the iovec list and re-issues — no
memmove — so nothing else can interleave on the wire. With SQ flow
control off, a successful read elides the response capsule (SUCCESS
bit in C2HData). Slots are `release_tag()`ed only after the whole
batch send completes — under gather this is the memory-safety line,
not bookkeeping: the kernel reads slot pages during the send, and
teardown joins the send task before the queue is freed.

**Data copies.** The slot buffer (preallocated per tag: 8 KiB admin,
128 KiB = MDTS io) is the single rendezvous for payload bytes; the
transport costs one userspace memcpy on the write side and none on
the read side:

- **Host write (H2C)**: kernel → recv buffer (`ops::recv`); recv
  loop memcpys recv buffer → slot buffer (`RecvPhase::Data`); the
  backend then gets `&slot.data()[..len]` borrowed directly — the
  file backend issues `write_at` on that pointer, zero further
  copies.
- **Host read (C2H)**: the file backend `read_at`s straight into the
  slot buffer; send loop references the payload **in place** via the
  gather iovec (`stage_send_work`), one `ops::sendmsg_raw` → kernel.
  DDGST is a read-only `crc32c` pass over the slot, trailed in the
  arena.

The write-side copy is what lets one flat, MDTS-sized buffer absorb
arbitrarily fragmented TCP segments and H2CData splits, so backends
never see scatter (eliminating it for the unreceived tail of large
R2T transfers is a roadmap item); the read side's remaining copy is
the kernel's user→skb gather, which phase-2 `SEND_ZC` (§9) removes
over the same iovecs. The budget is the transport's: the memory
backend adds one payload copy per direction (chunk-wise across its
2 MiB chunks), null adds none (reads memset the slot — visible when
measuring protocol overhead with it), and the file backend adds
none — when the open gets O_DIRECT the device DMAs against the slot
pages; where the filesystem refuses (e.g. tmpfs) it falls back to
buffered IO and the kernel copies through the page cache
(`FileBackend::is_direct`). Everything else on the path is bounded
per PDU — header assembly in the decoder (headers can straddle
recvs), the 64-byte SQE stash, and header/digest encoding into the
send arena.

CRC32C runs while the bytes are cache-hot, never as a cold pass long
after: the recv side accumulates alongside the reassembly copy
(digest negotiation gates only verification and emission), and the
send side reads the slot right after the backend filled it.

### 4.3 One IO command end to end

A host `Read` crosses every crate boundary exactly once per hop:

1. **Accept + handshake** (control thread): `setup_connection()` calls
   `accept_handshake()` then `read_connect()` [tcp]; the parsed
   `ConnectCommand` [nvme] yields the qid; a `QueueConn` is mailed to the
   owning queue thread [uring mailbox].
2. **Install**: `run_queue()` [tcp] builds `QueueCore` + `ConnCtx` [core]
   and spawns the slot tasks; the stashed Connect SQE is the first
   `claim_tag()`/`submit()`.
3. **Receive**: `recv_loop` [tcp] awaits `ops::recv` [uring], feeds bytes
   to `PduDecoder` [nvme], claims a tag and `submit()`s the SQE [core].
   Writes larger than the inline limit first `solicit()` an R2T (TTAG =
   slot index) and reassemble H2CData into the slot buffer.
4. **Dispatch**: the woken slot task calls `dispatch::execute()` [core],
   which routes fabrics/admin/io; `io::execute` resolves the namespace via
   the generation-checked `NsCache` and awaits `Backend::read()`
   [backend], which issues `ops::read_at` on the same thread's ring
   [uring].
5. **Respond**: `complete()` [core] queues a `SendWork`; `send_loop` [tcp]
   drains the whole send list, encodes C2HData/response headers [nvme]
   into the arena with payloads referenced from slot buffers, ships one
   gather `ops::sendmsg_raw` [uring], then `release_tag()` returns the
   slot to the freelist.

Boundary summary: **bin→tcp** is two handshake calls; **bin/tcp→uring** is
op futures + mailbox; **tcp→core** is the `QueueCore` slot API plus
`dispatch::execute`; **core→backend** is the `Backend` trait behind
`Arc<Namespace>`; **control→core** is `Registry` + `Subsystem`
add/remove + the NS-changed nudge; **core/tcp→nvme** is types and
encode/decode only — the codec never does IO, and the reactor never sees
a PDU.

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
  reactor timer (capped at 100 ms as a missed-wakeup backstop), reaps all
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
  table copied from nvmet semantics.
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
  single-shot `RECV` into a per-connection recv buffer. Phase 2: multishot
  recv with a provided-buffer ring. (RECV_ZC requires NIC header-data
  split; deferred to real-NIC benchmarking.)
- Send side — queue tasks emit `SendWork` onto the ordered send list;
  the per-connection sender ships vectored `SENDMSG` gather batches
  (as built); `SEND_ZC` with notification-gated slot reuse rides the
  same iovecs (phase 2).

## 7. Core model

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
    async fn flush(&self) -> Result<(), BackendError>;
    async fn discard(&self, ranges: &[LbaRange]) -> Result<(), BackendError>;
    async fn write_zeroes(&self, range: LbaRange) -> Result<(), BackendError>;
    // size / block_size / topology probes
}
```

(Signature sketch; exact buffer types are the slot-owned buffers.) Backends:
`Null`, `Memory` (bring-up + tests), `File` (O_DIRECT via ring `READ`/
`WRITE`, `FSYNC` flush, `FALLOCATE` punch-hole/zero-range), `Block` (raw
bdev). Disk ops are issued on the owning queue thread's own ring. IOPOLL is
not used: a polled ring cannot carry socket ops, and a second per-thread
IOPOLL ring is a measured-later roadmap item.

## 9. Buffer strategy: staged, measured

| Stage | Recv | Send | Disk |
|-------|------|------|------|
| Phase 1+M9 (current) | single-shot RECV → recv buffer → copy into slot buffer | batch-drain into one gather SENDMSG (header arena + slot iovecs) | READ/WRITE, O_DIRECT, 4K-aligned slot buffers |
| Phase 2 (each step benchmarked in isolation) | multishot RECV + provided buffer ring (ENOBUFS → single-shot fallback) | SEND_ZC, slot reuse gated on notification CQE | READ_FIXED/WRITE_FIXED on registered slot buffers |
| Deferred | RECV_ZC (zcrx) — needs real-NIC header-data split | bundles | second IOPOLL ring |

Slot data buffers: 128 KiB (= MDTS) on IO queues, 8 KiB on admin queues,
allocated once at queue install and registered with the ring in phase 2.

## 10. Control plane and configuration

- Unix domain socket, newline-delimited JSON: `ADD_NAMESPACE`,
  `REMOVE_NAMESPACE`, `LIST_NAMESPACE`, `LIST_CONTROLLER`, `GET_STATS`.
  Stats are aggregated by querying each queue thread's mailbox — no
  shared counters.
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
   target on the host; a vmtest VM (`~/git/utils/vmtest -c
   ~/git/linux-knext/vmtest.conf`) runs `nvme discover`, `nvme connect`,
   `nvme list/id-ctrl/id-ns`, fio `--verify=crc32c`, `nvme disconnect`
   against `10.0.2.2:4420`, across the digest × queue-count matrix.
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
| M9 | performance pass | part 1 done — batched send 4.2×; rest in roadmap |
| M10 | docs | comparison/usage/roadmap done; **nvmet benchmark deferred** (`benchmark-plan.md`) |

## 14. Risks

| Risk | Mitigation |
|------|-----------|
| Reactor orphan/missed-wakeup bugs | M1 first; drop-mid-flight stress; ASAN soak; 100 ms park backstop |
| Fabrics/enable sequencing vs real host | real nvme-cli connect at M4; pcap fixtures at M2 |
| R2T flow corruption | fragmentation torture; fio --verify; mid-R2T kill tests |
| io-uring crate API gaps | M1 feature probe; raw-registration fallback confined to one module |
| DEFER_TASKRUN park subtleties | strace-asserted echo test; mailbox-only cross-thread rule enforced by non-Send types |
