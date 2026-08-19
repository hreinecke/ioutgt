# ioutgt vs Linux kernel nvmet — design comparison

Reference sources: `drivers/nvme/target/{core.c,tcp.c,fabrics-cmd.c,
admin-cmd.c,discovery.c,io-cmd-bdev.c,io-cmd-file.c,nvmet.h}` (studied
at the start of the project and mirrored where interop depends on it).
Format per the project specification: for each subsystem — Linux
design, ioutgt design, differences, benefits, risks.

Status: current as of the gather-send and direct-to-slot recv work
(2026-06; §2 reflects both) and the nvmet-JSON multi-port config
work (2026-07; §9). Benchmark-backed claims are limited
to ioutgt-internal A/B measurements (`docs/perf-notes.md`); the
head-to-head against nvmet is deferred (`docs/benchmark-plan.md`).

---

## 1. Command lifecycle and request tracking

**Linux.** `struct nvmet_req` embedded in a transport command
(`nvmet_tcp_cmd`), preallocated per queue at install time
(`nvmet_tcp_alloc_cmds`, one per SQ entry). Lifecycle:
`nvmet_req_init → req->execute() → nvmet_req_complete`, with a
`percpu_ref` per SQ guarding teardown, `cmpxchg`-based sqhd
accounting, and a lock-free `llist` carrying completions from
executor context to the send side.

**ioutgt.** Preallocated `Slot<Sqe>` array sized by the negotiated
sqsize, plus **one persistent async task per tag** that loops
await-command → dispatch → complete. The TCP transfer tag *is* the
slot index; the host CID is stored and echoed, so no CID lookup
structure exists anywhere. Slot doorbells are same-thread
`Cell<Option<Waker>>`; sqhd is a plain `Cell<u16>`; completions ride a
queue-local `VecDeque`. Teardown uses an executing-slot counter and a
drain loop instead of percpu_ref.

**Differences.** nvmet expresses per-command state as an explicit
state machine driven from work-queue context; ioutgt expresses it as
suspended stack frames. nvmet's atomics (percpu_ref, cmpxchg sqhd,
llist) exist because executor and transport contexts can run on
different CPUs; ioutgt pins the whole queue to one thread, so the
same invariants hold with zero atomics.

**Benefits.** The data path reads as straight-line async Rust — the
R2T flow, AER parking, and backend waits are all just `.await`s.
Bounded concurrency makes the steady state allocation-free without a
request mempool. Cancellation correctness is centralized in one place
(the reactor's orphan protocol) rather than scattered per state.

**Risks.** A suspended task is opaque compared to an inspectable state
enum: nvmet can dump `cmd->state` for every command; ioutgt currently
infers stuck commands from slot-state counters only. Per-tag tasks
cost ~0.5 KiB each of future state (negligible at NVMe depths, but
real). Hidden wake-ordering bugs are subtler than missed `queue_work`
calls — this is why M1 stress-tested the reactor before anything was
built on it.

## 2. TCP transport: receive and send

**Linux.** Socket callbacks schedule `io_work` on a CPU chosen from
the queue index; `io_work` runs budgeted recv/send loops
(`NVMET_TCP_RECV_BUDGET`/`SEND_BUDGET` 8, total budget 64) using
`kernel_recvmsg`/`sendpage`. Recv states {PDU, DATA, DDGST}; send
states {DATA_PDU, DATA, R2T, DDGST, RESPONSE} walked one command at a
time. MAXH2CDATA 16 MiB, inline data 16 KiB, CRC32C digests optional.

**ioutgt.** Identical wire-state machines (deliberately), but driven
by io_uring completions on a pinned thread. Receive: one in-flight
recv into a 64 KiB connection buffer feeding the sans-io header
decoder; in-capsule payloads and buffered prefixes copy into slot
buffers with incremental CRC32C, while large H2C payload tails land
**directly in the slot** via a `MSG_WAITALL` raw recv — matching
nvmet's no-second-copy property (`kernel_recvmsg` into the command's
SG list after the header) for the bytes that dominate large writes.
Send: the loop drains *all* pending completions/R2Ts into one gather
batch (headers/digests in a small arena, payload iovecs pointing into
slot buffers) and ships a single sendmsg op — ordering on the socket
is preserved by construction, and the queue thread parks once per
batch instead of once per response (the M9 finding: the naive
one-op-per-response loop cost a full park/`io_uring_enter` cycle per
IO and capped a connection at ~85K 4K IOPS; batching took it to 202K
single-connection, 506K at four).

**Differences.** nvmet's budget loop and ioutgt's batch drain solve
the same problem (amortize wakeups, bound latency injection) from
opposite directions: nvmet caps how much it does per wakeup, ioutgt
caps how often it sleeps. ioutgt additionally offers opt-in
`SENDMSG_ZC` (`--send-zc`) — nvmet has no zero-copy send. Two
ZC-mode behavioral divergences: payload slot reuse waits for the
kernel's notification CQE (≈ the host's ACK), and a host exceeding
the negotiated queue depth is stalled by TCP backpressure instead of
receiving a C2HTermReq — the notification races the host's next
command, making a legitimate command indistinguishable from a depth
violation at the target. Payload copies now match nvmet on both
directions' hot paths: reads go by reference via the gather iovecs
(nvmet: `sendpage`), large write tails land direct (nvmet:
`recvmsg` into SG); ioutgt still copies in-capsule writes and
buffered prefixes (nvmet copies neither, at the price of
per-command bounded recvs — ioutgt's 64 KiB batched recv buys
cross-command batching for small IO instead).

**Benefits.** Submission batching is automatic (SQEs accumulate while
tasks run; one `io_uring_enter` flushes the lot — measured 0.065
syscalls/op on the echo fixture). No softirq sharing: the thread's
cycles are entirely its own.

**Risks.** DDGST failure fails the affected command with
`NVME_SC_DATA_XFER_ERROR` and keeps the connection, matching nvmet.
The remaining copy asymmetry vs nvmet is the small-write/prefix copy
noted above — accepted deliberately for the batching win
(`docs/perf-notes.md` has the A/B numbers).

## 3. Fabrics connect and controller model

**Linux.** Connect allocates `nvmet_ctrl` (cntlid from an IDA),
validates subsysnqn/hostnqn ACLs, installs queues with sqsize
validation against MQES; property get/set implement the register
surface (CAP/VS/CC/CSTS) with the enable/shutdown state machine;
keep-alive is a delayed work item firing fatal error on expiry, with
traffic-based keep-alive (`CTRATT.TBKAS`) letting any command on any
queue stand in for a Keep Alive (`sq->ctrl->reset_tbkas`).

**ioutgt.** The control thread performs the ICReq handshake and reads
the first (Connect) capsule on plain Tokio sockets — the qid decides
which queue thread receives the socket; the Connect command then
replays through the normal slot pipeline as the queue's first
command. Controllers live and die with their admin-queue connection;
a mutex-guarded registry (control-plane rate only) maps cntlid →
identity for IO-queue Connect validation. Register state machine and
CAP values mirror nvmet (MQES 0-based, CQR, TO=15s). Keep-alive is a
ring-timer watchdog that closes the socket past 2×KATO+grace. TBKAS
works the same way as nvmet's but arrives by a different route: the
controller's queues sit on different threads, so an IO queue publishes
"I saw traffic" into a shared flag once per keep-alive tick (never per
command) and the admin queue's watchdog consumes it. Only the TCP
transport publishes, so only it advertises the bit; RDMA still relies on
Keep Alive commands.

**Differences.** nvmet routes raw connections to whatever CPU the
socket callback lands on and learns the qid later; ioutgt pays one
control-thread hop to learn the qid first, so admin queues never
occupy IO threads and qid→core mapping is deterministic (lining up
with the host's per-CPU queue placement).

**Benefits.** Deterministic placement; the handshake (control-plane
rate) keeps queue threads free of accept/negotiation states entirely.

**Risks.** The control thread is a connect-rate serialization point —
irrelevant for storage workloads (hundreds of connections), but a
difference from nvmet's fully distributed accept. Host ACLs
(`allow_any_host = false` paths) are not yet enforced beyond a flag
check.

## 4. Admin commands, discovery, async events

**Linux.** Identify/features/log pages in `admin-cmd.c` with
carefully chosen values (sqes/cqes 0x66/0x44, IOCCSZ from inline
size, KAS); discovery subsystem in `discovery.c`; AER pool of 4 with
`nvmet_async_events_failall` on teardown; changed-NS log with RAE
semantics.

**ioutgt.** The same command surface with values copied where the
host depends on them — including two found-the-hard-way fields:
**OAES must advertise NS_ATTR** (the host masks its AEC against OAES;
without it, namespace-change notices are never enabled) and subnqn
must be NUL-terminated rather than space-padded. AERs park as
unresolved futures inside their slot tasks; namespace changes
complete one with the NS_ATTR notice; teardown fails parked AERs
(`ConnCtx::close`, the analog of `nvmet_async_events_failall`) — the
omission of which was a 389 KB-per-disconnect leak caught by the M8
RSS gate. Changed-NS log reports the 0xFFFFFFFF sentinel and clears
on read (RAE is not yet honored). Like nvmet, it advertises multi-path
capability — Identify Controller **CMIC** multi-controller (bit 1) and
Identify Namespace **NMIC** shared (bit 0) — so the host's NVMe-multipath
layer (`nvme_mpath_alloc_disk`, gated on `CMIC_MULTI_CTRL`) folds each
connection's path into one namespace head and exposes a per-controller
path device (`/dev/nvmeXcYnZ`); the namespace UUID from CNS 0x03 gives
that head its identity.

**Differences/Risks.** Log pages are minimal (zeroed error/SMART/FW);
RAE handling, SMART data, and multi-page error logs are future work.
Discovery log windowing (LPO) is implemented; generation-counter
churn protection is simplistic (static genctr). Unlike nvmet, **ANA is
not advertised** (CMIC bit 3 clear): ioutgt serves no ANA log page, so
the host runs plain (non-ANA) multipath — paths are all optimized.

## 5. IO backends

**Linux.** Two backends with different kernel machinery: `io-cmd-bdev`
builds bios onto the block layer; `io-cmd-file` uses kiocbs with a
buffered-IO fallback workqueue. Status mapping from blk_status_t;
ONCS advertises DSM/Write Zeroes.

**ioutgt.** One trait (`Backend`, async fn, monomorphized via an enum
— no per-IO boxing), four implementations: null, memory (sharded 2 MiB
lazily-allocated chunks), and one file/bdev backend — in userspace the
two collapse to "open O_DIRECT, probe geometry differently"
(st_size vs BLKGETSIZE64), with buffered fallback where O_DIRECT is
refused. Discard is punch-hole on files (hint semantics) and a no-op
on bdevs until uring-cmd lands; write-zeroes falls back
ZERO_RANGE → PUNCH_HOLE → zero-chunk writes. Slot buffers are
4 KiB-aligned for O_DIRECT, and teardown waits for executing slots
(the kernel may be DMAing into slot memory) with a deliberate
leak-on-wedge instead of a use-after-free.

**Direct-IO defaults.** Both targets default to direct IO, but nvmet
splits it across the two backends via the per-namespace `buffered_io`
configfs attribute (default `false`, `core.c`): a bdev namespace
`submit_bio()`s below the page cache — direct *by construction*, no
buffered mode at all — while a file namespace opens
`O_RDWR | O_DIRECT` unless `buffered_io` is set (`io-cmd-file.c`).
`buffered_io=1` is really a backend *selector*: `nvmet_bdev_ns_enable`
returns `-ENOTBLK`, so a "buffered block device" quietly means the
file backend over the bdev. ioutgt's single `FileBackend` keeps the
same default-direct-with-an-opt-out shape but with no buffered opt-in
— the buffered fallback engages only when the store refuses O_DIRECT
(e.g. tmpfs), and the mode that took effect is observable via
`FileBackend::is_direct`. Worth remembering when configuring A/B
benchmarks.

**Differences.** nvmet submits bios that the block layer may split
and parallelize; ioutgt issues one ring op per command region
(resuming short transfers). nvmet's FUA maps to REQ_FUA; ioutgt
flushes after the write — correct but one round trip more expensive.

**Risks.** No metadata/PI, no zoned support, bdev
discard/write-zeroes lack the ioctl/uring-cmd path, and the bdev side
has only been exercised through the file-backend code path plus
geometry probing (loop-device runs need root — deferred with the
benchmark work).

## 6. Threading and synchronization

**Linux.** Work-queue execution with queue→CPU binding by index;
correctness across contexts via percpu_ref, cmpxchg, llist; file IO
hops through a second workqueue.

**ioutgt.** One OS thread per NVMe queue: Tokio current-thread
runtime, `LocalSet`, one `SINGLE_ISSUER | DEFER_TASKRUN` ring, no
Tokio IO/time drivers. `io_uring_enter` *is* the park primitive.
Cross-thread input exists only as eventfd-doorbell mailboxes; the
namespace table is the one shared read-mostly structure, versioned
behind a generation counter so the per-command cost is a single
atomic load. Everything else on the data path is `Cell`/`RefCell`.

**Multiple subsystems share one pool — there is no pool-per-subsystem.**
A port may serve several subsystems, but they are all served by a single
fixed per-port pool: 1 admin thread + N IO threads (`build_pool` in
`crates/ioutgt-harness/src/lib.rs`), lazily spawned on the first connection and
reclaimed after an idle grace period. Connections hash onto it by qid
(`(qid−1) % N`); the subsystem is resolved **per connection** from the
Connect capsule's `subsysnqn` (`fabrics_exec.rs` → `PortConfig::subsystem`)
and carried on the controller, never on the thread. So two controllers on
different subsystems of the same port can land on the same IO thread, and
a `Subsystem` owns no execution context — only namespaces, serial/model,
and its host-allow flag. This mirrors nvmet exactly: `nvmet_subsys` is a
logical config object (namespaces, ACLs, `ctrls` list) with no kthread or
workqueue of its own; TCP I/O is per-queue `io_work` on one shared global
`nvmet_tcp_wq`, and the controller's subsystem is chosen at Connect via
`nvmet_find_get_subsys`. The one divergence is placement policy, not
granularity: nvmet steers each queue's `io_work` to the socket's RX CPU
(`sk_incoming_cpu`) dynamically, whereas ioutgt uses a fixed `spread_cpus`
pinning with deterministic qid→thread hashing.

**Benefits.** The "no locks on the hot path" claim is checkable by
construction (`!Send` types make violations compile errors). CPU
accounting is exact: a queue's cycles are its thread's cycles.

**Risks.** A blocking backend op stalls every queue on that thread
(nvmet's workqueues would just grow); the wedge path leaks by design
rather than stalls forever, but a slow disk degrades all connections
sharing the thread. The DEFER_TASKRUN park integration is the
project's most safety-critical code and carries a 1 s backstop
against missed-wakeup bugs.

## 7. Configuration and control plane

**Linux.** configfs: mkdir/echo per subsystem/namespace/port; runtime
namespace enable/disable fires AENs; nvmetcli wraps it.

**ioutgt.** One JSON file creates the whole target (validated before
any thread spawns); a Unix-socket JSON API (ADD/REMOVE/LIST_NAMESPACE,
LIST_CONTROLLER, GET_STATS) mutates the versioned namespace table at
runtime and
nudges controllers' AERs — verified end-to-end: a connected Linux
host saw the hot-added namespace appear without reconnecting.

**Differences/Risks.** Far smaller surface than configfs (no port
management at runtime, no host ACL objects, no passthru). GET_STATS
lacks per-queue IO counters until the deferred stats work lands.

## 8. Error handling and teardown

**Linux.** Per-command status mapping; transport-fatal errors raise
CSTS.CFS or tear the queue down; `nvmet_ctrl_fatal_error` on KA
expiry; release paths quiesce via percpu_ref kill+drain.

**ioutgt.** Malformed PDUs draw C2HTermReq with spec FES codes (8
abuse tests assert both the code and that the target stays healthy);
KA expiry closes the connection and reaps the controller; teardown
is close() (fail AERs) → drain executing slots → abort tasks →
registry removal, with the reactor reclaiming orphaned ops on their
terminal CQEs. The whole workspace runs clean under AddressSanitizer.

**Risks.** Termination is used in a few places where nvmet degrades
more gracefully (queue-depth overrun; DDGST mismatch is already
per-command, §2); a hostile or buggy host gets disconnected rather
than per-command errors. Defensible for a v1, but the gentler
responses are catalogued in the roadmap.

## 9. Target object model: ports, subsystems, controllers

The nouns first. A **port** is a listening address; a **subsystem** is
a named (NQN) set of namespaces plus a host ACL; a **namespace** binds
an NSID to storage; a **controller** is one host's live session with a
subsystem, created by an admin-queue Connect and gone with that
connection. Ports, subsystems, and namespaces come from configuration
and outlive connections; only the controller is dynamic. Both targets
share these nouns — the NVMe-oF spec fixes them — so the comparison
is about how each represents the *links* between them. (The Connect
flow that creates a controller is §3's topic; this section is about
the resulting structure.)

**Linux.** A global, mutable object graph rooted in configfs.
`nvmet_port` and `nvmet_subsys` are directories; the port↔subsystem
link is a symlink (`ports/<id>/subsystems/<nqn>`, backed by
`nvmet_subsys_link`), and host ACLs are `nvmet_host` objects
symlinked into `subsystems/<nqn>/allowed_hosts`. All of it is mutable
on a live target. `nvmet_ctrl` is allocated at admin Connect
(`nvmet_alloc_ctrl`): cntlid from a global IDA within the subsystem's
`cntlid_min/max` window, an entry on `subsys->ctrls`, lifetime
kref-counted against its queues.

**ioutgt.** The same graph, flattened at startup and split per
process. The config file's `ports[].subsystems` NQN list *is* the
symlink, in JSON (`crates/ioutgt-control/src/nvmet.rs`); parsing
resolves it into one self-contained bundle per port, each served by
its own process (§7). At startup `build_port`
(`crates/ioutgt-harness/src/lib.rs`) freezes the bundle into
`PortConfig`: an immutable NQN → `Arc<Subsystem>` map shared
read-only with every queue thread. `Subsystem` itself
(`crates/ioutgt-core/src/subsystem.rs`) is immutable identity
(NQN/serial/model/ACL) around the one mutable part, the versioned
namespace table (§6). The controller is not one struct but three
pieces, split by which thread must see them:

```
port ──exports──▶ subsystem ──contains──▶ namespace ──backs──▶ backend
  │                   ▲
  │ admin Connect:    │ Arc<Subsystem>, bound per connection
  │ resolve NQN,      │
  ▼ admits(), cntlid  │
controller ───────────┘
  ├─ Registry[cntlid]  routing: hostnqn, queues   (all threads, mutex)
  ├─ AdminState        CC/CSTS, KATO, AERs        (the qid-0 connection)
  └─ IoState × N       cached ns table (NsCache)  (one per IO connection)
```

The `Registry` (`crates/ioutgt-core/src/registry.rs`) is the
cross-thread record — control-plane rate only, so a mutex is fine —
and it names the subsystem by NQN string, keeping it protocol-neutral.
The per-connection pieces live in `ConnCtx`
(`crates/ioutgt-nvme/src/dispatch.rs`) as plain `Cell`/`RefCell`
state on their own thread. cntlids come from a per-process disjoint
slice rather than a per-subsystem window: a subsystem exported on two
ports is two independent instances in two processes but one subsystem
on the wire, and Linux hosts reject duplicate cntlids across paths
(`nvme_validate_cntlid`).

**Differences.** (1) Link mutability: nvmet can symlink a subsystem
into a live port; ioutgt fixes the port↔subsystem map at startup, so
runtime mutation exists only inside a subsystem (the namespace table,
§7) and changing the export set means restarting that port's process.
(2) Controller shape: one kref-counted struct on `subsys->ctrls`
versus three thread-scoped pieces with no controller list on the
subsystem at all — and no cross-thread teardown command either
(§6's mailbox rule): on TCP the host closes its own IO-queue sockets
when the admin path dies (each recv loop exits on EOF) and registry
removal is admin-teardown cleanup; the RDMA transport's IO queues,
with no socket EOF to lean on, poll `Registry::contains` and follow
their controller down. (3) Hosts
are plain strings in `allowed_hosts`, not linked objects.

**Benefits.** Connect-time resolution is a lock-free read of a frozen
map. There is no teardown-ordering problem between port, subsystem,
and controller — the `Arc` graph is acyclic and the long-lived nodes
immutable, where nvmet needs kref + percpu_ref choreography. Process
per port makes port isolation an OS guarantee.

**Risks.** No live re-export, as above. A subsystem on several ports
is duplicated state: serial/model/identity stay consistent only
because they derive from the same config (deterministic namespace
UUIDs, §4), and a runtime namespace add must be replayed against each
port's control socket.

**Could it be simpler?** One candidate found and applied, one
considered-and-kept, one that only looks redundant.
`Subsystem::max_qid` was a port-wide value (always the port's
IO-thread count) duplicated into every subsystem; it now lives on
`PortConfig`, leaving `Subsystem` pure NVM-subsystem identity —
discovery controllers keep their explicit zero-IO-queue special case
in `admin.rs` / `fabrics_exec.rs` either way. The discovery
controller is the opposite call: nvmet models discovery as a real
subsystem object (`nvmet_disc_subsys`); ioutgt uses a flag plus
`Option<Arc<Subsystem>>` in `AdminState` — a namespace-less
`Subsystem` would delete those branches, but discovery's
Identify/log-page behavior diverges enough that the flag is the
smaller special case today. And the three subsystem representations
(serde shape → `SubsystemConfig` → `Subsystem`) are not duplication:
each conversion is the single place its invariants are enforced;
collapsing them would leak kernel-JSON quirks inward.

---

## What ioutgt deliberately does differently — summary

1. **Task-per-tag instead of state machines** — NVMe's bounded CID
   space turns "async task per request" from an anti-pattern into a
   preallocation strategy; this is the project's central bet and the
   main readability win over both nvmet and SPDK.
2. **No CID lookup anywhere** — TTAG = slot index; the CID is data,
   not a key.
3. **Park-driven batching instead of budget loops** — submission and
   completion batching fall out of the runtime-idle hook; the send
   side completes the picture by draining per batch (one syscall
   amortized over the whole burst in each direction).
4. **One backend implementation where the kernel needs two** —
   userspace O_DIRECT erases most of the bdev/file split.
5. **Generation-cached namespace tables instead of RCU** — the same
   read-mostly semantics with one atomic load, no RCU machinery.
6. **A startup-frozen object graph instead of live configfs** — the
   port↔subsystem link is resolved once at parse time and the
   controller is decomposed by thread visibility; runtime mutability
   is confined to the namespace table and the controller registry.
