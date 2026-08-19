# NVMe/RDMA transport (`ioutgt-nvme-rdma`)

Status: **built** (M15 transport + M16 perf + M17 adaptive `--poll`).
A standalone binary on the shared `ioutgt-harness` pool; kernel-host
interop green on soft-RoCE (VM gates) and mlx5 (two-NIC box), matching
or beating nvmet-rdma on every single-job fio_perf phase on the test box
(64k +38-44%, 4k within ±3%).

This doc covers only the RDMA-specific layer — memory registration, CQ
draining through the reactor, RDMA-CM acceptance, and the recv/reap
machinery. The machinery it sits on is documented elsewhere:

- thread model, reactor, slot engine, transport contract —
  [`architecture.md`](architecture.md) (§2, §4, §5.1)
- NVMe model / dispatch / backends, reused unchanged from TCP —
  [`architecture.md`](architecture.md) (§6, §7)
- where this transport plugs into the shared base —
  [`architecture.md`](architecture.md) (§5.1.2)
- flow control vs nvmet / SPDK —
  [`rdma-flow-control-nvmet-vs-spdk.md`](rdma-flow-control-nvmet-vs-spdk.md)

Wire behavior mirrors kernel nvmet-rdma (`drivers/nvme/target/rdma.c`).
Errors produce typed NVMe status CQEs or CM rejects, not panics or silent
closes — except the paths the "Known v1 divergences" below call out, which
surface as a torn-down queue.

## Wire protocol

Mirrors kernel `drivers/nvme/target/rdma.c`. The host SENDs a command
capsule (SQE + keyed SGL `{addr, rkey, len}` naming its registered
buffer — the 16-byte keyed descriptor in the SQE `dptr` at offset 24:
`addr@0`, 24-bit `len@8`, `rkey@11`; `parse_keyed_sgl`); the target
moves data one-sided and SENDs the CQE back. No R2T, no C2HData PDUs,
no digests.

```text
 WRITE (host → target)               READ (target → host)

 host ──SEND capsule──► target       host ──SEND capsule──► target
         claim tag, lease pool buf           claim tag, dispatch,
         RDMA READ host buf → slot           backend fills slot
         READ completion → submit            RDMA WRITE slot → host
         dispatch, backend                   │
 host ◄──SEND CQE────── target       host ◄──SEND CQE (QP-ordered
                                             after the WRITE)
```

Write-data placement forks on size:

- **≤ 4 KiB — inline in the capsule.** IOCCSZ advertises
  `RDMA_INLINE_DATA_SIZE` (4 KiB) and SGLS sets the address-as-offset bit
  (SAOS — what actually gates the host's `use_inline_data`). One ~100 ns
  copy capsule → pool lease, no RDMA READ round-trip; the capsule's RECV
  re-post is deferred until the copy (ring-safe: no response yet, so the
  host can't reuse that SQ slot). This closed the last single-flow
  4k-randwrite deficit vs nvmet.
- **Larger — one-sided RDMA READ.** The target posts an RDMA READ into
  the slot's pool-registered segments *without submitting the slot*;
  submission defers to the READ completion (`submit_pending`), which wakes
  the slot task against the now-filled slot.

Malformed SGLs (bad descriptor, out-of-bounds offset, zero length) fail
without dispatch via a queued error CQE (`SGL_INVALID_TYPE` /
`DATA_SGL_LEN_INVALID`).

## Bindings: sideway (verbs) + rdma-mummy-sys (CM)

**[`sideway`](https://github.com/RDMA-Rust/sideway)** (pinned `=0.4.3`)
covers verbs: device / PD / MR / CQ + completion channel / RC QP. It
links system rdma-core via pkg-config with a current bindgen — the
`*-sys` alternatives don't build on the dev boxes (vendored cmake needs
libnl dev packages; `rdma-sys` pins a bindgen too old for modern
headers).

sideway's **CM** API can't do what an NVMe target needs — read
CONNECT_REQUEST private data (`nvme_rdma_cm_req` carrying the qid),
reply with private data, `reject` — so `cm.rs` drives librdmacm
directly over `rdma-mummy-sys` (sideway's own FFI backend; types
unify), mirroring sideway's naming for a mechanical future swap back
once upstream grows `Event::private_data()`,
`ConnectionParameter::setup_private_data`, `Identifier::reject`,
`setup_send_with_inv`, and pre-established `DeviceContext` access.
Three seams:

- `Identifier::get_device_context()` — layout-asserted transmute to a
  sideway `DeviceContext` (why the `=0.4.3` pin).
- `get_qp_attr` wraps `rdma_init_qp_attr`; attrs applied via raw
  `ibv_modify_qp` through sideway's `qp()` accessor.
- `SEND_WITH_INV` — one raw extended-verbs pair
  (`wr.rs::wr_send_with_inv`); used when the host's SGL requests
  remote invalidation (`SGL_FMT_INVALIDATE`).

## Threading: CM thread → control thread → queue thread

The binary runs on the shared harness pool via
`ioutgt_harness::spawn::<RdmaTransport>` — multi-core queues, control
socket, `ConnPermit`, idle-teardown, same as TCP. Where TCP hands a socket
straight from the control thread to a queue thread, RDMA inserts a third
thread first. The reason: the RDMA-CM event channel is awaited by parking
its fd on an io_uring `POLL_ADD` (`CmChannel::next_event`), but the harness
control loop is plain Tokio with no reactor. So `bind` spawns a dedicated
**CM reactor thread**, and a connection crosses all three — one job each:

- **CM thread** (`cm_thread_main`, own reactor) — owns the one CM event
  channel (it multiplexes every cm_id) and runs `RdmaListener::accept`:
  validate + adopt (or typed-`rdma_reject`) each `CONNECT_REQUEST`, ack
  `ESTABLISHED`, and on `DISCONNECTED` send the DREP + fire the queue's
  `stop` + prune. Emits one `RdmaRaw` per good connect; never touches a QP.
- **Control thread** (harness plain-Tokio loop, shared with TCP) — bridges
  CM → queue: `accept` drains the CM→control mpsc, `handshake` wraps the
  `RdmaRaw` as an `RdmaConn` (no wire I/O — the fabrics Connect arrives later
  over the QP), and the harness routes by qid, exactly as TCP: **qid 0 →
  admin thread, qid n → io thread `(n-1) % N`**.
- **Queue thread** (pinned; owns this queue's io_uring) — `run_conn` does
  everything verbs-bound: build the QP on the cm_id's device, drive
  `INIT → RTR → RTS`, prime RECVs + arm the CQ *before* `rdma_accept`, then
  run the reap loop + slot tasks (the "Queue pipeline" section below).

```text
  CM thread — cm_thread_main, own io_uring reactor
  │ RdmaListener::accept   (one CM channel multiplexes all cm_ids)
  │   CONNECT_REQUEST → parse CmReq; bad recfmt/len → typed
  │                     rdma_reject; else adopt the child cm_id
  │   ESTABLISHED     → ack
  │   DISCONNECTED    → DREP, fire the conn's stop, prune conns
  ▼   RdmaRaw { id (cm_id), qid, hsqsize, stop } ─ bounded mpsc (≤256) ─►

  control thread — harness plain-Tokio loop (shared with TCP)
  │ accept()     drain the mpsc → one RdmaRaw
  │ handshake()  wrap as RdmaConn   (no wire I/O; Connect comes later)
  │ route by qid    qid 0 → admin thread,   qid n → io thread (n-1) % N
  ▼   RdmaConn (Send) ──────────────────────────────────────────────►

  queue thread — pinned; its io_uring reaps this queue's completions
  │ run_conn:
  │   build PD / comp-channel / CQ / QP on the cm_id's device
  │   INIT → RTR → RTS         (CM-derived QP attributes)
  │   prime RECVs + arm CQ     ── BEFORE rdma_accept, so the host's
  │                               first capsule is never lost
  │   rdma_accept              (reply CmRep{crqsize}; initiator_depth arg)
  │   run(): reap loop + per-tag slot tasks
```

The cm_id makes both hops: created and pumped for lifecycle events on the
CM thread, but its QP is built and `rdma_accept`ed on the queue thread. It
travels as the `Send` `RdmaRaw`/`RdmaConn` (`Identifier: Send + Sync`;
librdmacm cm_id operations are thread-safe).

Accept-time QP details: admin depth clamped to 32; the reply's
`initiator_depth` = the QP's real `max_rd_atomic` (a hardcoded value
here once caused reconnect storms);  CQs are spread across completion
vectors (admin on 0, IO by qid; rxe falls back to 0).

## Queue pipeline (`target.rs`)

`RdmaQueue` joins `QueueCore<Sqe>` with the QP and four MRs:

| MR | Covers | Used by |
|----|--------|---------|
| pool arena | data pool (also an io_uring fixed buffer) | RDMA READ target, RDMA WRITE source |
| recv bufs | per-slot command-capsule buffers | RECV |
| resp bufs | per-slot CQE staging | SEND |
| cdata buf | admin Connect-data landing zone | RDMA READ (keyed-SGL ConnectData) |

(Connect carries a 1024-byte `ConnectData`: IO queues send it
in-capsule; the admin host sends it host-resident via keyed SGL, RDMA
READ into the cdata buffer before controller bootstrap.)

Work-request ids encode the WR class + owner, the same trick as TCP's
TTAG = slot index (no lookup maps):

```text
 wr_id = kind << 40 | low32      RECV         → recv-buffer index
                                 SEND / WRITE → tag
                                 READ         → tag  (request WR — not
                                                counted in inflight[])
```

The **reap loop** is the sole owner of the QP, `inflight[]`, and the
recv/response buffers; slot tasks only hold `Rc<QueueCore>` + the
`SendList` and post nothing. Commands execute on preallocated per-tag
tasks (as on TCP) — required, not a perf choice: a parked Async Event
Request would stall inline dispatch, but only idles its own slot task.
A per-tag `JoinSet` aborts every slot task when it drops at `run()`
exit.

```text
 run() — select! over five arms:
   comp-channel readiness (multishot POLL_ADD) ─► poll CQ, process CQEs
   park-probe staged CQEs (--poll spin)        ─► same drain
   SendList.next()                             ─► post responses
   stop.notified()                             ─► teardown
   backstop timer (1 s)                        ─► keep-alive watchdog
                                                  every 2nd tick ≈ 2 s
```

Doorbells are batched: all RDMA READs from one CQ drain go out on one
`ibv_post_send`, and all drained responses (WRITE + SEND pairs) on
another. Per-class WR counts and log2 batch-size histograms are
exported in `GET_STATS` under `"wr"`.

A tag is released only when **both** signaled response WRs (WRITE and
SEND; SEND alone for data-less commands) have completed —
`inflight[tag]` — i.e. when the NIC provably no longer references slot
memory (transport-contract obligation 5). The response SEND carries
the Solicited flag (host solicited-only CQs take an interrupt) and
becomes SEND_WITH_INV when the host requested invalidation.

## Backpressure: park, never drop

Two transient-full conditions defer a command instead of failing it,
mirroring nvmet's `rsp_wr_wait_list` / SPDK's pending queues (see
`rdma-flow-control-nvmet-vs-spdk.md`):

- **`parked` — all slot tags held.** The response SEND frees the
  host's SQ slot before our own SEND completion releases the tag, so a
  conforming host at full depth can deliver command N+1 while every
  tag is busy. Capsules park and drain oldest-first as tags free;
  exceeding the negotiated depth outright stays fatal.
- **`pool_wait` — pool lease unavailable.** A write's lease must come
  from the registered arena (it is the RDMA READ's local target; heap
  would be unregistered) and the pool is deliberately smaller than
  depth × MDTS. The command parks (tag already claimed) and retries
  front-only as completions release leases. The old
  fail-with-`DATA_XFER_ERROR|DNR` turned full-depth write bursts into
  host EIOs.

## Teardown and host loss

Our QP is built manually and bound via `rdma_accept`'s `qp_num`, so it
is *not* cm_id-associated: `rdma_disconnect` doesn't flush it and a
host disconnect produces no flushed completions. Teardown signals:

- **Graceful (DREQ)**: the CM thread's Disconnected arm sends the DREP,
  prunes the connection's `ConnSlot` from `conns` (bounded across
  reconnect churn — soak-tested), and fires the connection's
  `stop: Arc<Notify>`. The reap loop ends on `stop`, resolves parked
  AERs (`ctx.close()`), and drains in-flight dispatches
  (`executing() > 0`, bounded) before freeing — a backend op can't
  target the pool arena as it's freed. The stop is a bare `Notify`
  (not the mailbox doorbell), so a fully idle queue may only observe
  it at the ~1 s park backstop; routing it through the mailbox would
  make teardown prompt (the harness mailbox-only invariant covers the
  data path, and this is a second cross-thread wake channel).
- **Abrupt (host vanishes, no DREQ)**: nothing on the QP or CM
  notices, so the backstop's keep-alive watchdog (every 2nd tick,
  ≈ 2 s) covers it: an admin queue silent past KATO×2 + one keep-alive
  tick tears down and removes its controller from the registry; IO
  queues whose controller has left the registry follow — a dead host's QPs, permits
  and slots all recycle. "Silent" here means the admin queue alone: this
  transport does not publish IO-queue traffic to the watchdog, so (unlike
  TCP) it does not advertise `CTRATT.TBKAS` and its hosts keep sending
  Keep Alive commands.

`RdmaQueue::drop` drains the comp channel before destroying the CQ
(`ibv_destroy_cq` hangs on unacked events); field order in the struct
is the verbs destruction order and is load-bearing.

## Poll mode (`--poll`)

Opt-in adaptive busy-poll: while a queue has commands in flight (plus
a 200 µs grace), the reactor's park hook spins — draining the CQ via
the park-probe each pass and entering the kernel with `GETEVENTS` so
`DEFER_TASKRUN` completions (backend IO, mailbox) keep flowing —
instead of sleeping on comp-channel events. An idle queue stops
burning its core within the grace; the next comp event resumes the
spin. The admin queue never spins. Measured (single job 4k, SSD): qd1
randread 52.6 → 43.1 µs, randwrite 39.5 → 29.4 µs; qd128 unchanged.
A spinning io-thread owns its core — don't share it with benchmark
clients.

## CLI

Mirrors the TCP binary (`--config`, `--listen`, `--subsys-nqn`,
`--backend`, `--mem-size-mb`, `--io-queue-size`, `--queue-buf-mb`,
`--io-threads`, `--no-pin`, `--control-socket`,
`--idle-teardown-secs`), plus `--poll`; TCP-only knobs (digests,
`--send-zc`, `--recv-buf-mb`) are absent. The `ctl`/`list`/`stat`
subcommands are shared through `ioutgt_harness::client`.

## Known v1 divergences (deferred)

- `conns` is pruned only on a graceful Disconnected; an abrupt host
  loss leaves the listener-side `ConnSlot` (the queue itself tears
  down via the watchdog). A periodic weak-ref sweep would bound it.
- `staged_len` clamps to `min(keyed SGL len, MDTS)` but never
  cross-checks the command's NLB-derived length; an undersized host
  SGL surfaces as an RDMA remote-access-error completion (→ queue
  teardown), not a clean NVMe `DATA_XFER_ERROR`. RDMA protection
  prevents any local memory-safety issue.
- Over `MAX_CONNECTIONS` the harness drops the connection without
  `rdma_reject`, so the host times out instead of a clean CM reject.
  (Malformed connects *do* get a typed reject.)
- `bind` reports the configured listen address verbatim — `--listen
  …:0` would misadvertise port 0.

## Testing

| Gate | Script | What it proves |
|------|--------|----------------|
| verbs loopback | `testing/run_rdma_loopback.sh` | the verbs layer alone, rxe loopback in the VM |
| bring-up | `testing/run_rdma_connect.sh` | rxe + kernel host: discover → connect → write/read-back `cmp` → fio verify → disconnect |
| A/B correctness | `testing/run_rdma_compare.sh` | same driver against ioutgt **and** nvmet-rdma (loop bdev backends; guest tmpfs can't host either file backend) |
| box perf | `testing/two_nic/realwire_rdma.sh` | two real mlx5 NICs, forced wire, fio/fio_perf vs nvmet-rdma |

VM gotcha: the guests re-add the netdev IP after `rdma link add` to
force the rxe RoCEv2 GID to populate, which otherwise races bind on
some boots.

`testing/common/common.sh` selects the fabric via `TRANSPORT=tcp|rdma`
(binary, kernel modules, `addr_trtype`, `nvme -t`; forces digests +
zero-copy send off for rdma). The `fio_verify` verb is the
data-integrity gate (mixed 4k–128k writes at pool-exhausting pressure
+ crc32c read-back); `ibperf` is the raw link baseline.

Box gotchas baked into the two-NIC driver:

- nvmet-rdma's CM listener is hardcoded to `init_net`, so the target
  stays in the root netns and only the initiator NIC is isolated
  (`rdma system set netns exclusive` — which needs a quiesced host:
  any other netns, e.g. systemd `PrivateNetwork`, makes it `EBUSY`).
- A freshly-added RoCEv2 GID reaches the rdma_cm cache only after a
  netdev **carrier** event; the driver link-flaps and re-adds the IP,
  else `rdma_bind_addr` returns `EADDRNOTAVAIL` (identical for ioutgt,
  nvmet-rdma, and `rping`).
- Host network management can destroy the fabric mid-run: the
  historical "64k congestion wedge" was NetworkManager's DHCP loop
  flushing the test IP/GID — see `rdma-64k-congestion-wedge.md`.

