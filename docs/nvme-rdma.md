# NVMe/RDMA transport (`ioutgt-nvme-rdma`)

Status: **work in progress** (the crate is currently lib-only). Sibling fabric to
`ioutgt-nvme-tcp`, to ship as a standalone binary `ioutgt-nvme-rdma`. v1 target: discovery + connect + read/write
over RC queue pairs, with the host's keyed SGL driving target-issued RDMA READ
(write data) / RDMA WRITE (read data). Reuses the transport-neutral harness
(`ioutgt-harness`) and the NVMe model / slot engine / backend (`ioutgt-core`,
`ioutgt-nvme`) unchanged; only the RDMA-specific pieces are new (memory
registration, completion-queue draining via the reactor, RDMA-CM connection
acceptance, the recv/send loops).

## Wire protocol (read & write)

Mirrors kernel `drivers/nvme/target/rdma.c`. The host SENDs a command capsule
(NVMe SQE + a keyed SGL: `{addr, rkey, length}` naming the host's registered
buffer); the target moves the data one-sided and SENDs a response capsule (CQE):

- **WRITE**: target RECVs the command, leases a pool buffer, posts an **RDMA
  READ** to pull the write data from host memory into the slot's segments, then
  runs the command and SENDs the CQE. No R2T / H2CData round-trips.
- **READ**: target RECVs the command, runs it, posts an **RDMA WRITE** to push
  the read data into the host buffer, then SENDs the CQE (ordered after the
  WRITE). No C2HData PDUs.

See <https://ming1.github.io/storage/linux-nvme-target-explained#156-nvme-rdma-wire-protocol-for-read--write>.

## RDMA bindings: `sideway`

We use **[`sideway`](https://github.com/RDMA-Rust/sideway)** (safe ibverbs +
RDMA-CM bindings). It covers everything the target needs — device / PD / MR
(lkey+rkey) / CQ + completion channel / RC QP / RDMA-CM — and the completion
channel exposes a `RawFd` (`AsRawFd`), which is exactly what we register in the
per-thread io_uring via `IORING_OP_POLL_ADD` for event-driven CQ draining.

The two `*-sys` crates the original plan named do **not** build on a modern
rdma-core dev box:

- `ibverbs-sys` (jonhoo) **vendors** rdma-core and builds it with cmake, which
  needs `libnl-3.0` / `libnl-route-3.0` **dev** packages (not just the runtime
  libs) — unavailable without root on the dev box.
- `rdma-sys` (datenlord) links the system libs but pins **bindgen 0.59**, which
  cannot parse the modern `infiniband/ib_user_ioctl_verbs.h` (anonymous-union
  ident error).

`sideway` links the installed system rdma-core via `pkg-config` with a current
bindgen, so it needs only the already-present `libibverbs` / `librdmacm` dev
headers.

## Reactor integration

Event-driven (the project's one-io_uring-per-thread model): the ibverbs
completion-channel fd and the RDMA-CM event-channel fd are registered in the
queue thread's io_uring with `IORING_OP_POLL_ADD`; a readiness CQE wakes the
thread, which drains `ibv_poll_cq` / processes CM events. Busy-poll is a
deferred opt-in.

## Threading & the harness Transport

The binary runs on the shared `ioutgt-harness` queue-thread pool (admin thread +
N IO threads, CPU-pinned) via `ioutgt_harness::spawn::<RdmaTransport>` — the same
seam the TCP binary uses, bringing multi-core queues, the control socket
(`ctl`/`list`/`stat`), `ConnPermit`/`MAX_CONNECTIONS`, and idle-teardown.

The one structural mismatch: the CM event channel parks on its fd via io_uring
`POLL_ADD`, which needs an io_uring reactor, but the harness control thread is
plain tokio. So `RdmaTransport::bind` spawns a **dedicated CM reactor thread**
(`cm_thread_main`, its own `QueueRuntime`) that runs the `RdmaListener` and
forwards each accepted connection — an `RdmaRaw` (cm_id + qid + hsqsize), all
`Send` — over a bounded tokio mpsc channel to the control thread. `accept` drains
that channel; `handshake` packages `RdmaRaw` + `ConnPermit` into an `RdmaConn`;
`run_queue` runs `run_conn` on the routed (by qid) **queue thread**, which is
where all reactor-bound work happens: build the PD/comp-channel/CQ/QP on the
**cm_id's own device context**, drive it INIT→RTR→RTS from the CM-derived attrs,
prime the RECVs and arm the CQ *before* `rdma_accept` (so the host's first
capsule is never dropped), then run the queue. So one cm_id is created + has its
lifecycle events (Established/Disconnected) pumped on the CM thread, but builds
its QP + `rdma_accept`s on a queue thread — sideway declares `Identifier: Send +
Sync` and librdmacm's cm_id ops are thread-safe. The cm_id is held for the
process lifetime (clean per-queue teardown is deferred).

`serve()` remains a single-threaded path (CM listener + every queue on one
io_uring reactor) — a simpler fallback that bypasses the harness.

## Queue pipeline (`target.rs`)

`RdmaQueue` joins `QueueCore<Sqe>` with the connection's RDMA resources and three
registered buffers: the data-pool arena (the local source for read-data RDMA
WRITEs, `pool_lkey`), the per-slot RECV capsule buffers, and a per-slot CQE
staging buffer. The pipeline per command:

1. RECV a command capsule → parse the `Sqe` (Connect also carries a 1024-byte
   in-capsule `ConnectData`) and immediately re-arm that RECV buffer.
2. `claim_tag` → `submit` → `await_command` → `dispatch::execute` → `Outcome`.
3. If `data_len > 0` (a read), RDMA WRITE `slot.data().segs()` to the host's
   keyed SGL (`parse_keyed_sgl`: `addr@0`, `rkey@11` of the 16-byte descriptor
   in the SQE `dptr` at offset 24).
4. SEND the CQE capsule.
5. Release the slot once **both** outstanding response WRs complete, tracked by
   `inflight[tag]`. Work-request ids encode `kind<<40 | low32` (RECV→recv-buf
   index, SEND/WRITE→tag), reaped centrally in `run()`'s `cq::wait` drain loop.

The first capsule on qid 0 bootstraps a controller (`ConnCtx::new_admin`); qid n
attaches by cntlid (`ConnCtx::new_io`). The binary `ioutgt-nvme-rdma` mirrors the
transport-neutral CLI of `ioutgt-nvme-tcp` — `--config` (JSON), `--listen`,
`--subsys-nqn`, `--backend` (memory/null/file), `--mem-size-mb`,
`--io-queue-size`, `--queue-buf-mb`, `--io-threads`, `--no-pin`,
`--control-socket`, `--idle-teardown-secs` — and builds a `TargetConfig` for
`ioutgt_harness::spawn`. TCP-only knobs (digests, `--send-zc`, `--recv-buf-mb`)
are absent; the `ctl`/`list`/`stat` client subcommands are not yet wired into
this binary (the control socket is served, so the TCP binary's clients or a raw
socket work against it).

Commands execute on **preallocated per-tag tasks** (one persistent task per slot,
spawned once at queue install — zero per-command allocation, mirroring the TCP
frontend). Each loops `await_command → dispatch::execute → begin_respond → push
the response onto a preallocated `SendList``. Dispatching off the reap loop is
required, not a perf choice — an Async Event Request (admin opcode 0x0C) is held
open until an async event fires, so awaiting dispatch inline would stall the whole
queue on the parked AER; here a parked AER just leaves its one slot task waiting.
The reap-loop task remains the sole owner of the QP, `inflight[]`, and the
response/recv buffers (slot tasks only hold `Rc<QueueCore>`/`Rc<ConnCtx>`/the
`SendList` and post nothing), keeping tag release and WR posting single-owner and
lock-free. The reap loop `select!`s the CQ against the `SendList`, posting each
finished response (read-data RDMA WRITE, then the CQE SEND). The per-tag
`JoinSet` aborts every slot task when it drops at `run` exit.

**Per-queue teardown.** Our QP is built manually + bound via `rdma_accept`'s
`qp_num`, so it is *not* cm_id-associated — `rdma_disconnect` does not flush it,
and a host disconnect produces no flushed completions on the queue thread. The
prompt teardown signal is the CM **Disconnected** event (on the CM thread): its
arm sends the DREP (`id.disconnect()`), prunes the connection's `ConnSlot` from
`conns` (so `conns` stays bounded across reconnect churn — passes a reconnect
soak), and fires the connection's `stop: Arc<Notify>`. The queue thread's reap
loop `select!`s on `stop` (alongside the CQ and the response queue) and ends on
it; then a teardown block resolves parked AERs (`ctx.close()`) and drains
in-flight dispatches (`while executing() > 0`, bounded) before returning — so a
backend op can't target the pool arena as it's freed.

The stop signal is delivered with a bare `tokio::sync::Notify` from the CM thread
into the queue thread, *not* through the queue thread's io_uring mailbox. It is
never lost (the wake is latched), but on a fully idle queue it is only observed
at the reactor's park backstop (~1s), so teardown can lag up to ~1s. This is a
second cross-thread wake channel (the harness's mailbox-only invariant covers the
data path); routing the stop through the mailbox doorbell would make it prompt.

**Known v1 divergences (deferred):**
- *`conns` is pruned only on a graceful `Disconnected`.* An abrupt host loss that
  surfaces as `TimewaitExit`/`DeviceRemoval`/a CM error leaves the listener's
  `ConnSlot` in `conns` (the *queue* still tears down via its own stop/flush, so
  this is listener-side slot accumulation only). The reconnect soak exercises
  graceful reconnects; a periodic weak-ref sweep would bound the abrupt case.
- *`write_read_data` does not validate `data_len` against the host's keyed-SGL
  length.* An undersized host SGL surfaces as an RDMA remote-access-error
  completion (→ queue teardown) rather than a clean NVMe `DATA_XFER_ERROR`. RDMA
  protection prevents any local memory-safety issue.
- *Over `MAX_CONNECTIONS`, the host times out instead of being rejected.* The
  harness drops the over-limit `RdmaRaw`; for RDMA that means `rdma_accept` is
  simply never called (no `rdma_reject`), so the host times out rather than
  getting a clean CM reject.
- *`bind` reports the configured listen address verbatim* (no ephemeral-port
  resolution), so `--listen …:0` would misadvertise port 0 — fine for the fixed
  RDMA port, unsupported for `:0`.

**Write-data path (host→controller).** `io::write`/`io::dsm` read the write data
straight from the slot, which over RDMA must be RDMA-READ from the host's keyed
SGL *before* dispatch. `handle_recv` detects a host-data-in command
(`host_data_in()`: IO `WRITE`/`DSM`), claims the tag, leases a pool buffer, sets
the slot's received length (`set_data_len`), stashes the SQE in `pending_read`,
and posts an RDMA READ (`WR_READ`) of the host's keyed-SGL buffer into the slot's
pool-registered segments — **without submitting the slot**. Submission is
**deferred** to the `WR_READ` completion (`submit_pending`), which wakes the
slot task to dispatch against the now-filled slot. The READ is a request WR, not
a response, so it is not counted in `inflight[]` — only the trailing CQE SEND
gates slot release. Commands the transport cannot satisfy — a non-keyed
(in-capsule) SGL, a zero-length SGL, or an owned (unregistered) buffer when
`lease_or_owned` falls back under pool pressure — are failed without dispatch via
`respond_receiving` + a queued error CQE (`SGL_INVALID_TYPE` /
`DATA_SGL_LEN_INVALID` / `DATA_XFER_ERROR`), so the host retries rather than the
queue corrupting or tearing down.

## Testing

- **v1 bring-up gate**: `testing/run_rdma_connect.sh` builds the binary and runs
  `testing/vmtest/ioutgt_rdma_connect.sh` in the vmtest guest — soft-RoCE
  (`rdma_rxe`) on the guest NIC, then the in-kernel nvme-rdma host through
  `nvme discover` → `connect` → `id-ctrl` → a namespace **write + read-back
  verify** (`cmp`) → an `fio --verify=crc32c` randwrite pass → `disconnect`.
  (The guest re-adds the netdev IP after `rdma link add` to force the rxe
  RoCEv2 GID to populate, which otherwise races bind on some boots.)
- **A/B correctness gate (VM)**: `testing/run_rdma_compare.sh` builds the
  release binary and runs `testing/vmtest/ioutgt_rdma_compare.sh` in the guest
  — soft-RoCE (rxe), then the SAME `testing/local_tgt.sh` verbs with
  `TRANSPORT=rdma` against BOTH `ioutgt-nvme-rdma` and in-kernel `nvmet-rdma`,
  asserting a clean `fio --verify=crc32c` on each. Backends are loop block
  devices (the guest root is tmpfs, which supports neither `O_DIRECT` nor the
  nvmet file backend). Requires `rdma_rxe`, `nvmet_rdma`, `nvme_rdma`.
- **Shared harness knob**: `testing/common.sh` selects the fabric with
  `TRANSPORT=tcp|rdma` (default tcp): it picks the binary
  (`ioutgt-nvme-$TRANSPORT`), the kernel modules (`nvmet-$TRANSPORT` /
  `nvme-$TRANSPORT`), the port `addr_trtype`, and `nvme -t`, and forces digests
  + zero-copy-send off for rdma. Both `local_tgt.sh` and the two-NIC drivers
  share it.
- **Box perf (two real mlx5 NICs)**: `testing/two_nic_realwire_rdma.sh` —
  `rdma system set netns exclusive`, forces RoCE across the physical link, and
  runs `fio` / `fio_perf` for `ioutgt-nvme-rdma` vs `nvmet-rdma` back to back.
  *Asymmetric topology*: nvmet-rdma's CM listener is hardcoded to `init_net`
  (`rdma_create_id(&init_net,…)` / `inet_pton_with_scope(&init_net,…)` in
  `drivers/nvme/target/rdma.c`), so it can only listen in the root netns —
  unlike nvmet-tcp (`sock_create` in the writer's netns). The driver therefore
  keeps the **target** (NIC_T + IP_T + its rdma device) in root and isolates
  only the **initiator** (NIC_I) in its own netns; the wire is still forced
  because root reaches the initiator IP only out NIC_T → the physical link. On
  the box, confirm RoCE actually egresses the wire (NIC counters during fio)
  rather than HCA-looping.
- Verify a link first with `ibv_devinfo` / `rping` / `ib_send_bw`.
