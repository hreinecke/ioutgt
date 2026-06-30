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

## Queue pipeline (`target.rs`, focused v1)

One reactor thread owns the CM listener (`serve()`) and every queue. On each
`CONNECT_REQUEST`, `accept_one` parses the host's `CmReq` (qid, hsqsize), builds
a PD/comp-channel/CQ/QP on the **cm_id's own device context**, drives it
INIT→RTR→RTS from the CM-derived attributes, primes the RECVs and arms the CQ
*before* `accept` (so the host's first capsule is never dropped), then spawns
`RdmaQueue::run` on the same thread. The cm_id is held for the process lifetime
(clean per-queue teardown is deferred).

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
attaches by cntlid (`ConnCtx::new_io`). The binary `ioutgt-nvme-rdma` serves one
in-memory subsystem (`--listen/--nqn/--mem-size-mb`).

Commands execute **concurrently**: the reap loop spawns each command onto a
`tokio::task::JoinSet` and posts its response when `join_next()` yields it. This
is required, not a perf choice — an Async Event Request (admin opcode 0x0C) is
held open until an async event fires, so awaiting dispatch inline would stall the
whole queue on the parked AER. The reap-loop task remains the sole owner of the
QP, `inflight[]`, and the response/recv buffers (the spawned tasks only hold
`Rc<QueueCore>`/`Rc<ConnCtx>` and post nothing), which keeps tag release and WR
posting single-owner and lock-free.

**Known v1 divergences (deferred):**
- *Per-command task allocation.* Spawning a JoinSet task per command heap-allocates
  on the IO path, unlike the TCP frontend's preallocated persistent task-per-tag.
  The task-per-tag shape (one looping task per slot) is the intended convergence —
  it solves the parked-AER problem per tag without a per-command spawn — and lands
  with the harness integration (RD6).
- *Teardown drops command tasks mid-dispatch.* When `run()` exits (peer gone),
  the JoinSet aborts in-flight command tasks, which may be parked inside
  `dispatch::execute` with a backend op in flight into the pool arena. This relies
  on the reactor's slab-owns-resources invariant (the orphaned op's slab entry,
  not the future, keeps its buffer alive until the terminal CQE — the
  `drop_stress.rs` guarantee); the memory backend used by v1 issues no such op, so
  it is not yet exercised. Confirm before enabling the file backend over RDMA.
- *`write_read_data` does not validate `data_len` against the host's keyed-SGL
  length.* An undersized host SGL surfaces as an RDMA remote-access-error
  completion (→ queue teardown) rather than a clean NVMe `DATA_XFER_ERROR`. RDMA
  protection prevents any local memory-safety issue.

**Not yet implemented — write-data path (RD4).** `dispatch::execute` reads write
data straight from the slot, which over RDMA must be RDMA-READ from the host's
keyed SGL *before* dispatch (a per-tag deferred-dispatch state machine). Until
that lands, `host_data_in()` fails IO `WRITE`/`DSM` with `DATA_XFER_ERROR | DNR`
rather than dispatch against an unfilled slot. So v1 supports connect, discovery,
Identify, and the IO-queue **read** path only.

## Testing

- **v1 bring-up gate**: `testing/run_rdma_connect.sh` builds the binary and runs
  `testing/vmtest/ioutgt_rdma_connect.sh` in the vmtest guest — soft-RoCE
  (`rdma_rxe`) on the guest NIC, then the in-kernel nvme-rdma host through
  `nvme discover` → `connect` → `id-ctrl` → a namespace **read** → `disconnect`.
  Read-only (the write path is gated, above).
- **Full correctness (RD4+)**: `fio --verify=crc32c` against both
  `ioutgt-nvme-rdma` and in-kernel `nvmet-rdma`. Requires `rdma_rxe`,
  `nvmet_rdma`, `nvme_rdma`.
- **Box perf**: two physical mlx5 NICs in RoCE mode, `fio` / `fio_perf` sweep
  comparing `ioutgt-nvme-rdma` vs `nvmet-rdma`.
- Verify a link first with `ibv_devinfo` / `rping` / `ib_send_bw`.
