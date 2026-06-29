# NVMe/RDMA transport (`ioutgt-nvme-rdma`)

Status: **work in progress.** Sibling fabric to `ioutgt-nvme-tcp`, shipped as a
standalone binary `ioutgt-nvme-rdma`. v1 target: discovery + connect + read/write
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

## Testing

- **VM / local correctness**: soft-RoCE (`rdma_rxe`) over a veth pair in two
  network namespaces, `fio --verify=crc32c` against both `ioutgt-nvme-rdma` and
  in-kernel `nvmet-rdma`. Requires `rdma_rxe`, `nvmet_rdma`, `nvme_rdma`.
- **Box perf**: two physical mlx5 NICs in RoCE mode, `fio` / `fio_perf` sweep
  comparing `ioutgt-nvme-rdma` vs `nvmet-rdma`.
- Verify a link first with `ibv_devinfo` / `rping` / `ib_send_bw`.
