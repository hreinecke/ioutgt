# Roadmap

What exists today: a hardened, interop-verified NVMe/TCP target —
discover/connect/IO against stock Linux hosts, R2T writes, digests,
runtime namespace management with AENs, O_DIRECT file/bdev backend,
ASAN-clean, failure-injection tested, 506K 4K IOPS on loopback with
two IO threads. This file orders what comes next.

## 1. Performance (continuation of M9; each step measured in isolation)

- **Multishot recv + provided buffer rings** (`IOU_PBUF_RING`,
  `ENOBUFS` → single-shot fallback): removes the recv re-arm SQE and
  the owned-buffer round trip per batch. The `RecvSource` seam in the
  transport was designed for exactly this swap.
- **Registered (fixed) slot buffers + `READ_FIXED`/`WRITE_FIXED`** for
  the O_DIRECT backend: removes per-op page pinning; evaluate by
  CPU-per-IOP on ext4, not loopback IOPS.
- **`SEND_ZC` with notification-gated buffer reuse**: eliminates the
  staging memcpy on reads. Loopback falls back to copying
  (REPORT_USAGE confirms), so honest evaluation needs a real NIC.
- **`RECV_ZC` (zcrx)**: requires NIC header-data split + flow
  steering; revisit when 100G hardware is on the bench.
- Recv/send **budget tuning** (configurable, swept), per-queue
  **stats counters** surfaced through `GET_STATS`, optional second
  **IOPOLL ring** per thread for disk ops, `OpEntry` slab/waker
  micro-costs.

## 2. Benchmark vs kernel nvmet (deferred by request)

Everything needed is in place: `docs/benchmark-plan.md` (methodology),
`bench`-ready loadgen, the VM harness, and
`testing/capture-nvmet-fixtures.sh` shows the configfs setup that
`bench/setup-nvmet.sh` will reuse. Needs root (nvmet configfs, loop
devices, real NIC runs) and quiet-machine time. Deliverable:
`docs/benchmark-report.md` with IOPS/BW/p99/CPU-per-IOP and
flamegraphs for both targets, plus the loop-device validation of the
bdev backend path.

## 3. New transports (the framework bet)

The split that makes this tractable: `ioutgt-nvme` is sans-io,
`ioutgt-core` owns slots/dispatch/controllers, and a transport
supplies (a) connection setup that yields a routed queue, (b) a recv
path that fills slot buffers and submits SQEs, (c) a send path that
drains `SendWork`. NVMe/TCP's `connection.rs` is the template.

- **NVMe/RDMA** (`ioutgt-rdma`): same fabrics/core unchanged; the
  transport maps `SendWork::Response`/data to RDMA SEND/WRITE and
  R2T-equivalents to RDMA READ. Queue-thread model survives intact
  (one CQ per thread polled from the reactor — investigate io_uring
  attached verbs vs libibverbs polling integration; the mailbox/park
  design accommodates an extra fd-pollable CQ channel).
- **NBD** (`ioutgt-nbd`): much simpler wire protocol; maps onto slots
  with synthetic tags (NBD handles are 64-bit cookies — store the
  cookie in the slot like the CID). Good second transport to prove
  the abstraction because it is *not* NVMe-shaped.
- **iSCSI** (`ioutgt-iscsi`): largest protocol surface (login,
  task management, R2T-like data-out); slot model maps via ITT.
  Last in line.

## 4. Protocol/robustness backlog

- Gentler error responses where nvmet degrades per-command instead of
  terminating: DDGST mismatch → `DATA_XFER_ERROR` on the command;
  queue-depth overrun handling.
- RAE semantics on log pages; real SMART/error-log content; discovery
  genctr maintenance; Get Log Page offset support beyond discovery.
- Host ACLs (per-subsystem allowed-host lists) in config + control
  API; multiple ports; TLS (kTLS or userspace) for NVMe/TCP secure
  channels.
- bdev discard/write-zeroes via `IORING_OP_URING_CMD`
  (BLKDISCARD-equivalent) once a root test rig exists; NVMe
  passthrough backend over uring-cmd to a host controller.
- Metadata/PI formats, Write Protect, reservations — driven by
  demand.

## 5. Operational

- Per-queue/throughput counters in `GET_STATS`; optional Prometheus
  text endpoint on the control socket.
- Graceful shutdown command (drain + SHST_COMPLETE on all
  controllers) instead of process kill.
- Packaging: systemd unit, config reload via control socket.
- libFuzzer targets built on the existing seeded-fuzz drivers; CI
  recipe (fmt, clippy, test, ASAN job, VM suite as a manual gate).
