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
- **Direct-to-slot payload recv** (shrink the recv_loop memcpy —
  what nvmet does after the header): once a PDU header places the
  payload (`RecvPhase::Data`), recv the not-yet-arrived tail of
  large R2T transfers straight into the slot buffer at the
  reassembly offset (`ops::recv_raw` exists for exactly this). The
  already-buffered prefix and in-capsule payloads still copy; full
  elimination would need header-bounded recvs at ≥2 ops/command,
  forfeiting cross-command batching. Costs: one or more bounded
  recvs per H2CData PDU (`MSG_WAITALL` to cap), DDGST loses its
  copy-time accumulation (separate pass over the slot), teardown
  must drain *receiving* slots too (raw-op contract: slot memory
  valid until terminal CQE), and §6's `RecvSource` chunk model gets
  a payload-bypass variant. Irreconcilable with multishot recv +
  provided buffers (kernel picks the buffer there) — a third recv
  strategy, measured by CPU-per-IOP on large writes
  (architecture.md §4.2 documents the copy budget).
- **Registered (fixed) slot buffers + `READ_FIXED`/`WRITE_FIXED`** for
  the O_DIRECT backend: removes per-op page pinning; evaluate by
  CPU-per-IOP on ext4, not loopback IOPS.
- **`SEND_ZC` with notification-gated buffer reuse**: removes the
  kernel user→skb copy, riding the gather send's existing iovecs
  (the userspace staging copy is already gone); slot reuse gates on
  the notification CQE instead of the send CQE. Loopback falls back
  to copying (REPORT_USAGE confirms), so honest evaluation needs a
  real NIC.
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
- RAE semantics on log pages; real SMART/error-log content; Get Log
  Page offset support beyond discovery.
- **Persistent discovery controllers**: discovery genctr maintenance
  (bump on subsystem add/remove instead of the hardcoded 1), the
  DISC_CHANGE AEN fired to connected discovery controllers on topology
  changes (nvmet: `nvmet_port_disc_changed`), and OAES advertising
  DISC_CHANGE instead of NS_ATTR on discovery controllers — the host
  masks its AEC against OAES, so without the bit the notice is never
  enabled (same trap as NS_ATTR on IO controllers). One coherent work
  item; becomes load-bearing once runtime subsystem add or multi-port
  lands.
- **Wildcard-traddr fixup in discovery log entries**: a target bound to
  `0.0.0.0` (the default `--listen`) advertises `0.0.0.0` verbatim as
  traddr, which `nvme connect-all` would try to dial. nvmet substitutes
  the connection's actual local address via the `disc_traddr` transport
  callback (`nvmet_tcp_disc_port_addr`); ioutgt should do the same —
  use the accepted socket's local address when the configured traddr is
  a wildcard. The entry's hardcoded `adrfam = IPv4` should be derived
  from the same address while at it.
- Host ACLs (per-subsystem allowed-host lists) in config + control
  API.
- **Multiple ports**: one process currently serves one listen address
  (`spawn_target()` binds a single listener; nvmet allows N ports with
  subsystems linked into each). The model is already port-shaped —
  `PortConfig` carries traddr/trsvcid plus its own subsystem map — so
  the work is assembly and config: a `ports` array in the JSON schema
  (each with listen address, digest policy, subsystem list), one accept
  loop per port on the control thread routing into the *shared* queue
  threads, per-port discovery log pages, and port identity in
  `GET_STATS`. Queue threads and the registry need no changes;
  controllers are already keyed by (subsystem, host), not by port.
- TLS (kTLS or userspace) for NVMe/TCP secure channels.
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
