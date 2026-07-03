# ioutgt

A high-performance userspace storage target framework built on io_uring,
written in Rust. It speaks **NVMe/TCP** and **NVMe/RDMA** today; the
architecture is transport-independent and designed to grow NBD and iSCSI
behind the same core.

## Motivation

### io_uring keeps going

io_uring is where Linux IO development happens: multishot operations,
`DEFER_TASKRUN`, provided-buffer rings, registered files and buffers,
zero-copy send and receive — new capabilities land every kernel cycle.
A userspace target built directly on io_uring picks these up as they
ship, with one wait primitive (`io_uring_enter`) driving sockets, disks
and timers alike.

### Why Rust

A storage target runs for months holding other people's data, which
makes the classic tradeoff painful: C is fast but every buffer lifetime
is on you; garbage-collected languages are safe but pause. Rust removes
the tradeoff — the invariants this design depends on (buffers outliving
DMA, no allocation on the IO path, connection state never crossing
threads) are enforced by the compiler, at zero runtime cost, while
async/await keeps a fully pipelined state machine readable.

### Why userspace (compared with kernel nvmet)

The kernel target is excellent, but living in the kernel has costs:
features arrive with kernel releases, a bug can take the machine down,
and profiling or patching means kernel work. A userspace target deploys
like any other binary — upgrade with a restart, crash in isolation,
profile with `perf` on a normal process, tune per deployment (CPU
pinning, adaptive polling). And it does not cost performance: on our
test box, ioutgt matches or beats kernel nvmet on every single-job
`fio_perf` phase, on both transports — up to 2× on NVMe/TCP (numbers
below).

## Design highlights

- **One thread per NVMe queue**, each with its own io_uring instance
  (`SINGLE_ISSUER | DEFER_TASKRUN`) and its own Tokio current-thread
  runtime — no work stealing, no cross-queue scheduling, no shared locks
  on the data path.
- **Bounded concurrency as a first-class primitive**: NVMe queue depth and
  command IDs bound all in-flight state, so every command slot, buffer, and
  async task is preallocated at queue creation. Steady state performs zero
  allocations — SPDK's request-tracker model with async/await readability.
- **Sans-io protocol core**: the NVMe/TCP PDU codec operates on byte slices
  only, shared by the target, the test client, and the fuzzer.
- **Backends** (null, memory, file, block device) implement one async trait
  and have no protocol awareness, mirroring the Linux kernel nvmet split.

## Performance

One process, one queue thread per NVMe queue, no locks in the data path:

```text
   host (nvme-cli / fio)                    ioutgt target
   ┌───────────────┐    NVMe/TCP or      ┌──────────────────────────┐
   │ kernel nvme   │    NVMe/RDMA        │ queue thread (pinned)    │
   │ host driver   │ ◄════ wire ═══════► │  transport ⇄ slot engine │
   └───────────────┘                     │  ⇄ backend (io_uring)    │
                                         └────────────┬─────────────┘
                                                      ▼
                                              NVMe SSD (O_DIRECT)
```

Measured with `fio_perf` (single job, qd 128, 15 s/phase, real NVMe SSD
backends, same host kernel driver for both targets; collected via
`taskset -c 45 rdma2.sh fio_perf` / `taskset -c 45 nic2.sh fio_perf`):

**NVMe/RDMA** (100 GbE mlx5, RoCEv2)

| phase | ioutgt | kernel nvmet | delta |
|-------|--------|--------------|-------|
| 4k randread | 165.2k IOPS | 160.9k IOPS | +2.7% |
| 4k randwrite | 176.8k IOPS | 179.1k IOPS | −1.3% |
| 64k randread | 5948 MiB/s | 4297 MiB/s | **+38%** |
| 64k randwrite | 5803 MiB/s | 4044 MiB/s | **+44%** |

**NVMe/TCP** (same wire)

| phase | ioutgt | kernel nvmet | delta |
|-------|--------|--------------|-------|
| 4k randread | 242.9k IOPS | 115.0k IOPS | **+111%** |
| 4k randwrite | 242.4k IOPS | 116.8k IOPS | **+108%** |
| 64k randread | 3289 MiB/s | 1792 MiB/s | **+84%** |
| 64k randwrite | 2124 MiB/s | 1072 MiB/s | **+98%** |

## Roadmap

- Receive zero-copy for NVMe/TCP (io_uring `RECV_ZC`).
- Trace and close the remaining single-flow 4k gap between our RDMA and
  TCP transports — the evidence points at the host-side driver (kernel
  `nvme-rdma` submits per-command with no `queue_rqs`/`commit_rqs`
  batching, unlike `nvme-tcp`/`nvme-pci`).
- In-band authentication (NVMe DH-HMAC-CHAP).
- TLS for NVMe/TCP.
- Cleanup and code simplification passes.
- Improve `--poll` (io_uring `IOPOLL` for the backend once the uverbs
  event fd grows `read_iter`; hybrid polling).
- More targets behind the same core: NBD, iSCSI.

## Workspace layout

| Crate | Role |
|-------|------|
| `ioutgt-uring` | per-thread io_uring reactor + op futures, Tokio park integration |
| `ioutgt-nvme` | sans-io NVMe spec types, NVMe/TCP PDU codec, CRC32C digests |
| `ioutgt-core` | subsystems, controllers, namespaces, queues, dispatch, the slot engine |
| `ioutgt-stream` | protocol-neutral stream send/recv harness (`StreamSender`/`StreamReader`) |
| `ioutgt-nvme-tcp` | NVMe/TCP transport state machines |
| `ioutgt-nvme-rdma` | NVMe/RDMA transport + binary (verbs, CM, adaptive `--poll`) |
| `ioutgt-backend` | null / memory / file / block backends |
| `ioutgt-control` | UDS JSON control plane + config schema |
| `ioutgt-harness` | shared binary harness: spawn, queue-thread pool, control server, `stat` client |
| `ioutgt-cpus` | userspace `group_cpus_evenly()` for topology-aware pinning |
| `ioutgt` | the NVMe/TCP target binary |

## Documentation

- [`docs/usage.md`](docs/usage.md) — command line, config file, control
  API, host connection, test harnesses.
- [`docs/architecture.md`](docs/architecture.md) — the architecture
  specification (thread model, reactor, command-slot lifecycle, PDU flows).
- [`docs/nvme-rdma.md`](docs/nvme-rdma.md) — the NVMe/RDMA transport:
  wire protocol, CM, queue pipeline, poll mode.
- [`docs/nvmet-comparison.md`](docs/nvmet-comparison.md) — subsystem-by-
  subsystem comparison with the Linux kernel NVMe target.
- [`docs/perf-notes.md`](docs/perf-notes.md) — measured optimization log.
- [`docs/roadmap.md`](docs/roadmap.md) — what's next (RDMA/NBD/iSCSI,
  remaining perf work, deferred nvmet benchmark).
- [`docs/benchmark-plan.md`](docs/benchmark-plan.md) — benchmark methodology
  vs kernel nvmet (execution deferred).

## Requirements

- Linux ≥ 6.11 (`DEFER_TASKRUN` + multishot era; developed on 6.19)
- Rust ≥ 1.88 stable

## Status

Early development. Milestones and progress are tracked in
`docs/architecture.md`; interoperability is validated continuously
against the Linux kernel NVMe/TCP and NVMe/RDMA host drivers (VM gates
over loopback/rxe, plus data-integrity and performance gates on real
100 GbE RDMA hardware).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or
  <http://opensource.org/licenses/MIT>)

at your option.

**Exception**: the `ioutgt-cpus` crate is licensed **GPL-2.0-only** — it
is a derivative of the Linux kernel's `lib/group_cpus.c` (see
[`crates/ioutgt-cpus/LICENSE`](crates/ioutgt-cpus/LICENSE)). It is used
only by the `ioutgt` binary, so the binary as distributed is governed by
GPL-2.0 (the other crates contribute under their MIT option); every
library crate other than `ioutgt-cpus` remains dual-licensed as above.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
