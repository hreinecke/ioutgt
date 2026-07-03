# ioutgt

A high-performance userspace storage target framework built on io_uring,
written in Rust. It speaks **NVMe/TCP** and **NVMe/RDMA** today; the
architecture is transport-independent and designed to grow NBD and iSCSI
behind the same core.

## io_uring keeps going

I/O Batch Processing

Continuous performance optimization

New features are constantly emerging (multishot, net recv zero copy,
iopoll, io-uring slots in future, dmabuf read/write in future, ...)

## Rust

Memory safe modern programming language

Async/.await

## userspace (compared with kernel nvme target)

Easy to develop and maintain

Crash in isolation

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

| phase | ioutgt IOPS | ioutgt BW | nvmet IOPS | nvmet BW | ioutgt vs nvmet |
|-------|------------|-----------|------------|----------|-----------------|
| 4k randread | 165.2k | 645 MiB/s | 160.9k | 629 MiB/s | +2.7% |
| 4k randwrite | 176.8k | 691 MiB/s | 179.1k | 700 MiB/s | −1.3% |
| 64k randread | 95.2k | 5948 MiB/s | 68.7k | 4297 MiB/s | **+38.6%** |
| 64k randwrite | 92.9k | 5803 MiB/s | 64.7k | 4044 MiB/s | **+43.6%** |

**NVMe/TCP** (same wire)

| phase | ioutgt IOPS | ioutgt BW | nvmet IOPS | nvmet BW | ioutgt vs nvmet |
|-------|------------|-----------|------------|----------|-----------------|
| 4k randread | 242.9k | 949 MiB/s | 115.0k | 449 MiB/s | **+111.2%** |
| 4k randwrite | 242.4k | 947 MiB/s | 116.8k | 456 MiB/s | **+107.5%** |
| 64k randread | 52.6k | 3289 MiB/s | 28.7k | 1792 MiB/s | **+83.3%** |
| 64k randwrite | 34.0k | 2124 MiB/s | 17.2k | 1072 MiB/s | **+97.7%** |

For scale: the backing SSD does 122k IOPS (7.6 GiB/s) at 64k random
locally, and the raw wire carries 98 Gb/s (`ibperf`) — the single-job
64k numbers are one queue thread driving ~80% of the drive's 64k
ceiling through one QP.

## Roadmap

- Receive zero-copy for NVMe/TCP (io_uring `RECV_ZC`).
- Trace and close the remaining single-flow 4k gap between our RDMA and
  TCP transports — the evidence points at the host-side driver (kernel
  `nvme-rdma` submits per-command with no `queue_rqs`/`commit_rqs`
  batching, unlike `nvme-tcp`/`nvme-pci`).
- In-band authentication.
- Cleanup and code simplification passes.
- Performance optimization.
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
| `ioutgt-cpus` | locality-aware even CPU grouping for topology-aware pinning |
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

### Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
