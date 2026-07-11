# ioutgt

A high-performance userspace storage target framework built on io_uring,
written in Rust. It speaks **NVMe/TCP** and **NVMe/RDMA** today; the
architecture is transport-independent and designed to grow NBD & iSCSI,
..., behind the same core.

## io_uring keeps going

I/O Batch Processing

Continuous performance optimization

New features are constantly emerging (multishot, net recv zero copy,
iopoll, io-uring slots in future, dmabuf read/write in future, ...)

## Rust

Memory safe modern programming language

Async/.await with Tokio & io_uring reactor

## userspace (compared with kernel nvme target)

Easy to develop and maintain

Crash in isolation

## Performance

Pass xfstests(./check -g quick) on nvme-tcp & nvme-rdma backed by
ioutgt target.

One process, one queue thread per NVMe queue, no locks in the data path,
single fio JOB with queue depth 128.

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

**NVMe/RDMA** (100 GbE mlx5, RoCEv2)

| phase | ioutgt IOPS | ioutgt BW | nvmet IOPS | nvmet BW | ioutgt vs nvmet |
|-------|------------|-----------|------------|----------|-----------------|
| 4k randread | 254.2k | 993 MiB/s | 228.7k | 894 MiB/s | **+11.1%** |
| 4k randwrite | 259.6k | 1014 MiB/s | 261.0k | 1020 MiB/s | −0.5% |
| 64k randread | 128.7k | 8044 MiB/s | 63.3k | 3957 MiB/s | **+103.3%** |
| 64k randwrite | 127.1k | 7943 MiB/s | 61.4k | 3836 MiB/s | **+107.0%** |

**NVMe/TCP** (same wire)

| phase | ioutgt IOPS | ioutgt BW | nvmet IOPS | nvmet BW | ioutgt vs nvmet |
|-------|------------|-----------|------------|----------|-----------------|
| 4k randread | 239.5k | 936 MiB/s | 111.0k | 434 MiB/s | **+115.8%** |
| 4k randwrite | 249.3k | 974 MiB/s | 116.0k | 453 MiB/s | **+114.9%** |
| 64k randread | 52.9k | 3308 MiB/s | 28.1k | 1754 MiB/s | **+88.3%** |
| 64k randwrite | 34.8k | 2176 MiB/s | 17.0k | 1062 MiB/s | **+104.7%** |

For scale: the backing SSD does 122k IOPS (7.6 GiB/s) at 64k random
locally, and the raw wire carries 98 Gb/s (`ibperf`) — the single-job
64k RDMA numbers are one queue thread saturating the drive's 64k
ceiling through one QP.

Both tables come from the in-repo realwire drivers,
`testing/two_nic/realwire_tcp.sh` and `testing/two_nic/realwire_rdma.sh`:
target and initiator run on one host but each NIC is isolated in its own
network namespace, so the only path between them is the physical cable —
real hardware traffic, kernel host driver on the initiator side, and the
in-kernel nvmet target measured back to back on the same wire. fio ran
as a single job at qd 128, 15 s/phase, on real NVMe SSD backends, pinned
with `taskset`; post-connect the harness pins the RX/CQ IRQ placement on
both the target and the initiator side, so single-job numbers are
reproducible across reconnects instead of an RSS/vector placement
lottery. To reproduce:

```sh
export NIC_T=<port0> NIC_I=<port1>       # two cabled ports, NOT your mgmt NIC
export IOUTGT_BACKEND=/dev/nvmeXn1 NVMET_BACKEND=/dev/nvmeYn1
export NR_QUEUES=16 QUEUE_SIZE=128 FIO_JOBS=1 FIO_QD=128 FIO_SECS=15
testing/two_nic/realwire_tcp.sh up       # or two_nic/realwire_rdma.sh
testing/two_nic/realwire_tcp.sh start && testing/two_nic/realwire_tcp.sh connect
taskset -c <cpu> testing/two_nic/realwire_tcp.sh fio_perf
testing/two_nic/realwire_tcp.sh disconnect && testing/two_nic/realwire_tcp.sh stop
testing/two_nic/realwire_tcp.sh down
```

## Roadmap

- Performance optimization.
- Analysis & Comparison with SPDK.
- Receive zero-copy for NVMe/TCP (io_uring `RECV_ZC`).
- In-band authentication.
- Metadata/PI formats.
- Cleanup and code simplification passes.
- More targets behind the same core: NBD, iSCSI.

## Workspace layout

| Crate | Role |
|-------|------|
| `ioutgt-uring` | per-thread io_uring reactor + op futures, Tokio park integration |
| `ioutgt-nvme` | sans-io NVMe spec types + PDU codec, plus NVMe command execution: dispatch, admin/IO handlers, fabrics |
| `ioutgt-core` | protocol-neutral queue engine (slots, buffer pool, permits, `Backend`) + the structural target model (subsystems, namespaces, controller registry) |
| `ioutgt-stream` | protocol-neutral stream send/recv harness (`StreamSender`/`StreamReader`) |
| `ioutgt-nvme-tcp` | NVMe/TCP transport + binary |
| `ioutgt-nvme-rdma` | NVMe/RDMA transport + binary (verbs, CM, adaptive `--poll`) |
| `ioutgt-backend` | null / memory / file / block backends |
| `ioutgt-control` | UDS JSON control plane + config schema |
| `ioutgt-harness` | shared binary harness: spawn, queue-thread pool, control server, `stat` client |
| `ioutgt-cpus` | locality-aware even CPU grouping for topology-aware pinning |

## Documentation

- [`docs/usage.md`](docs/usage.md) — command line, config file, control
  API, host connection, test harnesses.
- [`docs/architecture.md`](docs/architecture.md) — the transport-neutral
  architecture specification (thread model, reactor, command-slot
  lifecycle, transport contract).
- [`docs/nvme-tcp.md`](docs/nvme-tcp.md) — the NVMe/TCP transport: PDU
  phase machine, gather/zero-copy send, copy budget, recv ring.
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

- Linux ≥ 6.11 (`DEFER_TASKRUN` + multishot era; developed on 7.1)
- Rust ≥ 1.88 stable
- clang libs ≤ 19 for building `ioutgt-nvme-rdma`: its `rdma-mummy-sys`
  dependency runs bindgen at build time, which fails against
  clang-libs-22 (see
  [rdma-mummy-sys#24](https://github.com/RDMA-Rust/rdma-mummy-sys/issues/24));
  point `LIBCLANG_PATH` at a clang-19 install when the system clang is
  newer

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
