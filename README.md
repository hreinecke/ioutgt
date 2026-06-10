# ioutgt

A high-performance userspace storage target framework built on io_uring,
written in Rust. The first transport is **NVMe/TCP**; the architecture is
transport-independent and designed to grow NVMe/RDMA, NBD, and iSCSI
implementations behind the same core.

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

## Workspace layout

| Crate | Role |
|-------|------|
| `ioutgt-uring` | per-thread io_uring reactor + op futures, Tokio park integration |
| `ioutgt-nvme` | sans-io NVMe spec types, NVMe/TCP PDU codec, CRC32C digests |
| `ioutgt-core` | subsystems, controllers, namespaces, queues, dispatch, `Backend`/`Transport` traits |
| `ioutgt-tcp` | NVMe/TCP transport state machines |
| `ioutgt-backend` | null / memory / file / block backends |
| `ioutgt-control` | UDS JSON control plane + config schema |
| `ioutgt` | the target binary |

## Documentation

- [`docs/architecture.md`](docs/architecture.md) — the architecture
  specification (thread model, reactor, command-slot lifecycle, PDU flows).
- [`docs/nvmet-comparison.md`](docs/nvmet-comparison.md) — subsystem-by-
  subsystem comparison with the Linux kernel NVMe target.
- [`docs/benchmark-plan.md`](docs/benchmark-plan.md) — benchmark methodology
  vs kernel nvmet.

## Requirements

- Linux ≥ 6.11 (`DEFER_TASKRUN` + multishot era; developed on 6.19)
- Rust ≥ 1.88 stable

## Status

Early development. Milestones and progress are tracked in
`docs/architecture.md`; interoperability is validated continuously against
the Linux kernel NVMe/TCP host driver (`nvme-cli` discover/connect from a
VM).
