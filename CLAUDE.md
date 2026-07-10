# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

ioutgt is a userspace NVMe/TCP storage target built on io_uring, in Rust
(edition 2024, MSRV 1.88, Linux ≥ 6.11). `docs/architecture.md` is the
authoritative as-built spec — thread model, reactor, command-slot
lifecycle, crate map, milestone status. Keep it updated when behavior
changes.

The project is still early-stage, with a public GitHub repo:
refactoring and public-API changes are fine when they improve the design
— no deprecation shims or backward-compatibility layers needed.

## Commands

```sh
cargo build --release -p ioutgt-nvme-tcp   # the target binary
cargo test --workspace            # unit + in-process integration suites
cargo test -p ioutgt-uring --test echo        # one integration test file
cargo test -p ioutgt-nvme-tcp io_verify       # filter by test name
cargo clippy --workspace --all-targets
cargo fmt --all
```

Lints are workspace-level (root `Cargo.toml`): `unsafe_op_in_unsafe_fn`
is deny; `missing_docs`, `undocumented_unsafe_blocks`, and
`clippy::cast_possible_truncation` are warn — new public items need doc
comments, new `unsafe` blocks need `// SAFETY:` comments. The release
profile keeps `debug = true` for perf/flamegraph work.

### VM interop (primary acceptance test)

```sh
testing/run_interop.sh            # full matrix: discover/connect, fio --verify, fs stage
testing/run_interop.sh ioutgt_fio # only the fio data-integrity stage
testing/run_affinity.sh           # multi-NUMA guest: spread_cpus placement (default-on)
```

Requires the external vmtest harness (`https://github.com/ublk-org/vmtest`, config
`vmtest.conf`; override via `VMTEST`/`VMTEST_CONF`).
Knobs: `IOUTGT_BACKEND=memory|null|file`, `IOUTGT_ENABLE_KILL=1`
(kill/recovery), `IOUTGT_SOAK_ONLY=N` (reconnect-leak gate),
`IOUTGT_SEND_ZC=1` (zero-copy send path), `IOUTGT_IO_QUEUE_SIZE=N`
(advertised IO MAXCMD ceiling; set below the host's depth, e.g. 64, to
see the guest kernel clamp to N). The harness
binds port **14420**, not 4420 — 4420 is often owned by other targets on
a dev box. Host↔guest signalling goes through the vmtest 9p marker
directory, not env vars.

### Loopback load generator

```sh
cargo run --release --example loadgen -- \
    --addr 127.0.0.1:14420 --conns 4 --qd 32 --bs 4096 --secs 10 --rw randread
```

## Architecture

Ten crates in a strict dependency DAG (full diagrams: architecture.md
§3). The two foundation leaves are `ioutgt-core` (the protocol-neutral
queue engine — slot array, buffer pool, permits, `Backend` trait, and
the generic per-connection context `QueueCore<C>` (`QueueCore<Sqe>`
for NVMe, `QueueCore<NbdReq>` for a future NBD); zero dependencies)
and `ioutgt-uring` (pure IO: reactor + op futures + gather-send
mechanics, zero protocol knowledge). `ioutgt-nvme` layers the NVMe
world on the engine: sans-IO codec modules (bytes ↔ structs — shared
by target, test client, and the decoder fuzz test) plus the
transport-independent model (subsystems, controllers, dispatch),
mirroring kernel nvmet's `core.c`. `ioutgt-stream` is the
transport-shared, ZC-aware gather-send harness `StreamSender`, layered
above core + uring (walked end to end in `docs/stream-sender.md`). The
frontends compose these:
`ioutgt-nvme-tcp` (NVMe/TCP transport — joins `QueueCore<Sqe>` with a
`SendList<SendWork>` as `NvmeTcpQueue` and drives `StreamSender`) and
`ioutgt-nvme-rdma` (NVMe/RDMA transport), plus `ioutgt-backend` and
`ioutgt-control`. Each transport crate is self-contained: it ships its
own binary and assembles the target in `spawn_target()` / `main()`
(`crates/ioutgt-nvme-tcp/src/lib.rs`, mirrored by `ioutgt-nvme-rdma`),
running on the shared `ioutgt-harness` queue-thread pool. The last leaf,
`ioutgt-cpus`, provides locality-aware even CPU grouping (`spread_cpus`)
for topology-aware IO-thread pinning (used only by the binaries).

Threading: a control thread on plain Tokio does accept + ICReq handshake
+ first-Connect parse, then routes the socket by qid to a pinned queue
thread (qid 0 → admin thread, qid n → io thread `(n-1) % N`). Each queue
thread runs its own io_uring (`SINGLE_ISSUER | DEFER_TASKRUN`) under a
Tokio current-thread runtime with no Tokio IO driver; the reactor hooks
`on_thread_park` so idle waits become one `submit_and_wait` syscall.

Per connection, `run_queue()` (ioutgt-nvme-tcp) spawns one persistent task per
command slot ("task per tag"); the recv loop, slot tasks, and send loop
never call each other — their only rendezvous is `NvmeTcpQueue`
(ioutgt-nvme-tcp): `claim_tag`/`submit` → `await_command` →
`dispatch::execute` → `complete()` (= `begin_respond` + push a
`SendWork`) → the `StreamSender` send loop drains `queue.send` →
`release_tag`.

### Invariants — do not break

- **Zero steady-state allocation, zero locks, zero atomic RMW on the IO
  path.** All slots/buffers/tasks are preallocated at queue install.
- **Cross-thread communication into a queue thread goes only through its
  mailbox** (ioutgt-uring). Queue-thread handles are deliberately not
  `Send`; the type system enforces this rule.
- **The codec modules of `ioutgt-nvme` stay sans-IO**: no sockets, no
  async, no allocation-driven APIs in `spec`/`pdu`/`identify`/
  `fabrics`/`status`/`digest` — the decoder fuzz test and the
  control-thread handshake depend on it. `ioutgt-core` stays
  dependency-free and in particular must not depend on `ioutgt-uring`.
  (The transport-shared send harness that needs both — `StreamSender` —
  lives in its own crate `ioutgt-stream`.)
- **Reactor cancellation safety**: the slab entry, not the op future,
  owns kernel-visible resources. A future dropped mid-flight orphans its
  entry; the entry is freed only on the terminal CQE. Anything touching
  `ioutgt-uring` op lifecycles must preserve this (stress-tested by
  `drop_stress.rs`).
- Protocol behavior mirrors kernel nvmet (`drivers/nvme/target/tcp.c`);
  `docs/nvmet-comparison.md` tracks the mapping. Errors produce C2HTermReq
  / NVMe status codes, never panics or silent closes.
