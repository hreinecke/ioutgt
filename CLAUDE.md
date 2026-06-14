# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

ioutgt is a userspace NVMe/TCP storage target built on io_uring, in Rust
(edition 2024, MSRV 1.88, Linux ≥ 6.11). `docs/architecture.md` is the
authoritative as-built spec — thread model, reactor, command-slot
lifecycle, crate map, milestone status. Keep it updated when behavior
changes.

The project is still early-stage and lives in a private GitHub repo with
no outside users, so there is no API/ABI stability obligation:
refactoring and public-API changes are fine when they improve the design
— no deprecation shims or backward-compatibility layers needed.

## Commands

```sh
cargo build --release -p ioutgt   # the target binary
cargo test --workspace            # unit + in-process integration suites
cargo test -p ioutgt-uring --test echo        # one integration test file
cargo test -p ioutgt io_verify                # filter by test name
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
testing/run_affinity.sh           # multi-NUMA guest: group_cpus_evenly placement (default-on)
```

Requires the external vmtest harness (`https://github.com/ublk-org/vmtest`, config
`vmtest.conf`; override via `VMTEST`/`VMTEST_CONF`).
Knobs: `IOUTGT_BACKEND=memory|null|file`, `IOUTGT_ENABLE_KILL=1`
(kill/recovery), `IOUTGT_SOAK_ONLY=N` (reconnect-leak gate),
`IOUTGT_SEND_ZC=1` (zero-copy send path). The harness
binds port **14420**, not 4420 — 4420 is often owned by other targets on
a dev box. Host↔guest signalling goes through the vmtest 9p marker
directory, not env vars.

### Loopback load generator

```sh
cargo run --release --example loadgen -- \
    --addr 127.0.0.1:14420 --conns 4 --qd 32 --bs 4096 --secs 10 --rw randread
```

## Architecture

Nine crates in a strict dependency DAG (full diagrams: architecture.md
§4). Two deliberately opposite leaves: `ioutgt-nvme` is **sans-IO**
(bytes ↔ structs only, no sockets/async — shared by target, test client,
and the decoder fuzz test) and `ioutgt-uring` is **pure IO** (reactor +
op futures, zero protocol knowledge). `ioutgt-core` sits between (NVMe model + dispatch + the protocol-neutral
slot engine `ioutgt-core::slotq`; the per-connection queue context is
the generic `QueueCore<C>` in core (`QueueCore<Sqe>` for NVMe,
`QueueCore<NbdReq>` for a future NBD), with the transport-side
`NvmeTcpQueue` composing it with a `SendList<SendWork>`; the
transport-shared ZC gather-send harness `StreamSender` lives in its own
crate `ioutgt-stream`, layered above core+uring); `ioutgt-uring` gained
`sendbatch` — the protocol-free `GatherBatch` shared by stream
transports;
`ioutgt-nvme-tcp`, `ioutgt-backend`, `ioutgt-control` compose the core
crates; the
`ioutgt` binary assembles everything in `spawn_target()`
(`crates/ioutgt/src/lib.rs`). A third leaf, `ioutgt-cpus`, ports the
kernel's `group_cpus_evenly()` for topology-aware IO-thread pinning
(used only by the binary).

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
`dispatch::execute` → `begin_respond` + `SendWork::push` → the
`StreamSender` send loop drains `queue.send` → `release_tag`.

### Invariants — do not break

- **Zero steady-state allocation, zero locks, zero atomic RMW on the IO
  path.** All slots/buffers/tasks are preallocated at queue install.
- **Cross-thread communication into a queue thread goes only through its
  mailbox** (ioutgt-uring). Queue-thread handles are deliberately not
  `Send`; the type system enforces this rule.
- **The codec stays sans-IO**: no sockets, no async, no allocation-driven
  APIs in ioutgt-nvme. `ioutgt-core` must not depend on `ioutgt-uring`.
  (The transport-shared send harness that needs both — `StreamSender` —
  lives in its own crate `ioutgt-stream`, layered above the two leaves.)
- **Reactor cancellation safety**: the slab entry, not the op future,
  owns kernel-visible resources. A future dropped mid-flight orphans its
  entry; the entry is freed only on the terminal CQE. Anything touching
  `ioutgt-uring` op lifecycles must preserve this (stress-tested by
  `drop_stress.rs`).
- Protocol behavior mirrors kernel nvmet (`drivers/nvme/target/tcp.c`);
  `docs/nvmet-comparison.md` tracks the mapping. Errors produce C2HTermReq
  / NVMe status codes, never panics or silent closes.
