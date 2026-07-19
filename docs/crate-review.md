# Crate coupling & complexity review

Snapshot review of inter-crate dependencies, coupling strength, and
per-crate internal complexity. Taken at commit `e2523a3` (2026-07-11),
just after the harness/control de-NVMe refactor. Metrics are
reproducible with the commands in the appendix; re-run them before
trusting the numbers on a much newer tree.

## 1. Dependency graph

```text
leaves     ioutgt-core (0 deps)   ioutgt-uring (0)   ioutgt-cpus (0)
mid        ioutgt-nvme    → core, cpus       ioutgt-backend → core, uring
           ioutgt-stream  → core, uring      ioutgt-control → core, backend, cpus
infra      ioutgt-harness → core, backend, control, cpus, uring
frontends  ioutgt-nvme-tcp  → core, nvme, uring, stream, backend, control, harness
           ioutgt-nvme-rdma → core, nvme, uring,         backend, control, harness
```

Strict DAG, no cycles, three zero-dependency leaves. The de-NVMe
property holds: `ioutgt-harness` and `ioutgt-control` reach no NVMe
code — the structural model they consume (subsystem tables, controller
registry, engine limits) lives in `ioutgt-core`.

## 2. Edge coupling strength

Measured as total `ioutgt_X::` references and distinct import roots per
edge (a root ≈ one module or item family; low root counts mean narrow,
intention-revealing interfaces).

| Rank | Edge | Refs / roots | Verdict |
|---|---|---|---|
| 1 | tcp → core | 23 / 9 | widest edge, but tcp is *the composing frontend* — by design |
| 2 | nvme → core | 18 / 6 | the model edge: backend, queue, registry, subsystem + 2 consts — exactly the split's seam |
| 3 | rdma → uring | 16 / 6 | verbs/CQ plumbing on the reactor; expected |
| 4 | rdma → core, tcp → nvme | 15 / 6–7 | architectural |
| 5 | rdma → nvme | 11 / 6 | capsule dispatch |
| — | all others | ≤ 9 refs, ≤ 6 roots | narrow |

Notable *thin* edges: `harness → control` (5 refs, 2 roots: config
types + `CtlState`), `control → backend` (1 root: `AnyBackend` for the
backend factory), `nvme → cpus` (1 root: thread-identity snapshot at
Connect). No edge imports more than 9 distinct roots, so no crate
reaches inside another. **No suspicious coupling found.**

## 3. Internal complexity ranking (most → least complex)

Composite of size, `unsafe` count, shared-state density
(`Cell`/`RefCell`/`Rc`/`Arc`/atomics), async surface, branch count
(`if`/`match`/`while`/`for`), and how concentrated the code is in
single files.

| # | Crate | src LOC | unsafe | branches | state sites | Why it ranks here |
|---|---|---|---|---|---|---|
| 1 | `ioutgt-nvme-rdma` | 4560 | **66** | 268 | 68 | Largest by every measure; `target.rs` alone is 1584 LOC (even after one split round); verbs FFI unsafe + QP/CQ lifecycle + capsule state machine in one crate |
| 2 | `ioutgt-uring` | 3280 | 47 | 179 | 55 | The most *dangerous* complexity: unsafe tied to kernel-visible op lifetimes (the cancellation-safety invariant); biggest API (78 pub fns); three ~750-LOC cores (ops/reactor/bufring) |
| 3 | `ioutgt-core` | 2333 | 15 | 110 | **74** | Highest state density — the slot/pool/permit state machines; 11 generic bounds; files stay small (max 672) |
| 4 | `ioutgt-nvme` | 2604 | **0** | 123 | 51 | Big but *broad, not deep*: protocol breadth (pdu 682, admin 393), lowest branch density (4.7/100 LOC), zero unsafe — the healthiest large crate |
| 5 | `ioutgt-nvme-tcp` | 2125 | 14 | 121 | 35 | `recv.rs` (764) is a dense PDU/R2T/digest state machine; 3842 test LOC is a strength, not complexity |
| 6 | `ioutgt-harness` | 1817 | 0 | 111 | 37 | Orchestration monolith: `lib.rs` 1013 LOC, thread/pool/teardown lifecycle + 11 generic bounds in one file |
| 7 | `ioutgt-stream` | 1228 | 14 | 89 | 5 | ZC send lifecycle; compact and well-documented |
| 8 | `ioutgt-cpus` | 922 | 6 | 73 | 0 | *Highest branch density* (7.9/100 LOC) — pure topology algorithms, but no async/state, so cognitively contained |
| 9 | `ioutgt-backend` | 854 | 14 | 65 | 6 | O_DIRECT/statx unsafe; small |
| 10 | `ioutgt-control` | 672 | 0 | 35 | 4 | Plain serde/tokio request handling — simplest |

Largest files per top crate (complexity concentration):
`rdma/target.rs` 1584, `rdma/cm.rs` 938, `harness/lib.rs` 1013,
`uring/ops.rs` 797, `uring/reactor.rs` 764, `tcp/recv.rs` 764,
`nvme/pdu.rs` 682, `core/pool.rs` 672.

## 4. Watch items

- **`rdma/target.rs` (1584) and `harness/lib.rs` (1013)** are the two
  remaining >1000-LOC files; both already survived one split round —
  the next split candidates if they keep growing.
- **`ioutgt-uring` ranks #2 with the highest-stakes unsafe** — its
  `drop_stress.rs` gate is what makes that acceptable; keep it
  mandatory for any op-lifecycle change.
- **Test-coverage asymmetry**: tcp carries 3842 in-crate test LOC;
  **rdma carries 0** (its coverage lives in the VM interop gates). The
  most complex crate has the least in-tree testing — remember this when
  touching it.
- `harness/client.rs` (804 LOC in-process test client) ships inside the
  prod lib; harmless (dead-code stripped) but it inflates the crate's
  apparent size.

## Bottom line

The dependency structure is genuinely clean — a strict DAG with narrow,
intention-revealing edges that got *narrower* with the de-NVMe
refactor. Complexity is concentrated exactly where the problem is
hardest (RDMA verbs, io_uring op lifecycle), not leaked into
infrastructure. The one structural asymmetry worth tracking is rdma's
size-to-in-crate-test ratio.

## Appendix: how the numbers were produced

```sh
# dep edges
for c in crates/*/; do grep -oE '^ioutgt-[a-z-]+' $c/Cargo.toml; done

# edge coupling (refs + distinct roots), for crate $c using dep $dep
grep -rho "ioutgt_${dep}::[A-Za-z_:]*" $c/src | wc -l
grep -rho "ioutgt_${dep}::[A-Za-z_]*"  $c/src | sort -u | wc -l

# per-crate metrics
find $c/src -name '*.rs' | xargs cat | wc -l                        # LOC
grep -rc "unsafe" $c/src --include='*.rs'                           # unsafe
grep -rhoE "RefCell|Cell<|Rc<|Arc<|Atomic" $c/src | wc -l           # state sites
grep -rhoE "\bif \b|\bmatch \b|\bwhile \b|\bfor \b" $c/src | wc -l  # branches
```
