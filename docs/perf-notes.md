# Performance notes (M9)

Measurement rig: `cargo run --release --example loadgen` (raw NVMe/TCP
client on the sans-io codec, pipelined QD per connection, bulk-read RX
parsing) against the target on loopback, null backend, 2 IO threads,
16-core laptop (i7-11850H). fio-in-VM rides slirp and saturates the NAT
long before the target; loadgen exists so target changes are measurable.
Each step below was applied in isolation, measured, and kept only on a
win; the full VM interop suite re-ran green after the pass.

## Results (4K randread unless noted)

| step | QD1 ×1 | QD32 ×1 | QD32 ×4 | QD32 ×4 write | 128K QD16 ×4 |
|---|---|---|---|---|---|
| phase-1 baseline | 50.7K | 85.2K | 121.0K | 215.5K | 5.69 GiB/s |
| + zero-alloc send staging | 49.1K | 84.8K | 130.8K (+8%) | 217.6K | 6.13 GiB/s (+8%) |
| + batched send (one op per drain) | **63.2K** | **202.0K (+138%)** | **505.6K (+287%)** | **468.1K** | **6.64 GiB/s** |

p50 latency at QD32 ×4 fell from 1037 µs to 172 µs; p999 from 2856 µs
to 452 µs. Cumulative: **4.2×** on the 4-connection 4K read workload.

## The big one: the send loop made the queue thread sleep per response

Symptom (reported by Ming, confirmed by measurement): IO threads nowhere
near saturated despite deep queues. Mechanism: ops are submitted to the
ring only when the thread goes idle (`on_thread_park` →
`io_uring_enter`), and the send loop awaited every response send
individually — so each response forced a full park/enter/reap/wake
cycle. At QD32 the thread slept tens of thousands of times per second
with a full completion queue.

Independent send SQEs on one socket have **no ordering guarantee**, so
pipelining N send ops is not an option. The fix mirrors nvmet's
budgeted `io_work`: drain the entire completion/R2T queue into one
staging buffer and ship it as a *single* send op — one park per batch
instead of one per IO. Short sends retry the staged remainder before
anything else touches the wire.

## Other kept changes

- **Op entries inline their first CQE** (`first: Option<CqeResult>` +
  overflow `VecDeque`): single-shot ops never allocate result storage.
- **Send staging is recycled**: owned buffers travel into the reactor
  slab and come back on completion (`send_partial`,
  `send_vectored_partial`) — zero steady-state allocations on the send
  path, at the cost of one payload memcpy into staging (which the
  batched encoder needs anyway for ordering). *Since superseded: the
  gather send below removed staging and `send_partial` entirely;
  payloads now ship by reference.*

## Measured environment pitfalls (for reproducers)

- The client must bulk-read: a byte-at-a-time RX parser costs ~26
  syscalls/op and caps a connection near 40K IOPS — the first "target"
  baseline was actually a client bottleneck.
- Port 4420 on a dev box may be owned by other NVMe targets; the
  interop/bench harness uses 14420 (published to the VM guest through
  the 9p marker).

## Gather send: staging buffer removed (2026-06-11)

Design: `docs/superpowers/specs/2026-06-11-gather-send-design.md`.
send_loop's slot → staging payload memcpy replaced by one SENDMSG
gather per batch (headers/digests in a small arena, payload iovecs
pointing into slot buffers). Wire bytes identical (golden tests +
full interop matrix including the digest leg).

Rig for this A/B: memory backend, 4 IO threads, loadgen
`--conns 4 --qd 32 --rw randread`, loopback, same machine both
binaries (baseline = the commit immediately before the gather
rewire). loadgen does not negotiate digests, so the digest-on
config is covered functionally by the interop fio matrix, and for an
ioutgt-vs-nvmet A/B by the two-target driver scripts' `HDGST=1`/`DDGST=1`
knob (`testing/common.sh`), which negotiates the digest identically on
both targets (host requests it; ioutgt accepts, nvmet always honours it).

| config | 4K IOPS (repeat mean, range) | 4K CPU µs/IOP | 128K BW¹ | 128K CPU µs/IOP | 128K p50 |
|---|---|---|---|---|---|
| staging | 392K (382–403K) | 6.1–6.9 | 7.73 GiB/s | 55.9 | 1938 µs |
| gather | 378K (355–405K) | 6.7–7.2 | **9.47 GiB/s (+22%)** | **44.0 (−21%)** | **1559 µs** |

¹ BW is loadgen's own figure (over its measured elapsed, ~9.8 s),
so it does not exactly cross-check against ops ÷ 10 s; the +22%
relative claim holds either way. The very first single-shot pair
(staging 445K, gather 408K) sat above both repeat ranges — a
quiet-period artifact, excluded from the table.

Large reads are an unambiguous win: +22% bandwidth, −21% CPU per
IOP, p50 −20% (confirmed on Ming's rig as a clear sequential-read
improvement). 4K reads carry a small consistent cost, root-caused
in a follow-up (2026-06-11, null backend, 2 threads, per-run
utime/stime split):

- gather *lowers* user CPU per op ~5% — the staging `__memmove`
  (13.5% of target CPU in perf) is gone — but *raises* kernel CPU
  per op ~4.6%; kernel is ~85% of the per-op budget, so 4K nets a
  few percent loss (−3..5% per paired run here; acceptable on
  Ming's rig).
- Mechanism confirmed by experiment, not just suspected: a
  prototype inlining payloads ≤16K into the arena — one contiguous
  iovec segment, still SENDMSG — restored kernel CPU and IOPS to
  staging parity while keeping the large-read by-reference win
  (+13% at 128K qd16×2). The cost is the kernel's multi-segment
  `ITER_IOVEC` walk (3 segments per small response vs staging's 1
  contiguous stream), not the SENDMSG opcode itself.

Verdict: keep gather as-is. If small-IO CPU ever matters on a
workload, the validated fix is threshold-inlining — staging as the
*small* case of gather, arena sized sqsize × (64 + threshold) ≈ the
old staging footprint; a one-entry batch could additionally drop to
plain SEND (`ITER_UBUF`).

## Direct-to-slot payload recv (2026-06-11)

Design: `docs/superpowers/specs/2026-06-11-direct-slot-recv-design.md`.
Large H2C payload tails (`remaining >= H2C_DIRECT_MIN` = 16 KiB at
buffer-drain time) recv straight into the slot at the reassembly
offset via one `MSG_WAITALL` raw op; in-capsule payloads and buffered
prefixes keep the fused copy+CRC path. Removes the recv-buffer → slot
memcpy for the bytes that dominate large writes, plus the buffer
refill wakeups for the tail.

Rig: null backend, 4 IO threads (pin default-on), loopback, digests
off, loadgen `--rw randwrite --bs 131072`, 10 s runs, 3 reps, medians;
ON = commit 316288a (threshold 16 KiB), OFF = same commit with
`H2C_DIRECT_MIN = u32::MAX`. CPU from `perf stat -p <target>` over the
middle 8 s window (target process only — loadgen's R2T answering grew
its own cost). Load avg ~2 during runs (background dev box, not a
clean bench).

| config | 128K ×1 qd32 IOPS | c/IOP¹ | 128K ×4 qd32 IOPS (BW) | c/IOP¹ | insns/IOP |
|---|---|---|---|---|---|
| copy (off) | 25.7K | 86.6K | 56.9K (6.9 GiB/s) | 109.8K | ~178K |
| direct (on) | **29.5K (+15%)** | **47.8K (−45%)** | **73.4K (+29%, 9.0 GiB/s)** | **61.4K (−44%)** | **~92K (−48%)** |

¹ target cycles per IOP (perf `cycles` ÷ ops in window).

4K guards flat (single runs): randread 255K/3.3Kc (on) vs 262K/3.4Kc
(off); randwrite (in-capsule, path untouched) 249K/6.1Kc vs 237K/6.3Kc
— within noise. p50 at 128K ×1 fell ~1110 µs → ~590 µs.

Correctness gates on this branch: workspace + ASAN green, the
9-test io_direct_recv matrix ×stability runs, and the VM interop
fio --verify matrix on the FILE backend
(`IOUTGT_BACKEND=file testing/run_interop.sh ioutgt_fio` →
`PASS fio-verify`, which includes 128 KiB R2T writes through the
real kernel host driver — i.e. the direct path end to end).

Instructions/IOP halving says the win is more than the memcpy: the
copy path also re-arms the 64 KiB buffer recv 2–3× per 128 KiB
payload (each a wakeup + state-machine pass), where the direct path
parks once on the WAITALL tail. Verdict: **keep, threshold 16 KiB**
(8–64 KiB sweep not needed at this margin). Note the strategy
tension recorded in the design: this is irreconcilable with multishot
recv + provided buffers (next-steps item below) on the same
connection — landing that item means a per-connection strategy
choice.

## Next steps (in measured-isolation order)

1. Optional: threshold-inline small payloads (validated by the
   prototype above) if a real workload surfaces the 4K kernel-side
   cost.
2. Multishot recv + provided buffer ring (`ENOBUFS` → single-shot
   fallback): saves the recv re-arm SQE and the recv-buffer round trip
   for the small-IO path; for large H2C tails the direct-to-slot path
   above already removes both, so this targets headers + in-capsule
   traffic (and needs the per-connection strategy seam).
3. Registered (fixed) slot buffers + `READ_FIXED`/`WRITE_FIXED` for the
   file backend: removes per-op page pinning; measure CPU/IOP on
   O_DIRECT ext4.
4. `SEND_ZC`: loopback falls back to copying (REPORT_USAGE confirms),
   so this needs a real NIC to evaluate honestly; rides the same
   gather iovecs with notification-gated slot reuse.
5. Recv/send budget sweeps.

## Stats counters cost check (2026-06-12)

Per-queue/per-thread counters (GET_STATS `threads`, `ioutgt stat`) are
`Cell<u64>` adds on the owning thread — design said free, A/B confirms:
null backend, 4 IO threads, loadgen 4K randread conns=4 qd=32, 3
interleaved reps. Baseline median 409.4K IOPS (393.3–415.6K), with
counters 408.1K (407.6–409.7K), p50 ~172 µs both sides — well inside
the run-to-run spread. Design:
`docs/superpowers/specs/2026-06-12-queue-stats-design.md`.

## SENDMSG_ZC loopback A/B (2026-06-12)

`--send-zc` (design: `docs/superpowers/specs/2026-06-12-send-zc-design.md`)
landed opt-in. An earlier table in this section recorded −4%/−61% —
those runs were **invalid**: queues were silently wedging on ENOMEM
from ZC pinned-page accounting (see the RLIMIT_MEMLOCK note below;
fixed the same day, with the field symptom being ~27 s IO hangs under
`t/io_uring` until the host's 30 s IO timeout reset the controller).
Corrected loopback A/B, memory backend, 4 IO threads, loadgen conns=4
qd=32, 10 s runs, default 8 MiB `ulimit -l`:

| Workload | Baseline | --send-zc | Δ |
|---|---|---|---|
| 4K randread | 378.3K IOPS, p50 184 µs | 353.3K IOPS, p50 201 µs | −6.6% |
| 128K read | 10140 MiB/s, p50 1489 µs | 7215 MiB/s, p50 1911 µs | −29% |

What the numbers mean on loopback:

- ZC always degrades to a kernel copy here — REPORT_USAGE confirms
  `zc_copied == zc_batches` — so the copy is still paid, plus page
  pinning, a second CQE per op, and notif-gated tag reuse.
- **RLIMIT_MEMLOCK is the binding constraint**: ZC pins are charged
  against the per-user memlock limit (8 MiB default), *shared across
  all connections*; two full-depth 128K batches alone fill it. At
  this load ~29% of batches hit ENOMEM and fell back to the copying
  path (`zc_fallbacks` ≈ 4K of ≈ 14K batches per queue). Raising
  `ulimit -l` (or systemd `LimitMEMLOCK`) is part of any honest
  real-NIC evaluation; with the default limit a large fraction of
  "ZC" traffic is copies plus a failed-pin round trip.
- These runs used the since-removed 16 KiB `SEND_ZC_MIN` per-batch
  threshold (at qd32 even 4K batches exceeded it, so the numbers
  carry over); the threshold was dropped the same day for simplicity
  — every batch ships ZC under `--send-zc`, with the ENOMEM fallback
  as the only gate. A (noisy, shared-box) spot check after removal
  showed the expected new cost on the traffic the threshold used to
  shield: 4K qd1 p50 +≈5 µs (15.8 → 20.4) from per-op pin + notif +
  second CQE. Re-measure clean alongside the real-NIC evaluation;
  reintroduce a threshold only if small-IO ZC proves a loss there.

Verdict: keep `--send-zc` experimental/default-off; the honest
evaluation needs a real NIC **and a raised memlock limit** (roadmap).

## Tracing DRAM read/write bandwidth on AMD Zen4 (2026-06-16)

Why this is here: the payload **copy** on the send path (slot → skb,
`_copy_from_iter`) is invisible at the syscall/throughput layer but loud
at the memory controller — it is one DRAM read of the slot plus one DRAM
write into the skb, on top of the NIC's DMA. So DRAM read/write counters
are how you *see* `--send-zc` work: zero-copy makes the NIC DMA straight
from the slot pages and that copy traffic disappears. This is the runbook
for an EPYC 9004 (Genoa, Zen4) box; the technique generalizes.

There are two relevant PMUs. Prefer the first.

### 1. `amd_umc` — the memory-controller CAS counters (gold standard)

Each Unified Memory Controller (UMC) channel counts DRAM column-access
(CAS) commands, split into read/write. One CAS moves one 64-byte cache
line, so `bytes = CAS × 64`. The kernel exposes **one `amd_umc_N` PMU per
active channel** — on a sparsely-populated box that may be just two
(`amd_umc_0`, `amd_umc_1`); that is also how many channels actually carry
traffic, so summing them is complete, not a sample of a larger set.

```sh
# discover how many channels the kernel exposes
ls -d /sys/bus/event_source/devices/amd_umc_* ; perf list | grep umc_cas_cmd

# measure read+write across every exposed channel for 8 s, system-wide.
# NOTE the dots: umc_cas_cmd.rd / .wr (not underscores).
EV=$(for p in /sys/bus/event_source/devices/amd_umc_*; do n=$(basename "$p")
       printf '%s/umc_cas_cmd.rd/,%s/umc_cas_cmd.wr/,' "$n" "$n"; done | sed 's/,$//')
perf stat -a -e "$EV" -- sleep 8 2>perf.txt

rd=$(grep umc_cas_cmd.rd perf.txt | awk '{gsub(/,/,"",$1); s+=$1} END{print s}')
wr=$(grep umc_cas_cmd.wr perf.txt | awk '{gsub(/,/,"",$1); s+=$1} END{print s}')
awk -v r=$rd -v w=$wr 'BEGIN{printf "DRAM read %.1f, write %.1f, total %.1f GB/s\n",
  r*64/8/1e9, w*64/8/1e9, (r+w)*64/8/1e9}'
```

This is **4 events for 2 channels → no multiplexing**, separate read vs
write, no derived-metric formula. It is the most trustworthy method on
this hardware. Run as root.

### 2. `likwid-perfctr -g MEM` — convenient but caveated here

likwid's `MEM` group reads the same UMC CAS counters and prints
`Memory read/write bandwidth [MBytes/s]` (decimal MB/s; ÷1000 → GB/s).
Stethoscope mode measures a window while a *separate* process (the target)
runs:

```sh
likwid-perfctr -c S0:0@S1:0 -g MEM -S 8s     # one thread per socket
likwid-perfctr -c M0:0@M1:0@M2:0@M3:0@M4:0@M5:0@M6:0@M7:0 -g MEM -S 8s   # per NUMA domain
```

`-c` here is not "measure these cores' work" — uncore counters are
per-domain, so it means "use a thread in each domain to *reach* the local
memory controllers." One thread per memory domain is needed to touch
every channel.

Two Zen4 gotchas that cost real time:
- With the **perf_event** backend (likwid default here) only `amd_umc_0`
  returned data; other channels read `-`/`inf`. So likwid measured one
  channel and its Zen4 `MEM` formula reported read==write (suspiciously
  exact) — treat single-channel likwid totals as unreliable.
- Rebuilding likwid with **`ACCESSMODE=direct` does NOT fix it**: the
  Zen4 UMC counters sit behind a DF/PCI interface, not the MSRs likwid's
  direct path reads, so every UMC came back `-`. Conclusion: on this
  kernel, plain `perf -e amd_umc_*/...` (method 1) beats likwid for UMC.

### 3. `amd_df` — Data Fabric data beats (fallback / cross-check)

If `amd_umc` is unavailable, the Data Fabric counts data **beats** of
**32 bytes** each, per coherent station (channel): `bytes = beats × 32`.

```sh
EV=$(perf list | grep -oE 'amd_df/local_processor_(read|write)_data_beats_cs[0-9]+/' | paste -sd,)
perf stat -a -e "$EV" -- sleep 8 2>df.txt   # sum *_read_*/ *_write_* beats, ×32
```

Caveats that make it only a cross-check: `local_processor_*` counts
**socket-local** traffic only (add `remote_*` for full coverage), and the
DF PMU has ~4 counters so 24 channel events **multiplex** (perf scales by
the printed `(NN%)` enabled fraction). Good for a 2× effect, not for
absolute GB/s.

### Reading the result

Worked example, ioutgt 64K/QD128 reads over a real NIC, method 1
(both UMC channels), copy vs `--send-zc`:

| | copy | zero-copy |
|---|---|---|
| IO throughput | 989 MiB/s | 1073 MiB/s |
| DRAM read / write / total | 7.7 / 3.2 / 10.9 GB/s | 5.3 / 2.1 / 7.4 GB/s |

- **Normalize by throughput** when the two runs differ (they will — the
  copy is itself a bottleneck): copy moved ≈10.5 B of DRAM per IO byte,
  ZC ≈6.6 — about **−37% DRAM per byte served**, the savings concentrated
  on the **read** side (the slot read the copy did and ZC skips).
- ZC was also *faster* (989 → 1073 MiB/s): with the copy gone the io
  thread stopped capping throughput. A throughput win and a memory-traffic
  win are separate effects; report both.
- Confirm qualitatively with `perf record -g -p $(pgrep -f 'ioutgt --listen')
  -- sleep 8; perf report`: copy mode shows `tcp_sendmsg_locked →
  _copy_from_iter`; ZC mode replaces it with `skb_zerocopy_iter_stream →
  __zerocopy_sg_from_iter → iov_iter_get_pages2` (page pinning), and
  ioutgt cycles drop ~24%.

### Pitfalls

- **Loopback always copies** (the kernel copies for `lo`), so ZC shows
  zero DRAM benefit there — measure over a real NIC.
- **Raise `RLIMIT_MEMLOCK`** (`ulimit -l unlimited`) before starting the
  ZC target, or SENDMSG_ZC hits ENOMEM and silently falls back to a copy —
  you would measure "no difference" (see the SENDMSG_ZC note above).
- **`#PMUs == #measurable channels`**: if only `amd_umc_0/_1` exist, that
  *is* the whole story on this box; do not assume a 12-channel socket.
- Counter access needs root; `perf` uncore wants
  `perf_event_paranoid <= 0` (or root).

## Loopback C2HData gap vs nvmet is `init_on_alloc` page zeroing (2026-06-16)

Symptom (reported by Ming): on one host, two namespaces with *identical*
config — block-device backend (Samsung 9100 PRO each), 16 IO threads × 128
qd — one from ioutgt, one from kernel nvmet, driven by
`t/io_uring -p0 -b65536` (64 KiB QD128 randread, single thread). ioutgt
reached **only ~1/2–1/3** of nvmet's IOPS. `--send-zc` did **not** help.

Reproduced on the AMD Zen4 box (kernel 7.1.0, both backends bdev, single
fio thread `taskset -c 4` → one blk-mq hctx → one NVMe/TCP queue → one
ioutgt io-thread, so this is *single-connection* C2HData send throughput):

| config | IOPS | BW | io-thread CPU goes to |
|---|---|---|---|
| nvmet | 64.6K | 4.04 GiB/s | send `skb_splice_from_iter` ~1%, **no zeroing** |
| ioutgt copy | 35K | 2.2 GiB/s | **50% `kernel_init_pages`** ← `skb_page_frag_refill` ← `tcp_sendmsg` |
| ioutgt `--send-zc` | 18.5K | 1.15 GiB/s | **37% `kernel_init_pages`** ← `skb_copy_ubufs` (loopback **RX**) |

The ioutgt io-thread sits at ~90% of one core; fio's submitter is ~8%, the
host nvme_tcp kworker ~33% — the *target's send thread* is the ceiling.

Mechanism (perf `-g`, this kernel has **`init_on_alloc` on**):

- **Copy path.** A 64 KiB C2HData payload is copied through `tcp_sendmsg`
  into the socket's `sk_frag`. On **loopback there is no DMA**: the sent
  skb sits in the *local* receive queue still referencing those frag pages
  until the host nvme_tcp kworker drains it, so the frag can't be reused —
  every send **refills**, and `init_on_alloc` zeroes each fresh page
  (`kernel_init_pages`) right before the copy overwrites it. Half the
  thread's CPU is spent zeroing pages it is about to clobber.
- **`--send-zc` is worse, not better.** With no DMA engine on `lo`, the RX
  path must `skb_copy_ubufs()` the pinned user pages into fresh (again
  zeroed) kernel pages to release them for the ZC notification. ZC merely
  *relocates* the copy from TX to RX and adds page pinning + a second CQE +
  notif-gated tag reuse — hence the further drop to 18.5K.
- **nvmet avoids all of it** with `MSG_SPLICE_PAGES`: it donates the
  backend's own bio pages by refcount — no frag allocation, no zeroing, no
  copy (`skb_splice_from_iter` ~1%). The one big `memcpy` in nvmet's trace
  (`_copy_to_iter` ← `__tcp_read_sock`) is the **initiator-side** receive
  copy, identical for both targets. ioutgt can't take this path: its
  payload buffers are reused preallocated slots (zero-steady-state-alloc
  invariant) that need a TX-completion signal `MSG_SPLICE_PAGES` doesn't
  give — see architecture.md §4.2.2.

**Why a real NIC doesn't show this** (confirmed by Ming: real-NIC A/B copy
shows no gap). Two loopback-only effects compound exactly here:

1. *Frag lifetime.* On a real NIC the `sk_frag` pages are freed when the
   NIC DMA-completes the TX (µs), so `tcp_sendmsg` reuses the frag and
   rarely refills → little `kernel_init_pages`. Loopback pins the frag in
   the local RX queue → constant refill + zeroing.
2. *No wire ceiling.* On a real NIC both targets are link-bound (the
   two-NIC run was ~1.1 GiB/s wire-bound), so they hit the same IOPS and
   ioutgt's extra send-side CPU is absorbed by spare cores — invisible.
   Loopback has no link to hide behind, so the zeroing cost translates
   directly into the IOPS gap.

Verdict: **a loopback microbenchmark artifact, not an ioutgt deficiency on
the intended real-wire path.** There is no clean ioutgt-side fix that keeps
the zero-allocation invariant — copy is the cost on loopback,
`MSG_SPLICE_PAGES` is unavailable, and `MSG_ZEROCOPY` is counterproductive
on `lo`. Do not chase loopback C2HData throughput; evaluate the send path
over a real NIC (where `--send-zc` becomes a win — see the DRAM section
above). If you want to *quantify* the hardening artifact rather than the
transport, boot the host `init_on_alloc=0`: the ioutgt copy number should
jump substantially while nvmet (which never allocates the frag) stays put.
