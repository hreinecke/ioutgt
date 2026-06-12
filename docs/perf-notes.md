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
config is covered functionally by the interop fio matrix only.

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
5. Recv/send budget sweeps; per-queue stats counters for GET_STATS.
