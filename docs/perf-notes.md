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
  batched encoder needs anyway for ordering).

## Measured environment pitfalls (for reproducers)

- The client must bulk-read: a byte-at-a-time RX parser costs ~26
  syscalls/op and caps a connection near 40K IOPS — the first "target"
  baseline was actually a client bottleneck.
- Port 4420 on a dev box may be owned by other NVMe targets; the
  interop/bench harness uses 14420 (published to the VM guest through
  the 9p marker).

## Next steps (in measured-isolation order)

1. Multishot recv + provided buffer ring (`ENOBUFS` → single-shot
   fallback): saves the recv re-arm SQE and the recv-buffer round trip.
2. Registered (fixed) slot buffers + `READ_FIXED`/`WRITE_FIXED` for the
   file backend: removes per-op page pinning; measure CPU/IOP on
   O_DIRECT ext4.
3. `SEND_ZC`: loopback falls back to copying (REPORT_USAGE confirms),
   so this needs a real NIC to evaluate honestly; the notif-gated
   buffer-reuse design is sketched in the architecture doc.
4. Recv/send budget sweeps; per-queue stats counters for GET_STATS.
