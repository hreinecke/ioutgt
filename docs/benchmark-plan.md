# Benchmark plan

Status: skeleton; methodology fixed before M9 so optimization steps are
measured against a stable baseline. Results land in
`docs/benchmark-report.md` at M10.

## Systems under test
1. **ioutgt** (this project), null / memory / file / block backends.
2. **Kernel nvmet** via configfs (`bench/setup-nvmet.sh`), null_blk / file /
   block backends — configured identically (inline data size, queue count,
   MDTS, digests off unless stated).

## Load generation
- fio with the kernel NVMe host driver, from the vmtest VM (interop-true
  path) and from host loopback (higher ceiling, lower variance).
- Job files in `bench/fio/`: 4k-randread, 4k-randwrite, 128k-read,
  128k-write, randrw-70-30; QD ∈ {1, 32, 128}; numjobs ∈ {1, 4}; 60 s
  runs, 10 s ramp, 3 repetitions.

## Metrics (CSV via bench/run.sh)
IOPS, throughput, mean/p99/p99.9 latency, target CPU per IOP (pidstat),
RSS, syscalls/sec (strace -c sampling run), context switches.

## Profiling
perf record + flamegraph on both targets at 4k-randread QD32 and
128k-read QD32; blktrace on backend device where applicable.

## Controls
- CPU governor performance; pinned target threads; irqbalance off.
- Document kernel version, mitigations, NIC offloads for every run.

## Optimization gates (M9)
Each candidate (fixed buffers, multishot recv, SEND_ZC, budget tuning) is
merged only if it improves ≥ one primary metric at no p99 regression on
the 4 primary workloads.
