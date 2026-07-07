# NIC Queues, Steering, and Offloads — Background

A practical reference for the Ethernet/kernel-networking knobs that matter
when placing a high-IOPS NVMe/TCP target's packet processing on the right
CPUs. Each section gives the **motivation**, the **principle**, and the
**exact commands** to enable/disable/inspect it.

This is the background behind the affinity work in
`testing/two_nic/realwire_tcp.sh` and `docs/perf-notes.md`. The single
organising idea:

> Modern NICs have many hardware queues, each with its own IRQ on some CPU.
> The goal is that, for a given flow, the **NIC queue, its softirq, the
> application thread that consumes it, and (ideally) its memory** all live on
> the **same CPU / NUMA node** — so no cache lines and no inter-processor
> interrupts (IPIs) bounce across cores. Most of the knobs below exist to
> decide *which queue a packet uses* and *which CPU processes it*.

Throughout, `$DEV` is the NIC (e.g. `enp2s0f0np0`). When the NIC lives in a
network namespace `$NS`, run the `ethtool`/`/sys/class/net` commands inside
it (`ip netns exec $NS …`); `/proc/irq` and `/proc/interrupts` are **global**
(IRQs are physical, not namespaced).

---

## 1. Multiqueue NICs: RX/TX queues and channels

**Motivation.** A single RX/TX ring serialises all packet processing onto one
CPU and one lock — a hard ceiling well below line rate on 25/100G. Multiqueue
NICs expose many independent rings so traffic can be processed in parallel,
one ring per CPU.

**Principle.** The NIC has *N* RX rings and *M* TX rings, each with its own
MSI-X interrupt. A "channel" bundles rings that share an IRQ. Most NICs use
**combined** channels: channel *i* = RX ring *i* + TX ring *i* + one IRQ
(`<dev>-TxRx-i`), so rx-*i* and tx-*i* are serviced on the **same CPU**.
Others split into separate `rx`/`tx` channels with their own IRQs.

**Commands.**
```sh
ethtool -l $DEV                 # show channel maxes + current (RX/TX/Combined)
ethtool -L $DEV combined 16     # set 16 combined queue-pairs
ethtool -L $DEV rx 8 tx 8       # or split rx/tx (driver-dependent)
ls /sys/class/net/$DEV/queues/  # rx-0..rx-(N-1), tx-0..tx-(M-1) (kernel view)
```
Note: `ethtool -g` is the **ring size** (descriptors per queue), not the
queue *count*; `ethtool -l/-L` is the count.

---

## 2. IRQ affinity (which CPU services a queue's interrupt)

**Motivation.** Each queue's IRQ runs the NAPI poll / RX softirq on whatever
CPU the IRQ is steered to. Put it on the CPU that consumes the data and you
keep the skb cache-warm for the consumer; scatter it and you pay cross-CPU
cache misses.

**Principle.** `smp_affinity` is the *requested* CPU mask for an IRQ;
`effective_affinity` is the single CPU the kernel *actually* chose from it
(read-only). The hardware IRQ itself is **not** an IPI — it fires directly on
its CPU.

**Commands.**
```sh
# Find a NIC's IRQs and their labels:
grep "$DEV" /proc/interrupts                 # IRQ + per-CPU counts + label
ls /sys/class/net/$DEV/device/msi_irqs       # the device's MSI-X vectors

# Set / read (IRQ number N):
echo 4 > /proc/irq/N/smp_affinity_list       # request CPU 4 (list form)
printf "%x\n" $((1<<4)) > /proc/irq/N/smp_affinity   # or hex mask
cat /proc/irq/N/effective_affinity_list      # actual CPU (read-only)
```

**Managed IRQs.** A driver can allocate MSI-X with `PCI_IRQ_AFFINITY`
(`pci_alloc_irq_vectors_affinity()`), making the IRQs *kernel-managed*: their
`smp_affinity` is **read-only** — a userspace write returns **`-EPERM`**
(`write_irq_affinity()` → `irq_can_set_affinity_usr()` rejects managed IRQs in
`kernel/irq/proc.c`) — and the kernel spreads them itself with
`group_cpus_evenly()`. In current mainline this is a **block/storage** pattern,
not a NIC one: NVMe uses it (`drivers/nvme/host/pci.c`) so IRQ affinity follows
the blk-mq queue map. The mainstream **NIC** drivers — mlx5, ice, i40e, bnxt —
do **not** use `PCI_IRQ_AFFINITY`; their IRQs stay user-settable (they may
publish advisory affinity *hints*, which is different from managed), so you
*can* pin them and RPS/RFS/ntuple can steer. The managed IRQs you'll meet on a
single-box NVMe/TCP test are the host's NVMe **controller** IRQs (initiator
side), not the NIC's. (Verified against linux-next: the only ethernet driver
using `PCI_IRQ_AFFINITY` is `wangxun`.)

---

## 3. RSS — Receive Side Scaling (hardware hash → RX queue)

**Motivation.** Spread incoming flows across the RX queues automatically, so
multiple CPUs share the receive load without per-flow configuration.

**Principle.** The NIC hashes each packet's header (typically the TCP/IP
4-tuple) and indexes an **indirection table** to pick an RX queue. It is
flow-consistent (a flow always lands on the same queue) but **flow-blind**:
it has no idea which CPU/thread will consume the flow, so the queue it picks
is essentially random with respect to your application's placement.

**Commands.**
```sh
ethtool -x $DEV                       # show indirection table + hash key
ethtool -X $DEV equal 8               # spread across the first 8 RX queues
ethtool -n $DEV rx-flow-hash tcp4     # which header fields feed the hash
ethtool -N $DEV rx-flow-hash tcp4 sdfn  # hash on src/dst ip+port
```
RSS is always on for a multiqueue NIC; you tune its spread/hash, not an
on/off switch.

---

## 4. RPS — Receive Packet Steering (software RSS)

**Motivation.** A software fallback when the NIC lacks multiqueue/RSS, or to
re-spread receive softirq work onto more CPUs than there are hardware queues.

**Principle.** After the IRQ, the kernel hashes the packet and, if RPS says it
belongs on a different CPU, enqueues it to that CPU's backlog and **sends an
IPI** (`net_rps_send_ipi`, an `smp_call_function`) to run the softirq there.
That IPI shows up as **`Function call interrupts` (CAL)** in
`/proc/interrupts`. Useful sometimes, but the IPIs are not free.

**Commands.** Per RX queue, a hex CPU mask (empty = off):
```sh
echo 0 > /sys/class/net/$DEV/queues/rx-0/rps_cpus     # disable
echo ff > /sys/class/net/$DEV/queues/rx-0/rps_cpus    # spread to CPUs 0-7
```

---

## 5. RFS / aRFS — Receive Flow Steering (follow the consumer)

**Motivation.** RSS/RPS are flow-blind. RFS makes receive steering
**application-aware**: deliver a flow's packets to the CPU where the
application last called `recvmsg()` on that socket, so the data lands on the
consuming CPU.

**Principle.**
- **RFS (software):** the kernel records the consuming CPU per flow (in
  `rps_sock_flow_table` + per-queue `rps_flow_cnt`) and redirects the
  flow's softirq there — **via the same RPS IPI** (`net_rps_send_ipi`).
- **aRFS (accelerated):** with `ntuple` on and driver support
  (`ndo_rx_flow_steer`), the kernel programs a **hardware** filter so the NIC
  delivers the flow to the right RX queue directly. In steady state the
  packet arrives on the consumer's CPU with **no IPI** — but during rule
  setup/churn it falls back to the software (IPI) path.

**Caveat (measured).** On a read-heavy NVMe/TCP run, software RFS produced a
large CAL-interrupt storm (`net_rps_send_ipi`, ~33k/s) for no throughput
gain. The `rps_flow_cnt`/`rps_sock_flow_entries` knobs **persist across
runs**, so a stale config keeps generating IPIs. Prefer explicit hardware
`ntuple` rules (§6) for deterministic, IPI-free RX placement; this is what
`two_nic/realwire_tcp.sh` does (and it clears RFS every sync).

**Commands.**
```sh
# Enable RFS:
echo 32768 > /proc/sys/net/core/rps_sock_flow_entries
echo 2048  > /sys/class/net/$DEV/queues/rx-0/rps_flow_cnt   # per queue
ethtool -K $DEV ntuple on                                   # required for aRFS
# Disable RFS (clear all of it):
echo 0 > /proc/sys/net/core/rps_sock_flow_entries
for q in /sys/class/net/$DEV/queues/rx-*/rps_flow_cnt; do echo 0 > "$q"; done
```

---

## 6. ntuple / Flow Director — hardware flow classification

**Motivation.** Deterministically pin a specific flow (or class of flows) to a
chosen RX queue *in hardware*, with no software steering and no IPI — the
"local queue, no IPI" answer for RX.

**Principle.** The NIC's classifier matches an *n-tuple* (any subset of the
5-tuple: proto, src/dst IP, src/dst port) and delivers matching packets to a
named RX queue at ingress, before any CPU touches them. Unlike aRFS this is a
static rule you install, not an adaptive guess — so it never churns and never
falls back to IPIs. The dst port + the host's ephemeral src port uniquely
identify a connection, so one rule per connection steers it to its
consuming thread's queue.

**Commands.**
```sh
ethtool -K $DEV ntuple on                              # enable the feature
ethtool -N $DEV flow-type tcp4 \
        dst-port 14420 src-port 37294 action 2         # this flow -> RX queue 2
ethtool -n $DEV                                        # list installed rules
ethtool -N $DEV delete 7                               # remove rule id 7
```

---

## 7. XPS — Transmit Packet Steering (CPU → TX queue)

**Motivation.** The transmit counterpart of RX steering: make a thread's sends
go out the TX queue whose completion IRQ is on that same CPU, so TX
descriptor setup and completion stay local (no cross-CPU bounce on the heavy
*send* direction — e.g. a target serving reads).

**Principle.** `xps_cpus` maps *sending CPU → TX queue*: when a thread on CPU
*C* transmits, the kernel picks the TX queue whose `xps_cpus` contains *C*.
Pair it with IRQ affinity (the chosen TX queue's completion IRQ on *C*) and
the whole egress path is one CPU. (`xps_rxqs` is a variant that maps RX queue
→ TX queue, i.e. "TX follows the flow's RX queue".)

**Commands.** Per TX queue, a hex CPU mask (high 32-bit word first; e.g. CPU 4
= `00000010`, CPU 32 = `00000001,00000000`):
```sh
echo 00000010 > /sys/class/net/$DEV/queues/tx-2/xps_cpus   # CPU 4 -> tx-2
echo 0 > /sys/class/net/$DEV/queues/tx-2/xps_cpus          # disable
```

---

## 8. GRO — Generic Receive Offload (RX coalescing)

**Motivation.** At high packet rates, per-packet stack traversal dominates
CPU. Coalescing many small same-flow segments into one large skb amortises
that cost — fewer trips up the stack per byte. Directly relieves a
receive-bound target.

**Principle.** In the NAPI poll, the stack (GRO) — or the NIC (GRO-HW/LRO,
§10) — merges consecutive same-flow TCP segments into one big skb before
handing it up. Lossless and TCP-safe (unlike LRO), so it is the default-on
choice. Aggressiveness is tunable via the NAPI defer knobs.

**Commands.**
```sh
ethtool -K $DEV gro on            # / off
ethtool -k $DEV | grep generic-receive-offload   # check
# coalesce more (hold the NAPI poll briefly to batch more):
echo 20000 > /sys/class/net/$DEV/gro_flush_timeout
echo 2     > /sys/class/net/$DEV/napi_defer_hard_irqs
```

---

## 9. GSO / TSO — segmentation offload (TX)

**Motivation.** The mirror of GRO on transmit: hand the stack one large buffer
and segment it into MTU-sized frames as late as possible, so most of the TX
path runs once per *large* buffer instead of once per packet.

**Principle.**
- **TSO** (TCP Segmentation Offload): the **NIC hardware** splits a large TCP
  buffer into wire segments.
- **GSO** (Generic Segmentation Offload): the **kernel** does the same in
  software, as late as possible — the fallback when hardware TSO is absent or
  for non-TCP protocols.

**Commands.**
```sh
ethtool -K $DEV tso on            # hardware TCP segmentation
ethtool -K $DEV gso on            # software/generic segmentation
ethtool -k $DEV | grep -E "tcp-segmentation|generic-segmentation"
```

---

## 10. LRO — Large Receive Offload (and why it's usually off)

**Motivation/principle.** Like GRO but done in the **NIC**, and more
aggressive — it can merge segments lossily (dropping fields), which **breaks
routing/bridging/forwarding** and can hurt latency. For a host endpoint it
occasionally helps throughput, but GRO is the safe default; LRO is normally
left off.

**Commands.**
```sh
ethtool -K $DEV lro off           # / on
ethtool -k $DEV | grep large-receive-offload
```

---

## 11. Interrupt coalescing

**Motivation.** Trade a little latency for far fewer interrupts at high rate:
the NIC waits up to *N* µs or *K* frames before raising an IRQ, batching work
per interrupt.

**Commands.**
```sh
ethtool -c $DEV                            # show
ethtool -C $DEV rx-usecs 50 rx-frames 32   # coalesce RX
ethtool -C $DEV adaptive-rx on             # let the driver auto-tune
```

---

## 12. irqbalance — the userspace daemon that fights you

**Motivation/principle.** `irqbalance` periodically rewrites `smp_affinity`
to spread IRQs across CPUs by its own heuristics. Any manual IRQ pinning it
will silently undo, and it tends to cluster a NIC's queues onto a few CPUs.
For deterministic placement, stop it.

**Commands.**
```sh
systemctl stop irqbalance          # for this boot
systemctl disable --now irqbalance # persistently
```

---

## 13. Observability — what's actually happening

```sh
# Per-queue interrupt counts + the IPI rows (CAL = Function call,
# RES = Rescheduling):
watch -n1 "grep -E 'CPU|$DEV|Function call|Rescheduling' /proc/interrupts"

# Who is raising IPIs (find RPS/RFS, wakeups, etc.):
bpftrace -e 'tracepoint:ipi:ipi_send_cpu { @[ksym(args.callsite)] = count(); }
             interval:s:3 { exit(); }'

# Which thread (tid) does socket I/O on which 4-tuple (netstat/ss only show
# the pid; threads share the fd table):
bpftrace -e 'kprobe:tcp_sendmsg { $sk=(struct sock*)arg0;
    @[comm,tid] = ($sk->__sk_common.skc_dport >> 8) |
                  (($sk->__sk_common.skc_dport & 0xff) << 8); }
    interval:s:3 { print(@); clear(@); }'

# softirq CPU distribution, and NIC driver stats:
mpstat -P ALL 1                    # %soft per CPU
ethtool -S $DEV                    # driver/queue counters
ethtool -k $DEV                    # all offload feature states
```

---

## 14. Putting it together — the co-location recipe

For a flow served by application thread *T* pinned to CPU *C*, to keep the
whole flow on *C* with no IPIs:

1. **Stop irqbalance** (§12) so pinning sticks.
2. **IRQ affinity** (§2): NIC queue *T*'s IRQ → CPU *C*. (NIC IRQs are
   user-settable; the read-only managed-IRQ case is block/NVMe, not the NIC.)
3. **RX placement, IPI-free** (§6): a hardware `ntuple` rule steering the
   flow's 4-tuple → queue *T*. Do **not** rely on software RPS/RFS (§4–5) —
   it relocates via IPIs. Clear any stale RFS config.
4. **TX placement** (§7): `xps_cpus[tx-T] = C`, so *T*'s sends egress queue
   *T* and complete on *C*. (Combined channels mean rx-*T*/tx-*T* already
   share *C*.)
5. **Offloads** (§8–9): `gro on`, `gso/tso on` to cut per-packet CPU on both
   directions.

`testing/two_nic/realwire_tcp.sh` automates 1–5 for the ioutgt target NIC: it
reads each io-thread's CPU and its connection's peer port from `ioutgt list`,
aligns the queue IRQs to each io-thread's NUMA group, sets XPS, disables
RPS/RFS, and installs one ntuple rule per connection (`its src-port → its
io-thread's queue`).

### 14.1. Caveat — don't share one *logical CPU* between the RX softirq and the consumer

Step 2 above ("IRQ → CPU *C*", with the app thread also on *C*) minimizes IPIs
and cache-line bouncing, which is the right trade when **many** flows share each
core. But for a **single throughput-bound connection per core** it backfires:
the NIC RX softirq (GRO, TCP receive, copy into the socket buffer) and the
consumer's recv/copy are *both* heavy, and forcing them onto one logical CPU
**serializes** them — the CPU saturates and caps the flow.

The cure is to run them on **two logical CPUs** so they pipeline. Best is the
RX-IRQ CPU's **HT sibling** — a separate logical CPU on the *same physical core*,
so the consumer shares L1/L2 with the softirq that just landed the data
(cache-warm) without serializing on one CPU.

Measured on bnxt_en 10GbE (one connection, 64K, qd128), within one ioutgt
instance, moving only the io-thread:

| io-thread placement vs its RX IRQ | 64K randwrite | 64K randread |
|-----------------------------------|---------------|--------------|
| same logical CPU (co-located)     | 888 MiB/s     | — |
| different physical core, same node | ~1040 MiB/s  | — |
| **HT sibling (same core)**        | **~1075 MiB/s** | **~1107 MiB/s** |

The sibling is the fastest and most consistent (writes 1041–1092, beating nvmet
~825–1070; reads ~1107 ≈ nvmet ~1116, both near 10 GbE line rate). Reads — which
are send-heavy on the target — are unaffected by the choice. nvmet shows the
*same* lottery (it does no pinning, so its `io_work` lands on its own IRQ CPU at
random), which is why its write number swings widely run to run.

`two_nic/realwire_tcp.sh` does this: `iothread_cpu()` pins each io-thread to its
RX-IRQ CPU's HT sibling (falling back to a different physical core when SMT is
off); `status` reports `separation: OK` when every io-thread is on a different
CPU than its RX IRQ. NUMA locality is secondary — a far node measured just as
fast; the **CPU-level separation** is what matters.

## 15. Quick command reference

| Knob | Show | Enable / set | Disable |
|------|------|--------------|---------|
| Queues/channels | `ethtool -l $DEV` | `ethtool -L $DEV combined N` | — |
| IRQ affinity | `cat /proc/irq/N/effective_affinity_list` | `echo C > /proc/irq/N/smp_affinity_list` | (managed/NVMe IRQs: read-only, -EPERM) |
| RSS | `ethtool -x $DEV` | `ethtool -X $DEV equal N` | — |
| RPS | `cat .../rx-n/rps_cpus` | `echo MASK > .../rx-n/rps_cpus` | `echo 0 > …` |
| RFS/aRFS | `cat .../rx-n/rps_flow_cnt` | `rps_sock_flow_entries` + `rps_flow_cnt` + `ntuple on` | set both to `0` |
| ntuple | `ethtool -n $DEV` | `ethtool -N $DEV flow-type … action Q` | `ethtool -N $DEV delete ID` |
| XPS | `cat .../tx-n/xps_cpus` | `echo MASK > .../tx-n/xps_cpus` | `echo 0 > …` |
| GRO | `ethtool -k $DEV \| grep gro` | `ethtool -K $DEV gro on` | `gro off` |
| GSO/TSO | `ethtool -k $DEV` | `ethtool -K $DEV gso on tso on` | `gso off tso off` |
| LRO | `ethtool -k $DEV \| grep lro` | `ethtool -K $DEV lro on` | `lro off` |
| Coalescing | `ethtool -c $DEV` | `ethtool -C $DEV rx-usecs N` | `rx-usecs 0` |
| irqbalance | `systemctl status irqbalance` | — | `systemctl stop irqbalance` |
