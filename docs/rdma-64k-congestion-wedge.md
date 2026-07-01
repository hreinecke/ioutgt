# RDMA 64k Congestion Wedge — Investigation, Root Cause, and Lessons

Status: investigation writeup (June–July 2026). Fabric: RoCEv2 on ConnectX-5 Ex,
two physical ports direct-cabled, two network namespaces (target in root, initiator
in `nvmei`). Applies to **both** the in-kernel `nvmet-rdma` target and `ioutgt-nvme-rdma`.

## 1. Symptom

Sustained **large-IO (64k)** NVMe/RDMA traffic reliably **wedges the controller**:

- On a **freshly rebooted** box, `./rdma2.sh fio_perf nvmet` (the 4k/64k × randread/randwrite
  sweep, single job, qd=128) hangs during the **64k phase**. Throughput → 0, the controller
  resets, and then loops `rdma connection establishment failed (-104)` — it cannot reconnect.
- **4k phases always pass.** Only large IO wedges.
- Hits **nvmet (kernel target) too**, so it is *not* an ioutgt bug.
- **"Heals after some operation."** After a few reboof/reconnect cycles the same box sails
  through 64k at ~5 GB/s. The heal is really *the reconnect succeeding on a warm box*; the
  underlying congestion event still happens, it just recovers instead of wedging permanently.

## 2. Root cause (as understood)

Under the 64k burst the RoCE class is **not truly lossless**, so:

```
64k burst → link congests → real packet loss (roce_adp_retrans) →
target's RDMA ops go unACK'd → target QP local_ack_timeout →
2 QPs exhaust retry_cnt (req_transport_retries_exceeded) → QP → ERROR →
pending WRs flushed (resp/req_cqe_flush_error) → keep-alive stops →
nvmet 20 s KATO "keep-alive timer expired! fatal error" → controller teardown →
(fresh box) reconnect fails -104 → permanent wedge
```

**Unproven link (the honest gap).** The `keep-alive stops → KATO` arrow is *not* established.
Keep-alive runs on the **admin QP (QID 0)**; the 64k load and the `RETRY_EXC_ERR` are on the **IO
QPs**. At the wedge the initiator's admin QP is still `RTS` and error-free (§ below), so *why the
host stops emitting KeepAlive capsules* is not shown by IO-QP retry-exhaustion alone. A plausible
alternative — from this project's own earlier finding — is a **host-side completion-servicing stall
on the shared admin completion vector** (a QID-0 keep-alive timeout was once traced to a host CQ
freeze on comp vector 0), which would starve keep-alive even while the admin QP looks healthy.
Which of these actually kills keep-alive on a fresh box is **unresolved**: the congestion + loss
below is firmly established; the final hop to teardown is inferred. Attributing the 2 errored QPs to
admin-vs-IO QID (e.g. via per-QP `rdma statistic`) is the missing measurement that would close it.

The decisive evidence is in the target `mlx5_0` `hw_counters` at the wedge:

| Counter | Value | Meaning |
|---|---|---|
| `rp_cnp_handled` / `roce_slow_restart_cnps` | 47 / 47 | DCQCN **is** active — the host (notification point) is sending CNPs in response to ECN-CE marks, and the target (reaction point) is slowing |
| `roce_adp_retrans` | 34 | **Real packet loss/retransmit** (not just a timer) |
| `local_ack_timeout_err` | 15 | Requester ACK timer firing (the "smoking gun") |
| **`req_transport_retries_exceeded`** | **2** | **The wedge**: QPs blew through `retry_cnt=7` → `RETRY_EXC_ERR` |
| `resp_cqe_flush_error` / `req_cqe_flush_error` | 4160 / 257 | Flush *consequence* of the QP erroring |
| `packet_seq_err`, `out_of_sequence`, `rnr_nak_retry_err`, `out_of_buffer` | 0 | not reorder, not RNR |

DCQCN reacts but is **insufficient**: `cc_params` shows `rp_clamp_tgt_rate=0` (no target-rate
clamp), so after each CNP the sender additively climbs back toward line rate and **overshoots**
→ the queue re-fills → drops again. Classic DCQCN oscillation-into-loss. Global 802.3x pause is
administratively on but **never engages** (0 pause frames), and PFC is not configured on the
RoCE class.

At the wedge the **initiator side is pristine and idle**: host `mlx5_1` RC counters all zero, no
CPU busy, ~127 reads stuck inflight, host QP still `RTS`. The host isn't *failing to process* —
the target's data/ACKs simply stop flowing to it (target receive rate → 0), and the target
retransmits then times out.

## 3. What was ruled out (fresh-box, each still wedged)

The long tail of dead ends — recorded so we don't re-run them:

| Hypothesis | Verdict | Evidence |
|---|---|---|
| Target WR batching / missing doorbell | ✗ | code verified; `fio --verify` clean; pre-batching also wedged |
| **Missing `IBV_SEND_SOLICITED`** on responses | ✓ **real ioutgt bug, fixed** | but it's the *warm-box 64k host-CQ-sleep*, a **separate** issue (see §5) |
| IRQ affinity clustering on one CPU | ✗ | vectors spread across CPUs; `irqbalance` disabled |
| CPU freq / governor (schedutil) | ✗ | `performance` governor still wedges; collapse is abrupt from 5 GB/s, not a cold ramp |
| RX steering (`NIC_TUNE`/`ARFS`) | ✗ | `NIC_TUNE=0 ARFS=0` still wedges |
| Pause-HOL (global 802.3x pause) | ✗ | **0 pause frames** either direction at the wedge |
| `nvme_rdma.ack_timeout_ms` 0 → 4000 | ~ mitigates | fewer `local_ack_timeout` (11 vs 27) but still wedges; a 4 s timer still fires ⇒ peer silent >4 s, not "timeout too small" |
| MTU black-hole | ✗ | `active_mtu` = 4096 both ends |
| PFC on priority 0 | ✗ | never engaged (`p0pause=0`) — RoCE data isn't classified to prio 0 |
| `QUEUE_SIZE` 128 → 32 | ✗ | confirmed applied (`sqsize=31`), still wedges — kernel fires each command's full 64k RDMA immediately (32×64k = 2 MB burst) |

## 4. Diagnostic method and gotchas

- **The right counters** are `/sys/class/infiniband/<dev>/ports/1/hw_counters/` and the DCQCN
  `cc_params` in `/sys/kernel/debug/mlx5/<bdf>/cc_params/`. The decision tree from
  *[rdma-from-top-to-bottom §7.7](https://ming1.github.io/hardware/rdma-from-top-to-bottom)*
  maps directly: `local_ack_timeout` + rising pause/discards ⇒ PFC not holding; `local_ack_timeout`
  with no visible loss ⇒ timeout too small; here we have `local_ack_timeout` **+ real
  `roce_adp_retrans`** ⇒ genuine loss, i.e. *"the fabric isn't actually lossless" (#1 cause)*.
- **Measurement pitfalls that cost time:**
  - **netdev packet counters (`/sys/class/net/*/statistics/*_packets`) do NOT count RoCE** — they
    read 0 even at 5 GB/s. Use IB **port counters** (`ports/1/counters/port_{xmit,rcv}_packets`).
  - **per-priority byte counters (`rx_prioN_bytes`) read 0 on the priority we inspected (0)** — but
    these PPCNT counters *do* include RoCE; the 0 means **RoCE was classified to a different
    priority** (via the DSCP→prio map), not that the counters are blind to offloaded traffic. Find
    the RoCE priority via the DSCP→prio mapping, don't assume prio 0.
  - In **exclusive netns mode** the initiator device `mlx5_1` sysfs is only reachable via
    `ip netns exec nvmei cat /sys/class/infiniband/mlx5_1/...`.
- **Fresh-reboot is the only reliable repro.** Any `down`/`up`/traffic cycle can "heal" it, so the
  capture must run as the **first heavy op** after boot. Warm-box tests silently mask it — the
  multi-root-cause trap (see §5).

## 5. Lessons

1. **Warm-box testing masks environmental root causes.** Early on, nvmet looked "clean" and only
   ioutgt wedged, which led to the (real) `SOLICITED` fix. But that was a *warm* box; the
   *reliable* wedge is a **fresh-reboot fabric-congestion** issue that hits nvmet too. There were
   **two independent problems**, and fixing the visible one on a warm box hid the bigger one.
2. **`SOLICITED` was a genuine ioutgt bug** (non-solicited response SENDs → the host's
   solicited-armed CQ never interrupts → host sleeps with the CQE unreaped → 64k host-CQ-sleep
   wedge, host **idle in `poll_idle`**). Keep it. But its signature (host *idle, not woken*) is the
   **opposite** of the congestion wedge (host *pristine but starved of data*). Same "stall" symptom,
   inverse cause — the CPU-idle-vs-CPU-pegged (or here idle-vs-starved) contrast is how to tell them
   apart.
3. **`ack_timeout` / `retry_cnt` are band-aids, not fixes.** Widening them delays the give-up point
   but a peer that goes silent under congestion stays silent; the loss is the root.
4. **Queue depth ≠ bytes-in-flight in the kernel path.** `QUEUE_SIZE=32` bounds outstanding
   *commands*, but each command immediately issues its full 64k RDMA — so the *offered burst* is
   still large. Bounding the burst needs a **data-path** limit (see §6/§7).
5. **RoCE lossless is an operational burden**, not a default. Global pause "on" ≠ engaged; PFC needs
   the right DSCP→priority + trust mode (painful without `mlnx_qos`); DCQCN defaults
   (`rp_clamp_tgt_rate=0`) can *oscillate into loss*.

## 6. How popular projects solve flow control / congestion

Three schools:

1. **Require a lossless fabric (PFC + DCQCN).** The mainstream. **`nvme-rdma`/`nvmet-rdma`**,
   **NCCL/RCCL**, and SPDK all *assume* the class is lossless and do no data-path throttling. NCCL's
   networking guide is essentially "configure PFC + ECN correctly." The burden is on the operator.
2. **Tolerate a lossy fabric via HW retransmission + `retry_cnt`/`timeout` tuning.** NVIDIA's
   "lossy / semi-lossless / lossless" modes; ConnectX adaptive retransmission (`roce_adp_retrans`).
   RC uses Go-back-N, so loss is expensive, and blowing through `retry_cnt` errors the QP — exactly
   our wedge.
3. **Application-level credit / window flow control** — cap bytes-in-flight so you never overrun the
   receiver.

**SPDK is the instructive one.** Its NVMe-oF RDMA transport **decouples in-flight *data* from queue
depth** using a fixed, central **`iobuf` buffer pool** (`num_shared_buffers` → `iobuf-*-cache-size`):
a command must acquire a data buffer before it can transfer, and *"if iobuf does not have enough
buffers … the caller must try again later"* — the request is **queued, and the RDMA transfer is not
issued** until a buffer frees. So even at qd=128, only *pool-many* large transfers are in flight; the
target **paces itself** and keeps the offered load below the cliff. That is why our kernel
`QUEUE_SIZE=32` didn't help (kernel fires each command's full 64k immediately) but SPDK doesn't flood.

DCQCN tuning (`rp_clamp_tgt_rate=1` to clamp recovery instead of overshooting) is the fabric-side
lever within schools #1/#2.

## 7. Fix directions for ioutgt

Ranked by leverage and portability:

1. **Target-side send-window / bytes-in-flight credit (SPDK-style).** ioutgt already preallocates a
   fixed `BufPool` (the "zero steady-state allocation" invariant), and the **TCP read path already
   implements exactly this backpressure** — `lease_await` holds a command when the pool is exhausted
   (`docs/architecture.md` §9: "the pool is deliberately smaller than depth × MDTS"). So the fix is
   *wire that existing mechanism into the RDMA write path + size it*, not a new subsystem: when the
   pool/credit is exhausted, **hold the command** instead of firing another 64k RDMA-write.
   **Sizing caveat (critical):** the *default* pool is 8 MiB (`pool.rs` `DEFAULT_POOL_MB`), which is
   **4× larger than the 2 MB burst that already wedges** (32×64k) — so at default sizing the pool
   never exhausts before the fabric cliff and the throttle is inert. The credit must be set **below
   the congestion threshold** (a smaller pool, or a separate explicit in-flight-bytes credit well
   under the ~2 MB burst). Because the RDMA slot is held until the SEND completion, a pool-gated
   credit transitively bounds bytes-on-the-wire, so the mechanism is sound once sized correctly.
   Target-side, no fabric config, in our control — the recommended productized fix.
2. **DCQCN tuning** — `rp_clamp_tgt_rate=1` (+ slower recovery) on both ends. Fabric-side, no code,
   but per-box and doesn't survive reboot unless scripted.
3. **Proper PFC** on the RoCE priority (both ports) — the "correct" lossless answer, but needs the
   DSCP→priority/trust config that is painful here, and PFC misconfig "can amplify outages."
4. **Keep the `SOLICITED` fix** — unrelated, real, already committed.

## Appendix: persistent box config changed during the investigation

On `<dev-box>` these were made persistent (revert if undesired):

- `/etc/systemd/system/polkit.service.d/no-private-net.conf` → `PrivateNetwork=no`
  (frees the rdma netns copy so `rdma system set netns exclusive` works **without stopping polkit** —
  the previous setup blocker).
- `/etc/systemd/system/cpu-performance.service` → pins governor to `performance` (test artifact;
  did **not** fix the wedge — safe to disable).
- `/etc/modprobe.d/nvme-rdma-acktmo.conf` → `options nvme_rdma ack_timeout_ms=4000` (mitigates,
  does not fix).

## References

- [rdma-from-top-to-bottom §7 (ACK timeouts, retries, loss diagnosis)](https://ming1.github.io/hardware/rdma-from-top-to-bottom)
- [SPDK NVMe-oF Target (transport params)](https://spdk.io/doc/nvmf.html) ·
  [lib/nvmf/rdma.c](https://github.com/spdk/spdk/blob/master/lib/nvmf/rdma.c) ·
  [changelog (iobuf backpressure)](https://spdk.io/doc/changelog.html)
- [DCQCN](https://www.ipinfusion.com/technology/dcqcn/) ·
  [RoCEv2 lossless](https://www.ipinfusion.com/technology/rocev2/)
- [RDMA QP timeouts / `retry_cnt`](https://wiki.whamcloud.com/download/attachments/105096561/RDMA_timeouts_last.pdf) ·
  [ibv_modify_qp](https://www.rdmamojo.com/2013/01/12/ibv_modify_qp/)
- [NCCL networking troubleshooting](https://docs.nvidia.com/deeplearning/nccl/user-guide/docs/troubleshooting/networking_troubleshooting.html)
- [Micro-Behaviors of Hardware Offloaded RDMA (SIGCOMM'23)](https://www.microsoft.com/en-us/research/wp-content/uploads/2023/08/sigcomm23-final269-1.pdf)
