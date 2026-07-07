# The "RDMA 64k congestion wedge" — actual root cause: host network management

Status: **RESOLVED** (July 2026). The recurring controller wedge on the mlx5
dev box (`testing/two_nic/realwire_rdma.sh`) that was
investigated for weeks as a RoCE congestion problem was **not a congestion,
flow-control, or ioutgt/nvmet bug at all**. It was the host's network
management stack destroying the test fabric out from under the RDMA session.
This file replaces the earlier congestion-theory writeup (kept in git history);
§5's lessons are the part worth re-reading.

## 1. The real root cause

### 1a. NetworkManager flushes the target IP every 45 s — THE wedge

The target NIC (`enp161s0f0np0`) had an auto-generated NM profile ("Wired
connection 2", DHCP). With no DHCP server on the direct-cabled link, NM loops
forever: activate → DHCP transaction (45 s timeout) → `ip-config → failed →
disconnected` → **flush all addresses on the device** → retry. Each flush
deletes the script-added `192.168.50.1` **and its RoCE GID table entries**.

Every observed symptom follows:

| Symptom | Explanation |
|---|---|
| Controller wedges ~45–90 s after connect | the next NM flush after `up` re-added the IP |
| Keep-alive (QID 0, opcode 0x18) dies first | GID gone → target can no longer transmit; host KA gets no response |
| `local_ack_timeout_err`, `roce_adp_retrans`, `req_transport_retries_exceeded` on the target | the target's RC retransmits into a peer whose address vector no longer resolves — the exact counter fingerprint the congestion theory was built on |
| Both targets (ioutgt AND nvmet) wedge simultaneously, even an idle one | shared flushed IP, not shared load |
| Reconnects fail `-104`/`ADDR_ERROR` forever | no IP/GID → both passive-side REP resolution and active-side resolution fail |
| "Heals after some operation" / warm-box passes | any subsequent `up` re-adds the IP; whether a run survives depends on its phase vs. NM's 45 s cycle |
| "Only the 64k phase wedges" | timing coincidence: in the 4-phase fio sweep the 64k phases run at T+30–60 s, exactly where the flush lands |
| `QUEUE_SIZE`/iodepth/rate-limit/PFC/DCQCN/`ack_timeout` all "failed to fix it" | none of them touch NetworkManager |

**Fix (persistent, on the box):**
`/etc/NetworkManager/conf.d/99-rdma-test-unmanaged.conf` with
`[keyfile] unmanaged-devices=interface-name:enp161s0f0np0;interface-name:enp161s0f1np1`.
The test driver's `up` additionally runs `nmcli device set <NIC> managed no`
(defense in depth; runtime-only).

### 1b. Also found and reverted: PFC/buffer experiment leftovers

A PFC-armed-on-all-8-priorities + all-priorities-→-128KB-buffer-1 config from a
prior experiment session **persists in NIC firmware across reboots** (mlx5 DCB
is firmware-managed). It was restored to defaults (PFC off). Remember to revert
`dcb` experiments explicitly — a reboot does not.

## 2. What the topology actually is (important for interpreting any perf data)

Both the kernel initiator (`nvme-rdma`, `host/rdma.c:605`) **and** the nvmet
listener (`target/rdma.c:1873`) hardcode `rdma_create_id(&init_net, …)`. Under
`rdma system netns exclusive`, only `mlx5_0` is visible in `init_net`, so the
host's queues bind to **mlx5_0 — the same port the targets listen on**.
Verified by port counters (`port_xmit_packets == 0` across a boot that moved
gigabytes; QP pairs allocated consecutively, e.g. `lqpn 337 ↔ rqpn 336`, on one
device):

> **The whole NVMe/RDMA session is single-port self-loopback inside the HCA.
> The second port, the netns, and the cable are decorative for the RDMA path**
> (the netns still isolates nvme-cli, and ICMP "wire-prove" pings do cross the
> wire — which is what made it convincing).

A userspace target (ioutgt) *can* be pinned to the netns'd port to force wire
traffic, but the kernel host and nvmet cannot, so a same-box A/B comparison is
inherently HCA-loopback. That is acceptable for target-side A/B work — just
don't read it as wire/fabric behavior.

## 3. Real target-side bugs the investigation shook out (fixed)

1. **Fatal tag exhaustion under full-depth bursts** (`ioutgt-nvme-rdma`). On
   RDMA the response SEND delivers the CQE to the host — freeing its SQ slot —
   *before* the target reaps its own SEND completion and releases the tag, so a
   conforming host can deliver command N+1 while all tags are held. ioutgt
   treated `claim_tag() == None` as fatal and killed the queue (observed: qids
   5/8/9 died at the start of the 64k randwrite phase; host IO timeouts 30 s
   later). Now the command is **parked and drained oldest-first as tags free**
   (nvmet parity: `rsp_wr_wait_list` + 2× rsp pool; see
   `docs/rdma-flow-control-nvmet-vs-spdk.md`). Overrunning the parking lot
   (a host truly exceeding the negotiated depth) stays fatal.
2. **Write commands hard-failed with `DATA_XFER_ERROR|DNR` under pool pressure**
   (`ioutgt-nvme-rdma`). The write path leased host-data buffers with
   `lease_or_owned`, whose private-heap fallback is unusable on RDMA (the
   buffer is the RDMA READ's local target and must live in the registered
   arena), so the fallback was detected and the command failed — with DNR, so
   the host returned EIO immediately (the code comment claimed "the host
   retries"; DNR means the opposite). The pool is deliberately smaller than
   depth×MDTS, so any full-depth write burst hit it: `mkfs.xfs` + `git clone`
   on the device produced writeback errors in seconds; the `fio_verify` gate
   (8 jobs × qd64 mixed 4k–128k writes + crc32c read-back) reproduces it 1:1
   (every dmesg `sc 0x4 DNR` matched a pool-exhausted log line). Fixed by
   deferring instead: a pool-only `try_lease` + a `pool_wait` queue drained
   front-only by the reap loop as completions release leases (SPDK's
   `pending_buf_queue` shape; the TCP read path's `lease_await` analog).
3. **No keep-alive enforcement / abrupt-loss reaping** (`ioutgt-nvme-rdma`).
   The RDMA path has no socket death to unwind a vanished host: an aborted
   connect left a dead controller with 17 QPs in RTS, permanently. Now a
   watchdog on the reap-loop backstop cadence (a) tears down an admin queue
   silent past KATO×2+5 s (mirrors nvmet's KA timer and the TCP path's
   watchdog), (b) removes the controller from the registry at admin teardown
   (TCP parity — was also missing), and (c) tears down IO queues whose
   controller is gone from the registry.
4. **`IBV_SEND_SOLICITED` on response SENDs** (fixed earlier, commit `51bbe5c`)
   — real and independent: without it the host's solicited-armed CQ never
   interrupts and the host sleeps with the CQE unreaped.
5. **ORD/IRD negotiation** (fixed earlier, commit `6558c66`) — real and
   independent: accept advertised `initiator_depth=1`, NAKing concurrent
   write-data RDMA READs.

## 4. Verification (box, fresh boot, guards in place)

- Idle connect soak: keep-alives healthy past 3 min (previously fatal at
  +45–90 s).
- Full `two_nic/realwire_rdma.sh fio_perf` sweep (both targets, 4k/64k × randread/randwrite,
  qd=128): all 8 phases complete. ioutgt ≈ 157k/167k 4k IOPS, 6.3 GiB/s 64k
  randread; nvmet ≈ 156k/171k, 7.0/5.8 GiB/s.
- The one mid-sweep error recovery observed (qid 5/8/9 tag exhaustion, §3.1)
  reconnected in 1 attempt — and is fixed by the parking change.

## 5. Lessons (the part to re-read)

1. **Check who else manages the interface before debugging the fabric.** A
   sysadmin-layer agent (NetworkManager, systemd-networkd)
   silently rewriting addresses/routes produces *exactly* the counter
   fingerprint of transport-layer loss (`local_ack_timeout` → retransmits →
   `retries_exceeded`), because the peer genuinely stops answering. `ip
   monitor` / `journalctl -u NetworkManager` during one repro would have found
   this in minutes; weeks went into PFC/DCQCN/queue-depth theories instead.
2. **Time-correlation beats load-correlation.** The wedge tracked *wall time
   since connect* (~45–90 s), not IO size — but every repro ran fio sweeps
   where 64k phases happened to occupy that window, manufacturing a "64k
   congestion" story. When a failure is periodic, suspect timers, not
   throughput.
3. **Verify the topology you think you're testing.** Port counters
   (`port_xmit_packets`) falsified "traffic crosses the wire" in one command.
   Kernel ULPs pinning cm_ids to `init_net` defeat netns-based wire-forcing
   schemes silently.
4. **Firmware-persistent state (mlx5 DCB/PFC/buffers) survives reboots.**
   "Fresh reboot" is not "clean fabric config".
5. The counters that *were* real (`SOLICITED`, ORD, tag exhaustion) were found
   because nvmet-vs-ioutgt A/B isolated target-specific behavior — the A/B
   method was right even while the environment was lying.

## Appendix: persistent dev-box config

- `/etc/NetworkManager/conf.d/99-rdma-test-unmanaged.conf` — **the fix**; keep.
- `ip rule pref 5000 to 192.168.50.0/24 lookup main` — added by `up`, dropped by
  `down` (pins the test subnet to the main routing table).
- NIC firmware DCB restored: PFC off all priorities (buffer map left as
  all-prios→buffer-1 @ full size — functionally the lossy default; the
  `dcb buffer set` back to buffer-0 is chicken-egg-blocked).
- Earlier artifacts, still present, no longer load-bearing:
  `polkit PrivateNetwork=no` drop-in (needed for `rdma system set netns
  exclusive`), `cpu-performance.service` (safe to disable),
  `nvme_rdma ack_timeout_ms=4000` modprobe.d (harmless; predates root cause).
