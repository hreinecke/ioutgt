#!/usr/bin/env bash
#
# two_nic/realwire_tcp.sh — run an NVMe/TCP target and initiator on ONE host
# but force the traffic across two real NICs (real network hardware),
# for either the Linux kernel nvmet-tcp target or ioutgt.
#
# THE TRICK
# ---------
# Two IPs on one host short-circuit through the loopback fast-path: the
# kernel sees both as local addresses and never puts packets on the wire.
# To force real-hardware traffic we drop each NIC into its OWN network
# namespace. With no veth/bridge linking the namespaces, the *only* path
# between them is the physical link between the two cards. So a successful
# ping across the namespaces is itself proof the bytes crossed the wire.
#
#   root netns                 NS_T (target)        NS_I (initiator)
#   (your shell)        ┌──────────────────┐   ┌────────────────────┐
#                       │  NIC_T  IP_T     │   │  NIC_I  IP_I       │
#                       └────────┬─────────┘   └─────────┬──────────┘
#                                │   physical cable/switch │
#                                └─────────────────────────┘
#
# WIRING REQUIREMENT
#   NIC_T and NIC_I must be physically connected: either a direct
#   back-to-back Ethernet cable, or both ports on the same switch/VLAN.
#
# !!! SAFETY !!!
#   Moving a NIC into a namespace removes it from your root namespace. Do
#   NOT use the NIC that carries your SSH/management connection — you will
#   cut yourself off. Use two dedicated test NICs.
#
# Each target has its own hardcoded port + NQN + backend, so both can run at
# once on the same target IP and a single env setup drives everything:
#   ioutgt : 14420  nqn...:ioutgt   IOUTGT_BACKEND
#   nvmet : 24420  nqn...:nvmet   NVMET_BACKEND
#
# USAGE (one env block, then subcommands; selector verbs take nvmet|ioutgt)
#   export NIC_T=enp1s0f0 NIC_I=enp1s0f1
#   export IOUTGT_BACKEND=/dev/sdb NVMET_BACKEND=/dev/sdc
#   sudo -E ./two_nic/realwire_tcp.sh up
#   sudo -E ./two_nic/realwire_tcp.sh start                # both (omit selector)
#   sudo -E ./two_nic/realwire_tcp.sh connect ioutgt       # or just one
#   sudo -E ./two_nic/realwire_tcp.sh fio                  # both, back to back
#   sudo -E ./two_nic/realwire_tcp.sh disconnect
#   sudo -E ./two_nic/realwire_tcp.sh stop                 # stop targets, then
#   sudo -E ./two_nic/realwire_tcp.sh down                 # remove netns
#
# KNOBS (env vars)
#   IOUTGT_BACKEND / NVMET_BACKEND   each target's file or block device
#   BACKEND_GB=2        size of an auto-created backing file
#   NR_QUEUES=4         IO queues   (ioutgt --io-threads;    connect -i)
#   QUEUE_SIZE=128      IO qdepth    (ioutgt --io-queue-size; connect -q)
#   IOUTGT_SENDZC=0     ioutgt zero-copy send (--send-zc); 1 to enable
#   HDGST=0 / DDGST=0   negotiate TCP header/data digest (CRC32C); 1 to enable
#
set -euo pipefail

# ---- config (override via environment) -------------------------------
# NIC_T / NIC_I are required, but validated below the 'help' handler so that
# 'help' works without them.
NS_T="${NS_T:-nvmet}"            # target network namespace
NS_I="${NS_I:-nvmei}"           # initiator network namespace
IP_T="${IP_T:-192.168.50.1}"    # target IP (on NIC_T, inside NS_T)
IP_I="${IP_I:-192.168.50.2}"    # initiator IP (on NIC_I, inside NS_I)
PREFIX="${PREFIX:-24}"
# Jumbo frames by default: a 4 KiB read's C2HData PDU (~4120 B) then rides one
# wire packet instead of three at MTU 1500, cutting the NIC packet rate ~3x on
# the small-IO path. Both NICs (cabled back-to-back) must agree. Override
# MTU=1500 for a NIC/link that cannot do jumbo.
MTU="${MTU:-9000}"
# Ports/NQNs/HOSTNQN come from common.sh's shared identity block (ioutgt
# 14420, nvmet 24420, distinct NQNs, so both run at once on one target IP).

# Transport context consumed by common.sh: the target listens on IP_T and
# the initiator's nvme-cli runs inside NS_I (so its socket egresses NIC_I).
# shellcheck disable=SC2034  # TARGET_IP consumed by common.sh's verbs
TARGET_IP="$IP_T"
ini_exec() { ip netns exec "$NS_I" "$@"; }

# Shared helpers + knob defaults: target_params, run_for_targets,
# ensure_backing, find_dev/find_ctrl/wait_dev, the discover/connect/
# disconnect/fio verbs, plus NR_QUEUES, QUEUE_SIZE, BACKEND_GB, IOUTGT_BIN,
# IOUTGT_SENDZC and the FIO_* knobs.

# TCP fabric: pin it before common.sh (which otherwise honors an inherited
# TRANSPORT via ${TRANSPORT:-tcp}), mirroring realwire_rdma.sh's export=rdma, so
# a stale TRANSPORT=rdma in the environment can't silently turn a tcp run into
# rdma — whether invoked directly or via run_fio_perf.sh.
export TRANSPORT=tcp
. "$(dirname "$0")/../common/common.sh"

# Adopt the NR_QUEUES persisted by the last 'up' (nic.sh nrq_state_init),
# and the control socket the ioutgt target binds (queried by the
# post-connect IRQ-affinity sync via `ioutgt list`).
nrq_state_init
IOUTGT_SOCK="${IOUTGT_SOCK:-/tmp/ioutgt-realwire.sock}"

# Whether to touch any NIC hardware setting. NIC_TUNE=0 makes the harness only
# create the netns, move the NICs in and address them -- no offloads toggle, no
# ethtool -L channel retune, no IRQ-affinity/ntuple/XPS/RPS sync. Default on.
# Defined here (before usage()) so 'help' can print its current value.
NIC_TUNE="${NIC_TUNE:-1}"

# Per-target backing (file backing only — a regular file or block device).
# Each target has its OWN, so a single env setup drives both at once. A
# missing non-/dev path is auto-created at BACKEND_GB; a /dev/* path must
# already exist. Each is validated only when its target is started.
NVMET_BACKEND="${NVMET_BACKEND:-}"   # nvmet device_path
IOUTGT_BACKEND="${IOUTGT_BACKEND:-}"   # ioutgt --backend (BACKEND_GB from common.sh)

# Queueing is capped TARGET-side on both targets and also requested by the
# initiator, so each side grants min(host request, target cap):
#   ioutgt : --io-threads / --io-queue-size
#   nvmet  : subsystem attr_qid_max / port param_max_queue_size
#   connect: --nr-io-queues / --queue-size
# NR_QUEUES / QUEUE_SIZE come from common.sh.

# ioutgt target-process knobs (IOUTGT_BIN / IOUTGT_SENDZC from common.sh).
IOUTGT_PIDFILE="${IOUTGT_PIDFILE:-/tmp/ioutgt-realwire.pid}"
IOUTGT_LOG="${IOUTGT_LOG:-/tmp/ioutgt-realwire.log}"

usage() {
    cat <<EOF
two_nic/realwire_tcp.sh — drive an NVMe/TCP target + initiator across two real
NICs on one host, isolating each NIC in its own netns to force the wire.

Targets (same target IP $IP_T, distinct port/NQN/backend):
  ioutgt   :$IOUTGT_PORT   $IOUTGT_NQN   (IOUTGT_BACKEND)
  nvmet    :$NVMET_PORT   $NVMET_NQN   (NVMET_BACKEND)

Usage: $0 <subcommand> [nvmet|ioutgt]
       (selector verbs act on BOTH targets when the selector is omitted)

  up                            create netns, move NICs in, address, prove wire
  down                          remove netns, return NICs (run 'stop' first)
  start         [nvmet|ioutgt]  start the target(s) (nvmet = in-kernel)
  stop          [nvmet|ioutgt]  stop the target(s)
  discover      [nvmet|ioutgt]  nvme discover
  connect       [nvmet|ioutgt]  nvme connect; wait for the namespace device
  disconnect    [nvmet|ioutgt]  nvme disconnect
  fio           [nvmet|ioutgt]  fio on the connected device(s)
  fio_verify    [nvmet|ioutgt]  data-integrity gate: mixed-size (4k-128k) writes
                                + crc32c read-back verify (FIO_VERIFY_MB/job)
  iperf                         raw-TCP iperf3 over the wire (server@NIC_T, client@NIC_I);
                                link-only baseline, run before 'fio_perf'
  fio_perf      [nvmet|ioutgt]  perf sweep: randread/randwrite x bs={4k,64k},
                                one line per combo (iops/BW/fio_cpu)
  status                        netns, addresses, listeners, connected devices
  help                          this message

Required env: NIC_T, NIC_I (two dedicated NICs cabled back-to-back) and the
started target's backend (IOUTGT_BACKEND / NVMET_BACKEND, a file or bdev).
Knobs: BACKEND_GB=$BACKEND_GB NR_QUEUES=$NR_QUEUES QUEUE_SIZE=$QUEUE_SIZE IOUTGT_SENDZC=$IOUTGT_SENDZC
  HDGST=$HDGST DDGST=$DDGST
  NIC_TUNE=$NIC_TUNE   (0 = netns + addressing only; no offloads/channel/affinity tuning)
  IP_T=$IP_T IP_I=$IP_I PREFIX=$PREFIX MTU=$MTU  FIO_RW/BS/QD/JOBS/SECS

Example:
  export NIC_T=enp1s0f0 NIC_I=enp1s0f1 IOUTGT_BACKEND=/dev/sdb
  sudo -E $0 up && sudo -E $0 start ioutgt
  sudo -E $0 connect ioutgt && sudo -E $0 fio ioutgt
EOF
}

# 'help'/'usage' must work without root or NIC_T/NIC_I, so handle it here.
case "${1:-}" in help|usage|-h|--help) usage; exit 0 ;; esac

[ "$(id -u)" -eq 0 ] || { echo "must run as root (use sudo)"; exit 1; }

# NIC_T/NIC_I are only needed to move the cards into/out of the namespaces
# (up/down); status/connect/etc. just use the namespaces themselves.

# in_net / NSDIR (nsenter --net into a namespace, keeping the mount ns so
# configfs stays visible) live in common.sh, shared with two_nic/realwire_rdma.

# Target context for common.sh's nvmet_setup/ioutgt_start. nvmet's configfs
# script runs via in_net (nsenter --net, keeping the mount ns so configfs stays
# visible) so the listener socket is born in NS_T. ioutgt is pure userspace (no
# configfs), so plain `ip netns exec` is enough for its launch prefix.
nvmet_exec() { in_net "$NS_T" bash -c "$1"; }
# shellcheck disable=SC2034  # consumed by common.sh's ioutgt_start
IOUTGT_NETNS=(ip netns exec "$NS_T")

# Target NIC tuning context for common.sh's nic_default_queues / nic_offloads /
# tune_target_nic / tune_status: the target NIC and the netns it lives in (the
# NIC-side ethtool/sysfs ops run there; /proc and taskset stay global). With
# NIC_TUNE=0 we blank TUNE_NIC, which is exactly the "skip" guard tune_target_nic
# and tune_status already honor.
# shellcheck disable=SC2034  # consumed by common.sh's nic_* / tune_* helpers
TUNE_NIC="${NIC_T:-}"
# shellcheck disable=SC2034  # blanked so tune_target_nic / tune_status no-op
[ "$NIC_TUNE" = 1 ] || TUNE_NIC=""
# shellcheck disable=SC2034  # consumed by common.sh's nic_* / tune_* helpers
TUNE_NS="$NS_T"
# Initiator-side twin (tune_initiator_tcp): NIC_I and the netns it lives in.
# shellcheck disable=SC2034  # consumed by common.sh's tune_initiator_tcp
TUNE_NIC_INI="${NIC_I:-}"
# shellcheck disable=SC2034  # blanked so tune_initiator_tcp no-ops
[ "$NIC_TUNE" = 1 ] || TUNE_NIC_INI=""
# shellcheck disable=SC2034  # consumed by common.sh's tune_initiator_tcp
TUNE_NS_INI="$NS_I"

# =====================================================================
cmd_up() {
    require_nics
    realwire_netns_create   # shared: create netns, move NICs in, address + MTU + up

    if [ "$NIC_TUNE" != 1 ]; then
        # Untuned baseline: no offloads toggle, no channel retune, no affinity
        # sync. Just run with NR_QUEUES as-is (user value or the default).
        echo "$NR_QUEUES" > "$NRQ_STATE"
        echo "   NIC_TUNE=0: no NIC hardware tuning; NR_QUEUES=$NR_QUEUES (--io-threads), NIC settings untouched"
        cmd_up_prove_wire
        return 0
    fi

    # Offloads (gro/gso/tso) on both NICs; see common.sh:nic_offloads.
    nic_offloads "$NIC_T" on "$NS_T"
    nic_offloads "$NIC_I" on "$NS_I"

    # Size NR_QUEUES against NIC_T and persist it (nic.sh nic_size_queues).
    nic_size_queues "$NIC_T"

    cmd_up_prove_wire
}

# Prove traffic crosses the physical link (the only path between the two
# namespaces), shared by the tuned and untuned 'up' paths. realwire_prove_wire
# (common.sh) returns non-zero on failure; a failed wire is fatal to 'up'.
cmd_up_prove_wire() { realwire_prove_wire || exit 1; }

cmd_down() {
    echo ">> removing namespaces and returning NICs to root"
    # Stop the targets first with 'stop' — the nvmet configfs teardown must
    # nsenter into NS_T while it still exists. (We do not stop them here; the
    # nvmet port would otherwise leak in the now-deleted netns.)
    # We deliberately do NOT toggle offloads here. 'up' enabled gro/gso/tso
    # (which is also the mlx5 driver default), and these settings are per-netdev
    # and persist across the move back to root. Forcing them *off* would leave
    # the NIC degraded for later, unrelated tests -- e.g. a single-stream iperf
    # over this link drops from ~20 to ~8 Gb/s with GRO off.
    realwire_netns_delete   # shared: return NICs to root, delete the namespaces
    echo "   namespaces removed; NICs returned to root (reconfigure addresses as needed)."
}

# ---- targets: 'start'/'stop [SELECTOR]' route to common.sh's shared
# start_one/stop_one (setup/teardown + the `:?` backend aborts live there);
# realwire only supplies the NS_T addressing + context hooks above.

# The discover / connect / disconnect / fio verbs and the sysfs device
# resolvers (find_dev / find_ctrl / wait_dev) come from common.sh; they run
# the initiator's nvme-cli through ini_exec (defined above as
# 'ip netns exec NS_I') and dial TARGET_IP (= IP_T).

cmd_status() {
    echo "== namespaces =="; ip netns list | grep -E "$NS_T|$NS_I" || echo "(none)"
    echo "== $NS_T link/addr =="; ip netns exec "$NS_T" ip -br addr 2>/dev/null || true
    echo "== $NS_I link/addr =="; ip netns exec "$NS_I" ip -br addr 2>/dev/null || true
    echo "== $NS_T listeners =="; ip netns exec "$NS_T" ss -ltn 2>/dev/null | grep -E ":$IOUTGT_PORT|:$NVMET_PORT" || echo "(none)"
    echo "== connected devices =="
    echo "  ioutgt ($IOUTGT_NQN): $(find_dev "$IOUTGT_NQN" || echo none)"
    echo "  nvmet ($NVMET_NQN): $(find_dev "$NVMET_NQN" || echo none)"
    # NIC IRQ <-> io-thread separation table (common.sh; no-op if TUNE_NIC unset).
    tune_status
}

# Raw-TCP iperf3 throughput over the wire, independent of any target: the
# server runs behind the TARGET NIC (in NS_T, bound to IP_T), the client behind
# the INITIATOR NIC (in NS_I) -- the same roles fio uses. Needs only 'up' (no
# start/connect), so run it before 'fio_perf' for a transport baseline. Single
# stream by default. Knobs: STREAMS=1 IPERF_SECS=10 IPERF_PORT=5201 IPERF_OMIT=2.
cmd_iperf() {
    command -v iperf3 >/dev/null 2>&1 || { echo "iperf3 not found (install iperf3)"; exit 1; }
    ip netns list 2>/dev/null | grep -q "$NS_T" || { echo "link not up -- run 'up' first"; exit 1; }
    local streams="${STREAMS:-1}" secs="${IPERF_SECS:-10}" port="${IPERF_PORT:-5201}" omit="${IPERF_OMIT:-2}"
    # -1: server handles this one client then exits, so nothing is left running.
    local -a srv=(iperf3 -s -p "$port" -1)
    local -a cli=(iperf3 -c "$IP_T" -p "$port" -t "$secs" -O "$omit" -P "$streams")
    echo ">> iperf3 over the $NIC_I -> $NIC_T wire ($streams stream(s), ${secs}s, ${omit}s omit)"
    echo "   server (behind $NIC_T, in $NS_T): ip netns exec $NS_T ${srv[*]}"
    echo "   client (behind $NIC_I, in $NS_I): ip netns exec $NS_I ${cli[*]}"
    # exec in the backgrounded subshell so $! is iperf3 itself (reliable kill).
    { exec ip netns exec "$NS_T" "${srv[@]}" >/dev/null 2>&1; } &
    local spid=$!
    # shellcheck disable=SC2064  # expand spid now
    trap "kill $spid 2>/dev/null || true" RETURN
    sleep 0.5
    ip netns exec "$NS_I" "${cli[@]}"
}

# IRQ affinity sync needs the IO queues connected (their pthread tids
# appear in `ioutgt list`).
post_connect_tune() {
    case "$1" in ioutgt|"") tune_target_nic; tune_initiator_tcp ;; esac
}
extra_verbs() { case "$1" in iperf) cmd_iperf ;; *) return 1 ;; esac; }

realwire_dispatch "$@"
