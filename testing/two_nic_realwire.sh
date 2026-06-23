#!/usr/bin/env bash
#
# two_nic_realwire.sh — run an NVMe/TCP target and initiator on ONE host
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
#   sudo -E ./two_nic_realwire.sh up
#   sudo -E ./two_nic_realwire.sh start                # both (omit selector)
#   sudo -E ./two_nic_realwire.sh connect ioutgt       # or just one
#   sudo -E ./two_nic_realwire.sh fio                  # both, back to back
#   sudo -E ./two_nic_realwire.sh disconnect
#   sudo -E ./two_nic_realwire.sh stop                 # stop targets, then
#   sudo -E ./two_nic_realwire.sh down                 # remove netns
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
# Each target has its OWN hardcoded port + NQN so both can run at once on
# the same target IP and be addressed unambiguously. ioutgt keeps the repo's
# conventional 14420; the nvmet target uses 24420.
IOUTGT_PORT=14420
IOUTGT_NQN="nqn.2026-06.io.realwire:ioutgt"
NVMET_PORT=24420
NVMET_NQN="nqn.2026-06.io.realwire:nvmet"
# shellcheck disable=SC2034  # HOSTNQN consumed by common.sh's connect/discover
HOSTNQN="nqn.2026-06.io.realwire:host"

# Transport context consumed by common.sh: the target listens on IP_T and
# the initiator's nvme-cli runs inside NS_I (so its socket egresses NIC_I).
# shellcheck disable=SC2034  # TARGET_IP consumed by common.sh's verbs
TARGET_IP="$IP_T"
ini_exec() { ip netns exec "$NS_I" "$@"; }

# Shared helpers + knob defaults: target_params, run_for_targets,
# ensure_backing, find_dev/find_ctrl/wait_dev, the discover/connect/
# disconnect/fio verbs, plus NR_QUEUES, QUEUE_SIZE, BACKEND_GB, IOUTGT_BIN,
# IOUTGT_SENDZC and the FIO_* knobs.

# Was NR_QUEUES set in the environment? Captured BEFORE common.sh applies its
# :-4 default, so 'up' may auto-size it from the NIC only when the user did not.
NRQ_USER_SET="${NR_QUEUES+1}"

. "$(dirname "$0")/common.sh"

# Persisted auto-sized NR_QUEUES (so the separate up/start/connect/status
# invocations agree) and the control socket the ioutgt target binds (queried
# by the post-connect IRQ-affinity sync via `ioutgt list`).
NRQ_STATE="${NRQ_STATE:-/tmp/ioutgt-realwire.nrq}"
IOUTGT_SOCK="${IOUTGT_SOCK:-/tmp/ioutgt-realwire.sock}"
if [ -z "$NRQ_USER_SET" ] && [ -f "$NRQ_STATE" ]; then
    NR_QUEUES="$(cat "$NRQ_STATE")"
fi

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
two_nic_realwire.sh — drive an NVMe/TCP target + initiator across two real
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
  status                        netns, addresses, listeners, connected devices
  help                          this message

Required env: NIC_T, NIC_I (two dedicated NICs cabled back-to-back) and the
started target's backend (IOUTGT_BACKEND / NVMET_BACKEND, a file or bdev).
Knobs: BACKEND_GB=$BACKEND_GB NR_QUEUES=$NR_QUEUES QUEUE_SIZE=$QUEUE_SIZE IOUTGT_SENDZC=$IOUTGT_SENDZC
  HDGST=$HDGST DDGST=$DDGST
  IP_T=$IP_T IP_I=$IP_I PREFIX=$PREFIX  FIO_RW/BS/QD/JOBS/SECS

Example:
  export NIC_T=enp1s0f0 NIC_I=enp1s0f1 IOUTGT_BACKEND=/dev/sdb
  sudo -E $0 up && sudo -E $0 start ioutgt
  sudo -E $0 connect ioutgt && sudo -E $0 fio ioutgt
EOF
}

# 'help'/'usage' must work without root or NIC_T/NIC_I, so handle it here.
case "${1:-}" in help|usage|-h|--help) usage; exit 0 ;; esac

NSDIR=/run/netns
[ "$(id -u)" -eq 0 ] || { echo "must run as root (use sudo)"; exit 1; }

# NIC_T/NIC_I are only needed to move the cards into/out of the namespaces
# (up/down); status/connect/etc. just use the namespaces themselves.
require_nics() {
    : "${NIC_T:?set NIC_T to the target-side NIC, e.g. NIC_T=enp1s0f0}"
    : "${NIC_I:?set NIC_I to the initiator-side NIC, e.g. NIC_I=enp1s0f1}"
}

# nsenter --net enters ONLY the net namespace, leaving the mount namespace
# alone — crucial for the kernel target, because `ip netns exec` remounts a
# fresh /sys and thereby SHADOWS the configfs at /sys/kernel/config. We need
# configfs visible AND the socket-creating process inside NS_T (nvmet-tcp
# creates its listener in the *current* process's netns), so we use nsenter.
in_net() { nsenter --net="$NSDIR/$1" "${@:2}"; }

# Target context for common.sh's nvmet_setup/ioutgt_start. nvmet's configfs
# script runs via in_net (nsenter --net, keeping the mount ns so configfs stays
# visible) so the listener socket is born in NS_T. ioutgt is pure userspace (no
# configfs), so plain `ip netns exec` is enough for its launch prefix.
nvmet_exec() { in_net "$NS_T" bash -c "$1"; }
# shellcheck disable=SC2034  # consumed by common.sh's ioutgt_start
IOUTGT_NETNS=(ip netns exec "$NS_T")

# Auto-size IO queues from NIC_T (inside NS_T): min(rx, tx, nproc). rx/tx are
# RX+Combined / TX+Combined from `ethtool -l`; falls back to counting the
# sysfs rx-*/tx-* queue dirs. Used by 'up' to default NR_QUEUES so ioutgt's
# --io-threads matches the NIC channel count (1:1 IRQ <-> io-thread mapping).
nic_default_queues() {
    local nic="$1" out comb rx tx ncpu m
    out="$(ip netns exec "$NS_T" ethtool -l "$nic" 2>/dev/null \
        | sed -n '/Current hardware settings/,$p' || true)"
    comb="$(printf '%s\n' "$out" | awk '/^Combined:/{print $2; exit}')"
    rx="$(printf '%s\n' "$out" | awk '/^RX:/{print $2; exit}')"
    tx="$(printf '%s\n' "$out" | awk '/^TX:/{print $2; exit}')"
    rx=$(( ${rx:-0} + ${comb:-0} ))
    tx=$(( ${tx:-0} + ${comb:-0} ))
    if [ "$rx" -eq 0 ]; then
        rx="$(ip netns exec "$NS_T" bash -c "ls -d /sys/class/net/$nic/queues/rx-* 2>/dev/null | wc -l" || echo 0)"
    fi
    if [ "$tx" -eq 0 ]; then
        tx="$(ip netns exec "$NS_T" bash -c "ls -d /sys/class/net/$nic/queues/tx-* 2>/dev/null | wc -l" || echo 0)"
    fi
    ncpu="$(nproc 2>/dev/null || echo 1)"
    m="$rx"
    if [ "$tx" -lt "$m" ]; then m="$tx"; fi
    if [ "$ncpu" -lt "$m" ]; then m="$ncpu"; fi
    if [ "${m:-0}" -lt 1 ]; then m=1; fi
    printf '%s\n' "$m"
}

# =====================================================================
cmd_up() {
    require_nics
    echo ">> creating namespaces $NS_T / $NS_I and moving NICs in"
    ip netns add "$NS_T"
    ip netns add "$NS_I"

    # Move each physical NIC into its namespace, address it, bring it up.
    ip link set "$NIC_T" netns "$NS_T"
    ip link set "$NIC_I" netns "$NS_I"

    ip netns exec "$NS_T" ip addr add "$IP_T/$PREFIX" dev "$NIC_T"
    ip netns exec "$NS_I" ip addr add "$IP_I/$PREFIX" dev "$NIC_I"

    ip netns exec "$NS_T" ip link set lo up
    ip netns exec "$NS_I" ip link set lo up
    ip netns exec "$NS_T" ip link set "$NIC_T" up
    ip netns exec "$NS_I" ip link set "$NIC_I" up

    # GRO (rx coalescing — relieves the recv-bound path), GSO, and hardware TSO
    # (offloads TCP TX segmentation to the NIC — relieves the send-heavy read
    # path; GSO stays on as the software fallback), both NICs.
    ip netns exec "$NS_T" ethtool -K "$NIC_T" gro on gso on tso on 2>/dev/null \
        || echo "   note: could not toggle gro/gso/tso on $NIC_T"
    ip netns exec "$NS_I" ethtool -K "$NIC_I" gro on gso on tso on 2>/dev/null \
        || echo "   note: could not toggle gro/gso/tso on $NIC_I"

    # Auto-size NR_QUEUES from NIC_T unless the user set it, so ioutgt's
    # --io-threads matches the NIC channel count. Persisted for 'start' etc.
    if [ -z "$NRQ_USER_SET" ]; then
        NR_QUEUES="$(nic_default_queues "$NIC_T")"
        echo "$NR_QUEUES" > "$NRQ_STATE"
        echo "   NR_QUEUES defaulted to $NR_QUEUES (min rx/tx of $NIC_T, capped at nproc)"
    fi

    echo ">> waiting for link/carrier, then proving the wire with ping"
    sleep 2
    if ip netns exec "$NS_I" ping -c 3 -W 2 "$IP_T" >/dev/null; then
        echo "   OK: $IP_I -> $IP_T reachable. Only path is the physical"
        echo "   link between $NIC_I and $NIC_T, so traffic crosses the wire."
    else
        echo "   FAIL: no ping. Check the cable/switch between $NIC_I and"
        echo "   $NIC_T, that both have carrier (ip netns exec $NS_T ip -br link),"
        echo "   and that IP_T/IP_I share subnet /$PREFIX."
        exit 1
    fi
}

cmd_down() {
    echo ">> removing namespaces and returning NICs to root"
    # Stop the targets first with 'stop' — the nvmet configfs teardown must
    # nsenter into NS_T while it still exists. (We do not stop them here; the
    # nvmet port would otherwise leak in the now-deleted netns.)
    # Undo the gro/gso/tso 'up' enabled, while the NICs are still in their netns
    # (best-effort; the settings are per-netdev and survive the netns move).
    [ -n "${NIC_T:-}" ] && ip netns exec "$NS_T" ethtool -K "$NIC_T" gro off gso off tso off 2>/dev/null || true
    [ -n "${NIC_I:-}" ] && ip netns exec "$NS_I" ethtool -K "$NIC_I" gro off gso off tso off 2>/dev/null || true
    # Return NICs to root if we know their names; deleting the netns also
    # auto-returns physical NICs, so this is best-effort and env-tolerant.
    [ -n "${NIC_T:-}" ] && in_net "$NS_T" ip link set "$NIC_T" netns 1 2>/dev/null || true
    [ -n "${NIC_I:-}" ] && in_net "$NS_I" ip link set "$NIC_I" netns 1 2>/dev/null || true
    ip netns del "$NS_T" 2>/dev/null || true
    ip netns del "$NS_I" 2>/dev/null || true
    echo "   namespaces removed; NICs returned to root (reconfigure addresses as needed)."
}

# ---- targets: 'start'/'stop [SELECTOR]' route to one (or both) of these.
# The setup/teardown live in common.sh (nvmet_setup/nvmet_teardown,
# ioutgt_start/ioutgt_stop); realwire only supplies the NS_T addressing and the
# nvmet_exec/IOUTGT_NETNS context hooks (above). The post-connect affinity sync
# (ioutgt_sync_affinity) is realwire-specific and stays here.
# realwire's backends have no default (unlike local_tgt's /tmp files), so the
# `:?` expansion keeps the friendly "set NVMET_BACKEND..." abort.
start_one() {
    case "$1" in
        nvmet)  nvmet_setup  "$NVMET_NQN"  "$NVMET_PORT"  "$IP_T" \
                    "${NVMET_BACKEND:?set NVMET_BACKEND to the nvmet target backing file or block device}" ;;
        ioutgt) ioutgt_start "$IOUTGT_NQN" "$IOUTGT_PORT" "$IP_T" \
                    "${IOUTGT_BACKEND:?set IOUTGT_BACKEND to the ioutgt target backing file or block device}" ;;
    esac
}

stop_one() {
    case "$1" in
        nvmet)  nvmet_teardown "$NVMET_NQN" ;;
        ioutgt) ioutgt_stop ;;
    esac
}

# IRQ serving NIC queue index $2 of nic $1: combined "TxRx" else "rx", from the
# global /proc/interrupts (the NIC sits in NS_T but its IRQ labels persist).
# Distinct IRQs serving NIC queue index $2 of nic $1: combined "TxRx", or split
# "rx"/"tx" (one or two IRQs). From the global /proc/interrupts (the NIC sits in
# NS_T but its IRQ action labels persist).
nic_queue_irqs() {
    awk -v n="$1" -v q="$2" '
        $NF ~ ("^" n "-TxRx-" q "$") || $NF ~ ("^" n "-rx-" q "$") || $NF ~ ("^" n "-tx-" q "$") {
            irq=$1; sub(/:/,"",irq); print irq
        }' /proc/interrupts | sort -nu
}

# Hex CPU mask for one CPU as .../xps_cpus expects: comma-separated 32-bit
# words, high word first (cpu 4 -> "00000010", cpu 24 -> "01000000",
# cpu 32 -> "00000001,00000000", cpu 62 -> "40000000,00000000").
cpu_xps_mask() {
    local cpu="$1"
    local word=$((cpu / 32)) bit=$((cpu % 32)) i w out=""
    for ((i = word; i >= 0; i--)); do
        if [ "$i" -eq "$word" ]; then printf -v w '%08x' $((1 << bit)); else w='00000000'; fi
        out="${out:+$out,}$w"
    done
    printf '%s' "$out"
}

# Converge NIC_T's rx/tx queue IRQs and ioutgt's io-thread CPUs. Run AFTER
# connect: the queue-thread pool spawns lazily on the first connection and
# `ioutgt list` reports each IO queue's pthread tid + full online CPU group
# (all CPUs in its group_cpus_evenly group) only once it is connected. Per NIC
# queue i (== qid i+1 == io-thread i):
#   1. push the io-thread's whole CPU group onto the queue's rx/tx IRQ
#      smp_affinity (NIC follows ioutgt -- a no-op on a managed/read-only IRQ);
#   2. read the rx/tx IRQ *effective* affinity, combine it, and taskset the
#      io-thread (by tid) to that combination (io-thread follows where the IRQ
#      softirq actually lands -- the only direction available for managed IRQs).
#   3. XPS: map the io-thread's CPU -> this queue's tx ring (xps_cpus), so the
#      thread's sends egress here and the TX completion IRQ lands on the same
#      CPU (the heavy direction for reads).
# Steps 1-3 only align queue<->thread CPUs; they do NOT decide which queue a
# *flow* uses (RSS picks the RX queue, decoupled from the qid->io-thread route);
# per-flow RX co-location is added separately via hardware ntuple rules.
# We deliberately DISABLE software RPS/RFS: it relocates RX softirqs to the
# consumer CPU with smp_call_function IPIs (net_rps_send_ipi) -- measured as a
# ~33k/s Function-call-interrupt storm for no throughput gain -- and its knobs
# persist across runs (a prior aRFS run poisons later ones), so the sync clears
# them every time. ALL privileged work runs here in the (root) harness, so the
# target needs no privileges. irqbalance would fight the pinning, so stop it.
ioutgt_sync_affinity() {
    [ -n "${NIC_T:-}" ] || { echo "   (NIC_T unset; skipping IRQ affinity sync)"; return 0; }
    command -v jq >/dev/null 2>&1 || { echo "   (jq not found; skipping IRQ affinity sync)"; return 0; }
    local json rows
    json="$("$IOUTGT_BIN" ctl --socket "$IOUTGT_SOCK" '{"op":"LIST_CONTROLLER"}' 2>/dev/null || true)"
    # qid, tid, active CPU, full online CPU group (cpulist), peer ip:port -- all
    # single whitespace-free tokens.
    rows="$(printf '%s' "$json" \
        | jq -r '.data.controllers[]?.queues[]? | select(.qid >= 1) | "\(.qid) \(.tid) \(.cpus) \(.group_cpus) \(.peer)"' \
            2>/dev/null | sort -n -u || true)"
    if [ -z "$rows" ]; then
        echo "   (no connected IO queues; run 'connect' first)"; return 0
    fi
    systemctl stop irqbalance 2>/dev/null || true
    echo ">> converging $NIC_T queue IRQ affinity <-> ioutgt io-threads"
    local qid tid cpus group peer sport nicq irqs irq combo eff pushed xcpu xps
    while read -r qid tid cpus group peer; do
        [ -n "$qid" ] || continue
        nicq=$((qid - 1))
        irqs="$(nic_queue_irqs "$NIC_T" "$nicq")"
        if [ -z "$irqs" ]; then
            echo "   q$nicq (qid $qid): no NIC IRQ found; skipped"; continue
        fi
        combo=""; pushed=""
        for irq in $irqs; do
            # 1. push the io-thread's whole CPU group onto the (unmanaged) IRQ,
            #    giving the kernel the full group to place the IRQ within. A
            #    valid cpulist only -- "*"/"?" (unpinned/unknown) can't be set.
            case "$group" in
                ''|'*'|'?'|*[!0-9,-]*) ;;
                *) if echo "$group" > "/proc/irq/$irq/smp_affinity_list" 2>/dev/null; then
                       pushed="${pushed:+$pushed,}$irq"
                   fi ;;
            esac
            # 2. collect this IRQ's effective affinity for the combination.
            eff="$(cat "/proc/irq/$irq/effective_affinity_list" 2>/dev/null || true)"
            [ -n "$eff" ] && combo="${combo:+$combo,}$eff"
        done
        if [ -n "$combo" ] && taskset -cp "$combo" "$tid" >/dev/null 2>&1; then
            # 3. XPS: a send from this io-thread's CPU egresses tx-$nicq, so the
            #    TX completion IRQ lands on the same CPU. xps_cpus is netdev
            #    sysfs (the NIC is in NS_T), and wants a hex CPU bitmask.
            xcpu="${combo%%[,-]*}"; xps=skip
            case "$xcpu" in
                ''|*[!0-9]*) ;;
                *) if ip netns exec "$NS_T" bash -c \
                        "echo $(cpu_xps_mask "$xcpu") > /sys/class/net/$NIC_T/queues/tx-$nicq/xps_cpus" \
                        2>/dev/null; then xps="cpu $xcpu"; fi ;;
            esac
            echo "   q$nicq irq[$(echo $irqs | tr '\n' ' ')] group=$group pushed=[${pushed:-none}] -> io-thread tid $tid aff=$combo, xps tx-$nicq=$xps (was cpu $cpus)"
        else
            echo "   q$nicq irq[$(echo $irqs | tr '\n' ' ')] group=$group pushed=[${pushed:-none}]; taskset tid $tid to '$combo' failed"
        fi
    done <<EOF
$rows
EOF
    # Disable software RPS/RFS (the net_rps_send_ipi storm). These knobs persist
    # across runs, so clear them every sync: the global flow table and, on each
    # NIC_T rx queue, rps_flow_cnt (RFS) and rps_cpus (plain RPS).
    echo 0 > /proc/sys/net/core/rps_sock_flow_entries 2>/dev/null || true
    ip netns exec "$NS_T" bash -c '
        for q in /sys/class/net/'"$NIC_T"'/queues/rx-*; do
            echo 0 > "$q/rps_flow_cnt" 2>/dev/null
            echo 0 > "$q/rps_cpus" 2>/dev/null
        done' 2>/dev/null || true
    echo "   RPS/RFS disabled (RX softirqs stay on their queue CPU; no relocation IPIs)"

    # Hardware ntuple RX steering: have the NIC deliver each connection's RX
    # directly to its io-thread's queue (qid-1) -- no software RFS, no IPI, and
    # stable (a fixed rule, not an adaptive guess). Match the inbound flow by the
    # host's ephemeral source port (unique per connection) + our listen port.
    # The combined channel shares the CPU with tx (and XPS steers tx there too).
    ip netns exec "$NS_T" ethtool -K "$NIC_T" ntuple on >/dev/null 2>&1 || true
    # Clear stale rules (previous runs' source ports) for a clean slate.
    ip netns exec "$NS_T" bash -c 'ethtool -n '"$NIC_T"' 2>/dev/null | awk "/Filter:/{print \$2}" \
        | while read -r id; do ethtool -N '"$NIC_T"' delete "$id" >/dev/null 2>&1; done' 2>/dev/null || true
    echo ">> steering each flow to its io-thread queue via NIC ntuple (no IPI)"
    while read -r qid tid cpus group peer; do
        [ -n "$qid" ] || continue
        nicq=$((qid - 1)); sport="${peer##*:}"
        case "$sport" in ''|*[!0-9]*) echo "   q$nicq: no peer port ($peer); skipped"; continue ;; esac
        if ip netns exec "$NS_T" ethtool -N "$NIC_T" flow-type tcp4 \
                src-port "$sport" dst-port "$IOUTGT_PORT" action "$nicq" >/dev/null 2>&1; then
            echo "   q$nicq: src-port $sport -> rx queue $nicq (hardware)"
        else
            echo "   q$nicq: ntuple rule (src-port $sport) rejected"
        fi
    done <<EOF
$rows
EOF
}

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
    if [ -n "${NIC_T:-}" ]; then
        echo "== $NIC_T queue IRQ vs ioutgt io-thread (live) affinity =="
        # `is-active` exits non-zero (and still prints the state) when not
        # running, so swallow the status rather than appending "unknown".
        echo "  irqbalance: $(systemctl is-active irqbalance 2>/dev/null || true)"
        # Per IO queue: the io-thread's LIVE affinity (re-read from `list`, so
        # post-connect re-pinning is reflected) beside its NIC queue IRQ's
        # effective CPU -- the two should match after a sync.
        local rows
        rows="$("$IOUTGT_BIN" ctl --socket "$IOUTGT_SOCK" '{"op":"LIST_CONTROLLER"}' 2>/dev/null \
            | jq -r '.data.controllers[]?.queues[]? | select(.qid >= 1) | "\(.qid) \(.tid) \(.cpus) \(.group_cpus)"' \
                2>/dev/null | sort -n -u || true)"
        if [ -z "$rows" ]; then
            echo "  (no connected IO queues)"
        else
            # Note: this verifies the SYNC INVARIANT (io-thread CPU == its
            # NIC-queue IRQ effective CPU). It does NOT prove a given flow is
            # co-located: the NIC steers a flow to a queue by RSS/tx-hash,
            # independent of which qid (io-thread) serves it -- so a flow on
            # io-thread T may ride a different NIC queue whose IRQ is elsewhere.
            # True per-flow co-location needs hardware ntuple steering (rx) +
            # XPS (tx) -- not software RPS/RFS, which co-locates via IPI storm.
            local qid tid cpus group nicq irqs irq eff verdict mism=0
            while read -r qid tid cpus group; do
                [ -n "$qid" ] || continue
                nicq=$((qid - 1))
                irqs="$(nic_queue_irqs "$NIC_T" "$nicq")"
                eff=""
                for irq in $irqs; do
                    eff="${eff:+$eff,}$(cat "/proc/irq/$irq/effective_affinity_list" 2>/dev/null || echo '?')"
                done
                if [ "$cpus" = "$eff" ]; then
                    verdict=OK
                else
                    verdict=MISMATCH; mism=$((mism + 1))
                fi
                printf "  q%-2s io-thread(tid %s) aff=%-10s group=%-12s | irq[%s] eff=%-10s %s\n" \
                    "$nicq" "$tid" "$cpus" "$group" "$(echo $irqs | tr '\n' ' ' | sed 's/ $//')" "${eff:-?}" "$verdict"
            done <<EOF
$rows
EOF
            if [ "$mism" -eq 0 ]; then
                echo "  sync invariant: OK (every io-thread CPU == its NIC queue IRQ effective)"
            else
                echo "  sync invariant: $mism queue(s) MISMATCHED -- re-run 'connect' (or irqbalance restarted?)"
            fi
        fi
    fi
}

# Selector verbs take 'nvmet' or 'ioutgt'; omitting it acts on BOTH.
case "${1:-}" in
    up)                  cmd_up ;;
    down)                cmd_down ;;
    start)               run_for_targets start_one      "${2:-}" ;;
    stop)                run_for_targets stop_one       "${2:-}" ;;
    discover)            run_for_targets discover_one   "${2:-}" ;;
    connect)             run_for_targets connect_one    "${2:-}"
                         # IRQ affinity sync needs the IO queues connected
                         # (their pthread tids appear in `ioutgt list`).
                         case "${2:-}" in ioutgt|"") ioutgt_sync_affinity ;; esac ;;
    disconnect)          run_for_targets disconnect_one "${2:-}" ;;
    fio)                 run_for_targets fio_one        "${2:-}" ;;
    status)              cmd_status ;;
    help|usage)          usage ;;
    *) usage >&2; exit 1 ;;
esac
