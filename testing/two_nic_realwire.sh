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
. "$(dirname "$0")/common.sh"

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
    # Return NICs to root if we know their names; deleting the netns also
    # auto-returns physical NICs, so this is best-effort and env-tolerant.
    [ -n "${NIC_T:-}" ] && in_net "$NS_T" ip link set "$NIC_T" netns 1 2>/dev/null || true
    [ -n "${NIC_I:-}" ] && in_net "$NS_I" ip link set "$NIC_I" netns 1 2>/dev/null || true
    ip netns del "$NS_T" 2>/dev/null || true
    ip netns del "$NS_I" 2>/dev/null || true
    echo "   namespaces removed; NICs returned to root (reconfigure addresses as needed)."
}

# ---- targets: 'start'/'stop [SELECTOR]' route to one (or both) of these
start_one() {
    case "$1" in
        nvmet)  cmd_nvmet_target ;;
        ioutgt) cmd_ioutgt_target ;;
    esac
}

stop_one() {
    case "$1" in
        nvmet)  cmd_nvmet_target_down ;;
        ioutgt) cmd_ioutgt_target_down ;;
    esac
}

# ---- nvmet-tcp target (Linux in-kernel; runs in NS_T) ----------------
cmd_nvmet_target() {
    local NQN=$NVMET_NQN PORT=$NVMET_PORT
    local BACKEND=${NVMET_BACKEND:?set NVMET_BACKEND to the nvmet target backing file or block device}
    echo ">> setting up nvmet-tcp target in $NS_T (backend $BACKEND)"
    modprobe nvmet
    modprobe nvmet-tcp
    ensure_backing || exit 1

    local cfg=/sys/kernel/config/nvmet
    # All configfs writes run via nsenter so the port's listener socket is
    # created inside NS_T (see in_net comment). configfs is a global
    # singleton, so the tree we write is the same one the kernel uses.
    in_net "$NS_T" bash -euc "
        cfg=$cfg
        sub=\$cfg/subsystems/$NQN
        mkdir -p \$sub
        echo 1 > \$sub/attr_allow_any_host
        # nr_queues -> nvmet's per-subsystem max queue id (qid 1..N).
        echo $NR_QUEUES > \$sub/attr_qid_max
        mkdir -p \$sub/namespaces/1
        echo -n $BACKEND > \$sub/namespaces/1/device_path
        # Force O_DIRECT on a file backend (parity with ioutgt's default);
        # must precede enable. Ignored for a block device.
        echo 0 > \$sub/namespaces/1/buffered_io 2>/dev/null || true
        echo 1 > \$sub/namespaces/1/enable

        # Claim a FREE configfs port id; the port tree is global (the netns
        # only scopes the listener SOCKET), so hardcoding port 1 would hijack
        # an existing nvmet port on the host ('Disable port before changing
        # attribute'). Never touch a port we did not create.
        pid=1; while [ -e \"\$cfg/ports/\$pid\" ]; do pid=\$((pid + 1)); done
        portdir=\$cfg/ports/\$pid
        mkdir \"\$portdir\"
        echo ipv4 > \"\$portdir/addr_adrfam\"
        echo $IP_T > \"\$portdir/addr_traddr\"
        echo $PORT > \"\$portdir/addr_trsvcid\"
        echo tcp  > \"\$portdir/addr_trtype\"
        # queue_size -> advertised per-queue depth (SQSIZE/MAXCMD); must be
        # set BEFORE the port is enabled (the symlink) or the kernel -EACCES.
        echo $QUEUE_SIZE > \"\$portdir/param_max_queue_size\"
        # Linking the subsystem ENABLES the port -> creates the listener
        # socket, in THIS process's netns (NS_T). That is the whole point.
        ln -sf \$sub \"\$portdir/subsystems/$NQN\"
        echo \"   configfs port id \$pid (qid_max=$NR_QUEUES, max_queue_size=$QUEUE_SIZE)\"
    "
    echo "   listening on $IP_T:$PORT, subsystem $NQN, backend $BACKEND"
}

cmd_nvmet_target_down() {
    local NQN=$NVMET_NQN
    local cfg=/sys/kernel/config/nvmet
    echo ">> removing nvmet-tcp target"
    # The port id was claimed dynamically, so find OUR port by its NQN
    # symlink and remove only that one — never another target's port.
    in_net "$NS_T" bash -c "
        cfg=$cfg
        for link in \"\$cfg\"/ports/*/subsystems/$NQN; do
            [ -e \"\$link\" ] || continue
            portdir=\$(dirname \"\$(dirname \"\$link\")\")
            rm -f \"\$link\"
            rmdir \"\$portdir\" 2>/dev/null || true
        done
        echo 0 > \$cfg/subsystems/$NQN/namespaces/1/enable 2>/dev/null || true
        rmdir  \$cfg/subsystems/$NQN/namespaces/1 2>/dev/null || true
        rmdir  \$cfg/subsystems/$NQN 2>/dev/null || true
    " || true
}

# ---- ioutgt target (runs in NS_T) ------------------------------------
cmd_ioutgt_target() {
    local NQN=$IOUTGT_NQN PORT=$IOUTGT_PORT
    local BACKEND=${IOUTGT_BACKEND:?set IOUTGT_BACKEND to the ioutgt target backing file or block device}
    [ -x "$IOUTGT_BIN" ] || { echo "build first: cargo build --release -p ioutgt (or set IOUTGT_BIN)"; exit 1; }
    ensure_backing || exit 1
    local zc=() zclabel=
    if [ "$IOUTGT_SENDZC" != 0 ]; then
        zc=(--send-zc); zclabel=", send-zc"
        # --send-zc uses SENDMSG_ZC, which pins payload pages against
        # RLIMIT_MEMLOCK. Under the default 8 MiB limit two in-flight batches
        # alone can exceed it; the kernel then returns ENOMEM/ENOBUFS and
        # ioutgt silently falls back to a copying send (correct, but no ZC
        # benefit). Raise the limit (inherited by the ioutgt child below) so
        # ZC actually engages. Best-effort: we already require root, but keep
        # going if it cannot be raised.
        ulimit -l unlimited 2>/dev/null || true
    fi
    echo ">> starting ioutgt in $NS_T on $IP_T:$PORT (backend $BACKEND, ${NR_QUEUES}q x $QUEUE_SIZE$zclabel)"
    # ioutgt is pure userspace: ip netns exec is fine (no configfs), and its
    # bind() lands in NS_T so the listener is on the wire-facing NIC.
    ip netns exec "$NS_T" "$IOUTGT_BIN" \
        --listen "$IP_T:$PORT" \
        --backend "$BACKEND" \
        --io-threads "$NR_QUEUES" \
        --io-queue-size "$QUEUE_SIZE" \
        "${zc[@]}" \
        "${IOUTGT_DGST[@]}" \
        --subsys-nqn "$NQN" \
        --control-socket /tmp/ioutgt-realwire.sock \
        >"$IOUTGT_LOG" 2>&1 &
    echo $! > "$IOUTGT_PIDFILE"
    sleep 1
    if kill -0 "$(cat "$IOUTGT_PIDFILE")" 2>/dev/null; then
        echo "   pid $(cat "$IOUTGT_PIDFILE"), log $IOUTGT_LOG"
    else
        echo "   ioutgt exited immediately; log follows:"; cat "$IOUTGT_LOG"; exit 1
    fi
}

cmd_ioutgt_target_down() {
    [ -f "$IOUTGT_PIDFILE" ] && kill "$(cat "$IOUTGT_PIDFILE")" 2>/dev/null || true
    rm -f "$IOUTGT_PIDFILE"
    echo ">> ioutgt stopped"
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
}

# Selector verbs take 'nvmet' or 'ioutgt'; omitting it acts on BOTH.
case "${1:-}" in
    up)                  cmd_up ;;
    down)                cmd_down ;;
    start)               run_for_targets start_one      "${2:-}" ;;
    stop)                run_for_targets stop_one       "${2:-}" ;;
    discover)            run_for_targets discover_one   "${2:-}" ;;
    connect)             run_for_targets connect_one    "${2:-}" ;;
    disconnect)          run_for_targets disconnect_one "${2:-}" ;;
    fio)                 run_for_targets fio_one        "${2:-}" ;;
    status)              cmd_status ;;
    help|usage)          usage ;;
    *) usage >&2; exit 1 ;;
esac
