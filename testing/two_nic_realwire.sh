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
HOSTNQN="nqn.2026-06.io.realwire:host"

# Map a target kind ('nvmet' | 'ioutgt') to its "PORT NQN" pair.
target_params() {
    case "${1:-}" in
        ioutgt) echo "$IOUTGT_PORT $IOUTGT_NQN" ;;
        nvmet) echo "$NVMET_PORT $NVMET_NQN" ;;
        *) echo "specify target: nvmet | ioutgt" >&2; return 1 ;;
    esac
}

# Run a per-target function $1 for the selected target $2, or for BOTH
# targets (ioutgt then nvmet) when no selector is given. Every selector verb
# (start/stop/discover/connect/disconnect/fio) dispatches through this.
run_for_targets() {
    local fn="$1"
    case "${2:-}" in
        ioutgt|nvmet) "$fn" "$2" ;;
        "")           "$fn" ioutgt; "$fn" nvmet ;;
        *) echo "specify target: nvmet | ioutgt (or omit for both)" >&2; exit 1 ;;
    esac
}

# Per-target backing (file backing only — a regular file or block device).
# Each target has its OWN, so a single env setup drives both at once. A
# missing non-/dev path is auto-created at BACKEND_GB; a /dev/* path must
# already exist. Each is validated only when its target is started.
NVMET_BACKEND="${NVMET_BACKEND:-}"   # nvmet device_path
IOUTGT_BACKEND="${IOUTGT_BACKEND:-}"   # ioutgt --backend
BACKEND_GB="${BACKEND_GB:-2}"          # size of an auto-created backing file

# Queueing, capped TARGET-side on both targets and also requested by the
# initiator, so each side grants min(host request, target cap):
#   ioutgt : --io-threads / --io-queue-size
#   nvmet  : subsystem attr_qid_max / port param_max_queue_size
#   connect: --nr-io-queues / --queue-size (so the host asks for that many)
NR_QUEUES="${NR_QUEUES:-4}"       # IO queues
QUEUE_SIZE="${QUEUE_SIZE:-128}"   # IO qdepth

# ioutgt target-process knobs
IOUTGT_BIN="${IOUTGT_BIN:-./target/release/ioutgt}"
IOUTGT_PIDFILE="${IOUTGT_PIDFILE:-/tmp/ioutgt-realwire.pid}"
IOUTGT_LOG="${IOUTGT_LOG:-/tmp/ioutgt-realwire.log}"

# fio knobs
FIO_RW="${FIO_RW:-randread}"
FIO_BS="${FIO_BS:-4k}"
FIO_QD="${FIO_QD:-32}"
FIO_JOBS="${FIO_JOBS:-4}"
FIO_SECS="${FIO_SECS:-30}"

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
Knobs: BACKEND_GB=$BACKEND_GB NR_QUEUES=$NR_QUEUES QUEUE_SIZE=$QUEUE_SIZE
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

# Ensure $BACKEND exists; both targets use it verbatim (nvmet auto-detects
# file vs block device; ioutgt opens it, never creates it). A missing
# non-/dev path is auto-created at BACKEND_GB; a missing /dev/* is an error.
ensure_backing() {
    case "$BACKEND" in
        /dev/*) [ -e "$BACKEND" ] || { echo "block device $BACKEND does not exist" >&2; return 1; } ;;
        /*)     [ -e "$BACKEND" ] || { echo "   creating backing file $BACKEND (${BACKEND_GB}G)" >&2
                                       truncate -s "${BACKEND_GB}G" "$BACKEND" \
                                         || { echo "failed to create $BACKEND" >&2; return 1; }; } ;;
        *)      echo "BACKEND must be an absolute file or block-device path" >&2; return 1 ;;
    esac
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
    echo ">> starting ioutgt in $NS_T on $IP_T:$PORT (backend $BACKEND, ${NR_QUEUES}q x $QUEUE_SIZE)"
    # ioutgt is pure userspace: ip netns exec is fine (no configfs), and its
    # bind() lands in NS_T so the listener is on the wire-facing NIC.
    ip netns exec "$NS_T" "$IOUTGT_BIN" \
        --listen "$IP_T:$PORT" \
        --backend "$BACKEND" \
        --io-threads "$NR_QUEUES" \
        --io-queue-size "$QUEUE_SIZE" \
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

# ---- initiator (runs in NS_I) — each takes a 'nvmet'|'ioutgt' target ----
discover_one() {
    local port nqn; read -r port nqn < <(target_params "${1:-}") || exit 1
    modprobe nvme-tcp
    ip netns exec "$NS_I" nvme discover -t tcp -a "$IP_T" -s "$port" --hostnqn "$HOSTNQN"
}

connect_one() {
    local port nqn; read -r port nqn < <(target_params "${1:-}") || exit 1
    modprobe nvme-tcp
    echo ">> connecting $1 from $NS_I -> $IP_T:$port (request ${NR_QUEUES}q x $QUEUE_SIZE)"
    # The host TCP socket is created in NS_I (current netns), so the data
    # path egresses NIC_I, crosses the wire, and reaches NIC_T in NS_T. The
    # kernel keeps using that socket afterward regardless of which netns I/O
    # is later submitted from.
    #
    # -i/-q make the host REQUEST this many queues / this depth; the target
    # (ioutgt flags or nvmet attr_qid_max/param_max_queue_size) caps it, so
    # the granted values are min(host request, target cap).
    ip netns exec "$NS_I" nvme connect -t tcp -a "$IP_T" -s "$port" \
        -n "$nqn" --hostnqn "$HOSTNQN" \
        --nr-io-queues "$NR_QUEUES" --queue-size "$QUEUE_SIZE"
    local dev
    if dev=$(wait_dev "$nqn"); then
        echo "   block device: $dev (controller $(find_ctrl "$nqn"), nqn $nqn)"
    else
        echo "   connected ($nqn) but no namespace block device appeared after 10s"
        echo "   controller: $(find_ctrl "$nqn" || echo '?'); check 'nvme list' / target namespace config"
    fi
}

disconnect_one() {
    local port nqn; read -r port nqn < <(target_params "${1:-}") || exit 1
    ip netns exec "$NS_I" nvme disconnect -n "$nqn" 2>/dev/null || true
    echo ">> disconnected $1 ($nqn)"
}

# Find the namespace block device for an NQN via sysfs (the device node is
# global; only the connection lived in NS_I). Resolve through
# /sys/block/*/device/subsysnqn — schema-independent and multipath-safe:
# with native NVMe multipath (default-on) the head device nvmeXnZ is NOT
# under the controller's sysfs dir (only the per-path node nvmeXcYnZ is), so
# the old /sys/class/nvme/nvmeN walk found nothing even when the device
# existed. A block dev's device/subsysnqn resolves to its controller or
# subsystem in either layout.
find_dev() {
    local nqn="$1" blk name head
    for blk in /sys/block/nvme*n*; do
        [ -e "$blk" ] || continue
        name=$(basename "$blk")
        case "$name" in *p[0-9]*) continue ;; esac      # skip partitions
        [ -r "$blk/device/subsysnqn" ] || continue
        [ "$(cat "$blk/device/subsysnqn")" = "$nqn" ] || continue
        # A match may be a per-path node nvmeXcYnZ (no /dev entry) — map it
        # to its head nvmeXnZ; head names pass through unchanged.
        if [[ $name =~ ^(nvme[0-9]+)c[0-9]+(n[0-9]+)$ ]]; then
            head="${BASH_REMATCH[1]}${BASH_REMATCH[2]}"
        else
            head="$name"
        fi
        [ -b "/dev/$head" ] && { echo "/dev/$head"; return 0; }
    done
    return 1
}

# Controller node (/dev/nvmeN) for an NQN, on stdout.
find_ctrl() {
    local nqn="$1" c
    for c in /sys/class/nvme/nvme*; do
        [ -r "$c/subsysnqn" ] || continue
        [ "$(cat "$c/subsysnqn")" = "$nqn" ] && { echo "/dev/$(basename "$c")"; return 0; }
    done
    return 1
}

# Poll up to ~10s for the namespace block device of $nqn, nudging a rescan
# each tick: namespace enumeration can lag the connect (more so on large
# devices), so a single check right after connect races and misses it.
wait_dev() {
    local nqn="$1" dev ctrl
    local deadline=$(( SECONDS + 10 ))
    while [ "$SECONDS" -lt "$deadline" ]; do
        dev=$(find_dev "$nqn") && { echo "$dev"; return 0; }
        # Nudge a namespace rescan on the matching controller. The `|| true`
        # is load-bearing: under `set -e` a non-zero ns-rescan would abort.
        ctrl=$(find_ctrl "$nqn") && nvme ns-rescan "$ctrl" 2>/dev/null || true
        sleep 0.5
    done
    return 1
}

fio_one() {
    local port nqn; read -r port nqn < <(target_params "${1:-}") || exit 1
    local dev; dev=$(find_dev "$nqn") || { echo "no connected device for $1 ($nqn); run 'connect $1' first"; exit 1; }
    echo ">> fio on $dev [$1]  ($FIO_RW bs=$FIO_BS qd=$FIO_QD jobs=$FIO_JOBS ${FIO_SECS}s)"
    # fio can run in root: once connected, I/O rides the kernel socket in
    # NS_I across the wire no matter where fio submits from.
    fio --name=realwire --filename="$dev" --rw="$FIO_RW" --bs="$FIO_BS" \
        --iodepth="$FIO_QD" --numjobs="$FIO_JOBS" --ioengine=io_uring \
        --direct=1 --runtime="$FIO_SECS" --time_based --group_reporting
}

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
