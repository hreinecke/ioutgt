#!/usr/bin/env bash
# common.sh — shared helpers for the NVMe/TCP target drivers
# (two_nic_realwire.sh, local_tgt.sh). Sourced, never executed.
#
# The sourcing script supplies the transport context; these helpers stay
# agnostic to whether the initiator runs in a network namespace (realwire)
# or directly on loopback (local_tgt):
#
#   TARGET_IP             IP the initiator dials / the target listens on
#   ini_exec <cmd...>     run an nvme-cli command in the initiator context
#                         (`ip netns exec NS_I ...` for realwire; a direct
#                         passthrough for local_tgt)
#   IOUTGT_PORT/_NQN, NVMET_PORT/_NQN, HOSTNQN   per-target addressing
#
# The target-start functions additionally set a caller-local BACKEND that
# ensure_backing/fio see via bash dynamic scope.

# ---- shared knobs (override via environment) -------------------------
# Fixed host identity. nvme-cli generates a RANDOM hostid per invocation
# when none is given, but the kernel requires one hostnqn to map to exactly
# one hostid — so connecting to the second target with the same HOSTNQN but
# a fresh random hostid is rejected ("same hostnqn but different hostid").
# Pin both, shared across all connects from this host.
HOSTID="${HOSTID:-2e3b0c44-1c2e-4f3a-9b6d-000000000001}"

NR_QUEUES="${NR_QUEUES:-4}"          # IO queues  (ioutgt --io-threads; connect -i)
QUEUE_SIZE="${QUEUE_SIZE:-128}"      # IO qdepth   (ioutgt --io-queue-size; connect -q)
BACKEND_GB="${BACKEND_GB:-2}"        # size of an auto-created backing file
IOUTGT_BIN="${IOUTGT_BIN:-./target/release/ioutgt}"
IOUTGT_SENDZC="${IOUTGT_SENDZC:-0}"  # 1 = ioutgt --send-zc (zero-copy send)

# fio knobs
FIO_RW="${FIO_RW:-randread}"
FIO_BS="${FIO_BS:-4k}"
FIO_QD="${FIO_QD:-32}"
FIO_JOBS="${FIO_JOBS:-4}"
FIO_SECS="${FIO_SECS:-30}"

require_root() { [ "$(id -u)" -eq 0 ] || { echo "must run as root (use sudo)"; exit 1; }; }

# Map a target kind ('nvmet'|'ioutgt') to its "PORT NQN" pair.
target_params() {
    case "${1:-}" in
        ioutgt) echo "$IOUTGT_PORT $IOUTGT_NQN" ;;
        nvmet)  echo "$NVMET_PORT $NVMET_NQN" ;;
        *) echo "specify target: nvmet | ioutgt" >&2; return 1 ;;
    esac
}

# Run a per-target function $1 for the selected target $2, or for BOTH
# targets (ioutgt then nvmet) when no selector is given.
run_for_targets() {
    local fn="$1"
    case "${2:-}" in
        ioutgt|nvmet) "$fn" "$2" ;;
        "")           "$fn" ioutgt; "$fn" nvmet ;;
        *) echo "specify target: nvmet | ioutgt (or omit for both)" >&2; exit 1 ;;
    esac
}

# Ensure $BACKEND (a caller local) exists. A missing non-/dev path is
# auto-created at BACKEND_GB; a missing /dev/* is an error.
ensure_backing() {
    case "$BACKEND" in
        /dev/*) [ -e "$BACKEND" ] || { echo "block device $BACKEND does not exist" >&2; return 1; } ;;
        /*)     [ -e "$BACKEND" ] || { echo "   creating backing file $BACKEND (${BACKEND_GB}G)" >&2
                                       truncate -s "${BACKEND_GB}G" "$BACKEND" \
                                         || { echo "failed to create $BACKEND" >&2; return 1; }; } ;;
        *)      echo "BACKEND must be an absolute file or block-device path" >&2; return 1 ;;
    esac
}

# Namespace block device for an NQN via sysfs (/sys/block/*/device/subsysnqn)
# — schema-independent and multipath-safe: with native NVMe multipath the
# head device nvmeXnZ is not under the controller's sysfs dir (only the
# per-path node nvmeXcYnZ is), so a /sys/class/nvme walk misses it; a block
# dev's device/subsysnqn resolves to its controller or subsystem in either
# layout. A per-path match (nvmeXcYnZ, no /dev entry) maps to its head.
find_dev() {
    local nqn="$1" blk name head
    for blk in /sys/block/nvme*n*; do
        [ -e "$blk" ] || continue
        name=$(basename "$blk")
        case "$name" in *p[0-9]*) continue ;; esac      # skip partitions
        [ -r "$blk/device/subsysnqn" ] || continue
        [ "$(cat "$blk/device/subsysnqn")" = "$nqn" ] || continue
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
# each tick: namespace enumeration can lag the connect.
wait_dev() {
    local nqn="$1" dev ctrl
    local deadline=$(( SECONDS + 10 ))
    while [ "$SECONDS" -lt "$deadline" ]; do
        dev=$(find_dev "$nqn") && { echo "$dev"; return 0; }
        # `|| true` is load-bearing under `set -e`: a non-zero rescan aborts.
        ctrl=$(find_ctrl "$nqn") && nvme ns-rescan "$ctrl" 2>/dev/null || true
        sleep 0.5
    done
    return 1
}

# ---- initiator verbs (transport via ini_exec + TARGET_IP) ------------
# Each takes a 'nvmet'|'ioutgt' selector. The nvme-cli command that creates
# the host socket runs through ini_exec (so realwire egresses NIC_I); the
# sysfs device lookups run in the current process (device nodes are global).
discover_one() {
    local port nqn; read -r port nqn < <(target_params "${1:-}") || exit 1
    modprobe nvme-tcp
    ini_exec nvme discover -t tcp -a "$TARGET_IP" -s "$port" \
        --hostnqn "$HOSTNQN" --hostid "$HOSTID"
}

connect_one() {
    local port nqn; read -r port nqn < <(target_params "${1:-}") || exit 1
    modprobe nvme-tcp
    echo ">> connecting $1 -> $TARGET_IP:$port (request ${NR_QUEUES}q x $QUEUE_SIZE)"
    # -i/-q make the host REQUEST this many queues / this depth; the target
    # caps it, so the granted values are min(host request, target cap).
    ini_exec nvme connect -t tcp -a "$TARGET_IP" -s "$port" \
        -n "$nqn" --hostnqn "$HOSTNQN" --hostid "$HOSTID" \
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
    ini_exec nvme disconnect -n "$nqn" 2>/dev/null || true
    echo ">> disconnected $1 ($nqn)"
}

fio_one() {
    local port nqn; read -r port nqn < <(target_params "${1:-}") || exit 1
    local dev; dev=$(find_dev "$nqn") || { echo "no connected device for $1 ($nqn); run 'connect $1' first"; exit 1; }
    echo ">> fio on $dev [$1]  ($FIO_RW bs=$FIO_BS qd=$FIO_QD jobs=$FIO_JOBS ${FIO_SECS}s)"
    fio --name=nvmetcp --filename="$dev" --rw="$FIO_RW" --bs="$FIO_BS" \
        --iodepth="$FIO_QD" --numjobs="$FIO_JOBS" --ioengine=io_uring \
        --direct=1 --runtime="$FIO_SECS" --time_based --group_reporting
}
