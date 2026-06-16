#!/bin/bash
# Capture a real kernel-host <-> kernel-nvmet NVMe/TCP session on loopback
# for byte-exact codec fixtures (docs/fixtures/). Run as root:
#
#   sudo testing/capture-nvmet-fixtures.sh
#   sudo BACKING=/dev/nvme0n1 NR_QUEUES=8 testing/capture-nvmet-fixtures.sh
#
# Knobs are env vars (set them before the command; sudo passes them on):
#   BACKING     backing file/bdev; default a fresh /tmp temp file we own
#   SIZE_MB     temp backing size, MB (default 64; ignored when BACKING set)
#   NR_QUEUES   nvmet subsystem attr_qid_max           (default 16)
#   QUEUE_SIZE  nvmet port param_max_queue_size        (default 128)
#   PORT        TCP service port on 127.0.0.1          (default 4420)
#
# The session reads only (discover, connect, identify, dd reads): it never
# writes to the connected namespace, so a caller-supplied BACKING (e.g. a
# real /dev/nvmeXn1) is read but never modified — even a wrong-node bug
# cannot destroy data.
#
# Safe to re-run on a box that already serves other nvmet targets: it
# claims a free configfs port id (never touches an existing port) and
# tears down its own :fixture subsystem left by a prior aborted run. The
# file backend is opened O_DIRECT. Produces docs/fixtures/nvmet-session.pcap.
# Requires: nvmet-tcp and nvme-tcp modules, nvme-cli, tcpdump.
set -euo pipefail

SIZE_MB=${SIZE_MB:-64}
NR_QUEUES=${NR_QUEUES:-16}
QUEUE_SIZE=${QUEUE_SIZE:-128}
PORT=${PORT:-4420}
NQN="nqn.2026-06.io.ioutgt:fixture"
CFG=/sys/kernel/config/nvmet
OUT="$(dirname "$0")/../docs/fixtures"
PORTDIR=""

# A caller-supplied BACKING is used as-is (SIZE_MB ignored); otherwise a
# fresh temp file we own and delete on exit.
if [ -n "${BACKING:-}" ]; then
    OWN_BACKING=0
else
    BACKING=$(mktemp /tmp/ioutgt-fixture-XXXX.img)
    OWN_BACKING=1
    truncate -s "${SIZE_MB}M" "$BACKING"
fi

# Idempotent teardown of *our* fixture only: disconnect, drop our
# subsystem's symlink from every port, remove the port we created this run
# (PORTDIR; empty until we claim one), then disable + remove our namespaces
# and subsystem. Never touches another target's port or subsystem. Sets
# +e so a missing piece is skipped, not fatal.
destroy_nvmet() {
    set +e
    nvme disconnect -n "$NQN" >/dev/null 2>&1
    for link in "$CFG"/ports/*/subsystems/"$NQN"; do
        [ -e "$link" ] && rm -f "$link"
    done
    [ -n "$PORTDIR" ] && rmdir "$PORTDIR" 2>/dev/null
    if [ -d "$CFG/subsystems/$NQN" ]; then
        for ns in "$CFG/subsystems/$NQN"/namespaces/*; do
            [ -d "$ns" ] || continue
            echo 0 > "$ns/enable" 2>/dev/null
            rmdir "$ns" 2>/dev/null
        done
        rmdir "$CFG/subsystems/$NQN" 2>/dev/null
    fi
}

cleanup() {
    set +e
    [ -n "${TCPDUMP_PID:-}" ] && kill "$TCPDUMP_PID" 2>/dev/null && wait "$TCPDUMP_PID" 2>/dev/null
    destroy_nvmet
    [ "$OWN_BACKING" = 1 ] && rm -f "$BACKING"
}
trap cleanup EXIT

modprobe nvmet-tcp
modprobe nvme-tcp
mkdir -p "$OUT"

# Clear leftovers from a prior aborted run. Subshell keeps destroy_nvmet's
# `set +e` local, and with PORTDIR still empty it removes only our
# subsystem + any stale link to it, never the temp backing.
( destroy_nvmet )

# Claim a free configfs port id so we never disturb an existing nvmet port.
PORTID=1
while [ -e "$CFG/ports/$PORTID" ]; do PORTID=$((PORTID + 1)); done
PORTDIR="$CFG/ports/$PORTID"
echo "fixture: configfs port $PORTID, tcp 127.0.0.1:$PORT, nr_queues=$NR_QUEUES queue_size=$QUEUE_SIZE"

# nvmet subsystem + namespace.
mkdir -p "$CFG/subsystems/$NQN"
echo 1 > "$CFG/subsystems/$NQN/attr_allow_any_host"
# nr-io-queues: nvmet's per-subsystem max queue id (qid 1..N).
echo "$NR_QUEUES" > "$CFG/subsystems/$NQN/attr_qid_max"
mkdir -p "$CFG/subsystems/$NQN/namespaces/1"
echo "$BACKING" > "$CFG/subsystems/$NQN/namespaces/1/device_path"
# Force O_DIRECT on the file backend (buffered_io=0; must precede enable).
echo 0 > "$CFG/subsystems/$NQN/namespaces/1/buffered_io"
echo 1 > "$CFG/subsystems/$NQN/namespaces/1/enable"

# TCP port on loopback (fresh dir from the free id claimed above).
mkdir "$PORTDIR"
echo tcp > "$PORTDIR/addr_trtype"
echo ipv4 > "$PORTDIR/addr_adrfam"
echo 127.0.0.1 > "$PORTDIR/addr_traddr"
echo "$PORT" > "$PORTDIR/addr_trsvcid"
# queue-size: nvmet's advertised per-queue depth (SQSIZE/MAXCMD). Must be
# set before the port is enabled (the symlink below) or the kernel
# returns -EACCES.
echo "$QUEUE_SIZE" > "$PORTDIR/param_max_queue_size"
ln -sf "$CFG/subsystems/$NQN" "$PORTDIR/subsystems/$NQN"

tcpdump -i lo "port $PORT" -w "$OUT/nvmet-session.pcap" &
TCPDUMP_PID=$!
sleep 1

# Drive the session: discover, connect, identify, read, disconnect. Reads
# only — never dd *to* the namespace, so a wrong-node bug cannot destroy
# data and a real device backing is safe.
nvme discover -t tcp -a 127.0.0.1 -s "$PORT"
# Fail immediately on a connect failure (nvme-cli can exit 0 on an async
# failure, so also verify the controller actually appeared below).
if ! nvme connect -t tcp -a 127.0.0.1 -s "$PORT" -n "$NQN"; then
    echo "nvme connect to 127.0.0.1:$PORT ($NQN) failed" \
         "— is $PORT already bound? retry with PORT=14420 (see dmesg)" >&2
    exit 1
fi
# Resolve our namespace block device by matching the subsystem NQN in
# sysfs — independent of nvme-cli's `nvme list` JSON schema and of whether
# native multipath is enabled (a namespace block dev's device/subsysnqn
# resolves to its controller or subsystem in either case). Poll briefly:
# the block device can lag the connect by a moment.
ns_dev_for_nqn() {
    local blk name head
    for blk in /sys/block/nvme*n*; do
        [ -e "$blk" ] || continue
        name=$(basename "$blk")
        case "$name" in *p[0-9]*) continue ;; esac   # skip partitions
        [ -r "$blk/device/subsysnqn" ] || continue
        [ "$(cat "$blk/device/subsysnqn")" = "$NQN" ] || continue
        # With native multipath the match may land on a per-path node
        # (nvmeXcYnZ, which has no /dev entry) — map it to its head block
        # device (nvmeXnZ). Head names pass through unchanged.
        if [[ $name =~ ^(nvme[0-9]+)c[0-9]+(n[0-9]+)$ ]]; then
            head="${BASH_REMATCH[1]}${BASH_REMATCH[2]}"
        else
            head="$name"
        fi
        [ -b "/dev/$head" ] && { echo "/dev/$head"; return 0; }
    done
    return 1
}
DEV=""
for _ in $(seq 25); do
    DEV=$(ns_dev_for_nqn) && [ -n "$DEV" ] && break
    sleep 0.2
done
if [ -z "$DEV" ]; then
    echo "no namespace block device for $NQN; present nvme namespaces:" >&2
    for blk in /sys/block/nvme*n*; do
        [ -r "$blk/device/subsysnqn" ] &&
            echo "  $(basename "$blk") -> $(cat "$blk/device/subsysnqn")" >&2
    done
    exit 1
fi
nvme id-ctrl "$DEV" > /dev/null
nvme id-ns "$DEV" -n 1 > /dev/null
# Reads only (4K + 128K) — never write to the namespace.
dd if="$DEV" of=/dev/null bs=4k count=8 iflag=direct status=none
dd if="$DEV" of=/dev/null bs=128k count=4 iflag=direct status=none
nvme disconnect -n "$NQN"
sleep 1

kill "$TCPDUMP_PID"; wait "$TCPDUMP_PID" 2>/dev/null || true
TCPDUMP_PID=""
echo "fixture: $OUT/nvmet-session.pcap"
