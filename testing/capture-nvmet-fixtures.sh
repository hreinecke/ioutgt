#!/bin/bash
# Capture a real kernel-host <-> kernel-nvmet NVMe/TCP session on loopback
# for byte-exact codec fixtures (docs/fixtures/). Run as root:
#
#   sudo testing/capture-nvmet-fixtures.sh [backing-file-size-MB]
#
# Produces docs/fixtures/nvmet-session.pcap. Requires: nvmet-tcp and
# nvme-tcp modules, nvme-cli, tcpdump.
set -euo pipefail

SIZE_MB=${1:-64}
NQN="nqn.2026-06.io.ioutgt:fixture"
PORT=4420
CFG=/sys/kernel/config/nvmet
OUT="$(dirname "$0")/../docs/fixtures"
BACKING=$(mktemp /tmp/ioutgt-fixture-XXXX.img)

cleanup() {
    set +e
    nvme disconnect -n "$NQN" >/dev/null 2>&1
    [ -n "${TCPDUMP_PID:-}" ] && kill "$TCPDUMP_PID" 2>/dev/null && wait "$TCPDUMP_PID" 2>/dev/null
    rm -f "$CFG/ports/1/subsystems/$NQN" 2>/dev/null
    rmdir "$CFG/ports/1" 2>/dev/null
    rmdir "$CFG/subsystems/$NQN/namespaces/1" 2>/dev/null
    rmdir "$CFG/subsystems/$NQN" 2>/dev/null
    rm -f "$BACKING"
}
trap cleanup EXIT

modprobe nvmet-tcp
modprobe nvme-tcp
mkdir -p "$OUT"

truncate -s "${SIZE_MB}M" "$BACKING"

# nvmet subsystem + namespace + TCP port on loopback.
mkdir -p "$CFG/subsystems/$NQN"
echo 1 > "$CFG/subsystems/$NQN/attr_allow_any_host"
mkdir -p "$CFG/subsystems/$NQN/namespaces/1"
echo "$BACKING" > "$CFG/subsystems/$NQN/namespaces/1/device_path"
echo 1 > "$CFG/subsystems/$NQN/namespaces/1/enable"
mkdir -p "$CFG/ports/1"
echo tcp > "$CFG/ports/1/addr_trtype"
echo ipv4 > "$CFG/ports/1/addr_adrfam"
echo 127.0.0.1 > "$CFG/ports/1/addr_traddr"
echo $PORT > "$CFG/ports/1/addr_trsvcid"
ln -sf "$CFG/subsystems/$NQN" "$CFG/ports/1/subsystems/$NQN"

tcpdump -i lo "port $PORT" -w "$OUT/nvmet-session.pcap" &
TCPDUMP_PID=$!
sleep 1

# Drive a representative session: discover, connect, identify, IO both
# directions (4K inline write, 128K R2T write, reads), disconnect.
nvme discover -t tcp -a 127.0.0.1 -s $PORT
nvme connect -t tcp -a 127.0.0.1 -s $PORT -n "$NQN"
sleep 1
DEV=$(nvme list -o json | python3 -c \
    "import sys,json; print([d['DevicePath'] for d in json.load(sys.stdin)['Devices'] if 'ioutgt' in d.get('SubsystemNQN','') or True][-1])")
nvme id-ctrl "$DEV" > /dev/null
nvme id-ns "$DEV" -n 1 > /dev/null
dd if=/dev/urandom of="$DEV" bs=4k count=8 oflag=direct status=none
dd if=/dev/urandom of="$DEV" bs=128k count=4 oflag=direct status=none
dd if="$DEV" of=/dev/null bs=4k count=8 iflag=direct status=none
dd if="$DEV" of=/dev/null bs=128k count=4 iflag=direct status=none
nvme disconnect -n "$NQN"
sleep 1

kill "$TCPDUMP_PID"; wait "$TCPDUMP_PID" 2>/dev/null || true
TCPDUMP_PID=""
echo "fixture: $OUT/nvmet-session.pcap"
