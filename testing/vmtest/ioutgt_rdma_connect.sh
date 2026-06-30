#!/bin/bash
# SPDX-License-Identifier: GPL-2.0
# vmtest-desc: ioutgt NVMe/RDMA `nvme connect` bring-up over soft-RoCE (rxe)
# vmtest-requires: root
#
# Brings up a soft-RoCE device on the guest NIC, starts the ioutgt-nvme-rdma
# target binary on it, and drives the in-kernel nvme-rdma host through
# discover -> connect -> identify -> a namespace read -> disconnect. The
# write-data path is not implemented yet (RD4), so IO here is read-only.
set -u
BIN="${1:?usage: ioutgt_rdma_connect <target-binary-path>}"
NQN="nqn.2025-01.io.ioutgt:rdma"
PORT=4420
LOG=/tmp/ioutgt-rdma.log

fail() { echo "[rdma] RESULT: FAIL ($*)"; exit 1; }

echo "[rdma] loading rdma_rxe + nvme_rdma"
modprobe rdma_rxe 2>&1 || true
modprobe nvme_rdma 2>&1 || true

# RoCEv2 needs an IP'd Ethernet netdev for a usable GID.
DEV=$(ip -o -4 addr show up scope global 2>/dev/null | awk '{print $2; exit}')
[ -z "${DEV:-}" ] && DEV=$(ip -o link show up 2>/dev/null | awk -F': ' '$2!="lo"{print $2; exit}')
[ -n "${DEV:-}" ] || fail "no usable netdev"
CIDR=$(ip -o -4 addr show dev "$DEV" scope global 2>/dev/null | awk '{print $4; exit}')
IP=${CIDR%%/*}
[ -n "${IP:-}" ] || fail "no IP on $DEV"
echo "[rdma] dev=$DEV cidr=$CIDR ip=$IP"
ip link set "$DEV" up 2>/dev/null || true
rdma link add rxe0 type rxe netdev "$DEV" 2>&1 || echo "[rdma] rdma link add note: $?"
for _ in $(seq 1 20); do
	ibv_devinfo 2>/dev/null | grep -q "PORT_ACTIVE" && break
	sleep 0.5
done
# rxe's RoCEv2 GID table enumerates the netdev IPs via async work; for an IP that
# pre-dates the rxe link it sometimes never syncs. Re-adding the IP after the
# link exists re-triggers the GID notifier so the IP's GID appears.
gid_ready() { show_gids 2>/dev/null | grep -qw "$IP"; }
if ! gid_ready; then
	echo "[rdma] GID for $IP missing; re-adding $CIDR on $DEV to trigger GID"
	ip addr del "$CIDR" dev "$DEV" 2>/dev/null || true
	ip addr add "$CIDR" dev "$DEV" 2>/dev/null || true
	for _ in $(seq 1 20); do gid_ready && break; sleep 0.5; done
fi
ibv_devinfo 2>&1 | grep -E "hca_id|state:|link_layer" | head -6
show_gids 2>/dev/null | grep -w "$IP" | head -2 || echo "[rdma] (no dotted GID listed for $IP)"

# Start the target on the rxe IP. Stream its log live (so a hang/panic is
# visible in the vmtest console, not hidden until cleanup).
echo "[rdma] starting target: $BIN --listen $IP:$PORT --subsys-nqn $NQN"
RUST_LOG=debug RUST_BACKTRACE=1 "$BIN" --listen "$IP:$PORT" --subsys-nqn "$NQN" --mem-size-mb 256 >"$LOG" 2>&1 &
TGT=$!
tail -f "$LOG" | sed 's/^/[tgt] /' &
TAILER=$!
cleanup() {
	nvme disconnect -n "$NQN" >/dev/null 2>&1 || true
	kill "$TGT" "$TAILER" 2>/dev/null || true
	echo "[rdma] --- full target log ---"; cat "$LOG" 2>/dev/null
}
trap cleanup EXIT
# Wait for the target to finish binding (the rxe GID/IP association can lag the
# netdev add, so bind_addr retries for up to a few seconds).
for _ in $(seq 1 70); do
	grep -q "nvme-rdma listening" "$LOG" && break
	kill -0 "$TGT" 2>/dev/null || { echo "[rdma] target died early:"; cat "$LOG"; fail "target exited"; }
	sleep 0.5
done
grep -q "nvme-rdma listening" "$LOG" || { echo "[rdma] target never listened:"; cat "$LOG"; fail "no listen"; }

echo "[rdma] === nvme discover ==="
timeout 20 nvme discover -t rdma -a "$IP" -s "$PORT" 2>&1 | head -20 || echo "[rdma] discover rc=$? (continuing)"

echo "[rdma] === nvme connect ==="
before=$(ls /dev/nvme*n* 2>/dev/null | sort)
timeout 20 nvme connect -t rdma -a "$IP" -s "$PORT" -n "$NQN" 2>&1 || fail "nvme connect (rc=$?)"
udevadm settle 2>/dev/null || sleep 1
after=$(ls /dev/nvme*n* 2>/dev/null | sort)
NS=$(comm -13 <(echo "$before") <(echo "$after") | head -1)
[ -n "${NS:-}" ] || fail "no namespace device appeared after connect"
echo "[rdma] connected namespace: $NS"

echo "[rdma] === nvme list / id-ctrl ==="
nvme list 2>&1 | head
nvme id-ctrl "$NS" 2>&1 | grep -E "^mn|^sn|^vid|subnqn" | head || fail "id-ctrl"

echo "[rdma] === namespace write + read-back verify (IO data path) ==="
dd if=/dev/urandom of=/tmp/w.bin bs=4096 count=256 2>/dev/null
dd if=/tmp/w.bin of="$NS" bs=4096 count=256 oflag=direct conv=fsync 2>&1 || fail "namespace write"
dd if="$NS" of=/tmp/r.bin bs=4096 count=256 iflag=direct 2>&1 || fail "namespace read"
cmp /tmp/w.bin /tmp/r.bin || fail "write/read data mismatch"
echo "[rdma] write+read verify OK (1 MiB)"

# A quick fio data-integrity pass over the namespace (crc32c verify) if present.
if command -v fio >/dev/null; then
	echo "[rdma] === fio --verify (4k/64k randwrite) ==="
	fio --name=v --filename="$NS" --direct=1 --rw=randwrite --bs=4k --size=8m \
	    --verify=crc32c --do_verify=1 --verify_fatal=1 --group_reporting 2>&1 \
	    | grep -iE "err=|verify|IO error" | head || fail "fio verify"
fi

# Reconnect churn: exercises the CM Disconnected path (cm_id prune) and the
# per-queue teardown drain. Each cycle disconnects all controllers, so the
# target sees Disconnected for the admin + IO queues, then reconnects.
echo "[rdma] === reconnect soak (8x) ==="
nvme disconnect -n "$NQN" >/dev/null 2>&1 || true
udevadm settle 2>/dev/null || sleep 1
for i in $(seq 1 8); do
	nvme connect -t rdma -a "$IP" -s "$PORT" -n "$NQN" 2>&1 || fail "reconnect $i"
	nvme disconnect -n "$NQN" >/dev/null 2>&1 || true
done
udevadm settle 2>/dev/null || sleep 1
# Final connect must still work + read after the churn.
before2=$(ls /dev/nvme*n* 2>/dev/null | sort)
nvme connect -t rdma -a "$IP" -s "$PORT" -n "$NQN" 2>&1 || fail "post-soak connect"
udevadm settle 2>/dev/null || sleep 1
after2=$(ls /dev/nvme*n* 2>/dev/null | sort)
NS2=$(comm -13 <(echo "$before2") <(echo "$after2") | head -1)
[ -n "${NS2:-}" ] || fail "post-soak no namespace appeared"
dd if="$NS2" of=/dev/null bs=4096 count=64 iflag=direct 2>&1 || fail "post-soak read"
echo "[rdma] reconnect soak OK"

echo "[rdma] RESULT: PASS"
exit 0
