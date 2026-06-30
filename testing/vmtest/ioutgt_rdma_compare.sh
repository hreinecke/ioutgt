#!/bin/bash
# SPDX-License-Identifier: GPL-2.0
# vmtest-desc: NVMe/RDMA A/B — ioutgt-nvme-rdma vs in-kernel nvmet-rdma over
#              soft-RoCE, both driven through testing/ (common.sh TRANSPORT=rdma)
# vmtest-requires: root nvme-cli fio
#
# Exercises the TRANSPORT=rdma parametrization of common.sh end to end in the
# guest: brings up a soft-RoCE (rxe) device, then runs the SAME local_tgt.sh
# verbs (start -> connect -> fio --verify -> disconnect -> stop) against both
# the ioutgt-nvme-rdma target and the in-kernel nvmet-rdma target, asserting a
# clean crc32c verify on each. This is the loopback sibling of the box perf
# comparison (two_nic_realwire.sh, TRANSPORT=rdma); here we gate correctness +
# the harness wiring, not throughput.
#
# Backends are LOOP BLOCK DEVICES, not files: the guest root is tmpfs, which
# supports neither O_DIRECT (ioutgt's default open mode) nor nvmet's file
# backend ("invalid LBA data size 0"). A loop bdev over a tmpfs file dodges
# both. The ioutgt release binary must be built on the host first (the guest
# sees the repo read-only).
set -euo pipefail

REPO="${IOUTGT_REPO:-$(cd "$(dirname "$0")/../.." && pwd)}"
cd "$REPO"
BIN=./target/release/ioutgt-nvme-rdma
[ -x "$BIN" ] || { echo "FAIL: $BIN missing (build on host: cargo build --release -p ioutgt-nvme-rdma)"; exit 1; }

RESULT="${VMTEST_DATA_DIR:-/tmp}/rdma_compare_result.txt"
: > "$RESULT"
log()  { printf '%s\n' "$*" | tee -a "$RESULT"; }
fail() { log "[compare] RESULT: FAIL ($*)"; exit 1; }

# --- soft-RoCE bring-up (rxe on the guest NIC) ------------------------------
log "[compare] loading rdma_rxe + nvme/nvmet-rdma"
modprobe rdma_rxe 2>/dev/null || true
modprobe nvme-rdma 2>/dev/null || true
modprobe nvmet-rdma 2>/dev/null || true

# RoCEv2 needs an IP'd Ethernet netdev for a usable GID.
DEV=$(ip -o -4 addr show up scope global 2>/dev/null | awk '{print $2; exit}')
[ -n "${DEV:-}" ] || fail "no usable netdev"
CIDR=$(ip -o -4 addr show dev "$DEV" scope global 2>/dev/null | awk '{print $4; exit}')
IP=${CIDR%%/*}
[ -n "${IP:-}" ] || fail "no IP on $DEV"
rdma link add rxe0 type rxe netdev "$DEV" 2>&1 || echo "[compare] rdma link add note: $?"
for _ in $(seq 1 20); do ibv_devinfo 2>/dev/null | grep -q "PORT_ACTIVE" && break; sleep 0.5; done
# rxe's RoCEv2 GID table enumerates netdev IPs asynchronously; re-adding the IP
# after the link exists re-triggers the GID notifier (see ioutgt_rdma_connect.sh).
gid_ready() { show_gids 2>/dev/null | grep -qw "$IP"; }
if ! gid_ready; then
	log "[compare] GID for $IP missing; re-adding $CIDR on $DEV"
	ip addr del "$CIDR" dev "$DEV" 2>/dev/null || true
	ip addr add "$CIDR" dev "$DEV" 2>/dev/null || true
	for _ in $(seq 1 20); do gid_ready && break; sleep 0.5; done
fi
log "[compare] rxe up on $DEV ip=$IP"

# --- loop-backed block-device backends --------------------------------------
truncate -s 512M /tmp/cmp-io.img /tmp/cmp-nv.img
LOOP_IO=$(losetup -f --show /tmp/cmp-io.img)
LOOP_NV=$(losetup -f --show /tmp/cmp-nv.img)
log "[compare] backends: ioutgt=$LOOP_IO nvmet=$LOOP_NV"

export TRANSPORT=rdma TARGET_IP="$IP"
export NR_QUEUES="${NR_QUEUES:-2}" QUEUE_SIZE="${QUEUE_SIZE:-64}"
export IOUTGT_BACKEND="$LOOP_IO" NVMET_BACKEND="$LOOP_NV"
export IOUTGT_LOG=/tmp/cmp-ioutgt.log

cleanup() {
	./testing/local_tgt.sh disconnect >/dev/null 2>&1 || true
	./testing/local_tgt.sh stop       >/dev/null 2>&1 || true
	losetup -d "$LOOP_IO" "$LOOP_NV" 2>/dev/null || true
}
trap cleanup EXIT

# Drive one target (ioutgt|nvmet) through the shared verbs and crc32c-verify it.
verify_one() {
	local tgt="$1" before after dev ftmp
	log "== [$tgt] start =="
	./testing/local_tgt.sh start "$tgt"
	# ioutgt's rxe bind retries until the GID is live; wait for the listener
	# before connecting (nvmet's listener is up synchronously on enable).
	if [ "$tgt" = ioutgt ]; then
		for _ in $(seq 1 60); do grep -q "nvme-rdma listening" "$IOUTGT_LOG" 2>/dev/null && break; sleep 0.5; done
		grep -q "nvme-rdma listening" "$IOUTGT_LOG" 2>/dev/null || { cat "$IOUTGT_LOG"; fail "$tgt never listened"; }
	fi
	before=$(ls /dev/nvme*n* 2>/dev/null | sort)
	log "== [$tgt] connect =="
	./testing/local_tgt.sh connect "$tgt"
	udevadm settle 2>/dev/null || sleep 1
	after=$(ls /dev/nvme*n* 2>/dev/null | sort)
	dev=$(comm -13 <(echo "$before") <(echo "$after") | head -1)
	[ -n "$dev" ] || fail "$tgt: no namespace device after connect"
	log "== [$tgt] fio --verify (4k randwrite, crc32c) on $dev =="
	ftmp=$(mktemp)
	if fio --name=v --filename="$dev" --direct=1 --rw=randwrite --bs=4k --size=64m \
	       --verify=crc32c --do_verify=1 --verify_fatal=1 --group_reporting >"$ftmp" 2>&1; then
		grep -iE "err= *0|verify" "$ftmp" | head -3 | tee -a "$RESULT" || true
		log "[compare] $tgt: verify OK"
	else
		cat "$ftmp" | tee -a "$RESULT"
		rm -f "$ftmp"; fail "$tgt fio verify"
	fi
	rm -f "$ftmp"
	./testing/local_tgt.sh disconnect "$tgt"
}

verify_one ioutgt
verify_one nvmet

log "[compare] RESULT: PASS (ioutgt-nvme-rdma + nvmet-rdma both crc32c-clean over rxe)"
exit 0
