#!/bin/bash
# SPDX-License-Identifier: GPL-2.0
# vmtest-desc: ioutgt NVMe/RDMA verbs rxe-loopback functional test
# vmtest-requires: root
set -u
BIN="${1:?usage: ioutgt_rdma_loopback <test-binary-path>}"
echo "[rdma] loading rdma_rxe"
modprobe rdma_rxe 2>&1 || true
# RoCEv2 needs an IP'd Ethernet netdev for a usable GID.
DEV=$(ip -o -4 addr show up scope global 2>/dev/null | awk '{print $2; exit}')
[ -z "${DEV:-}" ] && DEV=$(ip -o link show up 2>/dev/null | awk -F': ' '$2!="lo"{print $2; exit}')
echo "[rdma] netdev=${DEV:-<none>}"
[ -n "${DEV:-}" ] && rdma link add rxe0 type rxe netdev "$DEV" 2>&1 || echo "[rdma] rdma link add note: $?"
rdma link show 2>&1 | head -4
for _ in $(seq 1 20); do
	ibv_devinfo 2>/dev/null | grep -q "PORT_ACTIVE" && break
	sleep 0.5
done
# rxe's RoCEv2 GID table enumerates the netdev IPs via async work; for an IP
# that pre-dates the rxe link it sometimes never syncs, and the CM test's
# src-bound rdma_resolve_addr then fails ENODEV. Re-adding the IP after the
# link exists re-triggers the GID notifier (same fix as ioutgt_rdma_connect.sh).
CIDR=$(ip -o -4 addr show dev "${DEV:-}" scope global 2>/dev/null | awk '{print $4; exit}')
IP=${CIDR%%/*}
gid_ready() { show_gids 2>/dev/null | grep -qw "$IP"; }
if [ -n "${IP:-}" ] && ! gid_ready; then
	echo "[rdma] GID for $IP missing; re-adding $CIDR on $DEV to trigger GID"
	ip addr del "$CIDR" dev "$DEV" 2>/dev/null || true
	ip addr add "$CIDR" dev "$DEV" 2>/dev/null || true
	for _ in $(seq 1 20); do gid_ready && break; sleep 0.5; done
fi
ibv_devinfo 2>&1 | grep -E "hca_id|state:|link_layer" | head -6
# The CM loopback test connects to the rxe netdev's own IP; publish it.
if [ -n "${IP:-}" ]; then
	IOUTGT_RXE_IP="$IP"
	export IOUTGT_RXE_IP
fi
echo "[rdma] rxe ip=${IOUTGT_RXE_IP:-<none>}"
echo "[rdma] === running rxe_ tests ==="
"$BIN" --test-threads=1 --nocapture rxe_
rc=$?
echo "[rdma] rxe tests rc=$rc"
[ $rc -eq 0 ] && echo "[rdma] RESULT: PASS" || echo "[rdma] RESULT: FAIL"
exit $rc
