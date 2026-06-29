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
ibv_devinfo 2>&1 | grep -E "hca_id|state:|link_layer" | head -6
echo "[rdma] === running rxe_loopback ==="
"$BIN" --test-threads=1 --nocapture rxe_
rc=$?
echo "[rdma] rxe_loopback rc=$rc"
[ $rc -eq 0 ] && echo "[rdma] RESULT: PASS" || echo "[rdma] RESULT: FAIL"
exit $rc
