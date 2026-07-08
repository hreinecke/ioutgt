#!/bin/bash
# vmtest-desc: ioutgt NVMe/RDMA verbs rxe-loopback functional test
# vmtest-requires: root
set -u
BIN="${1:?usage: ioutgt_rdma_loopback <test-binary-path> <repo-top>}"
REPO_TOP="${2:?usage: ioutgt_rdma_loopback <test-binary-path> <repo-top>}"
echo "[rdma] loading rdma_rxe"
# shellcheck source=../common/rxe.sh
. "$REPO_TOP/testing/common/rxe.sh"
rxe_setup || echo "[rdma] rxe bring-up incomplete (no netdev/IP?) — proceeding"
ibv_devinfo 2>&1 | grep -E "hca_id|state:|link_layer" | head -6
# The CM loopback test connects to the rxe netdev's own IP; publish it.
if [ -n "${RXE_IP:-}" ]; then
	IOUTGT_RXE_IP="$RXE_IP"
	export IOUTGT_RXE_IP
fi
echo "[rdma] rxe ip=${IOUTGT_RXE_IP:-<none>}"
echo "[rdma] === running rxe_ tests ==="
"$BIN" --test-threads=1 --nocapture rxe_
rc=$?
echo "[rdma] rxe tests rc=$rc"
[ $rc -eq 0 ] && echo "[rdma] RESULT: PASS" || echo "[rdma] RESULT: FAIL"
exit $rc
