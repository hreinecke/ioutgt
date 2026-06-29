#!/bin/bash
# Build the ioutgt-nvme-rdma test binary on the host and run the verbs
# rxe-loopback functional test inside the vmtest guest, which provides a
# soft-RoCE (rdma_rxe) device. The guest entrypoint
# (testing/vmtest/ioutgt_rdma_loopback.sh) loads rdma_rxe, adds an rxe device
# on the guest NIC, and runs the prebuilt test binary.
set -euo pipefail

TOP="$(cd "$(dirname "$0")/.." && pwd)"
cd "$TOP"
VMTEST="${VMTEST:-$HOME/git/utils/vmtest/vmtest}"
VMTEST_CONF="${VMTEST_CONF:-$HOME/git/linux-knext/vmtest.conf}"

cargo test -p ioutgt-nvme-rdma --no-run
BIN=$(ls -t target/debug/deps/ioutgt_nvme_rdma-* | grep -vE '\.d$' | head -1)
[ -x "$BIN" ] || { echo "FAIL: no ioutgt-nvme-rdma test binary built"; exit 1; }

# Publish the guest entrypoint into the vmtest tests dir (vmtest runs tests/NAME.sh).
cp "$TOP/testing/vmtest/ioutgt_rdma_loopback.sh" "$(dirname "$VMTEST")/tests/ioutgt_rdma_loopback.sh"

exec "$VMTEST" -c "$VMTEST_CONF" run ioutgt_rdma_loopback "$BIN"
