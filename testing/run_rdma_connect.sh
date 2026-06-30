#!/bin/bash
# Build the ioutgt-nvme-rdma target binary on the host and run a real
# `nvme connect -t rdma` bring-up inside the vmtest guest (which provides a
# soft-RoCE rdma_rxe device and the in-kernel nvme-rdma host). The guest
# entrypoint (testing/vmtest/ioutgt_rdma_connect.sh) loads the modules, adds an
# rxe device on the guest NIC, starts the target, and drives discover/connect/
# identify/read/disconnect.
set -euo pipefail

TOP="$(cd "$(dirname "$0")/.." && pwd)"
cd "$TOP"
VMTEST="${VMTEST:-$HOME/git/utils/vmtest/vmtest}"
VMTEST_CONF="${VMTEST_CONF:-$HOME/git/linux-knext/vmtest.conf}"

cargo build -p ioutgt-nvme-rdma --bin ioutgt-nvme-rdma
BIN="$TOP/target/debug/ioutgt-nvme-rdma"
[ -x "$BIN" ] || { echo "FAIL: ioutgt-nvme-rdma binary not built"; exit 1; }

# Publish the guest entrypoint into the vmtest tests dir (vmtest runs tests/NAME.sh).
cp "$TOP/testing/vmtest/ioutgt_rdma_connect.sh" "$(dirname "$VMTEST")/tests/ioutgt_rdma_connect.sh"

exec "$VMTEST" -c "$VMTEST_CONF" run ioutgt_rdma_connect "$BIN"
