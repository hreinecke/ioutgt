#!/bin/bash
# Build the ioutgt-nvme-rdma target (release) on the host, then run the
# NVMe/RDMA A/B comparison inside the vmtest guest: ioutgt-nvme-rdma vs the
# in-kernel nvmet-rdma target, both driven through testing/local_tgt.sh with
# TRANSPORT=rdma over soft-RoCE, gated by a crc32c fio --verify on each.
#
# The guest sees the repo read-only over 9p and runs the entrypoint by path
# (it calls ./testing/local_tgt.sh), so — unlike run_rdma_connect.sh — there is
# nothing to copy into the vmtest tests dir; we just point vmtest at the script.
set -euo pipefail

TOP="$(cd "$(dirname "$0")/.." && pwd)"
cd "$TOP"
VMTEST="${VMTEST:-$HOME/git/utils/vmtest/vmtest}"
VMTEST_CONF="${VMTEST_CONF:-$HOME/git/linux-knext/vmtest.conf}"

cargo build --release -p ioutgt-nvme-rdma
[ -x "$TOP/target/release/ioutgt-nvme-rdma" ] || { echo "FAIL: ioutgt-nvme-rdma release binary not built"; exit 1; }

exec "$VMTEST" -c "$VMTEST_CONF" run "$TOP/testing/vmtest/ioutgt_rdma_compare.sh"
