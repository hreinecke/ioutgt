#!/bin/bash
# run_spdk_connect.sh — verify the SPDK nvmf target integration (TCP or RDMA)
# inside the vmtest guest over loopback / soft-RoCE (rxe), via
# testing/vmtest/spdk_connect.sh -> testing/local_tgt.sh's SPDK path. SPDK must
# already be built at ~/git/spdk (this does not build it). Exercises the
# same common.sh spdk_start the two_nic/realwire_spdk.sh driver uses.
#
#   ./testing/run_spdk_connect.sh tcp     # NVMe/TCP over loopback (default)
#   ./testing/run_spdk_connect.sh rdma    # NVMe/RDMA over rxe
set -euo pipefail
TOP="$(cd "$(dirname "$0")/.." && pwd)"; cd "$TOP"
. "$TOP/testing/common/vmtest.sh"     # VMTEST + VMTEST_CONF (env-overridable)
TRANSPORT="${1:-tcp}"
# The SPDK_BIN default lives in testing/common/spdk.sh (single source of truth) —
# source it rather than duplicating the path. TRANSPORT must be set first (the lib
# reads it at source time); an exported SPDK_BIN still overrides the default.
# shellcheck source=common/spdk.sh
source ./testing/common/spdk.sh
[ -x "$SPDK_BIN" ] || { echo "FAIL: SPDK not built ($SPDK_BIN) — cd ~/git/spdk && ./configure --with-rdma && make -j"; exit 1; }
# The vmtest kernel cmdline forces intel_iommu=on, which (with no VFIO-bound
# device) blocks SPDK/DPDK DMA memory. Append intel_iommu=off for THIS run only
# (last value wins; run_vm exposes VMTEST_KCMDLINE_EXTRA) so nvmf_tgt can
# allocate a malloc/aio bdev. Bare metal (102) uses VFIO instead and needs none.
export VMTEST_KCMDLINE_EXTRA="${VMTEST_KCMDLINE_EXTRA:-intel_iommu=off}"
# Run the guest entrypoint by path (it lives in the repo, seen over 9p; the
# guest cwd is this worktree, so it finds ./testing/local_tgt.sh).
# Pass the absolute SPDK binary path: the guest's $HOME is not /home/ming, but
# the host tree (incl. ~/git/spdk) is visible over 9p at its real path.
exec "$VMTEST" -c "$VMTEST_CONF" run "$TOP/testing/vmtest/spdk_connect.sh" "$TRANSPORT" "$SPDK_BIN"
