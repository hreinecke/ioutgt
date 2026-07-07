#!/bin/bash
# spdk_connect.sh (vmtest guest entrypoint) — verify the SPDK nvmf target
# integration end to end inside the guest, using testing/local_tgt.sh's SPDK
# path (the same common.sh spdk_start the two_nic/realwire_spdk.sh driver uses).
#
#   TCP  : loopback (127.0.0.1)
#   RDMA : soft-RoCE (rdma_rxe on the guest NIC)
#
# Runs: start spdk -> connect spdk -> short fio -> disconnect -> stop, on a
# malloc (RAM) bdev so no backing file/device is needed. Marks PASS/FAIL.
#
# Arg 1: transport (tcp|rdma, default tcp). Run by path from the repo worktree
# (guest cwd), so it finds ./testing/local_tgt.sh over the 9p mount.
set -u
TRANSPORT="${1:-tcp}"
export TRANSPORT
# SPDK binary/dir: the host runner passes an absolute path as arg 2 because the
# guest's $HOME is not the host's, but the host tree is visible over 9p at its
# real path. Set those first, then source common/spdk.sh so its SPDK_BIN/SPDK_DIR
# defaults (the single source of truth) fill in anything we didn't override —
# ${SPDK_BIN:-…} in that lib honors what we set here. With no arg 2 we inherit
# its plain $HOME/git/spdk default.
if [ -n "${2:-}" ]; then
    SPDK_BIN="$2"
    SPDK_DIR="$(dirname "$(dirname "$(dirname "$SPDK_BIN")")")"
fi
# shellcheck source=../common/spdk.sh
source ./testing/common/spdk.sh
export SPDK_BIN SPDK_DIR

log() { echo "[spdk] $*"; }
mark() { [ -n "${VMTEST_DATA_DIR:-}" ] && { mkdir -p "$VMTEST_DATA_DIR/tmp"; echo "$*" >>"$VMTEST_DATA_DIR/tmp/ioutgt_result"; } || true; }
DIAG="${VMTEST_DATA_DIR:-/tmp}/tmp/spdk-diag.log"
mkdir -p "$(dirname "$DIAG")" 2>/dev/null || true
diag() { echo "$*" >>"$DIAG"; }
fail() {
    log "RESULT: FAIL ($*)"; mark "FAIL $*"
    { echo "=== FAIL: $* ==="; echo "--- spdk log ---"; tail -40 "${SPDK_LOG:-/tmp/spdk-realwire.log}" 2>/dev/null; } >>"$DIAG"
    ./testing/local_tgt.sh stop spdk 2>/dev/null || true; exit 1
}

[ -x "$SPDK_BIN" ] || fail "SPDK not built ($SPDK_BIN)"

modprobe nvme-fabrics 2>/dev/null || true
modprobe "nvme-$TRANSPORT" 2>/dev/null || true

TARGET_IP=127.0.0.1
if [ "$TRANSPORT" = rdma ]; then
    log "loading rdma_rxe + adding an rxe device (matching the ioutgt rdma gate)"
    modprobe rdma_rxe 2>/dev/null || true
    # Pick the netdev that HAS a global IPv4 (not necessarily the default route).
    DEV=$(ip -o -4 addr show up scope global 2>/dev/null | awk '{print $2; exit}')
    [ -n "${DEV:-}" ] || fail "no usable netdev for rxe"
    CIDR=$(ip -o -4 addr show dev "$DEV" scope global 2>/dev/null | awk '{print $4; exit}')
    TARGET_IP=${CIDR%%/*}
    [ -n "$TARGET_IP" ] || fail "no IP on $DEV"
    ip link set "$DEV" up 2>/dev/null || true
    rdma link add rxe0 type rxe netdev "$DEV" 2>/dev/null || true
    for _ in $(seq 1 20); do ibv_devinfo 2>/dev/null | grep -q PORT_ACTIVE && break; sleep 0.5; done
    # rxe's RoCEv2 GID enumerates netdev IPs via async work; re-add the IP after
    # the link exists to re-trigger the GID notifier (else rdma_bind_addr fails).
    gid_ready() { show_gids 2>/dev/null | grep -qw "$TARGET_IP"; }
    if ! gid_ready; then
        ip addr del "$CIDR" dev "$DEV" 2>/dev/null || true
        ip addr add "$CIDR" dev "$DEV" 2>/dev/null || true
        for _ in $(seq 1 20); do gid_ready && break; sleep 0.5; done
    fi
    log "rxe dev=$DEV ip=$TARGET_IP (GID $(gid_ready && echo ready || echo MISSING))"
fi
export TARGET_IP

# Drive the SPDK target over loopback via local_tgt.sh. Small malloc (RAM) bdev
# so it fits the hugepage pool alongside DPDK's own reservation.
# Small hugepage footprint (512 MiB of 2M pages) + a 128 MiB malloc bdev. The VM
# has IOMMU on but no VFIO device bound, so DPDK can't derive physical DMA
# addresses — force IOVA=VA and pre-reserve memory (-s) so spdk_zmalloc succeeds.
# aio bdev (per-IO DMA buffers over a small file) rather than malloc (which
# spdk_zmalloc's the whole disk contiguously — hard in this constrained VM).
export TARGET_KINDS=spdk SPDK_BDEV=aio SPDK_BACKEND=/tmp/spdk-test.img BACKEND_GB=1 SPDK_HUGEMEM=512 FIO_SECS=5 FIO_JOBS=1 FIO_QD=16
# NOTE: this VM boots with intel_iommu=on and no VFIO-bound device, so DPDK
# cannot obtain DMA-mappable memory for a malloc/aio bdev (spdk_zmalloc ENOMEM)
# — a property of the vmtest kernel cmdline, not the harness. --iova-mode=va is
# the closest lever SPDK exposes; on bare metal (the 102 box) SPDK's setup.sh
# binds the NVMe device to VFIO and this is a non-issue.
export SPDK_EAL_EXTRA="${SPDK_EAL_EXTRA:---iova-mode=va -s 256}"
LT=./testing/local_tgt.sh
echo 256 > /proc/sys/vm/nr_hugepages 2>/dev/null || true
HP_TOTAL="$(awk '/HugePages_Total/{print $2}' /proc/meminfo 2>/dev/null)"
log "hugepages: ${HP_TOTAL:-0} total x2M ($(awk '/HugePages_Free/{print $2}' /proc/meminfo 2>/dev/null) free), MemTotal $(awk '/MemTotal/{print $2}' /proc/meminfo)kB"
: > "$DIAG" 2>/dev/null || true
diag "MemTotal=$(awk '/MemTotal/{print $2}' /proc/meminfo)kB HugePages_Total=${HP_TOTAL:-0}"
diag "hugetlbfs in /proc/filesystems: $(grep -q hugetlbfs /proc/filesystems && echo yes || echo NO)"
mkdir -p /dev/hugepages
if mount -t hugetlbfs nodev /dev/hugepages 2>/tmp/hpmnt.err; then diag "hugetlbfs mount: OK"; else diag "hugetlbfs mount FAILED: $(cat /tmp/hpmnt.err 2>/dev/null)"; fi
diag "mounts: $(grep hugetlbfs /proc/mounts || echo none)"
diag "cmdline: $(cat /proc/cmdline 2>/dev/null)"
diag "DMAR: $(dmesg 2>/dev/null | grep -iE 'DMAR:|Intel-IOMMU|IOMMU.*disabled|IOMMU enabled' | head -3 | tr '\n' '|')"
diag "iommu-enabled-count: $(dmesg 2>/dev/null | grep -c 'IOMMU enabled') ; vfio: $(lsmod 2>/dev/null | grep -c vfio)"
[ "${HP_TOTAL:-0}" -ge 64 ] || fail "VM reserved only ${HP_TOTAL:-0} hugepages — the SPDK harness is otherwise ready"

log "=== start SPDK nvmf target ($TRANSPORT, malloc bdev) ==="
$LT start spdk || fail "spdk start"
log "=== connect ==="
$LT connect spdk || { cat "${SPDK_LOG:-/tmp/spdk-realwire.log}" 2>/dev/null | tail -20; fail "connect"; }
DEV="$(TARGET_KINDS=spdk $LT status 2>/dev/null | awk '/spdk .*nvme/{print $NF}')"
log "connected device: ${DEV:-?}"
log "=== fio (${FIO_SECS}s) ==="
$LT fio spdk || fail "fio"
log "=== disconnect + stop ==="
$LT disconnect spdk 2>/dev/null || true
$LT stop spdk 2>/dev/null || true
log "RESULT: PASS"
mark "PASS spdk-$TRANSPORT"
