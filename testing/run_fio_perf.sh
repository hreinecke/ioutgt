#!/bin/bash
# run_fio_perf.sh — one-shot two-NIC fio perf run for a chosen target+transport.
#
# Drives the full lifecycle of a testing/two_nic/realwire_*.sh driver in one
# command instead of typing the seven phases by hand:
#
#   up -> start -> connect -> fio_perf -> disconnect -> stop -> down
#
# The target and transport pick the driver and per-verb selector:
#
#   ioutgt|nvmet + tcp   -> two_nic/realwire_tcp.sh   <target>
#   ioutgt|nvmet + rdma  -> two_nic/realwire_rdma.sh  <target>
#   ioutgt_nvmet + tcp   -> two_nic/realwire_tcp.sh   (no selector: BOTH, A/B)
#   ioutgt_nvmet + rdma  -> two_nic/realwire_rdma.sh  (no selector: BOTH, A/B)
#   spdk         + tcp   -> two_nic/realwire_spdk.sh  spdk   (TRANSPORT=tcp)
#   spdk         + rdma  -> two_nic/realwire_spdk.sh  spdk   (TRANSPORT=rdma)
#
# Both parameters are REQUIRED. All other test knobs keep the drivers'
# defaults and are overridable from the shell — this script only pins TRANSPORT
# from the transport arg (so a stale inherited value can't override an explicit
# run); any other variable you export wins. Run under sudo -E so env reaches the
# driver:
#
#   sudo -E ./testing/run_fio_perf.sh ioutgt rdma
#   sudo -E ./testing/run_fio_perf.sh ioutgt_nvmet rdma   # ioutgt + nvmet, A/B
#   FIO_RW=randwrite FIO_BS=4k NIC_T=mlx5p1 NIC_I=mlx5p2 \
#       sudo -E ./testing/run_fio_perf.sh spdk rdma
#
# On a mid-sequence failure or Ctrl-C, a best-effort teardown
# (disconnect -> stop -> down) runs so the box is not left with a dangling
# netns, a running target, or a connected controller; the script still exits
# with the failing phase's status.
set -euo pipefail

usage() {
    cat <<EOF
usage: sudo -E $0 <ioutgt|nvmet|ioutgt_nvmet|spdk> <tcp|rdma>

Runs up -> start -> connect -> fio_perf -> disconnect -> stop -> down against
the matching two_nic/realwire_*.sh driver. Both parameters are required.

ioutgt_nvmet runs ioutgt AND nvmet back-to-back on the same wire (the driver's
A/B comparison); the other targets run just that one.

Test knobs (FIO_RW/FIO_BS/FIO_QD/FIO_JOBS/FIO_SECS, NIC_T/NIC_I, *_BACKEND,
NR_QUEUES, QUEUE_SIZE, ...) keep the driver defaults; export them to override.
EOF
}

# 'help' works without root/args.
case "${1:-}" in help|usage|-h|--help) usage; exit 0 ;; esac

TARGET="${1:-}"
TRANSPORT_ARG="${2:-}"
[ -n "$TARGET" ] && [ -n "$TRANSPORT_ARG" ] || { usage >&2; exit 2; }

case "$TRANSPORT_ARG" in
    tcp | rdma) ;;
    *) echo "invalid transport: $TRANSPORT_ARG (want tcp|rdma)" >&2; usage >&2; exit 2 ;;
esac

# The transport arg is authoritative, so pin it in the env: a stale inherited
# TRANSPORT (e.g. a prior 'export TRANSPORT=rdma' the harness docs suggest) must
# not override an explicit 'tcp' run. realwire_rdma.sh hard-pins rdma itself,
# but realwire_tcp.sh inherits common.sh's ${TRANSPORT:-tcp}, so without this an
# inherited rdma would silently leak into a tcp run. (The free knobs stay
# overridable; only this positional-derived value is enforced.)
export TRANSPORT="$TRANSPORT_ARG"

# Resolve driver + selector from (target, transport). spdk uses one driver for
# both fabrics (via TRANSPORT); ioutgt/nvmet pick the fabric-specific driver.
case "$TARGET" in
    ioutgt | nvmet | ioutgt_nvmet)
        case "$TRANSPORT_ARG" in
            tcp)  DRIVER=realwire_tcp.sh ;;
            rdma) DRIVER=realwire_rdma.sh ;;
        esac
        # ioutgt_nvmet -> no selector, so the driver runs BOTH targets
        # back-to-back (its TARGET_KINDS "ioutgt nvmet") on the same wire.
        if [ "$TARGET" = ioutgt_nvmet ]; then SEL=""; else SEL="$TARGET"; fi
        ;;
    spdk)
        SEL=spdk
        DRIVER=realwire_spdk.sh
        ;;
    *) echo "invalid target: $TARGET (want ioutgt|nvmet|ioutgt_nvmet|spdk)" >&2; usage >&2; exit 2 ;;
esac

# Per-verb selector args: a single target, or none — ioutgt_nvmet passes no
# selector so the driver acts on both targets (an empty "$SEL" would otherwise
# be a distinct, invalid empty argument).
if [ -n "$SEL" ]; then sel_args=("$SEL"); else sel_args=(); fi

TESTING="$(cd "$(dirname "$0")" && pwd)"
DRV="$TESTING/two_nic/$DRIVER"
[ -x "$DRV" ] || { echo "driver not executable: $DRV" >&2; exit 1; }

# Root is needed only to actually drive the wire/targets; validate the command
# line first so usage errors don't require sudo.
[ "$(id -u)" -eq 0 ] || { echo "must run as root (use sudo -E)" >&2; exit 1; }

run() { echo "==> $DRIVER $*"; "$DRV" "$@"; }

# Best-effort teardown; each step tolerates an already-torn-down state so it
# never masks the original failure.
teardown() {
    echo "==> teardown (best-effort)"
    "$DRV" disconnect "${sel_args[@]}" || true
    "$DRV" stop "${sel_args[@]}"       || true
    "$DRV" down                        || true
}

# Fires on failure (set -e) or signal. After a clean finish (done=1) the
# sequence has already run down, so just propagate the status.
done=0
on_exit() {
    local status=$?
    trap - EXIT INT TERM
    if [ "$done" -eq 1 ]; then exit "$status"; fi
    echo "!! aborted (status $status) — tearing down" >&2
    teardown
    exit "$status"
}
trap on_exit EXIT INT TERM

echo "== fio_perf: target=$TARGET transport=$TRANSPORT_ARG driver=$DRIVER selector=${SEL:-<both>} =="
run up
run start      "${sel_args[@]}"
run connect    "${sel_args[@]}"
run fio_perf   "${sel_args[@]}"
run disconnect "${sel_args[@]}"
run stop       "${sel_args[@]}"
run down
done=1
