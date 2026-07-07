# rxe.sh — shared soft-RoCE (rdma_rxe) bring-up for the vmtest GUEST scripts.
# Sourced, not executed. GUEST scripts under testing/vmtest/ run by path source
# it via $(dirname "$0")/../common/rxe.sh; scripts run by name (copied into
# vmtest's tests dir) receive the repo TOP as a positional arg and source
# "$TOP/testing/common/rxe.sh" (the host tree is visible over 9p at its real
# path).
#
# rxe_setup: modprobe rdma_rxe, pick the guest netdev (prefer one with a global
# IPv4), add rxe0, wait for PORT_ACTIVE, then seat the RoCEv2 GID: rxe's GID
# table enumerates netdev IPs via async work, and for an IP that pre-dates the
# rxe link it sometimes never syncs — re-adding the IP re-triggers the GID
# notifier (else rdma_bind_addr fails ENODEV/EADDRNOTAVAIL).
# Publishes RXE_DEV / RXE_CIDR / RXE_IP. Returns 1 if there is no usable netdev
# or no IPv4 on it (the rxe link is still created in the latter case); the
# caller decides whether that is fatal.
rxe_gid_ready() {
    # Guard: an empty pattern would make grep match anything (and set -u would
    # trip on an unset RXE_IP if called before rxe_setup).
    [ -n "${RXE_IP:-}" ] || return 1
    show_gids 2>/dev/null | grep -qw "$RXE_IP"
}
rxe_setup() {
    modprobe rdma_rxe 2>/dev/null || true
    RXE_DEV=$(ip -o -4 addr show up scope global 2>/dev/null | awk '{print $2; exit}')
    [ -z "${RXE_DEV:-}" ] && RXE_DEV=$(ip -o link show up 2>/dev/null | awk -F': ' '$2!="lo"{print $2; exit}')
    [ -n "${RXE_DEV:-}" ] || { echo "[rxe] no usable netdev" >&2; return 1; }
    ip link set "$RXE_DEV" up 2>/dev/null || true
    rdma link add rxe0 type rxe netdev "$RXE_DEV" 2>&1 || echo "[rxe] rdma link add note: $?"
    local _
    for _ in $(seq 1 20); do ibv_devinfo 2>/dev/null | grep -q PORT_ACTIVE && break; sleep 0.5; done
    RXE_CIDR=$(ip -o -4 addr show dev "$RXE_DEV" scope global 2>/dev/null | awk '{print $4; exit}')
    RXE_IP=${RXE_CIDR%%/*}
    [ -n "${RXE_IP:-}" ] || { echo "[rxe] no IPv4 on $RXE_DEV" >&2; return 1; }
    if ! rxe_gid_ready; then
        echo "[rxe] GID for $RXE_IP missing; re-adding $RXE_CIDR on $RXE_DEV to trigger GID"
        ip addr del "$RXE_CIDR" dev "$RXE_DEV" 2>/dev/null || true
        ip addr add "$RXE_CIDR" dev "$RXE_DEV" 2>/dev/null || true
        for _ in $(seq 1 20); do rxe_gid_ready && break; sleep 0.5; done
    fi
    echo "[rxe] dev=$RXE_DEV ip=$RXE_IP (GID $(rxe_gid_ready && echo ready || echo MISSING))"
}
