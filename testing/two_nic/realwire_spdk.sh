#!/usr/bin/env bash
#
# two_nic/realwire_spdk.sh — the SPDK sibling of two_nic/realwire_{tcp,rdma}.sh:
# run an SPDK NVMe-oF target (nvmf_tgt) and a kernel initiator on ONE host but
# force the traffic across two real NICs, for both NVMe/TCP and NVMe/RDMA
# (RoCEv2), so SPDK can be compared back-to-back with the in-kernel nvmet target
# on the same wire. TRANSPORT=tcp|rdma selects the fabric AND the wire-forcing
# method:
#
#   TCP  — both NICs go into their own netns (NS_T target, NS_I initiator); with
#          no veth linking them, the only path is the physical cable.
#   RDMA — the TARGET side (NIC_T + its rdma device) stays in the ROOT netns
#          (so the SPDK listener and nvmet-rdma, whose CM is init_net-pinned,
#          coexist), and only the INITIATOR (NIC_I + its rdma device) is isolated
#          in NS_I. `rdma system netns exclusive` + a carrier flap seat the GID.
#
# The SPDK target is userspace (nvmf_tgt), configured via a JSON startup config
# (see common.sh spdk_start — rpc.py is unusable under the box's Python 3.14).
#
# Two targets, distinct port/NQN/backend, both reachable on the same target IP:
#   spdk  : 14420  nqn...:spdk   SPDK_BACKEND   (SPDK nvmf_tgt, SPDK_BDEV type)
#   nvmet : 24420  nqn...:nvmet  NVMET_BACKEND  (in-kernel nvmet)
#
# USAGE (one env block, then subcommands; selector verbs take spdk|nvmet)
#   export TRANSPORT=rdma NIC_T=mlx5p1 NIC_I=mlx5p2
#   export SPDK_BACKEND=/dev/nvme0n1 NVMET_BACKEND=/dev/nvme1n1
#   sudo -E ./two_nic/realwire_spdk.sh up
#   sudo -E ./two_nic/realwire_spdk.sh start                # both targets
#   sudo -E ./two_nic/realwire_spdk.sh connect spdk         # or just one
#   sudo -E ./two_nic/realwire_spdk.sh fio_perf             # perf sweep, both
#   sudo -E ./two_nic/realwire_spdk.sh disconnect
#   sudo -E ./two_nic/realwire_spdk.sh stop
#   sudo -E ./two_nic/realwire_spdk.sh down
#
# !!! SAFETY !!!  Moving a NIC into a namespace removes it from root. Do NOT use
#   the NIC that carries your SSH/management link. For RDMA, NIC_T and NIC_I must
#   be two SEPARATE cards (two ports of one card share one rdma device, which
#   cannot be split across netns).
#
# KNOBS (env; see also common.sh for SPDK_* / NR_QUEUES / FIO_* / HDGST/DDGST)
#   SPDK_BACKEND / NVMET_BACKEND    each target's file or block device
#   SPDK_BDEV=aio                   SPDK backend bdev: aio|malloc[:MiB]|uring|nvme:<BDF>
#   SPDK_CPUMASK / SPDK_HUGEMEM     nvmf_tgt reactor mask / hugepage MiB
#   BACKEND_GB=2   NR_QUEUES=4   QUEUE_SIZE=128
#   IP_T/IP_I/PREFIX/MTU            addressing (jumbo MTU 9000 by default)
set -euo pipefail

# Fabric: tcp | rdma. MUST be exported before common.sh (it keys the digest /
# module / addr_trtype selection and, for rdma, forces digests off).
TRANSPORT="${TRANSPORT:-tcp}"
case "$TRANSPORT" in tcp|rdma) ;; *) echo "TRANSPORT must be tcp or rdma (got '$TRANSPORT')" >&2; exit 1 ;; esac
export TRANSPORT

# This driver compares the SPDK target against the in-kernel nvmet target.
TARGET_KINDS="spdk nvmet"

# ---- config (override via environment) -------------------------------
NS_T="${NS_T:-nvmet}"           # target netns (TCP only; RDMA keeps target in root)
NS_I="${NS_I:-nvmei}"           # initiator netns (both transports)
IP_T="${IP_T:-192.168.50.1}"    # target IP
IP_I="${IP_I:-192.168.50.2}"    # initiator IP
PREFIX="${PREFIX:-24}"
MTU="${MTU:-9000}"              # jumbo by default (both NICs must agree)
SPDK_PORT=14420
SPDK_NQN="nqn.2026-06.io.realwire:spdk"
NVMET_PORT=24420
NVMET_NQN="nqn.2026-06.io.realwire:nvmet"
# shellcheck disable=SC2034  # HOSTNQN consumed by common.sh's connect/discover
HOSTNQN="nqn.2026-06.io.realwire:host"

# Transport context for common.sh: the target listens on IP_T; the initiator's
# nvme-cli runs inside NS_I so its socket / RDMA-CM resolve egresses NIC_I.
# shellcheck disable=SC2034  # TARGET_IP consumed by common.sh's verbs
TARGET_IP="$IP_T"
ini_exec() { ip netns exec "$NS_I" "$@"; }

. "$(dirname "$0")/../common/common.sh"

# Per-target backend (file or block device); validated only when started.
SPDK_BACKEND="${SPDK_BACKEND:-}"
NVMET_BACKEND="${NVMET_BACKEND:-}"

# Post-connect NIC/IRQ tuning (0 skips it). RDMA tunes comp-vector IRQs; TCP
# tunes channels/XPS/affinity (see common.sh tune_target_*).
NIC_TUNE="${NIC_TUNE:-1}"

# --- transport-branched target launch + tuning context ----------------
# RDMA keeps the target in ROOT; TCP puts it in NS_T. Both isolate the initiator.
if [ "$TRANSPORT" = rdma ]; then
    nvmet_exec() { bash -c "$1"; }            # nvmet-rdma binds in root (init_net)
    # shellcheck disable=SC2034  # SPDK_NETNS consumed by common.sh's spdk_start
    SPDK_NETNS=()                             # nvmf_tgt runs in root too
    # shellcheck disable=SC2034
    TUNE_NIC="${NIC_T:-}"; [ "$NIC_TUNE" = 1 ] || TUNE_NIC=""
    # shellcheck disable=SC2034  # a queue's IRQ index is its CQ comp-vector (=qid)
    TUNE_COMP_VECTOR=1
else
    nvmet_exec() { in_net "$NS_T" bash -c "$1"; }   # nvmet-tcp listener born in NS_T
    # shellcheck disable=SC2034  # SPDK_NETNS consumed by common.sh's spdk_start
    SPDK_NETNS=(ip netns exec "$NS_T")              # nvmf_tgt listener in NS_T
    # shellcheck disable=SC2034
    TUNE_NIC="${NIC_T:-}"; [ "$NIC_TUNE" = 1 ] || TUNE_NIC=""
    # shellcheck disable=SC2034
    TUNE_NS="$NS_T"; TUNE_NIC_INI="${NIC_I:-}"; [ "$NIC_TUNE" = 1 ] || TUNE_NIC_INI=""
    # shellcheck disable=SC2034
    TUNE_NS_INI="$NS_I"
fi

usage() {
    cat <<EOF
two_nic/realwire_spdk.sh — SPDK NVMe-oF ($TRANSPORT) target + initiator across two
real NICs on one host, compared back-to-back with the in-kernel nvmet target.
TRANSPORT=tcp|rdma selects the fabric and the wire method (both NICs in netns for
TCP; target in root, initiator isolated, for RDMA).

Targets (same target IP $IP_T, distinct port/NQN/backend):
  spdk   :$SPDK_PORT   $SPDK_NQN   (SPDK_BACKEND, SPDK_BDEV=$SPDK_BDEV)
  nvmet  :$NVMET_PORT   $NVMET_NQN   (NVMET_BACKEND)

Usage: $0 <subcommand> [spdk|nvmet]   (selector omitted = both, in order)

  up                            set up the wire (netns/addressing; RDMA: rdma
                                exclusive + isolate initiator) and prove it
  down                          tear the wire down
  start         [spdk|nvmet]    start the target(s)
  stop          [spdk|nvmet]    stop the target(s)
  discover      [spdk|nvmet]    nvme discover -t $TRANSPORT
  connect       [spdk|nvmet]    nvme connect -t $TRANSPORT; wait for the namespace
  disconnect    [spdk|nvmet]    nvme disconnect
  fio           [spdk|nvmet]    fio on the connected device(s)
  fio_verify    [spdk|nvmet]    data-integrity gate: mixed-size writes + crc32c
  fio_perf      [spdk|nvmet]    perf sweep: randread/randwrite x bs={4k,64k}
  status                        wire, addresses, listeners, connected devices
  help                          this message

Required env: TRANSPORT, NIC_T, NIC_I (two NICs cabled back-to-back; RDMA needs
two SEPARATE cards) and the started target's backend (SPDK_BACKEND / NVMET_BACKEND).
Knobs: SPDK_BDEV=$SPDK_BDEV SPDK_HUGEMEM=$SPDK_HUGEMEM  BACKEND_GB=$BACKEND_GB
  NR_QUEUES=$NR_QUEUES QUEUE_SIZE=$QUEUE_SIZE  IP_T=$IP_T IP_I=$IP_I MTU=$MTU
  NIC_TUNE=$NIC_TUNE  FIO_RW/BS/QD/JOBS/SECS

Example:
  export TRANSPORT=rdma NIC_T=mlx5p1 NIC_I=mlx5p2 SPDK_BACKEND=/dev/nvme0n1 NVMET_BACKEND=/dev/nvme1n1
  sudo -E $0 up && sudo -E $0 start && sudo -E $0 connect && sudo -E $0 fio_perf
  sudo -E $0 disconnect && sudo -E $0 stop && sudo -E $0 down
EOF
}

# 'help'/'usage' must work without root or NIC_T/NIC_I.
case "${1:-}" in help|usage|-h|--help) usage; exit 0 ;; esac

[ "$(id -u)" -eq 0 ] || { echo "must run as root (use sudo)"; exit 1; }

require_nics() {
    : "${NIC_T:?set NIC_T to the target-side NIC, e.g. NIC_T=mlx5p1}"
    : "${NIC_I:?set NIC_I to the initiator-side NIC, e.g. NIC_I=mlx5p2}"
}
fail() { echo "FAIL: $*" >&2; exit 1; }

# ---- RDMA-specific wire helpers (used only when TRANSPORT=rdma) -------
# The rdma (ibverbs) device name backing a netdev, from sysfs.
nic_ibdev() {
    local nic="$1" d
    for d in /sys/class/net/"$nic"/device/infiniband/*; do
        [ -e "$d" ] || continue
        basename "$d"; return 0
    done
    return 1
}
# Put the box in rdma netns-exclusive mode (global; idempotent).
rdma_netns_exclusive() {
    local mode; mode="$(rdma system show 2>/dev/null | grep -o 'netns [a-z]*' | awk '{print $2}')"
    [ "$mode" = exclusive ] && { echo "   rdma netns mode already exclusive"; return 0; }
    echo ">> setting rdma system netns mode = exclusive (global; was ${mode:-shared})"
    rdma system set netns exclusive 2>&1 || {
        echo "   could not set rdma netns exclusive — free any rdma device in a" >&2
        echo "   non-default netns / in use (no live nvme-rdma sessions), or set at boot." >&2
        return 1
    }
}
rdma_move_dev() { rdma dev set "$1" netns "$2" 2>/dev/null || true; }
rdma_gid_ready() {
    local nic="$1" ip="$2" ns="$3"; local -a x=(); [ -n "$ns" ] && x=(ip netns exec "$ns")
    local ib hex; ib="$("${x[@]}" sh -c "ls /sys/class/net/$nic/device/infiniband/ 2>/dev/null" | head -1)"
    [ -n "$ib" ] || return 1
    # shellcheck disable=SC2086  # deliberate split of the dotted quad into 4 args
    hex="$(printf '%02x%02x:%02x%02x' ${ip//./ })"
    "${x[@]}" sh -c "grep -qi 'ffff:$hex' /sys/class/infiniband/$ib/ports/*/gids/* 2>/dev/null"
}
# Address a RoCE NIC + seat its RoCEv2 GID in the rdma_cm cache (carrier flap).
rdma_address_nic() {
    local nic="$1" ip="$2" ns="$3"; local -a x=(); [ -n "$ns" ] && x=(ip netns exec "$ns")
    "${x[@]}" ip addr flush dev "$nic" 2>/dev/null || true
    "${x[@]}" ip link set "$nic" down
    "${x[@]}" ip link set "$nic" up
    "${x[@]}" ip link set "$nic" mtu "$MTU"
    local i
    for i in $(seq 1 40); do
        [ "$("${x[@]}" cat "/sys/class/net/$nic/carrier" 2>/dev/null)" = 1 ] && break
        sleep 0.5
    done
    "${x[@]}" ip addr add "$ip/$PREFIX" dev "$nic"
    "${x[@]}" ip link set lo up 2>/dev/null || true
    for i in $(seq 1 60); do rdma_gid_ready "$nic" "$ip" "$ns" && return 0; sleep 0.5; done
    echo "   warning: RoCEv2 GID for $ip on $nic ($ns netns) not visible after 30s" >&2
    return 0
}
rdma_verify_dev() {
    local ns="$1" dev="$2" i; local -a pfx=(); [ -n "$ns" ] && pfx=(ip netns exec "$ns")
    for i in $(seq 1 20); do
        "${pfx[@]}" rdma link show 2>/dev/null | grep "$dev/" | grep -qi "state ACTIVE" && return 0
        sleep 0.5
    done
    echo "   ${ns:-root} rdma link:" >&2; "${pfx[@]}" rdma link show 2>/dev/null | sed 's/^/     /' >&2 || true
    return 1
}

# ---- up/down: branch on transport ------------------------------------
cmd_up_rdma() {
    require_nics
    in_net "$NS_I" ip link set "$NIC_I" netns 1 2>/dev/null || true
    ip netns del "$NS_I" 2>/dev/null || true
    if command -v nmcli >/dev/null 2>&1; then
        nmcli device set "$NIC_T" managed no 2>/dev/null || true
        nmcli device set "$NIC_I" managed no 2>/dev/null || true
    fi
    ip rule del to "$IP_T/$PREFIX" lookup main pref 5000 2>/dev/null || true
    ip rule add to "$IP_T/$PREFIX" lookup main pref 5000 2>/dev/null || true
    local ibt ibi
    ibt="$(nic_ibdev "$NIC_T")" || fail "no rdma device under /sys/class/net/$NIC_T (RoCE NIC with mlx5_ib?)"
    ibi="$(nic_ibdev "$NIC_I")" || fail "no rdma device under /sys/class/net/$NIC_I (RoCE NIC?)"
    [ "$ibt" != "$ibi" ] || fail "NIC_T/$NIC_T and NIC_I/$NIC_I share rdma device $ibt — use two separate cards"
    echo ">> rdma devices: target $NIC_T -> $ibt (root), initiator $NIC_I -> $ibi (into $NS_I)"
    rdma_netns_exclusive || exit 1
    echo ">> addressing target $NIC_T=$IP_T/$PREFIX in root (carrier flap to seat GID)"
    rdma_address_nic "$NIC_T" "$IP_T" ""
    echo ">> isolating initiator $NIC_I=$IP_I/$PREFIX in $NS_I"
    ip netns add "$NS_I"
    ip link set "$NIC_I" netns "$NS_I"
    rdma_move_dev "$ibi" "$NS_I"
    rdma_address_nic "$NIC_I" "$IP_I" "$NS_I"
    realwire_prove_wire || exit 1
    rdma_verify_dev "$NS_I" "$ibi" || fail "$ibi not ACTIVE in $NS_I (carrier/GID/cable?)"
    rdma_verify_dev "" "$ibt"      || fail "$ibt not ACTIVE in root (carrier on $NIC_T?)"
    echo "   RoCE up: $ibt@root ($IP_T) <-> $ibi@$NS_I ($IP_I), ACTIVE, wire proven"
}
cmd_down_rdma() {
    echo ">> removing initiator namespace (returns NIC_I + rdma device to root)"
    in_net "$NS_I" ip link set "$NIC_I" netns 1 2>/dev/null || true
    ip netns del "$NS_I" 2>/dev/null || true
    [ -n "${NIC_T:-}" ] && ip addr del "$IP_T/$PREFIX" dev "$NIC_T" 2>/dev/null || true
    ip rule del to "$IP_T/$PREFIX" lookup main pref 5000 2>/dev/null || true
    echo "   $NS_I removed; $IP_T removed from ${NIC_T:-NIC_T}. (rdma netns mode left exclusive)"
}
cmd_up_tcp() {
    require_nics
    realwire_netns_create      # shared: create NS_T/NS_I, move NICs in, address + MTU
    [ "$NIC_TUNE" = 1 ] && { nic_offloads "$NIC_T" on "$NS_T"; nic_offloads "$NIC_I" on "$NS_I"; }
    realwire_prove_wire || exit 1
}
cmd_down_tcp() {
    echo ">> removing namespaces and returning NICs to root"
    realwire_netns_delete
}
cmd_up() {
    if [ "$TRANSPORT" = rdma ]; then cmd_up_rdma; else cmd_up_tcp; fi
}
cmd_down() {
    # If SPDK ran on its userspace NVMe driver (SPDK_BDEV=nvme), rebind the
    # backend PCI device to the kernel so it is a normal /dev/nvme again. A
    # failed rebind is loud and keeps its state for a retry — still tear the
    # wire down rather than aborting mid-cleanup (set -e).
    spdk_vfio_reset || true
    if [ "$TRANSPORT" = rdma ]; then cmd_down_rdma; else cmd_down_tcp; fi
}

# ---- targets: start/stop route to one (or both) ----------------------
start_one() {
    case "$1" in
        spdk)  spdk_start   "$SPDK_NQN"  "$SPDK_PORT"  "$IP_T" \
                   "${SPDK_BACKEND:?set SPDK_BACKEND to the SPDK target backing file or block device (or SPDK_BDEV=malloc)}" ;;
        nvmet) nvmet_setup  "$NVMET_NQN" "$NVMET_PORT" "$IP_T" \
                   "${NVMET_BACKEND:?set NVMET_BACKEND to the nvmet target backing file or block device}" ;;
    esac
}
stop_one() {
    case "$1" in spdk) spdk_stop ;; nvmet) nvmet_teardown "$NVMET_NQN" ;; esac
}

cmd_status() {
    echo "== transport: $TRANSPORT =="
    if [ "$TRANSPORT" = rdma ]; then
        echo "== root rdma link (target) =="; rdma link show 2>/dev/null || echo "(none)"
        echo "== $NS_I rdma link (initiator) =="; ip netns exec "$NS_I" rdma link show 2>/dev/null || echo "(none)"
    else
        echo "== namespaces =="; ip netns list 2>/dev/null | grep -E "$NS_T|$NS_I" || echo "(none)"
        echo "== $NS_T listeners =="; ip netns exec "$NS_T" ss -ltn 2>/dev/null | grep -E ":$SPDK_PORT|:$NVMET_PORT" || echo "(none)"
    fi
    echo "== connected devices =="
    echo "  spdk  ($SPDK_NQN): $(find_dev "$SPDK_NQN" || echo none)"
    echo "  nvmet ($NVMET_NQN): $(find_dev "$NVMET_NQN" || echo none)"
    tune_status
}

# ---- dispatch --------------------------------------------------------
case "${1:-}" in
    up)         cmd_up ;;
    down)       cmd_down ;;
    start)      run_for_targets start_one      "${2:-}" ;;
    stop)       run_for_targets stop_one       "${2:-}" ;;
    discover)   run_for_targets discover_one   "${2:-}" ;;
    connect)    run_for_targets connect_one    "${2:-}"
                if [ "$NIC_TUNE" = 1 ]; then
                    if [ "$TRANSPORT" = rdma ]; then tune_target_rdma
                    else tune_target_nic; tune_initiator_tcp; fi
                fi ;;
    disconnect) run_for_targets disconnect_one "${2:-}" ;;
    fio)        run_for_targets fio_one        "${2:-}" ;;
    fio_verify) run_for_targets fio_verify_one "${2:-}" ;;
    fio_perf)   run_for_targets fio_perf_one   "${2:-}" ;;
    status)     cmd_status ;;
    help|usage) usage ;;
    *) usage >&2; exit 1 ;;
esac
