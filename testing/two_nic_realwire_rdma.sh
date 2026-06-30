#!/usr/bin/env bash
#
# two_nic_realwire_rdma.sh — the NVMe/RDMA (RoCEv2) sibling of
# two_nic_realwire.sh: run an NVMe/RDMA target and initiator on ONE host but
# force the traffic across two real RoCE NICs (real hardware offload), for both
# the in-kernel nvmet-rdma target and ioutgt-nvme-rdma, so the two can be
# compared back to back on the same wire.
#
# TOPOLOGY — asymmetric, unlike the TCP sibling
# ---------------------------------------------
# nvmet-rdma's CM listener is hardcoded to the init (root) netns: the kernel
# does rdma_create_id(&init_net, ...) + inet_pton_with_scope(&init_net, ...)
# (drivers/nvme/target/rdma.c), ignoring the configfs writer's netns. So unlike
# nvmet-tcp (sock_create in the writer's netns), nvmet-rdma can ONLY bind in the
# root netns. We therefore keep the TARGET side (NIC_T, IP_T, and its rdma
# device) in the root netns and isolate only the INITIATOR in its own netns:
#
#   root netns (TARGET)                      NS_I (INITIATOR)
#   ┌───────────────────────────┐    ┌────────────────────────────┐
#   │ NIC_T  IP_T  + ibdev_T     │    │ NIC_I  IP_I  + ibdev_I      │
#   │ nvmet-rdma :24420          │    │ nvme connect -t rdma        │
#   │ ioutgt-nvme-rdma :14420    │    │                             │
#   └─────────────┬─────────────┘    └─────────────┬──────────────┘
#                 │      physical cable/switch       │
#                 └──────────────────────────────────┘
#
# The wire is still forced: with the initiator's IP/device in NS_I (and no veth
# to root), root reaches IP_I only out NIC_T → the physical link → NIC_I, and
# vice-versa. The loopback/HCA shortcut only triggers when both endpoints share
# a netns, so isolating just the initiator is enough. A cross-namespace ping
# (NS_I → IP_T) proves the bytes crossed the wire.
#
# !!! netns-exclusive is GLOBAL !!!
#   `rdma system set netns exclusive` is a host-wide mode (revert with
#   `rdma system set netns shared`). It can only be set while no rdma device
#   sits in a non-default netns or is in use. Set it on a box with no other
#   live RDMA users. 'down' leaves the mode exclusive on purpose.
#
# !!! SAFETY !!!  Moving NIC_I into a namespace removes it from root. Do NOT use
#   the NIC that carries your SSH/management link. NIC_T and NIC_I must be two
#   SEPARATE cards (two ports of one card share a single rdma device, which
#   cannot be split across netns).
#
# Two targets, distinct port/NQN/backend, both reachable on the same target IP:
#   ioutgt : 14420  nqn...:ioutgt   IOUTGT_BACKEND   (ioutgt-nvme-rdma)
#   nvmet  : 24420  nqn...:nvmet    NVMET_BACKEND    (in-kernel nvmet-rdma)
#
# USAGE (one env block, then subcommands; selector verbs take nvmet|ioutgt)
#   export NIC_T=mlx5p1 NIC_I=mlx5p2
#   export IOUTGT_BACKEND=/dev/nvme0n1 NVMET_BACKEND=/dev/nvme1n1
#   sudo -E ./two_nic_realwire_rdma.sh up
#   sudo -E ./two_nic_realwire_rdma.sh start                # both targets
#   sudo -E ./two_nic_realwire_rdma.sh connect ioutgt       # or just one
#   sudo -E ./two_nic_realwire_rdma.sh fio                  # both, back to back
#   sudo -E ./two_nic_realwire_rdma.sh fio_perf             # perf sweep, both
#   sudo -E ./two_nic_realwire_rdma.sh disconnect
#   sudo -E ./two_nic_realwire_rdma.sh stop
#   sudo -E ./two_nic_realwire_rdma.sh down
#
# KNOBS (env vars; see also common.sh)
#   IOUTGT_BACKEND / NVMET_BACKEND   each target's file or block device
#   BACKEND_GB=2        size of an auto-created backing file
#   NR_QUEUES=4         IO queues   (ioutgt --io-threads; connect -i)
#   QUEUE_SIZE=128      IO qdepth    (ioutgt --io-queue-size; connect -q)
#   IP_T/IP_I/PREFIX/MTU             addressing (jumbo MTU 9000 by default)
#   FIO_RW/BS/QD/JOBS/SECS           fio / fio_perf parameters
set -euo pipefail

# RDMA fabric: selects ioutgt-nvme-rdma, nvmet-rdma, `nvme -t rdma`, and (in
# common.sh) forces digests + send-zc off. Must be set before common.sh.
export TRANSPORT=rdma

# ---- config (override via environment) -------------------------------
NS_I="${NS_I:-nvmei}"           # initiator network namespace (target stays in root)
IP_T="${IP_T:-192.168.50.1}"    # target IP (on NIC_T, in the root netns)
IP_I="${IP_I:-192.168.50.2}"    # initiator IP (on NIC_I, inside NS_I)
PREFIX="${PREFIX:-24}"
# Jumbo by default; RoCE large-IO benefits from fewer, bigger frames. Both NICs
# (cabled back-to-back) must agree. Override MTU=1500 for a link that can't.
MTU="${MTU:-9000}"
# Distinct port + NQN per target so both run at once on the same target IP.
IOUTGT_PORT=14420
IOUTGT_NQN="nqn.2026-06.io.realwire:ioutgt"
NVMET_PORT=24420
NVMET_NQN="nqn.2026-06.io.realwire:nvmet"
# shellcheck disable=SC2034  # HOSTNQN consumed by common.sh's connect/discover
HOSTNQN="nqn.2026-06.io.realwire:host"

# Transport context consumed by common.sh: the target listens on IP_T in the
# root netns; the initiator's nvme-cli runs inside NS_I (so its RDMA-CM resolve
# egresses NIC_I across the wire).
# shellcheck disable=SC2034  # TARGET_IP consumed by common.sh's verbs
TARGET_IP="$IP_T"
ini_exec() { ip netns exec "$NS_I" "$@"; }

. "$(dirname "$0")/common.sh"

# Per-target backend (file or block device); each target has its OWN, so one env
# block drives both. Validated only when its target is started.
NVMET_BACKEND="${NVMET_BACKEND:-}"
IOUTGT_BACKEND="${IOUTGT_BACKEND:-}"

# ioutgt target-process knobs.
IOUTGT_SOCK="${IOUTGT_SOCK:-/tmp/ioutgt-realwire-rdma.sock}"
IOUTGT_PIDFILE="${IOUTGT_PIDFILE:-/tmp/ioutgt-realwire-rdma.pid}"
IOUTGT_LOG="${IOUTGT_LOG:-/tmp/ioutgt-realwire-rdma.log}"

usage() {
    cat <<EOF
two_nic_realwire_rdma.sh — NVMe/RDMA target + initiator across two real RoCE
NICs on one host. The target (NIC_T + its rdma device) stays in the ROOT netns
(nvmet-rdma can only listen there); only the initiator (NIC_I) is isolated in
its own netns, which still forces hardware-offloaded RoCE over the wire.
Compares ioutgt-nvme-rdma vs the in-kernel nvmet-rdma target.

Targets (same target IP $IP_T, distinct port/NQN/backend):
  ioutgt   :$IOUTGT_PORT   $IOUTGT_NQN   (IOUTGT_BACKEND)
  nvmet    :$NVMET_PORT   $NVMET_NQN   (NVMET_BACKEND)

Usage: $0 <subcommand> [nvmet|ioutgt]
       (selector verbs act on BOTH targets when the selector is omitted)

  up                            rdma-exclusive; address NIC_T in root, isolate
                                NIC_I (+ its rdma dev) in $NS_I; prove wire
  down                          remove $NS_I (returns NIC_I + its rdma dev to root)
  start         [nvmet|ioutgt]  start the target(s) (nvmet = in-kernel)
  stop          [nvmet|ioutgt]  stop the target(s)
  discover      [nvmet|ioutgt]  nvme discover -t rdma
  connect       [nvmet|ioutgt]  nvme connect -t rdma; wait for the namespace
  disconnect    [nvmet|ioutgt]  nvme disconnect
  fio           [nvmet|ioutgt]  fio on the connected device(s)
  fio_perf      [nvmet|ioutgt]  perf sweep: randread/randwrite x bs={4k,64k}
  status                        netns, rdma links, addresses, connected devices
  help                          this message

Required env: NIC_T, NIC_I (two SEPARATE RoCE cards cabled back-to-back) and the
started target's backend (IOUTGT_BACKEND / NVMET_BACKEND, a file or bdev).
Knobs: BACKEND_GB=$BACKEND_GB NR_QUEUES=$NR_QUEUES QUEUE_SIZE=$QUEUE_SIZE
  IP_T=$IP_T IP_I=$IP_I PREFIX=$PREFIX MTU=$MTU  FIO_RW/BS/QD/JOBS/SECS

Example:
  export NIC_T=mlx5p1 NIC_I=mlx5p2 IOUTGT_BACKEND=/dev/nvme0n1 NVMET_BACKEND=/dev/nvme1n1
  sudo -E $0 up && sudo -E $0 start
  sudo -E $0 connect && sudo -E $0 fio_perf && sudo -E $0 disconnect
  sudo -E $0 stop && sudo -E $0 down
EOF
}

# 'help'/'usage' must work without root or NIC_T/NIC_I, so handle it here.
case "${1:-}" in help|usage|-h|--help) usage; exit 0 ;; esac

[ "$(id -u)" -eq 0 ] || { echo "must run as root (use sudo)"; exit 1; }
command -v rdma >/dev/null 2>&1 || { echo "the 'rdma' tool is required (install iproute2/rdma-core)"; exit 1; }

require_nics() {
    : "${NIC_T:?set NIC_T to the target-side RoCE NIC, e.g. NIC_T=mlx5p1}"
    : "${NIC_I:?set NIC_I to the initiator-side RoCE NIC, e.g. NIC_I=mlx5p2}"
}

fail() { echo "FAIL: $*" >&2; exit 1; }

# Target context for common.sh's nvmet_setup/ioutgt_start: BOTH targets run in
# the ROOT netns (nvmet-rdma's listener is pinned to init_net; ioutgt-nvme-rdma
# binds IP_T on NIC_T's rdma device, which also stays in root). So no netns
# wrapper for either — a plain subshell and an empty ioutgt launch prefix.
nvmet_exec() { bash -c "$1"; }
# shellcheck disable=SC2034  # consumed by common.sh's ioutgt_start
IOUTGT_NETNS=()

# ---- rdma-device <-> netns helpers (RDMA-specific) -------------------
# The rdma (ibverbs) device name backing a netdev, read from sysfs while the
# NIC is still reachable in the current netns.
nic_ibdev() {
    local nic="$1" d
    for d in /sys/class/net/"$nic"/device/infiniband/*; do
        [ -e "$d" ] || continue
        basename "$d"; return 0
    done
    return 1
}

# Put the box in rdma netns-exclusive mode (so rdma devices honour netns and can
# be moved into one). Idempotent: a no-op if already exclusive.
rdma_netns_exclusive() {
    local mode
    mode="$(rdma system show 2>/dev/null | grep -o 'netns [a-z]*' | awk '{print $2}')"
    if [ "$mode" = exclusive ]; then
        echo "   rdma netns mode already exclusive"
        return 0
    fi
    echo ">> setting rdma system netns mode = exclusive (global; was ${mode:-shared})"
    rdma system set netns exclusive 2>&1 || {
        echo "   could not set rdma netns exclusive — it requires that no rdma" >&2
        echo "   device is in a non-default netns or in use (no live nvme-rdma/" >&2
        echo "   iSER/etc. sessions). Free them and retry, or set it at boot." >&2
        return 1
    }
}

# Move an rdma device into a namespace. In exclusive mode it may already have
# followed its netdev into the ns, so tolerate failure and let the verify decide.
rdma_move_dev() { rdma dev set "$1" netns "$2" 2>/dev/null || true; }

# True once the RoCEv2 GID for IPv4 $2 is present on $1's rdma device (in netns
# $3, "" for root). $2's dotted quad maps to the ::ffff:HHHH:HHHH GID suffix.
rdma_gid_ready() {
    local nic="$1" ip="$2" ns="$3"; local -a x=(); [ -n "$ns" ] && x=(ip netns exec "$ns")
    local ib hex; ib="$("${x[@]}" sh -c "ls /sys/class/net/$nic/device/infiniband/ 2>/dev/null" | head -1)"
    [ -n "$ib" ] || return 1
    # shellcheck disable=SC2086  # deliberate split of the dotted quad into 4 args
    hex="$(printf '%02x%02x:%02x%02x' ${ip//./ })"
    "${x[@]}" sh -c "grep -qi 'ffff:$hex' /sys/class/infiniband/$ib/ports/*/gids/* 2>/dev/null"
}

# Address a RoCE NIC and make its GID usable for rdma_bind_addr/resolve. Under
# `rdma system netns exclusive`, a freshly-added RoCE GID lands in the sysfs GID
# table but NOT the rdma_cm GID cache until a netdev carrier event fires — so
# bind/resolve return EADDRNOTAVAIL despite the GID being "present". mlx5 needs
# a real link down/up (an IP re-add alone is not enough), so flap the carrier,
# then add the IP while the link is up and wait for the GID to land. $3 = netns
# ("" for root). Verified on a two-card mlx5 box: without this, both
# ioutgt-nvme-rdma and nvmet-rdma (and even rping) fail to bind the target IP.
rdma_address_nic() {
    local nic="$1" ip="$2" ns="$3"; local -a x=(); [ -n "$ns" ] && x=(ip netns exec "$ns")
    "${x[@]}" ip addr flush dev "$nic" 2>/dev/null || true
    "${x[@]}" ip link set "$nic" down
    "${x[@]}" ip link set "$nic" up
    "${x[@]}" ip link set "$nic" mtu "$MTU"
    # A high-speed (100GbE) link can take several seconds to re-negotiate carrier
    # after the flap, and the RoCE GID only seats once carrier is up — so wait for
    # carrier BEFORE adding the IP, then allow ample time for the GID to land.
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

# Wait (carrier settles) for an rdma device to be present + ACTIVE. $1 is the
# netns ("" = root/current); $2 is the device name.
rdma_verify_dev() {
    local ns="$1" dev="$2" i; local -a pfx=()
    [ -n "$ns" ] && pfx=(ip netns exec "$ns")
    for i in $(seq 1 20); do
        if "${pfx[@]}" rdma link show 2>/dev/null | grep "$dev/" | grep -qi "state ACTIVE"; then
            return 0
        fi
        sleep 0.5
    done
    echo "   ${ns:-root} rdma link:" >&2
    "${pfx[@]}" rdma link show 2>/dev/null | sed 's/^/     /' >&2 || true
    return 1
}

# =====================================================================
cmd_up() {
    require_nics
    # Idempotency FIRST: clear a stale initiator namespace from a previous run,
    # which also returns NIC_I (+ its rdma device) to root — so the nic_ibdev
    # sysfs lookups below can see both NICs.
    in_net "$NS_I" ip link set "$NIC_I" netns 1 2>/dev/null || true
    ip netns del "$NS_I" 2>/dev/null || true

    # Resolve each NIC's rdma device (both NICs are in root now).
    local ibt ibi
    ibt="$(nic_ibdev "$NIC_T")" || fail "no rdma (RoCE) device under /sys/class/net/$NIC_T/device/infiniband — is $NIC_T a RoCE NIC with mlx5_ib loaded?"
    ibi="$(nic_ibdev "$NIC_I")" || fail "no rdma (RoCE) device under /sys/class/net/$NIC_I/device/infiniband — is $NIC_I a RoCE NIC?"
    [ "$ibt" != "$ibi" ] || fail "NIC_T ($NIC_T) and NIC_I ($NIC_I) share rdma device $ibt — two ports of one card cannot be split across netns; use two separate cards"
    echo ">> rdma devices: target $NIC_T -> $ibt (stays in root), initiator $NIC_I -> $ibi (into $NS_I)"
    rdma_netns_exclusive || exit 1

    # Target side stays in the root netns. rdma_address_nic carrier-flaps it so
    # the RoCEv2 GID lands in the rdma_cm cache (else bind/resolve hit
    # EADDRNOTAVAIL under exclusive mode).
    echo ">> addressing target $NIC_T=$IP_T/$PREFIX in root netns (carrier flap to seat GID)"
    rdma_address_nic "$NIC_T" "$IP_T" ""

    # Initiator side: isolate NIC_I in NS_I, move its rdma device there, then
    # address it (carrier flap) so its GID is usable from inside the netns.
    echo ">> isolating initiator $NIC_I=$IP_I/$PREFIX in $NS_I"
    ip netns add "$NS_I"
    ip link set "$NIC_I" netns "$NS_I"
    rdma_move_dev "$ibi" "$NS_I"   # may already have followed its netdev
    rdma_address_nic "$NIC_I" "$IP_I" "$NS_I"

    realwire_prove_wire || exit 1   # ping NS_I -> IP_T across the wire; settles carrier
    rdma_verify_dev "$NS_I" "$ibi" || fail "$ibi is not ACTIVE in $NS_I (carrier? GID? cable?)"
    rdma_verify_dev "" "$ibt"      || fail "$ibt is not ACTIVE in root (carrier on $NIC_T?)"
    echo "   RoCE up: target $ibt@root ($IP_T) <-> initiator $ibi@$NS_I ($IP_I), state ACTIVE, wire proven"
}

cmd_down() {
    echo ">> removing initiator namespace (returns NIC_I + its rdma device to root)"
    # Stop the targets first with 'stop'. Deleting NS_I returns BOTH NIC_I and
    # its assigned rdma device to the root netns (kernel rdma_dev_exit_net).
    in_net "$NS_I" ip link set "$NIC_I" netns 1 2>/dev/null || true
    ip netns del "$NS_I" 2>/dev/null || true
    # The target NIC stayed in root; drop the test IP we added to it.
    [ -n "${NIC_T:-}" ] && ip addr del "$IP_T/$PREFIX" dev "$NIC_T" 2>/dev/null || true
    echo "   $NS_I removed; NIC_I + rdma device returned to root; $IP_T removed from ${NIC_T:-NIC_T}."
    echo "   (rdma system netns mode left exclusive; 'rdma system set netns shared' to revert)"
}

# ---- targets: 'start'/'stop [SELECTOR]' route to one (or both) ------
start_one() {
    case "$1" in
        nvmet)  nvmet_setup  "$NVMET_NQN"  "$NVMET_PORT"  "$IP_T" \
                    "${NVMET_BACKEND:?set NVMET_BACKEND to the nvmet target backing file or block device}" ;;
        ioutgt) ioutgt_start "$IOUTGT_NQN" "$IOUTGT_PORT" "$IP_T" \
                    "${IOUTGT_BACKEND:?set IOUTGT_BACKEND to the ioutgt target backing file or block device}" ;;
    esac
}
stop_one() {
    case "$1" in
        nvmet)  nvmet_teardown "$NVMET_NQN" ;;
        ioutgt) ioutgt_stop ;;
    esac
}

cmd_status() {
    echo "== rdma system =="; rdma system show 2>/dev/null || true
    echo "== root rdma link (target) =="; rdma link show 2>/dev/null || echo "(none)"
    echo "== $NS_I rdma link (initiator) =="; ip netns exec "$NS_I" rdma link show 2>/dev/null || echo "(none)"
    echo "== root addr (target) =="; ip -br addr 2>/dev/null | grep -E "${NIC_T:-NoSuchNic}" || echo "($NIC_T not in root?)"
    echo "== $NS_I addr (initiator) =="; ip netns exec "$NS_I" ip -br addr 2>/dev/null || true
    echo "== listeners (root :$IOUTGT_PORT/:$NVMET_PORT) =="; ss -ltn 2>/dev/null | grep -E ":$IOUTGT_PORT|:$NVMET_PORT" || echo "(RDMA listeners are not TCP sockets; check rdma resource)"
    echo "== connected devices =="
    echo "  ioutgt ($IOUTGT_NQN): $(find_dev "$IOUTGT_NQN" || echo none)"
    echo "  nvmet ($NVMET_NQN): $(find_dev "$NVMET_NQN" || echo none)"
}

# Selector verbs take 'nvmet' or 'ioutgt'; omitting it acts on BOTH (the
# comparison). discover/connect/disconnect/fio/fio_perf come from common.sh.
case "${1:-}" in
    up)                  cmd_up ;;
    down)                cmd_down ;;
    start)               run_for_targets start_one      "${2:-}" ;;
    stop)                run_for_targets stop_one       "${2:-}" ;;
    discover)            run_for_targets discover_one   "${2:-}" ;;
    connect)             run_for_targets connect_one    "${2:-}" ;;
    disconnect)          run_for_targets disconnect_one "${2:-}" ;;
    fio)                 run_for_targets fio_one        "${2:-}" ;;
    fio_perf)            run_for_targets fio_perf_one   "${2:-}" ;;
    status)              cmd_status ;;
    help|usage)          usage ;;
    *) usage >&2; exit 1 ;;
esac
