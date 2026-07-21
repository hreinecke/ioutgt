#!/usr/bin/env bash
# common.sh — shared helpers for the NVMe target drivers
# (two_nic/realwire_tcp.sh, local_tgt.sh). Sourced, never executed. The fabric is
# selected by TRANSPORT=tcp|rdma (default tcp); see the knobs section.
#
# The sourcing script supplies the transport context; these helpers stay
# agnostic to whether the initiator runs in a network namespace (realwire)
# or directly on loopback (local_tgt):
#
#   TARGET_IP             IP the initiator dials / the target listens on
#   ini_exec <cmd...>     run an nvme-cli command in the initiator context
#                         (`ip netns exec NS_I ...` for realwire; a direct
#                         passthrough for local_tgt)
#   nvmet_exec <script>   run a configfs shell snippet in the TARGET's network
#                         context, so the nvmet listener socket is born there
#                         (`in_net NS_T bash -c ...` for realwire; a direct
#                         `bash -c ...` for local_tgt). Used by nvmet_setup/
#                         nvmet_teardown; configfs itself is a global singleton,
#                         only the enabling step is netns-sensitive.
#   IOUTGT_NETNS          array launch-prefix for the ioutgt target process
#                         (`(ip netns exec NS_T)` for realwire; `()` for
#                         local_tgt). Used by ioutgt_start.
#   IOUTGT_PORT/_NQN, NVMET_PORT/_NQN, HOSTNQN   per-target addressing
#
# The target-start functions additionally set a caller-local BACKEND that
# ensure_backing/fio see via bash dynamic scope.

# ---- shared knobs (override via environment) -------------------------
# Fixed host identity. nvme-cli generates a RANDOM hostid per invocation
# when none is given, but the kernel requires one hostnqn to map to exactly
# one hostid — so connecting to the second target with the same HOSTNQN but
# a fresh random hostid is rejected ("same hostnqn but different hostid").
# Pin both, shared across all connects from this host.
HOSTID="${HOSTID:-2e3b0c44-1c2e-4f3a-9b6d-000000000001}"

# Fabric: tcp (NVMe/TCP) or rdma (NVMe/RDMA over RoCEv2). One knob selects the
# ioutgt binary, the nvmet kernel modules + port addr_trtype, the `nvme -t`
# type, and — for rdma — forces digests and zero-copy-send OFF (neither exists
# on the RDMA fabric, and the ioutgt-nvme-rdma binary has no such flags).
TRANSPORT="${TRANSPORT:-tcp}"
case "$TRANSPORT" in
    tcp|rdma) ;;
    *) echo "TRANSPORT must be tcp or rdma (got '$TRANSPORT')" >&2; exit 1 ;;
esac

# Was NR_QUEUES set by the user? Captured BEFORE the :-4 default below, so
# a driver's 'up' may auto-size it from the NIC (nic_size_queues) only when
# the user did not choose. Read by nic.sh's nrq_state_init/nic_size_queues.
NRQ_USER_SET="${NR_QUEUES+1}"
NR_QUEUES="${NR_QUEUES:-4}"          # IO queues  (ioutgt --io-threads; connect -i)
QUEUE_SIZE="${QUEUE_SIZE:-128}"      # IO qdepth   (ioutgt --io-queue-size; connect -q)
BACKEND_GB="${BACKEND_GB:-2}"        # size of an auto-created backing file
# TCP digest negotiation (CRC32C), coupled across both ends so the two
# targets stay aligned: =1 asks the host to negotiate it (nvme connect
# --hdr-digest / --data-digest) and lets ioutgt accept it; =0 keeps it off
# and makes ioutgt refuse it (--no-hdgst / --no-ddgst). nvmet has no target
# knob — it just honours the host request — so coupling the host request and
# the ioutgt stance here is what keeps nvmet and ioutgt identical.
HDGST="${HDGST:-0}"   # header digest (TCP only)
DDGST="${DDGST:-0}"   # data digest (TCP only)
# Host-side request flags (used by connect/discover) and the matching ioutgt
# target refuse flags (used by each driver's ioutgt start). RDMA has no PDU
# digests — leave both arrays empty (the ioutgt-nvme-rdma binary has no
# --no-hdgst/--no-ddgst either) and note any ignored request.
CONNECT_DGST=()
IOUTGT_DGST=()
if [ "$TRANSPORT" = tcp ]; then
    if [ "$HDGST" = 1 ]; then CONNECT_DGST+=(--hdr-digest);  else IOUTGT_DGST+=(--no-hdgst); fi
    if [ "$DDGST" = 1 ]; then CONNECT_DGST+=(--data-digest); else IOUTGT_DGST+=(--no-ddgst); fi
elif [ "$HDGST" = 1 ] || [ "$DDGST" = 1 ]; then
    echo "   note: TRANSPORT=rdma has no PDU digests; ignoring HDGST/DDGST" >&2
fi

require_root() { [ "$(id -u)" -eq 0 ] || { echo "must run as root (use sudo)"; exit 1; }; }

fail() { echo "FAIL: $*" >&2; exit 1; }

# The target kinds a driver compares, in order. Defaults to the ioutgt-vs-nvmet
# pair; the spdk driver overrides it (e.g. TARGET_KINDS="spdk nvmet") before
# sourcing. Drives run_for_targets' "act on all" case and the selector check.
TARGET_KINDS="${TARGET_KINDS:-ioutgt nvmet}"

# Target identity: NQN namespace + port/NQN per kind, shared by every driver
# so the selector verbs, usage text, and start/stop agree. The two_nic
# drivers use the realwire defaults; local_tgt.sh overrides NQN_BASE (and
# SPDK_PORT, so all three kinds can share one IP). ioutgt and spdk share a
# default port: no driver serves both kinds at once.
NQN_BASE="${NQN_BASE:-nqn.2026-06.io.realwire}"
IOUTGT_PORT="${IOUTGT_PORT:-14420}"
IOUTGT_NQN="${IOUTGT_NQN:-$NQN_BASE:ioutgt}"
NVMET_PORT="${NVMET_PORT:-24420}"
NVMET_NQN="${NVMET_NQN:-$NQN_BASE:nvmet}"
SPDK_PORT="${SPDK_PORT:-14420}"
SPDK_NQN="${SPDK_NQN:-$NQN_BASE:spdk}"
HOSTNQN="${HOSTNQN:-$NQN_BASE:host}"

# Map a target kind ('nvmet'|'ioutgt'|'spdk') to its "PORT NQN" pair. Only the
# kinds a given driver defines the PORT/NQN vars for are usable.
target_params() {
    case "${1:-}" in
        ioutgt) echo "$IOUTGT_PORT $IOUTGT_NQN" ;;
        nvmet)  echo "$NVMET_PORT $NVMET_NQN" ;;
        spdk)   echo "$SPDK_PORT $SPDK_NQN" ;;
        *) echo "specify target: one of [$TARGET_KINDS]" >&2; return 1 ;;
    esac
}

# Run a per-target function $1 for the selected target $2, or for every kind in
# $TARGET_KINDS (in order) when no selector is given.
run_for_targets() {
    local fn="$1" sel="${2:-}" k
    if [ -n "$sel" ]; then
        for k in $TARGET_KINDS; do [ "$k" = "$sel" ] && { "$fn" "$sel"; return; }; done
        echo "specify target: one of [$TARGET_KINDS] (or omit for all)" >&2; exit 1
    fi
    for k in $TARGET_KINDS; do "$fn" "$k"; done
}

# Rig-ownership guard: abort if any nvme controller for one of this
# driver's NQNs exists in sysfs — whatever its transport. The tcp/rdma/
# spdk drivers share NQNs and the address plan by design, so this also
# catches ANOTHER driver's live session, which 'up'/'start' would
# otherwise silently dismantle ('up' deletes the initiator netns,
# nvmet_setup collides on the shared IP:port — how a live TCP comparison
# run got torn down mid-flight, 2026-07). $1 names the refusing verb; $2
# restricts the check to one kind ('start X' must stay legal while kind Y
# of OUR session is already connected — 'up' checks every kind).
guard_no_sessions() {
    local verb="$1" k params nqn ctrl
    for k in ${2:-$TARGET_KINDS}; do
        params="$(target_params "$k")" || continue
        nqn="${params#* }"
        ctrl="$(find_ctrl "$nqn")" || continue
        fail "$verb: live controller $ctrl for $nqn — a session (possibly another transport's driver; they share NQNs) is using this rig. Disconnect it first: 'nvme disconnect -n $nqn', or that driver's 'disconnect' + 'stop'."
    done
}

# Start/stop one target kind — shared by every driver (the identity block
# above supplies NQN/port; TARGET_IP comes from the driver). The `:?`
# aborts keep the realwire drivers' friendly "set X_BACKEND..." message;
# local_tgt.sh never hits them (its backends default to /tmp files).
start_one() {
    guard_no_sessions start "$1"
    case "$1" in
        ioutgt) ioutgt_start "$IOUTGT_NQN" "$IOUTGT_PORT" "$TARGET_IP" \
                    "${IOUTGT_BACKEND:?set IOUTGT_BACKEND to the ioutgt target backing file or block device}" ;;
        nvmet)  nvmet_setup  "$NVMET_NQN"  "$NVMET_PORT"  "$TARGET_IP" \
                    "${NVMET_BACKEND:?set NVMET_BACKEND to the nvmet target backing file or block device}" ;;
        spdk)   spdk_start   "$SPDK_NQN"   "$SPDK_PORT"   "$TARGET_IP" \
                    "${SPDK_BACKEND:?set SPDK_BACKEND to the SPDK target backing file or block device (or SPDK_BDEV=malloc)}" ;;
    esac
}
stop_one() {
    case "$1" in
        ioutgt) ioutgt_stop ;;
        nvmet)  nvmet_teardown "$NVMET_NQN" ;;
        spdk)   spdk_stop ;;
    esac
}

# Shared subcommand dispatch: every driver ends with `realwire_dispatch "$@"`
# after defining its hooks. Selector verbs act on one target kind, or on
# every kind in TARGET_KINDS when the selector is omitted. Hooks:
#   usage, cmd_status              required
#   cmd_up / cmd_down              wire setup/teardown; wire-less drivers
#                                  (local_tgt) omit them and the verbs fall
#                                  through to the usage error
#   post_connect_tune SELECTOR     optional, runs after 'connect'
#   extra_verbs VERB [ARGS...]     optional, tried for verbs this table
#                                  doesn't know (tcp: iperf; rdma: ibperf/
#                                  stat). Handled verbs must exit/exec —
#                                  not return — so their failure keeps its
#                                  exit status (as an && operand the hook
#                                  runs with errexit off, and a nonzero
#                                  return would fall through to the usage
#                                  error). Return 1 only for unknown verbs.
realwire_dispatch() {
    case "${1:-}" in
        up|down)    declare -F "cmd_$1" >/dev/null || { usage >&2; exit 1; }
                    "cmd_$1" ;;
        start)      run_for_targets start_one      "${2:-}" ;;
        stop)       run_for_targets stop_one       "${2:-}" ;;
        discover)   run_for_targets discover_one   "${2:-}" ;;
        connect)    run_for_targets connect_one    "${2:-}"
                    if declare -F post_connect_tune >/dev/null; then
                        post_connect_tune "${2:-}"
                    fi ;;
        disconnect) run_for_targets disconnect_one "${2:-}" ;;
        fio)        run_for_targets fio_one        "${2:-}" ;;
        fio_verify) run_for_targets fio_verify_one "${2:-}" ;;
        fio_perf)   run_for_targets fio_perf_one   "${2:-}" ;;
        status)     cmd_status ;;
        help|usage) usage ;;
        *)          if declare -F extra_verbs >/dev/null; then
                        extra_verbs "$@" && return 0
                    fi
                    usage >&2; exit 1 ;;
    esac
}

# Ensure $BACKEND (a caller local) exists. A missing non-/dev path is
# auto-created at BACKEND_GB; a missing /dev/* is an error.
ensure_backing() {
    case "$BACKEND" in
        /dev/*) [ -e "$BACKEND" ] || { echo "block device $BACKEND does not exist" >&2; return 1; } ;;
        /*)     [ -e "$BACKEND" ] || { echo "   creating backing file $BACKEND (${BACKEND_GB}G)" >&2
                                       truncate -s "${BACKEND_GB}G" "$BACKEND" \
                                         || { echo "failed to create $BACKEND" >&2; return 1; }; } ;;
        *)      echo "BACKEND must be an absolute file or block-device path" >&2; return 1 ;;
    esac
}

# Namespace block device for an NQN via sysfs (/sys/block/*/device/subsysnqn)
# — schema-independent and multipath-safe: with native NVMe multipath the
# head device nvmeXnZ is not under the controller's sysfs dir (only the
# per-path node nvmeXcYnZ is), so a /sys/class/nvme walk misses it; a block
# dev's device/subsysnqn resolves to its controller or subsystem in either
# layout. A per-path match (nvmeXcYnZ, no /dev entry) maps to its head.
find_dev() {
    local nqn="$1" blk name head
    for blk in /sys/block/nvme*n*; do
        [ -e "$blk" ] || continue
        name=$(basename "$blk")
        case "$name" in *p[0-9]*) continue ;; esac      # skip partitions
        [ -r "$blk/device/subsysnqn" ] || continue
        [ "$(cat "$blk/device/subsysnqn")" = "$nqn" ] || continue
        if [[ $name =~ ^(nvme[0-9]+)c[0-9]+(n[0-9]+)$ ]]; then
            head="${BASH_REMATCH[1]}${BASH_REMATCH[2]}"
        else
            head="$name"
        fi
        [ -b "/dev/$head" ] && { echo "/dev/$head"; return 0; }
    done
    return 1
}

# Controller node (/dev/nvmeN) for an NQN, on stdout.
find_ctrl() {
    local nqn="$1" c
    for c in /sys/class/nvme/nvme*; do
        [ -r "$c/subsysnqn" ] || continue
        [ "$(cat "$c/subsysnqn")" = "$nqn" ] && { echo "/dev/$(basename "$c")"; return 0; }
    done
    return 1
}

# Poll up to ~10s for the namespace block device of $nqn, nudging a rescan
# each tick: namespace enumeration can lag the connect.
wait_dev() {
    local nqn="$1" dev ctrl
    local deadline=$(( SECONDS + 10 ))
    while [ "$SECONDS" -lt "$deadline" ]; do
        dev=$(find_dev "$nqn") && { echo "$dev"; return 0; }
        # `|| true` is load-bearing under `set -e`: a non-zero rescan aborts.
        ctrl=$(find_ctrl "$nqn") && nvme ns-rescan "$ctrl" 2>/dev/null || true
        sleep 0.5
    done
    return 1
}

# ---- initiator verbs (transport via ini_exec + TARGET_IP) ------------
# Each takes a 'nvmet'|'ioutgt' selector. The nvme-cli command that creates
# the host socket runs through ini_exec (so realwire egresses NIC_I); the
# sysfs device lookups run in the current process (device nodes are global).
discover_one() {
    local port nqn; read -r port nqn < <(target_params "${1:-}") || exit 1
    modprobe "nvme-$TRANSPORT"
    ini_exec nvme discover -t "$TRANSPORT" -a "$TARGET_IP" -s "$port" \
        --hostnqn "$HOSTNQN" --hostid "$HOSTID" "${CONNECT_DGST[@]}"
}

connect_one() {
    local port nqn; read -r port nqn < <(target_params "${1:-}") || exit 1
    modprobe "nvme-$TRANSPORT"
    # Bump the host keep-alive timeout to 20s so a transient RC retransmit
    # under fabric congestion can't trip the keep-alive and wedge QID 0. Fixed
    # here on purpose — not an outside knob.
    local kato=20
    echo ">> connecting $1 -> $TARGET_IP:$port (request ${NR_QUEUES}q x $QUEUE_SIZE, keep-alive-tmo=${kato}s)"
    # -i/-q make the host REQUEST this many queues / this depth; the target
    # caps it, so the granted values are min(host request, target cap).
    ini_exec nvme connect -t "$TRANSPORT" -a "$TARGET_IP" -s "$port" \
        -n "$nqn" --hostnqn "$HOSTNQN" --hostid "$HOSTID" \
        --nr-io-queues "$NR_QUEUES" --queue-size "$QUEUE_SIZE" \
        --keep-alive-tmo "$kato" "${CONNECT_DGST[@]}"
    local dev
    if dev=$(wait_dev "$nqn"); then
        echo "   block device: $dev (controller $(find_ctrl "$nqn"), nqn $nqn)"
    else
        echo "   connected ($nqn) but no namespace block device appeared after 10s"
        echo "   controller: $(find_ctrl "$nqn" || echo '?'); check 'nvme list' / target namespace config"
    fi
}

disconnect_one() {
    local port nqn; read -r port nqn < <(target_params "${1:-}") || exit 1
    ini_exec nvme disconnect -n "$nqn" 2>/dev/null || true
    echo ">> disconnected $1 ($nqn)"
}

# ---- two-NIC netns scaffolding (shared by two_nic_realwire*.sh) ------
# Force target/initiator traffic across two PHYSICAL NICs on one host by
# isolating each in its OWN network namespace: with no veth/bridge linking the
# namespaces, the only path between them is the physical link between the cards,
# so a successful cross-namespace ping is itself proof the bytes crossed the
# wire. Transport-neutral — these helpers read the caller's NS_T/NS_I,
# NIC_T/NIC_I, IP_T/IP_I, PREFIX, MTU globals. (The RDMA driver additionally
# moves each NIC's rdma device into the netns; see two_nic/realwire_rdma.sh.)
NSDIR="${NSDIR:-/run/netns}"

# nsenter --net enters ONLY the net namespace, leaving the mount namespace
# alone — crucial for the kernel target, because `ip netns exec` remounts a
# fresh /sys and thereby SHADOWS the configfs at /sys/kernel/config.
in_net() { nsenter --net="$NSDIR/$1" "${@:2}"; }

# Create the two namespaces, move each physical NIC in, then address + MTU + up.
realwire_netns_create() {
    guard_no_sessions up            # never yank NICs from under a live session
    require_nics_in_root
    echo ">> creating namespaces $NS_T / $NS_I and moving NICs in"
    ip netns add "$NS_T"
    ip netns add "$NS_I"
    ip link set "$NIC_T" netns "$NS_T"
    ip link set "$NIC_I" netns "$NS_I"
    ip netns exec "$NS_T" ip addr add "$IP_T/$PREFIX" dev "$NIC_T"
    ip netns exec "$NS_I" ip addr add "$IP_I/$PREFIX" dev "$NIC_I"
    # MTU before carrier: both ends must match (mismatched MTU silently drops
    # oversized frames).
    ip netns exec "$NS_T" ip link set "$NIC_T" mtu "$MTU"
    ip netns exec "$NS_I" ip link set "$NIC_I" mtu "$MTU"
    ip netns exec "$NS_T" ip link set lo up
    ip netns exec "$NS_I" ip link set lo up
    ip netns exec "$NS_T" ip link set "$NIC_T" up
    ip netns exec "$NS_I" ip link set "$NIC_I" up
}

# Wait for carrier, then prove traffic crosses the physical link. Sizes the
# probe to the MTU (DF set) so an MTU mismatch / a link that cannot carry full
# frames fails here, loudly, instead of silently later. Returns non-zero on
# failure (the caller decides whether that is fatal).
realwire_prove_wire() {
    echo ">> waiting for link/carrier, then proving the wire with ping"
    sleep 2
    local psize=$((MTU - 28)) # minus IP(20)+ICMP(8) headers
    if ip netns exec "$NS_I" ping -c 3 -W 2 -M do -s "$psize" "$IP_T" >/dev/null; then
        echo "   OK: $IP_I -> $IP_T reachable at MTU $MTU (full-frame, DF). Only"
        echo "   path is the physical link between $NIC_I and $NIC_T -> wire."
    else
        echo "   FAIL: no full-frame ping at MTU $MTU. Check the cable/switch"
        echo "   between $NIC_I and $NIC_T, that both NICs have carrier, that"
        echo "   IP_T/IP_I share subnet /$PREFIX, and that the link supports"
        echo "   MTU $MTU (else re-run with MTU=1500)."
        return 1
    fi
}

# Return the NICs to root and delete the namespaces (best-effort; env-tolerant).
# Deleting a netns also auto-returns its physical NICs, so the explicit moves
# are belt-and-suspenders.
realwire_netns_delete() {
    [ -n "${NIC_T:-}" ] && in_net "$NS_T" ip link set "$NIC_T" netns 1 2>/dev/null || true
    [ -n "${NIC_I:-}" ] && in_net "$NS_I" ip link set "$NIC_I" netns 1 2>/dev/null || true
    ip netns del "$NS_T" 2>/dev/null || true
    ip netns del "$NS_I" 2>/dev/null || true
}

# ---- split-out helper libraries (same directory, sourced) ------------
# Per-target setup/teardown, fio verbs and NIC tuning live in sibling
# files; source them last so their knob defaults can see the shared knobs
# above.
_common_dir="$(dirname "${BASH_SOURCE[0]}")"
. "$_common_dir/nvmet.sh"
. "$_common_dir/ioutgt.sh"
. "$_common_dir/spdk.sh"
. "$_common_dir/fio.sh"
. "$_common_dir/nic.sh"
. "$_common_dir/rdma_wire.sh"
