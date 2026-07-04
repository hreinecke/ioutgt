#!/usr/bin/env bash
#
# iperf_link.sh — benchmark the raw TCP throughput of a back-to-back
# ethernet link between two local interfaces.
#
# The two NICs live on ONE host, cabled directly to each other (a physical
# "loopback"). To make traffic actually leave one port and arrive on the
# other over the wire — instead of the kernel short-circuiting it through
# loopback because both addresses are local — each NIC is moved into its
# own network namespace:
#
#       NS_A (server)                 NS_B (client)
#     ┌─────────────────┐           ┌─────────────────┐
#     │  NIC_A  IP_A     │═══════════│  NIC_B  IP_B     │   ← the cable
#     └─────────────────┘  the wire └─────────────────┘
#
# iperf3 then runs a server in NS_A and a client in NS_B, so every byte
# crosses the physical link. This is the same wiring `two_nic_realwire_tcp.sh`
# uses for the NVMe/TCP tests; this script is the standalone link
# benchmark (no target involved).
#
# USAGE
#   sudo ./iperf_link.sh enp1s0f0 enp1s0f1            # bench: up, run, down
#   sudo NIC_A=enp1s0f0 NIC_B=enp1s0f1 ./iperf_link.sh
#   sudo ./iperf_link.sh up   enp1s0f0 enp1s0f1       # set the link up
#   sudo ./iperf_link.sh run                          #   measure (repeatable)
#   sudo ./iperf_link.sh down                         #   tear the link down
#   sudo ./iperf_link.sh status
#
# KNOBS (env)
#   IP_A=192.168.60.1  IP_B=192.168.60.2  PREFIX=24   addressing
#   MTU=               set an MTU on both NICs (e.g. 9000 for jumbo)
#   SECS=10  STREAMS=4  OMIT=2  PORT=5201             iperf3 parameters
#   NS_A=iperf-a  NS_B=iperf-b                        namespace names
#
# Requires: iproute2, iperf3. The two NICs must be free (in the root
# namespace) when the link is brought up; `down` (and `bench`) return them.
set -euo pipefail

# ---- config ----------------------------------------------------------
NS_A="${NS_A:-iperf-a}"
NS_B="${NS_B:-iperf-b}"
IP_A="${IP_A:-192.168.60.1}"
IP_B="${IP_B:-192.168.60.2}"
PREFIX="${PREFIX:-24}"
MTU="${MTU:-}"
PORT="${PORT:-5201}"
SECS="${SECS:-10}"
STREAMS="${STREAMS:-4}"
OMIT="${OMIT:-2}"            # seconds of ramp-up to discard
NICS_STATE="${NICS_STATE:-/tmp/iperf-link.nics}"  # remembers NIC_A/NIC_B across verbs
SRV_STATE="${SRV_STATE:-/tmp/iperf-link.srv}"     # iperf3 server PID, for a scoped kill

usage() { sed -n '2,/^set -euo/p' "$0" | sed 's/^# \{0,1\}//; s/^#//' | head -n -1; }

require_root() {
    [ "$(id -u)" -eq 0 ] || { echo "must run as root (sudo)"; exit 1; }
}

have() { command -v "$1" >/dev/null 2>&1; }

# Run a command inside namespace $1.
nsx() { ip netns exec "$1" "${@:2}"; }

# Stop the iperf3 server we previously started (PID recorded in $SRV_STATE).
# Scoped to that one PID on purpose -- a host-wide `pkill -f iperf3` would also
# kill unrelated iperf3 servers sharing the default port on a dev box.
kill_server() {
    [ -f "$SRV_STATE" ] || return 0
    local pid=""; read -r pid < "$SRV_STATE" 2>/dev/null || true
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
    rm -f "$SRV_STATE"
}

# ---- subcommands -----------------------------------------------------
cmd_up() {
    local nic_a="$1" nic_b="$2"
    [ -n "$nic_a" ] && [ -n "$nic_b" ] || { echo "up needs two interface names"; exit 1; }
    have iperf3 || { echo "iperf3 not found — install it (apt/dnf install iperf3)"; exit 1; }
    printf '%s %s\n' "$nic_a" "$nic_b" > "$NICS_STATE"

    echo ">> creating namespaces $NS_A / $NS_B and moving the NICs in"
    ip netns add "$NS_A"
    ip netns add "$NS_B"
    ip link set "$nic_a" netns "$NS_A"
    ip link set "$nic_b" netns "$NS_B"

    nsx "$NS_A" ip addr add "$IP_A/$PREFIX" dev "$nic_a"
    nsx "$NS_B" ip addr add "$IP_B/$PREFIX" dev "$nic_b"
    if [ -n "$MTU" ]; then
        nsx "$NS_A" ip link set "$nic_a" mtu "$MTU"
        nsx "$NS_B" ip link set "$nic_b" mtu "$MTU"
    fi
    nsx "$NS_A" ip link set lo up
    nsx "$NS_B" ip link set lo up
    nsx "$NS_A" ip link set "$nic_a" up
    nsx "$NS_B" ip link set "$nic_b" up

    echo ">> waiting for carrier, then proving the wire with ping"
    sleep 2
    if nsx "$NS_B" ping -c 3 -W 2 "$IP_A" >/dev/null 2>&1; then
        echo "   OK: $IP_B -> $IP_A across the $nic_b <-> $nic_a wire."
    else
        echo "   FAIL: no ping over the link. Check the cable/carrier:"
        echo "     ip netns exec $NS_A ip -br link"
        exit 1
    fi
}

# Pull "<rate> <unit>" off an iperf3 text report, preferring the [SUM]
# receiver line (multi-stream) and otherwise the lone receiver line.
rate_of() {
    awk '
        /\[SUM\].*receiver/ { sum = $(NF-2) " " $(NF-1) }
        /receiver/          { last = $(NF-2) " " $(NF-1) }
        END                 { print (sum != "" ? sum : last) }'
}

# One direction: $1 label, remaining args appended to the client cmd.
measure() {
    local label="$1"; shift
    local out
    out="$(nsx "$NS_B" iperf3 -c "$IP_A" -p "$PORT" -t "$SECS" -O "$OMIT" \
        -P "$STREAMS" "$@" 2>&1)" || { echo "   $label: iperf3 failed"; echo "$out" | tail -3; return 1; }
    printf '   %-26s %s\n' "$label" "$(printf '%s\n' "$out" | rate_of)"
}

cmd_run() {
    have iperf3 || { echo "iperf3 not found"; exit 1; }
    ip netns list 2>/dev/null | grep -q "$NS_A" || { echo "link not up — run 'up' first"; exit 1; }

    # One persistent server in NS_A for the whole run. Clear only the server we
    # started last time (scoped to its recorded PID), never a host-wide pkill.
    kill_server
    # `exec` in the backgrounded subshell makes $! iperf3's own PID (not a
    # wrapper), so kill_server reliably stops exactly this server.
    { exec ip netns exec "$NS_A" iperf3 -s -p "$PORT" >/dev/null 2>&1; } &
    echo "$!" > "$SRV_STATE"
    trap 'kill_server' RETURN
    sleep 0.5

    echo ">> iperf3 over the link ($STREAMS streams, ${SECS}s, ${OMIT}s omit), receiver throughput:"
    measure "forward ($IP_B->$IP_A)" || true
    measure "reverse ($IP_A->$IP_B)" -R || true
    echo ">> bidirectional (both directions at once):"
    nsx "$NS_B" iperf3 -c "$IP_A" -p "$PORT" -t "$SECS" -O "$OMIT" -P "$STREAMS" --bidir 2>&1 \
        | grep -E '\[SUM\](\[TX\]|\[RX\])?.*(sender|receiver)' | sed 's/^/   /' || true
}

cmd_down() {
    local nic_a="" nic_b=""
    if [ -f "$NICS_STATE" ]; then read -r nic_a nic_b < "$NICS_STATE" || true; fi
    kill_server
    echo ">> returning NICs to root and removing namespaces"
    # Deleting a netns auto-returns physical NICs to root; do it explicitly
    # when we know the names so they come back promptly and named.
    [ -n "$nic_a" ] && nsx "$NS_A" ip link set "$nic_a" netns 1 2>/dev/null || true
    [ -n "$nic_b" ] && nsx "$NS_B" ip link set "$nic_b" netns 1 2>/dev/null || true
    ip netns del "$NS_A" 2>/dev/null || true
    ip netns del "$NS_B" 2>/dev/null || true
    rm -f "$NICS_STATE"
    echo "   done (re-address the NICs in root as needed)."
}

cmd_status() {
    echo "== namespaces =="
    ip netns list 2>/dev/null | grep -E "$NS_A|$NS_B" || echo "  (link down)"
    for ns in "$NS_A" "$NS_B"; do
        ip netns list 2>/dev/null | grep -q "$ns" || continue
        echo "== $ns =="
        nsx "$ns" ip -br addr show 2>/dev/null | grep -v '^lo' || true
    done
}

# ---- dispatch --------------------------------------------------------
case "${1:-}" in help | -h | --help) usage; exit 0 ;; esac
require_root

# First arg is a verb, or (for the default bench) the first interface.
verb="${1:-bench}"
case "$verb" in
    up | run | down | status | bench) shift ;;
    *) verb="bench" ;;
esac

# Interfaces come from args (preferred) or env.
NIC_A="${1:-${NIC_A:-}}"
NIC_B="${2:-${NIC_B:-}}"

case "$verb" in
    up)     cmd_up "$NIC_A" "$NIC_B" ;;
    run)    cmd_run ;;
    down)   cmd_down ;;
    status) cmd_status ;;
    bench)
        # Arm teardown before touching the NICs so any failure still
        # returns them to root.
        trap cmd_down EXIT
        cmd_up "$NIC_A" "$NIC_B"
        cmd_run
        ;;
    *) usage >&2; exit 1 ;;
esac
