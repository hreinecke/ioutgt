# rdma_wire.sh — RDMA/RoCE wire helpers shared by the two_nic drivers
# (realwire_rdma.sh, realwire_spdk.sh; require_nics also by realwire_tcp.sh).
# Sourced by common.sh (not a standalone script). Functions only — no
# source-time side effects. rdma_address_nic reads $MTU/$PREFIX at call time.

require_nics() {
    : "${NIC_T:?set NIC_T to the target-side NIC, e.g. NIC_T=enp1s0f0 / mlx5p1}"
    : "${NIC_I:?set NIC_I to the initiator-side NIC, e.g. NIC_I=enp1s0f1 / mlx5p2}"
}

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
