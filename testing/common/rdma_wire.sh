# rdma_wire.sh — RDMA/RoCE wire helpers shared by the two_nic drivers
# (realwire_rdma.sh, realwire_spdk.sh; require_nics also by realwire_tcp.sh),
# including the whole one-host RDMA topology (realwire_rdma_up/_down).
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

# ---- the one-host RDMA realwire topology (shared 'up'/'down') --------
# The TARGET side (NIC_T + its rdma device) stays in the ROOT netns —
# nvmet-rdma's CM listener is init_net-pinned — and only the INITIATOR
# (NIC_I + its rdma device) is isolated in $NS_I; with no veth to root,
# root reaches IP_I only across the physical link, so the wire is still
# forced. Reads the caller's NS_I, NIC_T/NIC_I, IP_T/IP_I, PREFIX, MTU.

realwire_rdma_up() {
    require_nics
    # Idempotency FIRST: clear a stale initiator namespace from a previous run,
    # which also returns NIC_I (+ its rdma device) to root — so the nic_ibdev
    # sysfs lookups below can see both NICs.
    in_net "$NS_I" ip link set "$NIC_I" netns 1 2>/dev/null || true
    ip netns del "$NS_I" 2>/dev/null || true

    # Defend the test NICs/subnet from the host's network management, which
    # has produced multi-day debugging wedges on this rig: an auto-DHCP
    # NetworkManager profile on the (profile-less) test NIC re-runs a 45 s
    # DHCP transaction forever; every timeout flushes ALL addresses on the
    # device — deleting IP_T and its RoCE GID mid-run. Established QPs then
    # retransmit into the void (local_ack_timeout → retries_exceeded),
    # keep-alive dies ~45-90 s after connect, and every reconnect fails
    # (-104) until the IP is re-added.
    if command -v nmcli >/dev/null 2>&1; then
        nmcli device set "$NIC_T" managed no 2>/dev/null || true
        nmcli device set "$NIC_I" managed no 2>/dev/null || true
    fi
    # Pin the test subnet to the main routing table, ahead of any
    # policy-routing rules the host may carry.
    ip rule del to "$IP_T/$PREFIX" lookup main pref 5000 2>/dev/null || true
    ip rule add to "$IP_T/$PREFIX" lookup main pref 5000 2>/dev/null || true

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

realwire_rdma_down() {
    echo ">> removing initiator namespace (returns NIC_I + its rdma device to root)"
    # Stop the targets first with 'stop'. Deleting NS_I returns BOTH NIC_I and
    # its assigned rdma device to the root netns (kernel rdma_dev_exit_net).
    in_net "$NS_I" ip link set "$NIC_I" netns 1 2>/dev/null || true
    ip netns del "$NS_I" 2>/dev/null || true
    # The target NIC stayed in root; drop the test IP we added to it.
    [ -n "${NIC_T:-}" ] && ip addr del "$IP_T/$PREFIX" dev "$NIC_T" 2>/dev/null || true
    # Drop the policy-routing guard added by 'up' (NM unmanaged state is kept:
    # re-managing would let NM's DHCP loop flush the NIC again on the next run).
    ip rule del to "$IP_T/$PREFIX" lookup main pref 5000 2>/dev/null || true
    echo "   $NS_I removed; NIC_I + rdma device returned to root; $IP_T removed from ${NIC_T:-NIC_T}."
    echo "   (rdma system netns mode left exclusive; 'rdma system set netns shared' to revert)"
}
