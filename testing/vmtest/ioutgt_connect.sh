#!/bin/bash
# SPDX-License-Identifier: GPL-2.0
# Guest-side M4 interop test: nvme discover/connect/identify/disconnect
# against an ioutgt target running on the host (slirp: 10.0.2.2).
#
# Sourced by the vmtest wrapper (tests/ioutgt_nvme_tcp.sh); expects
# lib/common.sh helpers and config already loaded.
set -eu

ADDR="${IOUTGT_ADDR:-10.0.2.2}"
PORT="${IOUTGT_PORT:-4420}"
NQN="${IOUTGT_NQN:-nqn.2026-06.io.ioutgt:test}"

vt_require_module nvme_tcp
vt_require_cmd nvme

ioutgt_discover() {
    vt_log "nvme discover $*"
    local out
    out=$(nvme discover -t tcp -a "$ADDR" -s "$PORT" "$@") ||
        vt_die "nvme discover failed"
    echo "$out" | grep -q "$NQN" || {
        echo "$out"
        vt_die "discovery log missing $NQN"
    }
    vt_log "discovery reports $NQN"
}

# ioutgt_connect_cycle [extra nvme connect flags...]
ioutgt_connect_cycle() {
    vt_log "nvme connect $*"
    nvme connect -t tcp -a "$ADDR" -s "$PORT" -n "$NQN" "$@" ||
        vt_die "nvme connect failed ($*)"

    # Wait for the controller device to materialize.
    local ctrl="" i
    for i in $(seq 50); do
        ctrl=$(nvme list-subsys 2>/dev/null | grep -B1 "$NQN" >/dev/null 2>&1 && \
               ls /sys/class/nvme 2>/dev/null | head -1) || true
        [ -n "$ctrl" ] && break
        sleep 0.2
    done
    [ -n "$ctrl" ] || { dmesg | tail -30; vt_die "no nvme controller appeared"; }
    vt_log "controller: $ctrl"

    nvme list || vt_die "nvme list failed"
    nvme id-ctrl "/dev/$ctrl" >/dev/null || vt_die "id-ctrl failed"
    nvme id-ctrl "/dev/$ctrl" | grep -E "^(mn|sqes|cqes|kas|mdts)" || true

    # The namespace block device (IO path is a later milestone; the
    # device may report IO errors on probe reads — that is fine here).
    if nvme id-ns "/dev/${ctrl}n1" >/dev/null 2>&1; then
        vt_log "namespace ${ctrl}n1 visible"
    else
        vt_log "note: ${ctrl}n1 not probed (acceptable before the IO milestone)"
    fi

    nvme disconnect -n "$NQN" >/dev/null || vt_die "nvme disconnect failed"
    vt_log "disconnect ok"
}

ioutgt_reconnect_soak() {
    local n="${1:-100}" i
    vt_log "reconnect soak: $n cycles"
    for i in $(seq "$n"); do
        nvme connect -t tcp -a "$ADDR" -s "$PORT" -n "$NQN" --nr-io-queues=2 \
            >/dev/null || vt_die "soak connect $i failed"
        nvme disconnect -n "$NQN" >/dev/null || vt_die "soak disconnect $i failed"
    done
    vt_log "reconnect soak done"
}

ioutgt_run_m4() {
    ioutgt_discover
    ioutgt_connect_cycle --nr-io-queues=1
    ioutgt_connect_cycle --nr-io-queues=2
    ioutgt_connect_cycle --nr-io-queues=2 --hdr-digest
    ioutgt_connect_cycle --nr-io-queues=2 --hdr-digest --data-digest
    ioutgt_reconnect_soak "${IOUTGT_SOAK_CYCLES:-100}"
    vt_pass "ioutgt M4 discover/connect matrix"
}
