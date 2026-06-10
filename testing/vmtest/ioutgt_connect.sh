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

# Connect, run fio --verify on the namespace, disconnect.
# Args: extra nvme connect flags.
ioutgt_fio_verify() {
    vt_log "fio verify cycle (connect flags: $*)"
    nvme connect -t tcp -a "$ADDR" -s "$PORT" -n "$NQN" --nr-io-queues=2 "$@" ||
        vt_die "nvme connect for fio failed"
    local dev="" i
    for i in $(seq 100); do
        dev=$(nvme list 2>/dev/null | awk -v nqn="$NQN" '$1 ~ /\/dev\/nvme/ {print $1}' | tail -1)
        [ -n "$dev" ] && [ -b "$dev" ] && break
        sleep 0.2
    done
    [ -n "$dev" ] || { dmesg | tail -30; vt_die "namespace device missing"; }
    vt_log "fio target: $dev"

    fio --name=v4k --filename="$dev" --rw=randwrite --bs=4k --size=16M \
        --verify=crc32c --verify_fatal=1 --direct=1 --ioengine=libaio \
        --iodepth=32 --output-format=terse >/dev/null ||
        vt_die "fio 4k verify failed"
    vt_log "fio 4k randwrite verify ok"

    fio --name=v128k --filename="$dev" --rw=write --bs=128k --size=32M \
        --verify=crc32c --verify_fatal=1 --direct=1 --ioengine=libaio \
        --iodepth=8 --output-format=terse >/dev/null ||
        vt_die "fio 128k verify failed"
    vt_log "fio 128k write verify ok"

    fio --name=vmix --filename="$dev" --rw=randrw --rwmixread=70 --bs=4k \
        --size=16M --runtime=20 --time_based --verify=crc32c \
        --verify_fatal=1 --direct=1 --ioengine=libaio --iodepth=32 \
        --output-format=terse >/dev/null ||
        vt_die "fio mixed verify failed"
    vt_log "fio 70/30 randrw verify ok"

    nvme disconnect -n "$NQN" >/dev/null || vt_die "disconnect after fio failed"
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

ioutgt_run_m5() {
    vt_require_cmd fio
    ioutgt_discover
    ioutgt_fio_verify
    ioutgt_fio_verify --hdr-digest --data-digest
    vt_pass "ioutgt M5 fio data-integrity matrix"
}

# Guest console output can be lossy under load; persist the verdict
# through the 9p-shared data dir so the host can assert on it.
ioutgt_mark() {
    [ -n "${VMTEST_DATA_DIR:-}" ] && mkdir -p "$VMTEST_DATA_DIR/tmp" &&
        echo "$*" >> "$VMTEST_DATA_DIR/tmp/ioutgt_result" || true
}

ioutgt_run_all() {
    : > "${VMTEST_DATA_DIR:-/tmp}/tmp/ioutgt_result" 2>/dev/null || true
    ioutgt_run_m4
    ioutgt_mark "PASS m4"
    ioutgt_run_m5
    ioutgt_mark "PASS m5"
}
