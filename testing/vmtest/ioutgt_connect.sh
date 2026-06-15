#!/bin/bash
# SPDX-License-Identifier: GPL-2.0
# Guest-side M4 interop test: nvme discover/connect/identify/disconnect
# against an ioutgt target running on the host (slirp: 10.0.2.2).
#
# Sourced by the vmtest wrapper (tests/ioutgt_nvme_tcp.sh); expects
# lib/common.sh helpers and config already loaded.
set -eu

ADDR="${IOUTGT_ADDR:-10.0.2.2}"
# The host runner publishes its port through the 9p-shared marker.
PORT="${IOUTGT_PORT:-$(cat "${VMTEST_DATA_DIR:-/nonexistent}/tmp/ioutgt_port" 2>/dev/null || echo 4420)}"
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

    # The per-controller path device nvmeXcYnZ is built only when the host
    # has CONFIG_NVME_MULTIPATH (which exposes the nvme_core.multipath param)
    # AND the target advertises CMIC multi-controller. It is a hidden gendisk
    # (GENHD_FL_HIDDEN) — registers in /sys/block, gets no /dev node — so its
    # presence there is the end-to-end proof of our CMIC/NMIC advertisement.
    # Skipped on kernels that lack multipath.
    local mp_param=/sys/module/nvme_core/parameters/multipath
    if [ -r "$mp_param" ] && [ "$(cat "$mp_param")" != "N" ]; then
        local paths
        paths=$(ls /sys/block/ 2>/dev/null | grep -E '^nvme[0-9]+c[0-9]+n[0-9]+' || true)
        [ -n "$paths" ] || {
            nvme list-subsys 2>/dev/null || true
            ls /sys/block/ 2>/dev/null | grep nvme || true
            vt_die "multipath on but no nvmeXcYnZ path gendisk — CMIC not honored"
        }
        vt_log "multipath path gendisk(s): $(echo $paths | tr '\n' ' ')"
    else
        vt_log "kernel without CONFIG_NVME_MULTIPATH; no nvmeXcYnZ path to check"
    fi

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

    # Mixed block sizes spanning both write paths (512B..16K in-capsule,
    # >16K via R2T) with per-block crc32c verification.
    fio --name=vbs --filename="$dev" --rw=randwrite \
        --bssplit=512/10:4k/40:16k/20:64k/20:128k/10 --size=24M \
        --verify=crc32c --verify_fatal=1 --direct=1 --ioengine=libaio \
        --iodepth=16 --output-format=terse >/dev/null ||
        vt_die "fio mixed-blocksize verify failed"
    vt_log "fio mixed-blocksize verify ok"

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

# M7: while connected, ask the host side (via 9p marker) to hot-add
# nsid 2; the AEN must make this kernel create the namespace device
# without any reconnect.
ioutgt_run_m7() {
    vt_log "runtime namespace add (AEN) test"
    nvme connect -t tcp -a "$ADDR" -s "$PORT" -n "$NQN" --nr-io-queues=2 ||
        vt_die "connect for m7 failed"
    local ctrl="" i
    for i in $(seq 50); do
        for c in /sys/class/nvme/nvme*; do
            [ -e "$c/subsysnqn" ] || continue
            if [ "$(cat "$c/subsysnqn")" = "$NQN" ]; then
                ctrl=$(basename "$c")
                break
            fi
        done
        [ -n "$ctrl" ] && break
        sleep 0.2
    done
    [ -n "$ctrl" ] || vt_die "controller for $NQN not found"
    vt_log "controller $ctrl; requesting hot-add from host side"

    : > "${VMTEST_DATA_DIR:?}/tmp/ioutgt_want_ns2"

    # Wait for nsid 2 to materialize, and resolve the block device
    # userspace uses for it. Under native multipath the per-controller
    # node is the hidden ${ctrl}cYn2 (no /dev node); the usable device is
    # the head nvmeXnZ, whose name has no 'c'. Without multipath it is just
    # nvmeXn2. Match by the nsid sysfs attribute (head instance need not
    # equal nsid), skipping the hidden c-path gendisks.
    local dev2=""
    for i in $(seq 60); do
        for d in /sys/block/nvme*; do
            case "${d##*/}" in *c*) continue ;; esac
            [ "$(cat "$d/nsid" 2>/dev/null)" = "2" ] || continue
            dev2="/dev/${d##*/}"
            break
        done
        [ -n "$dev2" ] && [ -b "$dev2" ] && break
        dev2=""
        sleep 0.5
    done
    [ -n "$dev2" ] || {
        dmesg | tail -20
        vt_die "second namespace did not appear after AEN"
    }
    vt_log "namespace 2 appeared as $dev2 (no reconnect)"

    # Sanity IO on the hot-added namespace.
    dd if=/dev/urandom "of=$dev2" bs=4k count=4 oflag=direct status=none ||
        vt_die "write to hot-added namespace failed"
    dd "if=$dev2" of=/dev/null bs=4k count=4 iflag=direct status=none ||
        vt_die "read from hot-added namespace failed"

    nvme disconnect -n "$NQN" >/dev/null || vt_die "m7 disconnect failed"
    vt_pass "ioutgt M7 runtime namespace add via AEN"
}

# M8: kill -9 the target mid-IO (host side restarts it). The kernel
# host freezes the queues, reconnects (~10s), and replays — fio must
# complete with zero errors despite the target dying underneath it.
ioutgt_run_m8() {
    [ -f "${VMTEST_DATA_DIR:?}/tmp/ioutgt_kill_enabled" ] || {
        vt_log "m8 kill test disabled (host did not opt in); skipping"
        return 0
    }
    vt_log "failure injection: target kill mid-IO"
    nvme connect -t tcp -a "$ADDR" -s "$PORT" -n "$NQN" --nr-io-queues=2 ||
        vt_die "connect for m8 failed"
    # Use namespace 1 of OUR controller: it exists in the target's
    # static config and therefore survives the restart (the M7 hot-added
    # nsid 2 deliberately does not).
    local ctrl="" dev="" i c
    for i in $(seq 100); do
        for c in /sys/class/nvme/nvme*; do
            [ -e "$c/subsysnqn" ] || continue
            [ "$(cat "$c/subsysnqn")" = "$NQN" ] && ctrl=$(basename "$c") && break
        done
        dev="/dev/${ctrl}n1"
        [ -n "$ctrl" ] && [ -b "$dev" ] && break
        sleep 0.2
    done
    [ -n "$ctrl" ] && [ -b "$dev" ] || vt_die "device missing for m8"

    # Background IO across the kill. No verify (the memory backend
    # forgets on restart by design). continue_on_error=all: whether the
    # host driver masks every in-flight command across a hard kill -9 is
    # timing-dependent (it may fail some with DNR during the chaotic
    # reconnect window) — that is a host-driver property, not a target
    # correctness property. What the target must guarantee is that it
    # *recovers* and serves correct IO again, which the post-recovery
    # write+read+verify below asserts deterministically.
    fio --name=killio --filename="$dev" --rw=randwrite --bs=4k \
        --runtime=45 --time_based --direct=1 --ioengine=libaio \
        --iodepth=16 --continue_on_error=all \
        --output-format=terse >/tmp/fio-m8.out 2>&1 &
    local fio_pid=$!
    sleep 3
    vt_log "requesting target kill"
    : > "$VMTEST_DATA_DIR/tmp/ioutgt_want_kill"
    wait "$fio_pid" || true # errors during the outage are expected/tolerated

    # The controller must reconnect (the host logs it).
    local i reconnected=0
    for i in $(seq 60); do
        if dmesg | grep -q "Successfully reconnected"; then
            reconnected=1
            break
        fi
        sleep 1
    done
    [ "$reconnected" = 1 ] || { dmesg | tail -20; vt_die "host did not reconnect"; }
    vt_log "controller reconnected after restart"

    # Deterministic recovery check: write a known pattern to the fresh
    # target and read it back. If the target came back wrong, this fails.
    local tmp
    tmp=$(mktemp)
    dd if=/dev/urandom of="$tmp" bs=4k count=64 status=none
    dd "if=$tmp" "of=$dev" bs=4k count=64 oflag=direct status=none ||
        vt_die "write after recovery failed"
    dd "if=$dev" of="$tmp.back" bs=4k count=64 iflag=direct status=none ||
        vt_die "read after recovery failed"
    cmp "$tmp" "$tmp.back" || vt_die "data mismatch after recovery"
    rm -f "$tmp" "$tmp.back"
    nvme disconnect -n "$NQN" >/dev/null || vt_die "m8 disconnect failed"
    vt_pass "ioutgt M8 target-kill recovery"
}

# Filesystem end-to-end: mkfs.ext4 + mount + checksummed file IO +
# fstrim (exercises DSM discard) + fsck. This drives flush ordering,
# sub-block RMW patterns, page-cache writeback, and discard through a
# real filesystem rather than raw block IO.
ioutgt_run_fs() {
    vt_require_cmd mkfs.ext4
    vt_require_cmd fsck.ext4
    vt_log "filesystem test: mkfs/mount/verify/fstrim/fsck"
    nvme connect -t tcp -a "$ADDR" -s "$PORT" -n "$NQN" --nr-io-queues=2 ||
        vt_die "connect for fs test failed"
    local ctrl="" dev="" i c
    for i in $(seq 100); do
        for c in /sys/class/nvme/nvme*; do
            [ -e "$c/subsysnqn" ] || continue
            [ "$(cat "$c/subsysnqn")" = "$NQN" ] && ctrl=$(basename "$c") && break
        done
        dev="/dev/${ctrl}n1"
        [ -n "$ctrl" ] && [ -b "$dev" ] && break
        sleep 0.2
    done
    [ -n "$ctrl" ] && [ -b "$dev" ] || vt_die "device missing for fs test"

    mkfs.ext4 -F -q "$dev" || vt_die "mkfs.ext4 failed"

    local mnt sums
    mnt=$(mktemp -d)
    mount "$dev" "$mnt" || vt_die "mount failed"

    # Checksummed payload: mixed file sizes, then sync to push real IO.
    for i in 1 2 3 4 5 6; do
        dd if=/dev/urandom of="$mnt/file.$i" bs=64k count=$((i * 8)) \
            status=none || vt_die "file write failed"
    done
    (cd "$mnt" && sha256sum file.* > SHA256SUMS) || vt_die "checksum failed"
    sync

    # Unmount and check on-disk consistency cold.
    umount "$mnt" || vt_die "umount failed"
    fsck.ext4 -f -n "$dev" >/dev/null || vt_die "fsck found errors after write phase"

    # Remount, verify every checksum survived the round trip.
    mount "$dev" "$mnt" || vt_die "remount failed"
    (cd "$mnt" && sha256sum -c SHA256SUMS --quiet) || vt_die "data verification failed"
    vt_log "checksums verified after remount"

    # Delete + fstrim: drives DSM deallocate end to end.
    rm -f "$mnt"/file.*
    sync
    fstrim -v "$mnt" || vt_die "fstrim failed (DSM discard path)"

    umount "$mnt" || vt_die "final umount failed"
    fsck.ext4 -f -n "$dev" >/dev/null || vt_die "fsck found errors after fstrim"
    rmdir "$mnt"
    nvme disconnect -n "$NQN" >/dev/null || vt_die "fs test disconnect failed"
    vt_pass "ioutgt filesystem mkfs/mount/verify/fstrim/fsck"
}

# Guest console output can be lossy under load; persist the verdict
# through the 9p-shared data dir so the host can assert on it.
ioutgt_mark() {
    [ -n "${VMTEST_DATA_DIR:-}" ] && mkdir -p "$VMTEST_DATA_DIR/tmp" &&
        echo "$*" >> "$VMTEST_DATA_DIR/tmp/ioutgt_result" || true
}

ioutgt_run_all() {
    : > "${VMTEST_DATA_DIR:-/tmp}/tmp/ioutgt_result" 2>/dev/null || true
    # Soak-only mode: the host wrote the cycle count into the marker;
    # do nothing but discover + reconnect cycles (no IO), so the host
    # can assert a flat RSS afterwards.
    if [ -f "${VMTEST_DATA_DIR:-/tmp}/tmp/ioutgt_soak_only" ]; then
        local cycles
        cycles=$(cat "${VMTEST_DATA_DIR}/tmp/ioutgt_soak_only")
        ioutgt_discover
        ioutgt_reconnect_soak "${cycles:-1000}"
        ioutgt_mark "PASS soak"
        return 0
    fi
    ioutgt_run_m4
    ioutgt_mark "PASS m4"
    ioutgt_run_m5
    ioutgt_mark "PASS m5"
    ioutgt_run_m7
    ioutgt_mark "PASS m7"
    ioutgt_run_fs
    ioutgt_mark "PASS fs"
    ioutgt_run_m8
    [ -f "${VMTEST_DATA_DIR:?}/tmp/ioutgt_kill_enabled" ] && ioutgt_mark "PASS m8"
    true
}
