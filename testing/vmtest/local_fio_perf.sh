#!/bin/bash
# SPDX-License-Identifier: GPL-2.0
# vmtest-desc: local_tgt.sh fio_perf sweep against ioutgt (all in guest)
# vmtest-requires: root nvme-cli fio
#
# Ad-hoc vmtest driver: runs the whole local_tgt.sh flow (start -> connect ->
# fio_perf -> stop) against the ioutgt target inside the guest over loopback,
# then checks the fio_perf sweep produced real numbers. Run with:
#   ~/git/utils/vmtest/vmtest -c ~/git/linux-knext/vmtest.conf run \
#       /path/to/ioutgt/testing/vmtest/local_fio_perf.sh
# The ioutgt release binary must be built on the host first (the guest sees
# the repo read-only, so it cannot build).
#
# Scoped to the ioutgt target: kernel nvmet's file backend does not work on
# the guest's tmpfs (host reports "invalid LBA data size 0"), which is a
# backend quirk unrelated to the fio_perf command under test.
set -euo pipefail

REPO="${IOUTGT_REPO:-$(cd "$(dirname "$0")/../.." && pwd)}"
cd "$REPO"
[ -x ./target/release/ioutgt-nvme-tcp ] || { echo "FAIL: ./target/release/ioutgt-nvme-tcp missing (build on host: cargo build --release -p ioutgt)"; exit 1; }

# Small + short so the sweep (4 combos) fits the VM and runs fast.
export FIO_JOBS="${FIO_JOBS:-1}" FIO_QD="${FIO_QD:-64}" FIO_SECS="${FIO_SECS:-3}" BACKEND_GB="${BACKEND_GB:-1}"

# Mirror results to the rw-shared vmtest data dir so the host can read them
# without parsing the interleaved guest console.
RESULT="${VMTEST_DATA_DIR:-/tmp}/fio_perf_result.txt"
: > "$RESULT"
log() { printf '%s\n' "$*" | tee -a "$RESULT"; }

cleanup() {
    ./testing/local_tgt.sh disconnect ioutgt >/dev/null 2>&1 || true
    ./testing/local_tgt.sh stop ioutgt >/dev/null 2>&1 || true
}
trap cleanup EXIT

log "== local_tgt: start ioutgt =="; ./testing/local_tgt.sh start ioutgt
log "== local_tgt: connect ioutgt =="; ./testing/local_tgt.sh connect ioutgt
log "== local_tgt: fio_perf ioutgt =="
out="$(./testing/local_tgt.sh fio_perf ioutgt)"
log "$out"

# 4 combo lines (randread/randwrite x 4k/64k), each must report non-zero IOPS.
nz="$(printf '%s\n' "$out" | grep -cE 'iops=[[:space:]]*[1-9]' || true)"
log "non-zero-iops combos: $nz / 4"
[ "$nz" -ge 4 ] || { log "FAIL: fio_perf produced < 4 non-zero-iops combos"; exit 1; }
log "PASS: local_tgt fio_perf"
