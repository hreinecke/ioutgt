#!/bin/bash
# Host-side affinity test runner: build ioutgt, then boot the vmtest VM
# — multi-NUMA per vmtest.conf (VMTEST_NUMA_NODES, currently 4) — where
# the guest runs the target (pinning default-on) and verifies the userspace
# spread_cpus placement against its /sys topology.
# Usage: testing/run_affinity.sh
set -eu

TOP="$(cd "$(dirname "$0")/.." && pwd)"
VMTEST="${VMTEST:-$HOME/git/utils/vmtest/vmtest}"
VMTEST_CONF="${VMTEST_CONF:-$HOME/git/linux-knext/vmtest.conf}"

cargo build --release --manifest-path "$TOP/Cargo.toml" -p ioutgt

# Tell the guest which checkout (and thus which binary) to use; env
# does not cross into the VM, the 9p marker directory does. Honour a
# VMTEST_DATA_DIR override (lets this run beside another vmtest VM,
# which holds locks on the default data dir's disk images).
MARKER_DIR="${VMTEST_DATA_DIR:-$(dirname "$VMTEST")/data}/tmp"
mkdir -p "$MARKER_DIR"
# The target runs inside the guest, so there is no host-side process to
# reap — only the marker, dropped on exit so a later manual vmtest run
# cannot pick up a stale checkout path.
trap 'rm -f "$MARKER_DIR/ioutgt_top"' EXIT
trap 'exit 129' INT TERM
echo "$TOP" > "$MARKER_DIR/ioutgt_top"

"$VMTEST" -c "$VMTEST_CONF" run ioutgt_affinity
