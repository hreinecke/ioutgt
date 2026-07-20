#!/usr/bin/env bash
#
# local_tgt.sh — run an NVMe/TCP target and initiator on ONE host over
# loopback (127.0.0.1), for either the Linux kernel nvmet-tcp target or
# ioutgt. The localhost sibling of two_nic/realwire_tcp.sh: same subcommand
# CLI, but no network namespaces / NICs — everything stays on lo.
#
# Each target has its own hardcoded port + NQN + backend, so both can run
# at once and a single env setup drives everything. Backends are file/bdev
# only (matching two_nic/realwire_tcp.sh):
#   ioutgt : 14420  nqn...:ioutgt   IOUTGT_BACKEND (default: a /tmp file)
#   nvmet  : 24420  nqn...:nvmet    NVMET_BACKEND  (default: a /tmp file)
#
# USAGE (subcommands; selector verbs take nvmet|ioutgt, both if omitted)
#   sudo ./local_tgt.sh start                # start both targets
#   sudo ./local_tgt.sh connect ioutgt       # or just one
#   sudo ./local_tgt.sh fio                  # both, back to back
#   sudo ./local_tgt.sh disconnect
#   sudo ./local_tgt.sh stop
#
# KNOBS (env vars; see also common.sh)
#   IOUTGT_BACKEND   ioutgt --backend file/bdev   (/tmp/local_tgt-ioutgt.img)
#   NVMET_BACKEND    nvmet device_path file/bdev  (/tmp/local_tgt-nvmet.img)
#   BACKEND_GB=2     size of an auto-created backing file
#   NR_QUEUES=4      IO queues   (ioutgt --io-threads;    connect -i)
#   QUEUE_SIZE=128   IO qdepth    (ioutgt --io-queue-size; connect -q)
#   IOUTGT_SENDZC=0  ioutgt zero-copy send (--send-zc); 1 to enable
#   HDGST=0 DDGST=0  negotiate TCP header/data digest (CRC32C); 1 to enable
#   TARGET_IP        loopback address to bind/dial (default 127.0.0.1)
#
set -euo pipefail

# ---- config (override via environment) -------------------------------
TARGET_IP="${TARGET_IP:-127.0.0.1}"
# Identity: common.sh's shared block derives ports/NQNs from NQN_BASE.
# local_tgt uses its own NQN namespace, and gives spdk its own port so all
# three kinds can run at once on one IP (the realwire drivers never mix
# ioutgt and spdk).
NQN_BASE="nqn.2026-06.io.localtgt"
SPDK_PORT=34420

# Which targets this run drives (override, e.g. TARGET_KINDS=spdk for a pure
# SPDK loopback smoke; default keeps the ioutgt-vs-nvmet pair).
TARGET_KINDS="${TARGET_KINDS:-ioutgt nvmet}"
# SPDK loopback backend (malloc = pure RAM, no file). See common.sh SPDK_* knobs.
SPDK_BACKEND="${SPDK_BACKEND:-/tmp/local_tgt-spdk.img}"

# Per-target backing: a regular file or block device only (file-backend
# only, matching two_nic/realwire_tcp.sh). A missing non-/dev path is
# auto-created at BACKEND_GB; a /dev/* path must already exist.
IOUTGT_BACKEND="${IOUTGT_BACKEND:-/tmp/local_tgt-ioutgt.img}"
NVMET_BACKEND="${NVMET_BACKEND:-/tmp/local_tgt-nvmet.img}"

# ioutgt target-process knobs
IOUTGT_SOCK="${IOUTGT_SOCK:-/tmp/local_tgt-ioutgt.sock}"
IOUTGT_LOG="${IOUTGT_LOG:-/tmp/local_tgt-ioutgt.log}"
IOUTGT_PIDFILE="${IOUTGT_PIDFILE:-/tmp/local_tgt-ioutgt.pid}"

# Initiator runs directly (no netns); the loopback socket reaches the
# loopback listener. common.sh's verbs call through this.
ini_exec() { "$@"; }
# Target context for common.sh's nvmet_setup/ioutgt_start: everything runs in
# the current process on loopback, so the executor is a plain subshell and the
# ioutgt launch prefix is empty (no netns).
nvmet_exec() { bash -c "$1"; }
# shellcheck disable=SC2034  # consumed by common.sh's ioutgt_start
IOUTGT_NETNS=()

# Shared helpers + knob defaults (NR_QUEUES, QUEUE_SIZE, BACKEND_GB, fio...).
# Sourced before usage() so the help text can show those defaults; it only
# defines things (require_root is called below, after the help handler).
. "$(dirname "$0")/common/common.sh"

usage() {
    cat <<EOF
local_tgt.sh — drive an NVMe/TCP target + initiator on one host over
loopback ($TARGET_IP), for the Linux nvmet-tcp target or ioutgt.

Targets (same IP $TARGET_IP, distinct port/NQN/backend):
  ioutgt   :$IOUTGT_PORT   $IOUTGT_NQN   (IOUTGT_BACKEND=$IOUTGT_BACKEND)
  nvmet    :$NVMET_PORT   $NVMET_NQN   (NVMET_BACKEND=$NVMET_BACKEND)

Usage: $0 <subcommand> [nvmet|ioutgt]
       (selector verbs act on BOTH targets when the selector is omitted)

  start         [nvmet|ioutgt]  start the target(s) (nvmet = in-kernel)
  stop          [nvmet|ioutgt]  stop the target(s)
  discover      [nvmet|ioutgt]  nvme discover
  connect       [nvmet|ioutgt]  nvme connect; wait for the namespace device
  disconnect    [nvmet|ioutgt]  nvme disconnect
  fio           [nvmet|ioutgt]  fio on the connected device(s)
  fio_verify    [nvmet|ioutgt]  data-integrity gate: mixed-size (4k-128k) writes
                                + crc32c read-back verify (FIO_VERIFY_MB/job)
  fio_perf      [nvmet|ioutgt]  perf sweep: randread/randwrite x bs={4k,64k},
                                one line per combo (iops/BW/fio_cpu)
  status                        listeners and connected devices
  help                          this message

Knobs: IOUTGT_BACKEND NVMET_BACKEND BACKEND_GB=$BACKEND_GB
  NR_QUEUES=$NR_QUEUES QUEUE_SIZE=$QUEUE_SIZE IOUTGT_SENDZC=$IOUTGT_SENDZC
  HDGST=$HDGST DDGST=$DDGST FIO_RW/BS/QD/JOBS/SECS

Example:
  sudo $0 start && sudo $0 connect && sudo $0 fio
  sudo $0 disconnect && sudo $0 stop
EOF
}

# 'help' must work without root, so handle it before the root check.
case "${1:-}" in help|usage|-h|--help) usage; exit 0 ;; esac

require_root

# 'start'/'stop' route to common.sh's shared start_one/stop_one; local_tgt
# only supplies the loopback addressing (and defaults its backends above,
# so the shared `:?` aborts never fire here).

cmd_status() {
    echo "== listeners ($TARGET_IP) =="
    ss -ltn 2>/dev/null | grep -E ":$IOUTGT_PORT|:$NVMET_PORT" || echo "(none)"
    echo "== ioutgt process =="
    if [ -f "$IOUTGT_PIDFILE" ] && kill -0 "$(cat "$IOUTGT_PIDFILE")" 2>/dev/null; then
        echo "  running pid $(cat "$IOUTGT_PIDFILE")"
    else
        echo "  stopped"
    fi
    echo "== connected devices =="
    echo "  ioutgt ($IOUTGT_NQN): $(find_dev "$IOUTGT_NQN" || echo none)"
    echo "  nvmet  ($NVMET_NQN): $(find_dev "$NVMET_NQN" || echo none)"
}

# Shared dispatch (no cmd_up/cmd_down: loopback needs no wire, so 'up'/
# 'down' fall through to the usage error). Gains fio_verify with it.
realwire_dispatch "$@"
