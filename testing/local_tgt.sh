#!/usr/bin/env bash
#
# local_tgt.sh — run an NVMe/TCP target and initiator on ONE host over
# loopback (127.0.0.1), for either the Linux kernel nvmet-tcp target or
# ioutgt. The localhost sibling of two_nic_realwire.sh: same subcommand
# CLI, but no network namespaces / NICs — everything stays on lo.
#
# Each target has its own hardcoded port + NQN + backend, so both can run
# at once and a single env setup drives everything. Backends are file/bdev
# only (matching two_nic_realwire.sh):
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
# Distinct port + NQN per target so both run at once on the same IP.
IOUTGT_PORT=14420
IOUTGT_NQN="nqn.2026-06.io.localtgt:ioutgt"
NVMET_PORT=24420
NVMET_NQN="nqn.2026-06.io.localtgt:nvmet"
# shellcheck disable=SC2034  # consumed by common.sh's connect/discover verbs
HOSTNQN="nqn.2026-06.io.localtgt:host"

# Per-target backing: a regular file or block device only (file-backend
# only, matching two_nic_realwire.sh). A missing non-/dev path is
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

# Shared helpers + knob defaults (NR_QUEUES, QUEUE_SIZE, BACKEND_GB, fio...).
# Sourced before usage() so the help text can show those defaults; it only
# defines things (require_root is called below, after the help handler).
. "$(dirname "$0")/common.sh"

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

# ---- ioutgt target (userspace, binds on $TARGET_IP) ------------------
ioutgt_start() {
    local NQN=$IOUTGT_NQN PORT=$IOUTGT_PORT
    local BACKEND=$IOUTGT_BACKEND
    [ -x "$IOUTGT_BIN" ] || { echo "build first: cargo build --release -p ioutgt (or set IOUTGT_BIN)"; exit 1; }
    ensure_backing || exit 1
    local zc=() zclabel=
    if [ "$IOUTGT_SENDZC" != 0 ]; then
        zc=(--send-zc); zclabel=", send-zc"
        # --send-zc pins payload pages against RLIMIT_MEMLOCK; raise it so ZC
        # engages instead of silently falling back to a copying send.
        ulimit -l unlimited 2>/dev/null || true
    fi
    echo ">> starting ioutgt on $TARGET_IP:$PORT (backend $BACKEND, ${NR_QUEUES}q x $QUEUE_SIZE$zclabel)"
    "$IOUTGT_BIN" \
        --listen "$TARGET_IP:$PORT" \
        --backend "$BACKEND" \
        --io-threads "$NR_QUEUES" \
        --io-queue-size "$QUEUE_SIZE" \
        "${zc[@]}" \
        "${IOUTGT_DGST[@]}" \
        --subsys-nqn "$NQN" \
        --control-socket "$IOUTGT_SOCK" \
        >"$IOUTGT_LOG" 2>&1 &
    echo $! > "$IOUTGT_PIDFILE"
    sleep 1
    if kill -0 "$(cat "$IOUTGT_PIDFILE")" 2>/dev/null; then
        echo "   pid $(cat "$IOUTGT_PIDFILE"), log $IOUTGT_LOG"
    else
        echo "   ioutgt exited immediately; log follows:"; cat "$IOUTGT_LOG"; exit 1
    fi
}

ioutgt_stop() {
    [ -f "$IOUTGT_PIDFILE" ] && kill "$(cat "$IOUTGT_PIDFILE")" 2>/dev/null || true
    rm -f "$IOUTGT_PIDFILE"
    echo ">> ioutgt stopped"
}

# ---- nvmet-tcp target (Linux in-kernel; configfs, binds on $TARGET_IP) --
nvmet_start() {
    local NQN=$NVMET_NQN PORT=$NVMET_PORT
    local BACKEND=$NVMET_BACKEND
    echo ">> setting up nvmet-tcp target on $TARGET_IP:$PORT (backend $BACKEND)"
    modprobe nvmet
    modprobe nvmet-tcp
    ensure_backing || exit 1

    local cfg=/sys/kernel/config/nvmet
    local sub="$cfg/subsystems/$NQN"
    mkdir -p "$sub"
    echo 1 > "$sub/attr_allow_any_host"
    # nr_queues -> nvmet's per-subsystem max queue id (qid 1..N).
    echo "$NR_QUEUES" > "$sub/attr_qid_max"
    mkdir -p "$sub/namespaces/1"
    echo -n "$BACKEND" > "$sub/namespaces/1/device_path"
    # Force O_DIRECT on a file backend (parity with ioutgt's default); must
    # precede enable. Ignored for a block device.
    echo 0 > "$sub/namespaces/1/buffered_io" 2>/dev/null || true
    echo 1 > "$sub/namespaces/1/enable"

    # Claim a FREE configfs port id; the port tree is a global singleton, so
    # hardcoding port 1 would hijack an existing nvmet port on the host
    # ("Disable port before changing attribute"). Never touch a port we did
    # not create.
    local pid=1
    while [ -e "$cfg/ports/$pid" ]; do pid=$((pid + 1)); done
    local portdir="$cfg/ports/$pid"
    mkdir "$portdir"
    echo ipv4 > "$portdir/addr_adrfam"
    echo "$TARGET_IP" > "$portdir/addr_traddr"
    echo "$PORT" > "$portdir/addr_trsvcid"
    echo tcp > "$portdir/addr_trtype"
    # queue_size -> advertised per-queue depth (SQSIZE/MAXCMD); must be set
    # BEFORE the port is enabled (the symlink) or the kernel returns -EACCES.
    echo "$QUEUE_SIZE" > "$portdir/param_max_queue_size"
    # Linking the subsystem ENABLES the port -> creates the listener socket.
    ln -sf "$sub" "$portdir/subsystems/$NQN"
    echo "   listening on $TARGET_IP:$PORT, subsystem $NQN (configfs port $pid, qid_max=$NR_QUEUES, max_queue_size=$QUEUE_SIZE)"
}

nvmet_stop() {
    local NQN=$NVMET_NQN cfg=/sys/kernel/config/nvmet
    echo ">> removing nvmet-tcp target"
    # Our port id was claimed dynamically; find OUR port by its NQN symlink
    # and remove only that one — never another target's port.
    local link portdir
    for link in "$cfg"/ports/*/subsystems/"$NQN"; do
        [ -e "$link" ] || continue
        portdir=$(dirname "$(dirname "$link")")
        rm -f "$link"
        rmdir "$portdir" 2>/dev/null || true
    done
    echo 0 > "$cfg/subsystems/$NQN/namespaces/1/enable" 2>/dev/null || true
    rmdir "$cfg/subsystems/$NQN/namespaces/1" 2>/dev/null || true
    rmdir "$cfg/subsystems/$NQN" 2>/dev/null || true
}

# ---- start/stop route to one (or both) targets -----------------------
start_one() { case "$1" in nvmet) nvmet_start ;; ioutgt) ioutgt_start ;; esac; }
stop_one()  { case "$1" in nvmet) nvmet_stop ;;  ioutgt) ioutgt_stop ;;  esac; }

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

# Selector verbs take 'nvmet' or 'ioutgt'; omitting it acts on BOTH.
case "${1:-}" in
    start)               run_for_targets start_one      "${2:-}" ;;
    stop)                run_for_targets stop_one       "${2:-}" ;;
    discover)            run_for_targets discover_one   "${2:-}" ;;
    connect)             run_for_targets connect_one    "${2:-}" ;;
    disconnect)          run_for_targets disconnect_one "${2:-}" ;;
    fio)                 run_for_targets fio_one        "${2:-}" ;;
    status)              cmd_status ;;
    help|usage)          usage ;;
    *) usage >&2; exit 1 ;;
esac
