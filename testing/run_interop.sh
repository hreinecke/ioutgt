#!/bin/bash
# Host-side interop runner: build ioutgt, start it on 127.0.0.1:4420,
# run the vmtest VM test against it (guest reaches us at 10.0.2.2:4420),
# tear down. Usage: testing/run_interop.sh [test-name]
set -eu

TOP="$(cd "$(dirname "$0")/.." && pwd)"
VMTEST="${VMTEST:-$HOME/git/utils/vmtest/vmtest}"
VMTEST_CONF="${VMTEST_CONF:-$HOME/git/linux-knext/vmtest.conf}"
TEST_NAME="${1:-ioutgt_nvme_tcp}"
# Dedicated port: 4420 is the canonical NVMe port and may be owned by
# other targets on a dev box (kernel nvmet, etc.).
PORT="${IOUTGT_PORT:-14420}"
LOG="$TOP/target/ioutgt-interop.log"

cargo build --release --manifest-path "$TOP/Cargo.toml" -p ioutgt

# IOUTGT_BACKEND=memory (default) | null | file
BACKEND_ARGS=()
case "${IOUTGT_BACKEND:-memory}" in
memory) BACKEND_ARGS=(--backend memory) ;;
null) BACKEND_ARGS=(--backend null) ;;
file)
    BACKING="$TOP/target/ioutgt-backing.img"
    truncate -s "${IOUTGT_FILE_MB:-256}M" "$BACKING"
    BACKEND_ARGS=(--backend "$BACKING")
    ;;
*) echo "unknown IOUTGT_BACKEND"; exit 1 ;;
esac

# IOUTGT_SEND_ZC=1: start the target with --send-zc (loopback-copy
# path inside the VM net; exercises notification-gated tag reuse).
ZC_ARGS=()
[ "${IOUTGT_SEND_ZC:-0}" = "1" ] && ZC_ARGS=(--send-zc)

# IOUTGT_IO_QUEUE_SIZE=N: advertise N as the IO MAXCMD ceiling
# (--io-queue-size). Unset → the binary default (128). Set below the
# host's requested depth (e.g. 64) to see the guest kernel clamp to N.
IOQS_ARGS=()
[ -n "${IOUTGT_IO_QUEUE_SIZE:-}" ] && IOQS_ARGS=(--io-queue-size "$IOUTGT_IO_QUEUE_SIZE")

CTL_SOCK="$TOP/target/ioutgt-interop.sock"
MARKER_DIR="$(dirname "$VMTEST")/data/tmp"
PID_FILE="$TOP/target/ioutgt-interop.pid"

# Kill the target (and watcher) however the script exits: a surviving
# target squats the port and poisons the next harness or bench run.
# Installed before anything starts so an early failure cannot leak;
# the stale PID file from a previous run is dropped first so cleanup
# can never kill an unrelated reused pid.
rm -f "$PID_FILE"
WATCHER_PID=""
cleanup() {
    [ -s "$PID_FILE" ] && kill "$(cat "$PID_FILE")" 2>/dev/null || true
    [ -n "$WATCHER_PID" ] && kill "$WATCHER_PID" 2>/dev/null || true
    # Drop the checkout marker so a later manual vmtest run cannot pick
    # up a stale path (run_affinity.sh does the same).
    rm -f "$PID_FILE" "$MARKER_DIR/ioutgt_top"
}
trap cleanup EXIT
# Fatal signals bypass the EXIT trap unless turned into an exit.
trap 'exit 129' INT TERM

mkdir -p "$MARKER_DIR"
rm -f "$MARKER_DIR/ioutgt_want_ns2" "$MARKER_DIR/ioutgt_want_kill" \
    "$MARKER_DIR/ioutgt_kill_enabled" "$MARKER_DIR/ioutgt_soak_only"
# Tell the guest which port we serve on (env does not cross into the VM).
echo "$PORT" > "$MARKER_DIR/ioutgt_port"
# Publish this checkout to the guest test wrappers (same 9p marker
# mechanism as run_affinity.sh) so they never need a hardcoded path.
echo "$TOP" > "$MARKER_DIR/ioutgt_top"
# Fresh log: the startup gate below greps it for the listener line.
: > "$LOG"
# IOUTGT_SOAK_ONLY=N: reconnect-leak mode — the guest only runs N
# connect/disconnect cycles, and we assert the target RSS stays flat.
[ -n "${IOUTGT_SOAK_ONLY:-}" ] && echo "$IOUTGT_SOAK_ONLY" > "$MARKER_DIR/ioutgt_soak_only"

start_target() {
    "$TOP/target/release/ioutgt" --listen "0.0.0.0:$PORT" --io-threads "${IOUTGT_IO_THREADS:-2}" \
        --control-socket "$CTL_SOCK" "${BACKEND_ARGS[@]}" "${ZC_ARGS[@]}" "${IOQS_ARGS[@]}" >>"$LOG" 2>&1 &
    echo $! >"$PID_FILE"
}
start_target
TARGET_PID=$(cat "$PID_FILE")

# Opt-in for the guest's kill/recovery test.
[ "${IOUTGT_ENABLE_KILL:-0}" = "1" ] && : > "$MARKER_DIR/ioutgt_kill_enabled"

# Watcher: serves guest-driven events — M7 hot-add and M8 kill/restart.
(
    while :; do
        sleep 0.5
        if [ -f "$MARKER_DIR/ioutgt_want_ns2" ]; then
            rm -f "$MARKER_DIR/ioutgt_want_ns2"
            "$TOP/target/release/ioutgt" ctl --socket "$CTL_SOCK" \
                '{"op":"ADD_NAMESPACE","nsid":2,"backend":{"type":"memory","size_mb":32}}' ||
                echo "ctl hot-add failed" >>"$LOG"
        fi
        if [ -f "$MARKER_DIR/ioutgt_want_kill" ]; then
            rm -f "$MARKER_DIR/ioutgt_want_kill"
            echo "watcher: kill -9 target" >>"$LOG"
            kill -9 "$(cat "$PID_FILE")" 2>/dev/null || true
            sleep 2
            start_target
            echo "watcher: target restarted (pid $(cat "$PID_FILE"))" >>"$LOG"
        fi
        kill -0 "$(cat "$PID_FILE")" 2>/dev/null || exit 0
    done
) &
WATCHER_PID=$!

# Wait for the listener.
for _ in $(seq 50); do
    if grep -q "listening" "$LOG" 2>/dev/null; then break; fi
    kill -0 $TARGET_PID 2>/dev/null || { cat "$LOG"; echo "target died"; exit 1; }
    sleep 0.1
done
echo "ioutgt up (pid $TARGET_PID); starting VM test $TEST_NAME"

set +e
"$VMTEST" -c "$VMTEST_CONF" run "$TEST_NAME"
RC=$?
set -e

FINAL_PID=$(cat "$PID_FILE" 2>/dev/null)
if [ -n "$FINAL_PID" ] && [ -r "/proc/$FINAL_PID/status" ]; then
    RSS_KB=$(awk '/VmRSS/{print $2}' "/proc/$FINAL_PID/status")
    echo "--- target RSS after run: $RSS_KB kB ---"
    if [ -n "${IOUTGT_SOAK_ONLY:-}" ] && [ "$RSS_KB" -gt 32768 ]; then
        echo "FAIL: RSS not flat after reconnect soak ($RSS_KB kB > 32 MB)"
        RC=1
    fi
fi
echo "--- target log ---"
tail -50 "$LOG"
exit $RC
