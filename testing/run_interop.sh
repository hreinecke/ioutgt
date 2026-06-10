#!/bin/bash
# Host-side interop runner: build ioutgt, start it on 127.0.0.1:4420,
# run the vmtest VM test against it (guest reaches us at 10.0.2.2:4420),
# tear down. Usage: testing/run_interop.sh [test-name]
set -eu

TOP="$(cd "$(dirname "$0")/.." && pwd)"
VMTEST="${VMTEST:-$HOME/git/utils/vmtest/vmtest}"
VMTEST_CONF="${VMTEST_CONF:-$HOME/git/linux-knext/vmtest.conf}"
TEST_NAME="${1:-ioutgt_nvme_tcp}"
PORT="${IOUTGT_PORT:-4420}"
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

CTL_SOCK="$TOP/target/ioutgt-interop.sock"
MARKER_DIR="$(dirname "$VMTEST")/data/tmp"
mkdir -p "$MARKER_DIR"
rm -f "$MARKER_DIR/ioutgt_want_ns2"

"$TOP/target/release/ioutgt" --listen "0.0.0.0:$PORT" --io-threads 2 \
    --control-socket "$CTL_SOCK" "${BACKEND_ARGS[@]}" >"$LOG" 2>&1 &
TARGET_PID=$!

# Hot-add watcher: the guest drops a marker when it wants nsid 2 added
# while it stays connected (the M7 AEN test).
(
    while [ ! -f "$MARKER_DIR/ioutgt_want_ns2" ]; do
        sleep 0.5
        kill -0 $TARGET_PID 2>/dev/null || exit 0
    done
    "$TOP/target/release/ioutgt" ctl --socket "$CTL_SOCK" \
        '{"op":"ADD_NAMESPACE","nsid":2,"backend":{"type":"memory","size_mb":32}}' ||
        echo "ctl hot-add failed" >>"$LOG"
) &
WATCHER_PID=$!
trap 'kill $TARGET_PID $WATCHER_PID 2>/dev/null || true' EXIT

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

echo "--- target log ---"
tail -50 "$LOG"
exit $RC
