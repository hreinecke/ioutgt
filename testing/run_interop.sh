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

"$TOP/target/release/ioutgt" --listen "0.0.0.0:$PORT" --io-threads 2 \
    >"$LOG" 2>&1 &
TARGET_PID=$!
trap 'kill $TARGET_PID 2>/dev/null || true' EXIT

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
