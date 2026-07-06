# common_ioutgt.sh — the ioutgt userspace target: knobs, start/stop,
# io-thread CPU sampling. Sourced by common.sh (not a standalone
# script); shares its knobs and helpers.

IOUTGT_BIN="${IOUTGT_BIN:-./target/release/ioutgt-nvme-$TRANSPORT}"
IOUTGT_SENDZC="${IOUTGT_SENDZC:-0}"  # 1 = ioutgt --send-zc (zero-copy send)
# Extra ioutgt flags appended verbatim, e.g.
#   IOUTGT_EXTRA="--queue-buf-mb 1"
IOUTGT_EXTRA="${IOUTGT_EXTRA:-}"

# ---- ioutgt target (userspace) ---------------------------------------
# ioutgt_start NQN PORT IP BACKEND — launch the ioutgt target as a background
# process and record its pid. The caller supplies IOUTGT_NETNS, an array launch
# prefix (`(ip netns exec NS_T)` for realwire; `()` for local_tgt); ioutgt is
# pure userspace (no configfs), so plain `ip netns exec` suffices. IOUTGT_BIN,
# IOUTGT_SENDZC, IOUTGT_DGST, IOUTGT_SOCK/_LOG/_PIDFILE come from the env/common.
ioutgt_start() {
    local nqn="$1" port="$2" ip="$3" backend="$4"
    [ -x "$IOUTGT_BIN" ] || { echo "build the $TRANSPORT target first (cargo build --release; or set IOUTGT_BIN=$IOUTGT_BIN)"; exit 1; }
    BACKEND="$backend" ensure_backing || exit 1
    local zc=() zclabel=
    if [ "$IOUTGT_SENDZC" != 0 ] && [ "$TRANSPORT" = tcp ]; then
        zc=(--send-zc); zclabel=", send-zc"
        # --send-zc pins payload pages against RLIMIT_MEMLOCK; raise it so ZC
        # engages instead of silently falling back to a copying send.
        ulimit -l unlimited 2>/dev/null || true
    elif [ "$IOUTGT_SENDZC" != 0 ]; then
        echo "   note: TRANSPORT=rdma has no --send-zc; ignoring IOUTGT_SENDZC" >&2
    fi
    local extra=()
    [ -n "$IOUTGT_EXTRA" ] && read -ra extra <<<"$IOUTGT_EXTRA"
    echo ">> starting ioutgt on $ip:$port (backend $backend, ${NR_QUEUES}q x $QUEUE_SIZE$zclabel${IOUTGT_EXTRA:+, $IOUTGT_EXTRA})"
    "${IOUTGT_NETNS[@]}" "$IOUTGT_BIN" \
        --listen "$ip:$port" \
        --backend "$backend" \
        --io-threads "$NR_QUEUES" \
        --io-queue-size "$QUEUE_SIZE" \
        "${zc[@]}" \
        "${extra[@]}" \
        "${IOUTGT_DGST[@]}" \
        --subsys-nqn "$nqn" \
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

# ioutgt_stop — kill the ioutgt target by its recorded pid (best-effort).
ioutgt_stop() {
    [ -f "$IOUTGT_PIDFILE" ] && kill "$(cat "$IOUTGT_PIDFILE")" 2>/dev/null || true
    rm -f "$IOUTGT_PIDFILE"
    echo ">> ioutgt stopped"
}

# "tid utime stime comm" for each ioutgt queue (io) thread, in clock ticks from
# /proc. Two cheap snapshots bracket an fio run (no polling *during* the run),
# so the active queue thread's user/system CPU can be sampled without
# perturbing it.
_ioutgt_io_ticks() {
    local pid="$1" tid comm stat rest
    [ -n "$pid" ] && [ -d "/proc/$pid/task" ] || return 0
    for tid in "/proc/$pid/task"/*; do
        tid="${tid##*/}"
        read -r comm < "/proc/$pid/task/$tid/comm" 2>/dev/null || continue
        case "$comm" in ioutgt-io*) ;; *) continue ;; esac
        read -r stat < "/proc/$pid/task/$tid/stat" 2>/dev/null || continue
        rest="${stat#*) }"          # drop "pid (comm) "; now state=$1 ...
        # shellcheck disable=SC2086  # deliberate split of the stat fields
        set -- $rest                # utime=field14=$12, stime=field15=$13
        printf '%s %s %s %s\n' "$tid" "${12}" "${13}" "$comm"
    done
}

