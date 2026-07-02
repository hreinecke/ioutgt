#!/usr/bin/env bash
# common.sh — shared helpers for the NVMe target drivers
# (two_nic_realwire.sh, local_tgt.sh). Sourced, never executed. The fabric is
# selected by TRANSPORT=tcp|rdma (default tcp); see the knobs section.
#
# The sourcing script supplies the transport context; these helpers stay
# agnostic to whether the initiator runs in a network namespace (realwire)
# or directly on loopback (local_tgt):
#
#   TARGET_IP             IP the initiator dials / the target listens on
#   ini_exec <cmd...>     run an nvme-cli command in the initiator context
#                         (`ip netns exec NS_I ...` for realwire; a direct
#                         passthrough for local_tgt)
#   nvmet_exec <script>   run a configfs shell snippet in the TARGET's network
#                         context, so the nvmet listener socket is born there
#                         (`in_net NS_T bash -c ...` for realwire; a direct
#                         `bash -c ...` for local_tgt). Used by nvmet_setup/
#                         nvmet_teardown; configfs itself is a global singleton,
#                         only the enabling step is netns-sensitive.
#   IOUTGT_NETNS          array launch-prefix for the ioutgt target process
#                         (`(ip netns exec NS_T)` for realwire; `()` for
#                         local_tgt). Used by ioutgt_start.
#   IOUTGT_PORT/_NQN, NVMET_PORT/_NQN, HOSTNQN   per-target addressing
#
# The target-start functions additionally set a caller-local BACKEND that
# ensure_backing/fio see via bash dynamic scope.

# ---- shared knobs (override via environment) -------------------------
# Fixed host identity. nvme-cli generates a RANDOM hostid per invocation
# when none is given, but the kernel requires one hostnqn to map to exactly
# one hostid — so connecting to the second target with the same HOSTNQN but
# a fresh random hostid is rejected ("same hostnqn but different hostid").
# Pin both, shared across all connects from this host.
HOSTID="${HOSTID:-2e3b0c44-1c2e-4f3a-9b6d-000000000001}"

# Fabric: tcp (NVMe/TCP) or rdma (NVMe/RDMA over RoCEv2). One knob selects the
# ioutgt binary, the nvmet kernel modules + port addr_trtype, the `nvme -t`
# type, and — for rdma — forces digests and zero-copy-send OFF (neither exists
# on the RDMA fabric, and the ioutgt-nvme-rdma binary has no such flags).
TRANSPORT="${TRANSPORT:-tcp}"
case "$TRANSPORT" in
    tcp|rdma) ;;
    *) echo "TRANSPORT must be tcp or rdma (got '$TRANSPORT')" >&2; exit 1 ;;
esac

NR_QUEUES="${NR_QUEUES:-4}"          # IO queues  (ioutgt --io-threads; connect -i)
QUEUE_SIZE="${QUEUE_SIZE:-128}"      # IO qdepth   (ioutgt --io-queue-size; connect -q)
BACKEND_GB="${BACKEND_GB:-2}"        # size of an auto-created backing file
IOUTGT_BIN="${IOUTGT_BIN:-./target/release/ioutgt-nvme-$TRANSPORT}"
IOUTGT_SENDZC="${IOUTGT_SENDZC:-0}"  # 1 = ioutgt --send-zc (zero-copy send)
# Extra ioutgt flags appended verbatim, e.g.
#   IOUTGT_EXTRA="--queue-buf-mb 1"
IOUTGT_EXTRA="${IOUTGT_EXTRA:-}"

# TCP digest negotiation (CRC32C), coupled across both ends so the two
# targets stay aligned: =1 asks the host to negotiate it (nvme connect
# --hdr-digest / --data-digest) and lets ioutgt accept it; =0 keeps it off
# and makes ioutgt refuse it (--no-hdgst / --no-ddgst). nvmet has no target
# knob — it just honours the host request — so coupling the host request and
# the ioutgt stance here is what keeps nvmet and ioutgt identical.
HDGST="${HDGST:-0}"   # header digest (TCP only)
DDGST="${DDGST:-0}"   # data digest (TCP only)
# Host-side request flags (used by connect/discover) and the matching ioutgt
# target refuse flags (used by each driver's ioutgt start). RDMA has no PDU
# digests — leave both arrays empty (the ioutgt-nvme-rdma binary has no
# --no-hdgst/--no-ddgst either) and note any ignored request.
CONNECT_DGST=()
IOUTGT_DGST=()
if [ "$TRANSPORT" = tcp ]; then
    if [ "$HDGST" = 1 ]; then CONNECT_DGST+=(--hdr-digest);  else IOUTGT_DGST+=(--no-hdgst); fi
    if [ "$DDGST" = 1 ]; then CONNECT_DGST+=(--data-digest); else IOUTGT_DGST+=(--no-ddgst); fi
elif [ "$HDGST" = 1 ] || [ "$DDGST" = 1 ]; then
    echo "   note: TRANSPORT=rdma has no PDU digests; ignoring HDGST/DDGST" >&2
fi

# fio knobs
FIO_RW="${FIO_RW:-randread}"
FIO_BS="${FIO_BS:-4k}"
FIO_QD="${FIO_QD:-32}"
FIO_JOBS="${FIO_JOBS:-4}"
FIO_SECS="${FIO_SECS:-30}"
# fio_verify knobs — deliberately separate from the perf knobs: the gate must
# run at a pressure that reproduces buffer-pool exhaustion (8 jobs x qd64 of
# mixed-size writes is what surfaced the RDMA write-path DNR failures; 1 job
# at default qd sails through). Jobs are laid out contiguously, so the device
# must hold FIO_VERIFY_JOBS x FIO_VERIFY_MB (the 2 GiB default backing file
# fits the defaults exactly).
FIO_VERIFY_MB="${FIO_VERIFY_MB:-256}"
FIO_VERIFY_JOBS="${FIO_VERIFY_JOBS:-8}"
FIO_VERIFY_QD="${FIO_VERIFY_QD:-64}"

require_root() { [ "$(id -u)" -eq 0 ] || { echo "must run as root (use sudo)"; exit 1; }; }

# Map a target kind ('nvmet'|'ioutgt') to its "PORT NQN" pair.
target_params() {
    case "${1:-}" in
        ioutgt) echo "$IOUTGT_PORT $IOUTGT_NQN" ;;
        nvmet)  echo "$NVMET_PORT $NVMET_NQN" ;;
        *) echo "specify target: nvmet | ioutgt" >&2; return 1 ;;
    esac
}

# Run a per-target function $1 for the selected target $2, or for BOTH
# targets (ioutgt then nvmet) when no selector is given.
run_for_targets() {
    local fn="$1"
    case "${2:-}" in
        ioutgt|nvmet) "$fn" "$2" ;;
        "")           "$fn" ioutgt; "$fn" nvmet ;;
        *) echo "specify target: nvmet | ioutgt (or omit for both)" >&2; exit 1 ;;
    esac
}

# Ensure $BACKEND (a caller local) exists. A missing non-/dev path is
# auto-created at BACKEND_GB; a missing /dev/* is an error.
ensure_backing() {
    case "$BACKEND" in
        /dev/*) [ -e "$BACKEND" ] || { echo "block device $BACKEND does not exist" >&2; return 1; } ;;
        /*)     [ -e "$BACKEND" ] || { echo "   creating backing file $BACKEND (${BACKEND_GB}G)" >&2
                                       truncate -s "${BACKEND_GB}G" "$BACKEND" \
                                         || { echo "failed to create $BACKEND" >&2; return 1; }; } ;;
        *)      echo "BACKEND must be an absolute file or block-device path" >&2; return 1 ;;
    esac
}

# ---- nvmet target (Linux in-kernel; configfs) ------------------------
# nvmet_setup NQN PORT IP BACKEND — create + enable an nvmet subsystem and
# a dynamically-claimed port. configfs is a global singleton, but the listener
# SOCKET is created in the netns of whatever process writes the enabling
# symlink, so the whole configfs script runs through the caller-supplied
# nvmet_exec (direct for local_tgt; inside NS_T for realwire). modprobe and the
# backing file are done in the current (global) mount ns first.
#
# Per-target values are interpolated by THIS (outer) shell; the script's own
# loop vars ($cfg/$sub/$pid/$portdir) are escaped (\$) so they evaluate in the
# target context.
nvmet_setup() {
    local nqn="$1" port="$2" ip="$3" backend="$4"
    modprobe nvmet; modprobe "nvmet-$TRANSPORT"
    BACKEND="$backend" ensure_backing || return 1
    echo ">> setting up nvmet-$TRANSPORT on $ip:$port (backend $backend)"
    nvmet_exec "
        set -euo pipefail
        cfg=/sys/kernel/config/nvmet; sub=\$cfg/subsystems/$nqn
        mkdir -p \$sub
        echo 1 > \$sub/attr_allow_any_host
        # nr_queues -> nvmet's per-subsystem max queue id (qid 1..N).
        echo $NR_QUEUES > \$sub/attr_qid_max
        mkdir -p \$sub/namespaces/1
        echo -n $backend > \$sub/namespaces/1/device_path
        # Force O_DIRECT on a file backend (parity with ioutgt's default); must
        # precede enable. Ignored for a block device.
        echo 0 > \$sub/namespaces/1/buffered_io 2>/dev/null || true
        echo 1 > \$sub/namespaces/1/enable
        # Claim a FREE configfs port id; the port tree is a global singleton, so
        # hardcoding port 1 would hijack an existing nvmet port on the host
        # ('Disable port before changing attribute'). Never touch a port we did
        # not create.
        pid=1; while [ -e \"\$cfg/ports/\$pid\" ]; do pid=\$((pid + 1)); done
        portdir=\$cfg/ports/\$pid; mkdir \"\$portdir\"
        echo ipv4 > \"\$portdir/addr_adrfam\"
        echo $ip   > \"\$portdir/addr_traddr\"
        echo $port > \"\$portdir/addr_trsvcid\"
        echo $TRANSPORT > \"\$portdir/addr_trtype\"
        # queue_size -> advertised per-queue depth (SQSIZE/MAXCMD); must be set
        # BEFORE the port is enabled (the symlink) or the kernel returns -EACCES.
        echo $QUEUE_SIZE > \"\$portdir/param_max_queue_size\"
        # Linking the subsystem ENABLES the port -> creates the listener socket,
        # in the nvmet_exec context's netns.
        ln -sf \$sub \"\$portdir/subsystems/$nqn\"
        echo \"   listening on $ip:$port, subsystem $nqn (configfs port \$pid, qid_max=$NR_QUEUES, max_queue_size=$QUEUE_SIZE)\"
    "
}

# nvmet_teardown NQN — remove only the port WE created (found by its NQN
# symlink, never another target's) and the subsystem. Best-effort (no set -e).
nvmet_teardown() {
    local nqn="$1"
    echo ">> removing nvmet-$TRANSPORT target ($nqn)"
    nvmet_exec "
        cfg=/sys/kernel/config/nvmet
        for link in \"\$cfg\"/ports/*/subsystems/$nqn; do
            [ -e \"\$link\" ] || continue
            portdir=\$(dirname \"\$(dirname \"\$link\")\")
            rm -f \"\$link\"
            rmdir \"\$portdir\" 2>/dev/null || true
        done
        echo 0 > \$cfg/subsystems/$nqn/namespaces/1/enable 2>/dev/null || true
        rmdir \$cfg/subsystems/$nqn/namespaces/1 2>/dev/null || true
        rmdir \$cfg/subsystems/$nqn 2>/dev/null || true
    " || true
}

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

# Namespace block device for an NQN via sysfs (/sys/block/*/device/subsysnqn)
# — schema-independent and multipath-safe: with native NVMe multipath the
# head device nvmeXnZ is not under the controller's sysfs dir (only the
# per-path node nvmeXcYnZ is), so a /sys/class/nvme walk misses it; a block
# dev's device/subsysnqn resolves to its controller or subsystem in either
# layout. A per-path match (nvmeXcYnZ, no /dev entry) maps to its head.
find_dev() {
    local nqn="$1" blk name head
    for blk in /sys/block/nvme*n*; do
        [ -e "$blk" ] || continue
        name=$(basename "$blk")
        case "$name" in *p[0-9]*) continue ;; esac      # skip partitions
        [ -r "$blk/device/subsysnqn" ] || continue
        [ "$(cat "$blk/device/subsysnqn")" = "$nqn" ] || continue
        if [[ $name =~ ^(nvme[0-9]+)c[0-9]+(n[0-9]+)$ ]]; then
            head="${BASH_REMATCH[1]}${BASH_REMATCH[2]}"
        else
            head="$name"
        fi
        [ -b "/dev/$head" ] && { echo "/dev/$head"; return 0; }
    done
    return 1
}

# Controller node (/dev/nvmeN) for an NQN, on stdout.
find_ctrl() {
    local nqn="$1" c
    for c in /sys/class/nvme/nvme*; do
        [ -r "$c/subsysnqn" ] || continue
        [ "$(cat "$c/subsysnqn")" = "$nqn" ] && { echo "/dev/$(basename "$c")"; return 0; }
    done
    return 1
}

# Poll up to ~10s for the namespace block device of $nqn, nudging a rescan
# each tick: namespace enumeration can lag the connect.
wait_dev() {
    local nqn="$1" dev ctrl
    local deadline=$(( SECONDS + 10 ))
    while [ "$SECONDS" -lt "$deadline" ]; do
        dev=$(find_dev "$nqn") && { echo "$dev"; return 0; }
        # `|| true` is load-bearing under `set -e`: a non-zero rescan aborts.
        ctrl=$(find_ctrl "$nqn") && nvme ns-rescan "$ctrl" 2>/dev/null || true
        sleep 0.5
    done
    return 1
}

# ---- initiator verbs (transport via ini_exec + TARGET_IP) ------------
# Each takes a 'nvmet'|'ioutgt' selector. The nvme-cli command that creates
# the host socket runs through ini_exec (so realwire egresses NIC_I); the
# sysfs device lookups run in the current process (device nodes are global).
discover_one() {
    local port nqn; read -r port nqn < <(target_params "${1:-}") || exit 1
    modprobe "nvme-$TRANSPORT"
    ini_exec nvme discover -t "$TRANSPORT" -a "$TARGET_IP" -s "$port" \
        --hostnqn "$HOSTNQN" --hostid "$HOSTID" "${CONNECT_DGST[@]}"
}

connect_one() {
    local port nqn; read -r port nqn < <(target_params "${1:-}") || exit 1
    modprobe "nvme-$TRANSPORT"
    # Bump the host keep-alive timeout to 20s so a transient RC retransmit
    # under fabric congestion can't trip the keep-alive and wedge QID 0. Fixed
    # here on purpose — not an outside knob.
    local kato=20
    echo ">> connecting $1 -> $TARGET_IP:$port (request ${NR_QUEUES}q x $QUEUE_SIZE, keep-alive-tmo=${kato}s)"
    # -i/-q make the host REQUEST this many queues / this depth; the target
    # caps it, so the granted values are min(host request, target cap).
    ini_exec nvme connect -t "$TRANSPORT" -a "$TARGET_IP" -s "$port" \
        -n "$nqn" --hostnqn "$HOSTNQN" --hostid "$HOSTID" \
        --nr-io-queues "$NR_QUEUES" --queue-size "$QUEUE_SIZE" \
        --keep-alive-tmo "$kato" "${CONNECT_DGST[@]}"
    local dev
    if dev=$(wait_dev "$nqn"); then
        echo "   block device: $dev (controller $(find_ctrl "$nqn"), nqn $nqn)"
    else
        echo "   connected ($nqn) but no namespace block device appeared after 10s"
        echo "   controller: $(find_ctrl "$nqn" || echo '?'); check 'nvme list' / target namespace config"
    fi
}

disconnect_one() {
    local port nqn; read -r port nqn < <(target_params "${1:-}") || exit 1
    ini_exec nvme disconnect -n "$nqn" 2>/dev/null || true
    echo ">> disconnected $1 ($nqn)"
}

fio_one() {
    local port nqn; read -r port nqn < <(target_params "${1:-}") || exit 1
    local dev; dev=$(find_dev "$nqn") || { echo "no connected device for $1 ($nqn); run 'connect $1' first"; exit 1; }
    echo ">> fio on $dev [$1]  ($FIO_RW bs=$FIO_BS qd=$FIO_QD jobs=$FIO_JOBS ${FIO_SECS}s)"
    fio --name=nvmetcp --filename="$dev" --rw="$FIO_RW" --bs="$FIO_BS" \
        --iodepth="$FIO_QD" --numjobs="$FIO_JOBS" --ioengine=io_uring \
        --direct=1 --runtime="$FIO_SECS" --time_based --group_reporting
}

# Data-integrity gate: sequential writes of MIXED block sizes (4k..128k — up
# to MDTS, which fio_perf never exercises and filesystem writeback does) with
# crc32c read-back verification interleaved via verify_backlog, so writes and
# verify-reads stress the target's buffer pool concurrently (the fs-workload
# shape that surfaced write failures fio_perf missed). Each job gets a private
# FIO_VERIFY_MB region (offset_increment), so verification is overlap-safe.
# Any write error (e.g. a target failing commands under pool pressure) or
# verify mismatch fails the run loudly (verify_fatal).
fio_verify_one() {
    local port nqn; read -r port nqn < <(target_params "${1:-}") || exit 1
    local dev; dev=$(find_dev "$nqn") || { echo "no connected device for $1 ($nqn); run 'connect $1' first"; exit 1; }
    echo ">> fio verify on $dev [$1]  (write bsrange=4k-128k qd=$FIO_VERIFY_QD jobs=$FIO_VERIFY_JOBS ${FIO_VERIFY_MB}MiB/job + crc32c read-back)"
    if fio --name=verify --filename="$dev" --rw=write --bsrange=4k-128k \
        --iodepth="$FIO_VERIFY_QD" --numjobs="$FIO_VERIFY_JOBS" --ioengine=io_uring \
        --direct=1 --size="${FIO_VERIFY_MB}m" --offset_increment="${FIO_VERIFY_MB}m" \
        --verify=crc32c --verify_fatal=1 --verify_backlog=64 \
        --group_reporting; then
        echo "   fio verify [$1]: PASS"
    else
        echo "   fio verify [$1]: FAIL (write error or data mismatch — see fio output / dmesg)"
        return 1
    fi
}

# fio terse v4 field indices (1-based, ';'-separated); see fio HOWTO and
# tools/test/func/hfio. Each fio run here is pure read OR pure write, so only
# the matching direction's iops/bw is non-zero.
FIO_T_RIOPS=8; FIO_T_RBW=7      # read iops, read bandwidth (KiB/s)
FIO_T_WIOPS=49; FIO_T_WBW=48    # write iops, write bandwidth (KiB/s)
FIO_T_UCPU=129; FIO_T_SCPU=130  # fio user / system CPU (%)

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

# Perf sweep: randread/randwrite x bs={4k,64k}, one compact line per combo
# (rw / iops / BW / fio_cpu), honoring FIO_JOBS/FIO_QD/FIO_SECS. Numbers come
# from fio's terse output (parsed, not scraped). Modeled on
# tools/test/func/hfio's _fio_perf. For the ioutgt target each line also ends
# with the busiest (active) queue thread and its user/system CPU%, sampled by
# bracketing the run with two /proc reads so it does not affect the result.
fio_perf_one() {
    local port nqn; read -r port nqn < <(target_params "${1:-}") || exit 1
    local dev; dev=$(find_dev "$nqn") || { echo "no connected device for $1 ($nqn); run 'connect $1' first"; exit 1; }
    echo ">> fio_perf on $dev [$1]  (jobs=$FIO_JOBS qd=$FIO_QD ${FIO_SECS}s/run)"
    local out; out="$(mktemp)"
    local hz; hz="$(getconf CLK_TCK 2>/dev/null || echo 100)"
    # Only ioutgt exposes user-space queue threads to sample; nvmet is in-kernel.
    local pid=""; [ "$1" = ioutgt ] && pid="$(cat "${IOUTGT_PIDFILE:-}" 2>/dev/null || true)"
    local bs rw line iops bw ucpu scpu before after t0 t1 iothr lineout
    for bs in 4k 64k; do
        for rw in randread randwrite; do
            before="$(_ioutgt_io_ticks "$pid")"; t0="$(date +%s.%N)"
            # `|| true`: a failed fio must fall through to the "no terse output"
            # guard below, not abort the whole sweep under set -e.
            fio --name=perf --filename="$dev" --rw="$rw" --bs="$bs" \
                --iodepth="$FIO_QD" --numjobs="$FIO_JOBS" --ioengine=io_uring \
                --direct=1 --runtime="$FIO_SECS" --time_based --group_reporting \
                --output-format=terse --terse-version=4 >"$out" 2>/dev/null || true
            t1="$(date +%s.%N)"; after="$(_ioutgt_io_ticks "$pid")"
            # The group line begins with the terse version ("4;"); ignore any
            # stray output. `|| true`: no match must fall through to the
            # "no terse output" guard below, not abort the sweep under set -e.
            line="$(grep '^4;' "$out" | tail -1 || true)"
            if [ -z "$line" ]; then
                printf "   %-9s bs=%-4s  (fio produced no terse output)\n" "$rw" "$bs"
                continue
            fi
            case "$rw" in
                *read*)  iops="$(echo "$line" | cut -d';' -f"$FIO_T_RIOPS")"; bw="$(echo "$line" | cut -d';' -f"$FIO_T_RBW")" ;;
                *)       iops="$(echo "$line" | cut -d';' -f"$FIO_T_WIOPS")"; bw="$(echo "$line" | cut -d';' -f"$FIO_T_WBW")" ;;
            esac
            ucpu="$(echo "$line" | cut -d';' -f"$FIO_T_UCPU")"
            scpu="$(echo "$line" | cut -d';' -f"$FIO_T_SCPU")"
            lineout="$(awk -v rw="$rw" -v bs="$bs" -v iops="${iops:-0}" -v bw="${bw:-0}" -v u="${ucpu:-0}" -v s="${scpu:-0}" \
                'BEGIN{printf "   %-9s bs=%-4s  iops=%8.1fk  BW=%9.2f MiB/s  fio_cpu(usr=%5.1f%% sys=%5.1f%%)", rw, bs, iops/1000, bw/1024, u, s}')"
            # ioutgt only: append the busiest queue thread (by delta utime+stime
            # over the run) and its user/system CPU%, from the two snapshots.
            iothr=""
            if [ -n "$pid" ] && [ -n "$before" ]; then
                iothr="$(awk -v t0="$t0" -v t1="$t1" -v hz="$hz" '
                    NR==FNR { bu[$1]=$2; bs[$1]=$3; next }
                    { du=$2-bu[$1]; ds=$3-bs[$1]; tot=du+ds
                      if (tot>mt) { mt=tot; mu=du; ms=ds; mn=$4 } }
                    END { dt=t1-t0; if (dt<=0) dt=1
                          if (mn!="") printf "  io_thr=%s(usr=%.1f%% sys=%.1f%%)", mn, 100*mu/(dt*hz), 100*ms/(dt*hz) }
                ' <(printf '%s\n' "$before") <(printf '%s\n' "$after"))"
            fi
            printf '%s%s\n' "$lineout" "$iothr"
        done
    done
    rm -f "$out"
}

# ===== NIC perf tuning (target NIC = $TUNE_NIC, in net namespace $TUNE_NS) =====
# Align a target NIC's RX/TX queue IRQs with ioutgt's io-threads. Reusable by
# any driver: set TUNE_NIC and TUNE_NS ("" = root netns, e.g. a single-NIC box)
# plus IOUTGT_BIN / IOUTGT_SOCK / IOUTGT_PORT. /proc/irq, /proc/interrupts,
# /proc/sys, taskset and `ioutgt ctl` are global; only NIC sysfs/ethtool ops go
# through the namespace (via nic_exec).

# Run a NIC-side command (ethtool, /sys/class/net writes) in $TUNE_NS.
nic_exec() { if [ -n "${TUNE_NS:-}" ]; then ip netns exec "$TUNE_NS" "$@"; else "$@"; fi; }

# Toggle gro+gso+tso for NIC $1 to state $2 (on|off) in netns $3 ("" = root).
# GRO relieves the recv-bound path; hardware TSO offloads TX segmentation (the
# send-heavy read path); GSO stays on as the software fallback.
nic_offloads() {
    local nic="$1" state="$2" ns="${3:-}" pre=()
    [ -n "$ns" ] && pre=(ip netns exec "$ns")
    "${pre[@]}" ethtool -K "$nic" gro "$state" gso "$state" tso "$state" 2>/dev/null \
        || echo "   note: could not toggle gro/gso/tso $state on $nic"
}

# Auto-size IO queues from NIC $1: min(rx, tx, nproc). rx/tx are RX+Combined /
# TX+Combined from `ethtool -l`; falls back to counting the sysfs rx-*/tx-*
# queue dirs. Used to default NR_QUEUES so --io-threads matches the NIC channel
# count (1:1 IRQ <-> io-thread mapping).
nic_default_queues() {
    local nic="$1" out comb rx tx ncpu m
    out="$(nic_exec ethtool -l "$nic" 2>/dev/null \
        | sed -n '/Current hardware settings/,$p' || true)"
    comb="$(printf '%s\n' "$out" | awk '/^Combined:/{print $2; exit}')"
    rx="$(printf '%s\n' "$out" | awk '/^RX:/{print $2; exit}')"
    tx="$(printf '%s\n' "$out" | awk '/^TX:/{print $2; exit}')"
    # ethtool prints "n/a" for an unsupported channel type; coerce any
    # non-numeric token to 0 (a bare "n/a" inside $(( )) would be parsed as
    # the variables n / a and abort under `set -u`). `${x:-0}` guards only
    # emptiness, not non-numeric values, so it is not enough on its own.
    case "$comb" in '' | *[!0-9]*) comb=0 ;; esac
    case "$rx" in '' | *[!0-9]*) rx=0 ;; esac
    case "$tx" in '' | *[!0-9]*) tx=0 ;; esac
    rx=$((rx + comb))
    tx=$((tx + comb))
    if [ "$rx" -eq 0 ]; then
        rx="$(nic_exec bash -c "ls -d /sys/class/net/$nic/queues/rx-* 2>/dev/null | wc -l" || echo 0)"
    fi
    if [ "$tx" -eq 0 ]; then
        tx="$(nic_exec bash -c "ls -d /sys/class/net/$nic/queues/tx-* 2>/dev/null | wc -l" || echo 0)"
    fi
    ncpu="$(nproc 2>/dev/null || echo 1)"
    m="$rx"
    if [ "$tx" -lt "$m" ]; then m="$tx"; fi
    if [ "$ncpu" -lt "$m" ]; then m="$ncpu"; fi
    if [ "${m:-0}" -lt 1 ]; then m=1; fi
    printf '%s\n' "$m"
}

# Hardware ceiling on NIC $1's Combined channels, from the "Pre-set maximums"
# section of `ethtool -l` (distinct from nic_default_queues, which reads the
# *current* setting). Prints 0 when the NIC has no combined channels (the line
# is absent or "n/a") so callers can skip an `ethtool -L combined` retune. The
# n/a coercion mirrors nic_default_queues — see the note there.
nic_max_combined() {
    local nic="$1" out comb
    out="$(nic_exec ethtool -l "$nic" 2>/dev/null \
        | sed -n '/Pre-set maximums/,/Current hardware settings/p' || true)"
    comb="$(printf '%s\n' "$out" | awk '/^Combined:/{print $2; exit}')"
    case "$comb" in '' | *[!0-9]*) comb=0 ;; esac
    printf '%s\n' "$comb"
}

# Delete all ntuple RX-flow filters on NIC $1 (best-effort). Filters persist on
# the netdev across runs/netns moves; stale ones pin RX queues and would block
# an `ethtool -L combined` reduction ("requested channel counts are too low for
# existing ntuple filter settings"). The converge step re-adds the live ones.
nic_clear_ntuple() {
    local nic="$1"
    nic_exec bash -c 'ethtool -n '"$nic"' 2>/dev/null | awk "/Filter:/{print \$2}" \
        | while read -r id; do ethtool -N '"$nic"' delete "$id" >/dev/null 2>&1; done' 2>/dev/null || true
}

# PCI bus address (bdf, e.g. 0000:a1:00.0) of NIC $1. Needed to match drivers
# (mlx5) that label their IRQs by completion vector + bdf rather than the netdev
# name. Memoized: the bdf is stable for the run and the lookup forks ethtool.
declare -A _NIC_BDF
nic_pci_bdf() {
    local nic="$1"
    if [ -z "${_NIC_BDF[$nic]:-}" ]; then
        _NIC_BDF[$nic]="$(nic_exec ethtool -i "$nic" 2>/dev/null \
            | awk '/^bus-info:/{print $2; exit}')"
    fi
    printf '%s\n' "${_NIC_BDF[$nic]}"
}

# Distinct IRQs serving NIC queue index $2 of nic $1: combined "TxRx", or split
# "rx"/"tx" (one or two IRQs). From the global /proc/interrupts (the NIC sits in
# a netns but its IRQ action labels persist). When the netdev-name labels do not
# match, fall back to mlx5's scheme: per-channel completion IRQs labelled
# "mlx5_comp<q>@pci:<bdf>" (one combined IRQ per queue), keyed by the NIC's PCI
# bdf so the right port of a multi-port card is selected.
nic_queue_irqs() {
    local nic="$1" q="$2" out bdf
    out="$(awk -v n="$nic" -v q="$q" '
        $NF ~ ("^" n "-TxRx-" q "$") || $NF ~ ("^" n "-rx-" q "$") || $NF ~ ("^" n "-tx-" q "$") {
            irq=$1; sub(/:/,"",irq); print irq
        }' /proc/interrupts | sort -nu)"
    if [ -z "$out" ]; then
        bdf="$(nic_pci_bdf "$nic")"
        if [ -n "$bdf" ]; then
            # The mlx5 label is an exact string ("mlx5_comp<q>@pci:<bdf>"), so
            # match it with == -- avoids regex-escaping the dots in the bdf.
            out="$(awk -v label="mlx5_comp${q}@pci:${bdf}" '
                $NF == label { irq=$1; sub(/:/,"",irq); print irq }' \
                /proc/interrupts | sort -nu)"
        fi
    fi
    [ -n "$out" ] && printf '%s\n' "$out"
}

# Hex CPU mask for one CPU as .../xps_cpus expects: comma-separated 32-bit
# words, high word first (cpu 4 -> "00000010", cpu 24 -> "01000000",
# cpu 32 -> "00000001,00000000", cpu 62 -> "40000000,00000000").
cpu_xps_mask() {
    local cpu="$1"
    local word=$((cpu / 32)) bit=$((cpu % 32)) i w out=""
    for ((i = word; i >= 0; i--)); do
        if [ "$i" -eq "$word" ]; then printf -v w '%08x' $((1 << bit)); else w='00000000'; fi
        out="${out:+$out,}$w"
    done
    printf '%s' "$out"
}

# Expand a kernel cpulist ("12-13,44-47") to space-separated cpu ids.
expand_cpus() {
    local part a b c
    # Only real cpulists; reject ''/'*'/'?'/unknown so the unquoted split below
    # can never glob filenames against the cwd.
    case "$1" in ''|*[!0-9,-]*) return 0 ;; esac
    for part in ${1//,/ }; do
        if [[ $part == *-* ]]; then
            a=${part%-*}; b=${part#*-}
            for ((c = a; c <= b; c++)); do printf '%s ' "$c"; done
        else
            printf '%s ' "$part"
        fi
    done
}

# Pick the HT sibling of the NIC RX-IRQ CPU $2 -- the other logical CPU on the
# SAME physical core. Co-locating the io-thread on its own RX-IRQ CPU serializes
# the NIC RX softirq and the recv/copy on one logical CPU, capping a single fast
# connection ~20% (measured on bnxt_en 10GbE: 888 vs ~1040 MiB/s, 64K randwrite,
# beating nvmet). The sibling runs them on two logical CPUs (no serialization)
# while sharing the core's L1/L2, so the data the softirq just landed is still
# warm for the io-thread's copy. Falls back, when SMT is off (no sibling), to a
# different physical core in group $1, then to irqcpu itself.
iothread_cpu() {
    local group="$1" irqcpu="$2" sib cpu
    # The IRQ CPU's HT sibling (the thread_siblings entry that is not itself).
    for cpu in $(expand_cpus "$(cat "/sys/devices/system/cpu/cpu$irqcpu/topology/thread_siblings_list" 2>/dev/null)"); do
        [ "$cpu" != "$irqcpu" ] && { echo "$cpu"; return 0; }
    done
    # SMT off: no sibling -- use a different physical core in the NUMA group.
    sib=" $(expand_cpus "$(cat "/sys/devices/system/cpu/cpu$irqcpu/topology/thread_siblings_list" 2>/dev/null)") "
    for cpu in $(expand_cpus "$group"); do
        case "$sib" in *" $cpu "*) continue ;; esac
        echo "$cpu"; return 0
    done
    echo "$irqcpu"
}

# Converge $TUNE_NIC's rx/tx queue IRQs and ioutgt's io-thread CPUs. Run AFTER
# connect: the queue-thread pool spawns lazily on the first connection and
# `ioutgt list` reports each IO queue's pthread tid + full online CPU group only
# once it is connected. Per NIC queue i (== qid i+1 == io-thread i):
#   1. push the io-thread's whole CPU group onto the queue's rx/tx IRQ
#      smp_affinity (NIC follows ioutgt -- a no-op on a managed/read-only IRQ);
#   2. read the rx/tx IRQ *effective* affinity, then taskset the io-thread (by
#      tid) to the IRQ CPU's HT SIBLING -- a different logical CPU on the same
#      physical core, NOT the IRQ CPU itself: co-locating io-thread and RX
#      softirq on one logical CPU serializes them and caps a single fast
#      connection ~20% (888 vs ~1040 MiB/s, 64K randwrite). See iothread_cpu().
#   3. XPS: map the io-thread's CPU -> this queue's tx ring (xps_cpus).
# Steps 1-3 only align queue<->thread CPUs; they do NOT decide which queue a
# *flow* uses (RSS picks the RX queue, decoupled from the qid->io-thread route);
# per-flow RX co-location is added separately via hardware ntuple rules. We
# deliberately DISABLE software RPS/RFS: it relocates RX softirqs to the consumer
# CPU with smp_call_function IPIs (net_rps_send_ipi) -- a Function-call-interrupt
# storm for no throughput gain -- and its knobs persist across runs, so the sync
# clears them every time. irqbalance would fight the pinning, so stop it.
tune_target_nic() {
    [ -n "${TUNE_NIC:-}" ] || { echo "   (TUNE_NIC unset; skipping IRQ affinity sync)"; return 0; }
    command -v jq >/dev/null 2>&1 || { echo "   (jq not found; skipping IRQ affinity sync)"; return 0; }
    local json rows
    json="$("$IOUTGT_BIN" ctl --socket "$IOUTGT_SOCK" '{"op":"LIST_CONTROLLER"}' 2>/dev/null || true)"
    # qid, tid, active CPU, full online CPU group (cpulist), peer ip:port.
    rows="$(printf '%s' "$json" \
        | jq -r '.data.controllers[]?.queues[]? | select(.qid >= 1) | "\(.qid) \(.tid) \(.cpus) \(.group_cpus) \(.peer)"' \
            2>/dev/null | sort -n -u || true)"
    if [ -z "$rows" ]; then
        echo "   (no connected IO queues; run 'connect' first)"; return 0
    fi
    systemctl stop irqbalance 2>/dev/null || true
    echo ">> converging $TUNE_NIC queue IRQ affinity <-> ioutgt io-threads"
    local qid tid cpus group peer sport nicq irqs irq combo eff pushed xcpu xps irqcpu iocpu
    while read -r qid tid cpus group peer; do
        [ -n "$qid" ] || continue
        nicq=$((qid - 1))
        irqs="$(nic_queue_irqs "$TUNE_NIC" "$nicq")"
        if [ -z "$irqs" ]; then
            echo "   q$nicq (qid $qid): no NIC IRQ found; skipped"; continue
        fi
        combo=""; pushed=""
        for irq in $irqs; do
            # 1. push the io-thread's whole CPU group onto the (unmanaged) IRQ.
            #    A valid cpulist only -- "*"/"?" (unpinned/unknown) can't be set.
            case "$group" in
                ''|'*'|'?'|*[!0-9,-]*) ;;
                *) if echo "$group" > "/proc/irq/$irq/smp_affinity_list" 2>/dev/null; then
                       pushed="${pushed:+$pushed,}$irq"
                   fi ;;
            esac
            # 2. collect this IRQ's effective affinity for the combination.
            eff="$(cat "/proc/irq/$irq/effective_affinity_list" 2>/dev/null || true)"
            [ -n "$eff" ] && combo="${combo:+$combo,}$eff"
        done
        # Place the io-thread on the IRQ CPU's HT sibling (different logical CPU,
        # same physical core; falls back to a different core when SMT is off).
        irqcpu="${combo%%[,-]*}"
        iocpu="$(iothread_cpu "$group" "$irqcpu")"
        if [ -n "$iocpu" ] && taskset -cp "$iocpu" "$tid" >/dev/null 2>&1; then
            # 3. XPS: a send from this io-thread's CPU egresses tx-$nicq.
            xcpu="$iocpu"; xps=skip
            case "$xcpu" in
                ''|*[!0-9]*) ;;
                *) if nic_exec bash -c \
                        "echo $(cpu_xps_mask "$xcpu") > /sys/class/net/$TUNE_NIC/queues/tx-$nicq/xps_cpus" \
                        2>/dev/null; then xps="cpu $xcpu"; fi ;;
            esac
            echo "   q$nicq irq[$(echo $irqs | tr '\n' ' ')] eff=$combo group=$group -> io-thread tid $tid cpu $iocpu (off irq cpu $irqcpu), xps tx-$nicq=$xps (was cpu $cpus)"
        else
            echo "   q$nicq irq[$(echo $irqs | tr '\n' ' ')] group=$group pushed=[${pushed:-none}]; taskset tid $tid to '${iocpu:-?}' (irq cpu $irqcpu) failed"
        fi
    done <<EOF
$rows
EOF
    # Disable software RPS/RFS (the net_rps_send_ipi storm). These knobs persist
    # across runs, so clear them every sync: the global flow table and, on each
    # rx queue, rps_flow_cnt (RFS) and rps_cpus (plain RPS).
    echo 0 > /proc/sys/net/core/rps_sock_flow_entries 2>/dev/null || true
    nic_exec bash -c '
        for q in /sys/class/net/'"$TUNE_NIC"'/queues/rx-*; do
            echo 0 > "$q/rps_flow_cnt" 2>/dev/null
            echo 0 > "$q/rps_cpus" 2>/dev/null
        done' 2>/dev/null || true
    echo "   RPS/RFS disabled (RX softirqs stay on their queue CPU; no relocation IPIs)"

    # Hardware ntuple RX steering: have the NIC deliver each connection's RX
    # directly to its io-thread's queue (qid-1) -- no software RFS, no IPI, and
    # stable. Match the inbound flow by the host's ephemeral source port (unique
    # per connection) + our listen port.
    nic_exec ethtool -K "$TUNE_NIC" ntuple on >/dev/null 2>&1 || true
    # Clear stale rules (previous runs' source ports) for a clean slate.
    nic_clear_ntuple "$TUNE_NIC"
    echo ">> steering each flow to its io-thread queue via NIC ntuple (no IPI)"
    while read -r qid tid cpus group peer; do
        [ -n "$qid" ] || continue
        nicq=$((qid - 1)); sport="${peer##*:}"
        case "$sport" in ''|*[!0-9]*) echo "   q$nicq: no peer port ($peer); skipped"; continue ;; esac
        if nic_exec ethtool -N "$TUNE_NIC" flow-type tcp4 \
                src-port "$sport" dst-port "$IOUTGT_PORT" action "$nicq" >/dev/null 2>&1; then
            echo "   q$nicq: src-port $sport -> rx queue $nicq (hardware)"
        else
            echo "   q$nicq: ntuple rule (src-port $sport) rejected"
        fi
    done <<EOF
$rows
EOF
}

# Show, per IO queue, the io-thread's LIVE affinity (from `ioutgt list`) beside
# its $TUNE_NIC RX IRQ effective CPU, with the separation verdict (OK = io-thread
# on a different logical CPU than its RX IRQ; SAME-CPU = the capping
# co-location). Reads only globals (/proc/irq, /proc/interrupts, ioutgt ctl).
tune_status() {
    [ -n "${TUNE_NIC:-}" ] || return 0
    echo "== $TUNE_NIC queue IRQ vs ioutgt io-thread (live) affinity =="
    # `is-active` exits non-zero (and still prints the state) when not running.
    echo "  irqbalance: $(systemctl is-active irqbalance 2>/dev/null || true)"
    local rows
    rows="$("$IOUTGT_BIN" ctl --socket "$IOUTGT_SOCK" '{"op":"LIST_CONTROLLER"}' 2>/dev/null \
        | jq -r '.data.controllers[]?.queues[]? | select(.qid >= 1) | "\(.qid) \(.tid) \(.cpus) \(.group_cpus)"' \
            2>/dev/null | sort -n -u || true)"
    if [ -z "$rows" ]; then
        echo "  (no connected IO queues)"; return 0
    fi
    local qid tid cpus group nicq irqs irq eff verdict mism=0 noirq=0 ircpu
    while read -r qid tid cpus group; do
        [ -n "$qid" ] || continue
        nicq=$((qid - 1))
        irqs="$(nic_queue_irqs "$TUNE_NIC" "$nicq")"
        eff=""
        for irq in $irqs; do
            eff="${eff:+$eff,}$(cat "/proc/irq/$irq/effective_affinity_list" 2>/dev/null || echo '?')"
        done
        # No matched IRQ means the separation is UNVERIFIABLE, not OK -- never
        # let missing data pass as a clean verdict.
        if [ -z "$irqs" ]; then
            verdict='NO-IRQ'; noirq=$((noirq + 1))
        else
            ircpu="${eff%%,*}"
            case "$cpus" in
                *[!0-9]*)  verdict='?' ;;          # unpinned/unknown io-thread
                "$ircpu")  verdict=SAME-CPU; mism=$((mism + 1)) ;;
                *)         verdict=OK ;;
            esac
        fi
        printf "  q%-2s io-thread(tid %s) aff=%-10s group=%-12s | irq[%s] eff=%-10s %s\n" \
            "$nicq" "$tid" "$cpus" "$group" "$(echo $irqs | tr '\n' ' ' | sed 's/ $//')" "${eff:-?}" "$verdict"
    done <<EOF
$rows
EOF
    if [ "$noirq" -gt 0 ]; then
        echo "  separation: UNKNOWN for $noirq queue(s) -- no NIC IRQ matched in /proc/interrupts (driver IRQ naming not recognised?)"
    fi
    if [ "$mism" -gt 0 ]; then
        echo "  separation: $mism queue(s) SAME-CPU as their RX IRQ -- re-run 'connect' (or irqbalance restarted?)"
    elif [ "$noirq" -eq 0 ]; then
        echo "  separation: OK (every io-thread on a different CPU than its NIC RX IRQ)"
    fi
}

# ---- two-NIC netns scaffolding (shared by two_nic_realwire*.sh) ------
# Force target/initiator traffic across two PHYSICAL NICs on one host by
# isolating each in its OWN network namespace: with no veth/bridge linking the
# namespaces, the only path between them is the physical link between the cards,
# so a successful cross-namespace ping is itself proof the bytes crossed the
# wire. Transport-neutral — these helpers read the caller's NS_T/NS_I,
# NIC_T/NIC_I, IP_T/IP_I, PREFIX, MTU globals. (The RDMA driver additionally
# moves each NIC's rdma device into the netns; see two_nic_realwire_rdma.sh.)
NSDIR="${NSDIR:-/run/netns}"

# nsenter --net enters ONLY the net namespace, leaving the mount namespace
# alone — crucial for the kernel target, because `ip netns exec` remounts a
# fresh /sys and thereby SHADOWS the configfs at /sys/kernel/config.
in_net() { nsenter --net="$NSDIR/$1" "${@:2}"; }

# Create the two namespaces, move each physical NIC in, then address + MTU + up.
realwire_netns_create() {
    echo ">> creating namespaces $NS_T / $NS_I and moving NICs in"
    ip netns add "$NS_T"
    ip netns add "$NS_I"
    ip link set "$NIC_T" netns "$NS_T"
    ip link set "$NIC_I" netns "$NS_I"
    ip netns exec "$NS_T" ip addr add "$IP_T/$PREFIX" dev "$NIC_T"
    ip netns exec "$NS_I" ip addr add "$IP_I/$PREFIX" dev "$NIC_I"
    # MTU before carrier: both ends must match (mismatched MTU silently drops
    # oversized frames).
    ip netns exec "$NS_T" ip link set "$NIC_T" mtu "$MTU"
    ip netns exec "$NS_I" ip link set "$NIC_I" mtu "$MTU"
    ip netns exec "$NS_T" ip link set lo up
    ip netns exec "$NS_I" ip link set lo up
    ip netns exec "$NS_T" ip link set "$NIC_T" up
    ip netns exec "$NS_I" ip link set "$NIC_I" up
}

# Wait for carrier, then prove traffic crosses the physical link. Sizes the
# probe to the MTU (DF set) so an MTU mismatch / a link that cannot carry full
# frames fails here, loudly, instead of silently later. Returns non-zero on
# failure (the caller decides whether that is fatal).
realwire_prove_wire() {
    echo ">> waiting for link/carrier, then proving the wire with ping"
    sleep 2
    local psize=$((MTU - 28)) # minus IP(20)+ICMP(8) headers
    if ip netns exec "$NS_I" ping -c 3 -W 2 -M do -s "$psize" "$IP_T" >/dev/null; then
        echo "   OK: $IP_I -> $IP_T reachable at MTU $MTU (full-frame, DF). Only"
        echo "   path is the physical link between $NIC_I and $NIC_T -> wire."
    else
        echo "   FAIL: no full-frame ping at MTU $MTU. Check the cable/switch"
        echo "   between $NIC_I and $NIC_T, that both NICs have carrier, that"
        echo "   IP_T/IP_I share subnet /$PREFIX, and that the link supports"
        echo "   MTU $MTU (else re-run with MTU=1500)."
        return 1
    fi
}

# Return the NICs to root and delete the namespaces (best-effort; env-tolerant).
# Deleting a netns also auto-returns its physical NICs, so the explicit moves
# are belt-and-suspenders.
realwire_netns_delete() {
    [ -n "${NIC_T:-}" ] && in_net "$NS_T" ip link set "$NIC_T" netns 1 2>/dev/null || true
    [ -n "${NIC_I:-}" ] && in_net "$NS_I" ip link set "$NIC_I" netns 1 2>/dev/null || true
    ip netns del "$NS_T" 2>/dev/null || true
    ip netns del "$NS_I" 2>/dev/null || true
}
