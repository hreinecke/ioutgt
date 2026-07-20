# nvmet.sh — the Linux in-kernel nvmet target (configfs):
# setup/teardown. Sourced by common.sh (not a standalone script);
# shares its knobs and helpers.

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
    # Refuse pre-existing state instead of writing into it (mkdir -p once
    # masked a live subsystem and the run died later on a cryptic EBUSY):
    # our subsystem already configured, or ANY port already bound to this
    # ip:port — e.g. another transport's driver, which shares the address
    # plan. configfs is a global singleton, so plain (non-netns) reads see it.
    local cfg=/sys/kernel/config/nvmet p
    [ -d "$cfg/subsystems/$nqn" ] &&
        fail "nvmet subsystem $nqn already configured — another session or stale state; run its driver's 'stop' first"
    for p in "$cfg"/ports/*/; do
        [ -e "$p/addr_traddr" ] || continue
        [ "$(cat "$p/addr_traddr")" = "$ip" ] && [ "$(cat "$p/addr_trsvcid")" = "$port" ] &&
            fail "nvmet configfs port $(basename "$p") ($(cat "$p/addr_trtype")) already bound to $ip:$port — another session owns this address; run its driver's 'stop' first"
    done
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

