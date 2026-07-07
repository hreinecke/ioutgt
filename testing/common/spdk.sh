# spdk.sh — the SPDK nvmf target (userspace, polled-mode):
# knobs, JSON-config start/stop, and the SPDK_BDEV=nvme (VFIO)
# device lifecycle. Sourced by common.sh (not a standalone script).

# ---- SPDK nvmf target (userspace, polled-mode) — used only by the spdk driver.
# The nvmf_tgt process hosts the subsystem(s)+listener(s); rpc.py drives its
# config over SPDK_RPC_SOCK. Built out-of-tree at SPDK_DIR (see spdk_start).
SPDK_DIR="${SPDK_DIR:-$HOME/git/spdk}"
SPDK_BIN="${SPDK_BIN:-$SPDK_DIR/build/bin/nvmf_tgt}"
SPDK_RPC="${SPDK_RPC:-$SPDK_DIR/scripts/rpc.py}"
SPDK_RPC_SOCK="${SPDK_RPC_SOCK:-/var/tmp/spdk-realwire.sock}"
SPDK_PIDFILE="${SPDK_PIDFILE:-/tmp/spdk-realwire.pid}"
SPDK_LOG="${SPDK_LOG:-/tmp/spdk-realwire.log}"
# nvmf_tgt reactor core mask (DPDK -m). Empty = core 0 only (fine functionally;
# set e.g. 0xff to spread qpair pollers for perf).
SPDK_CPUMASK="${SPDK_CPUMASK:-}"
SPDK_HUGEMEM="${SPDK_HUGEMEM:-2048}"     # MiB of 2M hugepages for DPDK/nvmf_tgt
# Extra DPDK EAL args, verbatim. On a memory-tight box (e.g. the vmtest VM)
# where hugepages can't be reserved, set SPDK_EAL_EXTRA="--no-huge -s 512" to
# run nvmf_tgt on regular memory (fine for TCP; RDMA wants hugepages).
SPDK_EAL_EXTRA="${SPDK_EAL_EXTRA:-}"
# Backend bdev type: aio (libaio over the file/bdev; maps like nvmet/ioutgt),
# malloc[:MiB] (RAM — no BACKEND needed), uring (io_uring; needs SPDK built
# --with-uring), or nvme:<PCI-BDF> (SPDK's userspace NVMe driver, the polled
# fast path — unbinds that device from the kernel).
SPDK_BDEV="${SPDK_BDEV:-aio}"
# SPDK's fabric type name (uppercase) for nvmf_create_transport / add_listener.
[ "$TRANSPORT" = rdma ] && SPDK_TRTYPE=RDMA || SPDK_TRTYPE=TCP
# Array launch-prefix for the nvmf_tgt process (`(ip netns exec NS_T)` when the
# target lives in a netns; `()` for root/loopback). The driver sets it after
# sourcing; default to none. The RPC socket is a filesystem path, reachable
# across netns, so rpc.py runs from anywhere.
[ -v SPDK_NETNS ] || SPDK_NETNS=()

# ---- SPDK nvmf target (userspace) ------------------------------------
# rpc.py is unusable here: SPDK v23.05's argparse predates Python 3.12's change
# that rejects action='store_true' on positional args, so it aborts on EVERY
# invocation under the box's Python 3.14 (and the vmtest guest inherits that
# Python over 9p). So we configure nvmf_tgt entirely through its C-loaded JSON
# startup config (`-c`) — one file, one launch, zero Python.

# Emit the bdev-subsystem config entry for SPDK_BDEV as a JSON object, and set
# the caller's `bdev_name` (the namespace's backing bdev). SPDK_BLK = the bdev
# block size (512 default; must match a real block device).
_spdk_bdev_json() {
    local backend="$1" blk="${SPDK_BLK:-512}"
    bdev_name=spdk_bdev
    case "$SPDK_BDEV" in
        malloc:*) printf '{"method":"bdev_malloc_create","params":{"name":"%s","num_blocks":%d,"block_size":%d}}' \
                      "$bdev_name" "$(( ${SPDK_BDEV#malloc:} * 1024 * 1024 / blk ))" "$blk" ;;
        malloc)   printf '{"method":"bdev_malloc_create","params":{"name":"%s","num_blocks":%d,"block_size":%d}}' \
                      "$bdev_name" "$(( BACKEND_GB * 1024 * 1024 * 1024 / blk ))" "$blk" ;;
        nvme:*)   bdev_name=spdk_bdevn1     # bdev_nvme names the namespace <name>n1
                  printf '{"method":"bdev_nvme_attach_controller","params":{"name":"spdk_bdev","trtype":"PCIe","traddr":"%s"}}' \
                      "${SPDK_BDEV#nvme:}" ;;
        uring)    BACKEND="$backend" ensure_backing >&2 || exit 1
                  printf '{"method":"bdev_uring_create","params":{"name":"%s","filename":"%s","block_size":%d}}' \
                      "$bdev_name" "$backend" "$blk" ;;
        aio|*)    BACKEND="$backend" ensure_backing >&2 || exit 1
                  printf '{"method":"bdev_aio_create","params":{"name":"%s","filename":"%s","block_size":%d}}' \
                      "$bdev_name" "$backend" "$blk" ;;
    esac
}

# spdk_start NQN PORT IP BACKEND — write the JSON startup config and launch one
# nvmf_tgt with it (SPDK is the single target here, one subsystem). A re-'start'
# while it is already up is a no-op. Same signature as nvmet_setup/ioutgt_start.
spdk_start() {
    local nqn="$1" port="$2" ip="$3" backend="$4"
    [ -x "$SPDK_BIN" ] || {
        echo "SPDK not built: $SPDK_BIN missing — build it:" >&2
        echo "  (cd $SPDK_DIR && git submodule update --init && ./configure --with-rdma && make -j)" >&2
        exit 1
    }
    if [ -f "$SPDK_PIDFILE" ] && kill -0 "$(cat "$SPDK_PIDFILE")" 2>/dev/null; then
        echo "   SPDK nvmf_tgt already running (pid $(cat "$SPDK_PIDFILE"))"; return 0
    fi
    # Hugepages for DPDK, allocated directly — NOT via scripts/setup.sh, which
    # would rebind the kernel NVMe backing an aio bdev.
    echo "$(( SPDK_HUGEMEM / 2 ))" > /proc/sys/vm/nr_hugepages 2>/dev/null || true
    mkdir -p /dev/hugepages 2>/dev/null || true
    grep -q hugetlbfs /proc/mounts 2>/dev/null || mount -t hugetlbfs nodev /dev/hugepages 2>/dev/null || true

    # The namespace's backing-bdev name is deterministic (a command-substituted
    # _spdk_bdev_json runs in a subshell, so its bdev_name can't propagate out):
    # bdev_nvme exposes its namespace as <ctrl>n1; every other type names the
    # bdev directly.
    local bdev_name=spdk_bdev; case "$SPDK_BDEV" in nvme:*) bdev_name=spdk_bdevn1 ;; esac
    local bdevjson; bdevjson="$(_spdk_bdev_json "$backend")"
    local cfg="${SPDK_CONFIG:-/tmp/spdk-realwire.json}"
    cat > "$cfg" <<JSON
{
  "subsystems": [
    { "subsystem": "bdev", "config": [ $bdevjson ] },
    { "subsystem": "nvmf", "config": [
      { "method": "nvmf_create_transport",     "params": { "trtype": "$SPDK_TRTYPE" } },
      { "method": "nvmf_create_subsystem",     "params": { "nqn": "$nqn", "allow_any_host": true, "serial_number": "SPDK$port" } },
      { "method": "nvmf_subsystem_add_ns",     "params": { "nqn": "$nqn", "namespace": { "bdev_name": "$bdev_name" } } },
      { "method": "nvmf_subsystem_add_listener","params": { "nqn": "$nqn", "listen_address": { "trtype": "$SPDK_TRTYPE", "adrfam": "IPv4", "traddr": "$ip", "trsvcid": "$port" } } }
    ] }
  ]
}
JSON
    local mask=(); [ -n "$SPDK_CPUMASK" ] && mask=(-m "$SPDK_CPUMASK")
    local eal=(); [ -n "$SPDK_EAL_EXTRA" ] && read -ra eal <<<"$SPDK_EAL_EXTRA"
    echo ">> starting SPDK nvmf_tgt ($SPDK_TRTYPE, $SPDK_BDEV) on $ip:$port (backend $backend${SPDK_EAL_EXTRA:+, EAL $SPDK_EAL_EXTRA})"
    "${SPDK_NETNS[@]}" "$SPDK_BIN" "${mask[@]}" "${eal[@]}" -r "$SPDK_RPC_SOCK" -c "$cfg" >"$SPDK_LOG" 2>&1 &
    echo $! > "$SPDK_PIDFILE"
    # Ready once the RPC socket appears (framework fully initialised, config
    # applied) and the process is still alive. No rpc.py to query, so this is the
    # readiness proxy; the connect step confirms the listener for real.
    local i pid; pid="$(cat "$SPDK_PIDFILE")"
    for i in $(seq 1 150); do
        kill -0 "$pid" 2>/dev/null || { echo "   nvmf_tgt exited during init; log:" >&2; tail -25 "$SPDK_LOG" >&2; exit 1; }
        [ -S "$SPDK_RPC_SOCK" ] && break
        sleep 0.2
    done
    [ -S "$SPDK_RPC_SOCK" ] || { echo "   nvmf_tgt not ready after 30s; log:" >&2; tail -25 "$SPDK_LOG" >&2; exit 1; }
    echo "   listening on $ip:$port, subsystem $nqn (bdev $bdev_name, transport $SPDK_TRTYPE), pid $pid"
}

# spdk_stop — kill the whole nvmf_tgt process (best-effort); it owns the
# subsystem + listener, so there is no per-target teardown.
spdk_stop() {
    [ -f "$SPDK_PIDFILE" ] && kill "$(cat "$SPDK_PIDFILE")" 2>/dev/null || true
    rm -f "$SPDK_PIDFILE" "$SPDK_RPC_SOCK"
    echo ">> SPDK nvmf_tgt stopped"
}

