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
# Records the PCI BDF that SPDK_BDEV=nvme bound to VFIO, so the driver's 'down'
# can rebind it to the kernel nvme driver.
SPDK_VFIO_STATE="${SPDK_VFIO_STATE:-/tmp/spdk-vfio-bdf}"
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

# ---- SPDK_BDEV=nvme: userspace NVMe driver (VFIO) lifecycle -----------
# Resolve a block-device backend ($1, a by-id or /dev/nvmeXnY path) to the PCI
# BDF of its NVMe controller — for SPDK's userspace NVMe driver (bdev_nvme).
spdk_backend_bdf() {
    local blk; blk="$(basename "$(readlink -f "$1" 2>/dev/null)")"
    local addr="/sys/block/$blk/device/address"
    [ -r "$addr" ] && cat "$addr"
}

# Pick a NUMA node that HAS memory and is closest to PCI $1: the device's own
# node if it has RAM, else the nearest memory node by NUMA distance. Modern
# multi-die CPUs expose memory-less device nodes (e.g. an SSD on a node with 0
# MemTotal), where SPDK/DPDK's node-local allocation would fail. Empty if
# unknown (no NUMA info), so callers fall back to the global pool.
_node_memkb() { awk '/MemTotal/{print $4+0; exit}' "/sys/devices/system/node/node$1/meminfo" 2>/dev/null || echo 0; }
spdk_mem_node_for() {
    local dn; dn="$(cat "/sys/bus/pci/devices/$1/numa_node" 2>/dev/null || echo -1)"
    [ "${dn:--1}" -ge 0 ] 2>/dev/null || return 0
    [ "$(_node_memkb "$dn")" -gt 0 ] && { echo "$dn"; return 0; }
    local dists n=0 d best="" bestd=1000000
    read -ra dists < <(cat "/sys/devices/system/node/node$dn/distance" 2>/dev/null)
    for d in "${dists[@]}"; do
        [ "$(_node_memkb "$n")" -gt 0 ] && [ "$d" -lt "$bestd" ] && { bestd="$d"; best="$n"; }
        n=$((n + 1))
    done
    echo "$best"
}

# Bind PCI $1 to a userspace driver (vfio-pci/uio) via SPDK setup.sh — RESTRICTED
# to that one BDF (PCI_ALLOWED), so other kernel NVMe (nvmet's backend, boot
# disks) stay put — and record it for spdk_vfio_reset. Also sets up hugepages.
spdk_vfio_bind() {
    local bdf="$1"
    echo "$bdf" > "$SPDK_VFIO_STATE"
    # bdev_nvme allocates the device's queue DMA memory on the DEVICE's NUMA node
    # (not the reactor's). On a multi-die CPU the SSD can sit on a memory-less
    # node, so those allocations fail: io_channel ENOMEM -> subsystem load fails
    # -> no listener (host then sees "invalid service ID"). Fix, mirroring what a
    # bare spdk_nvme_perf needs here: put the hugepages on, AND steer the device's
    # advertised numa_node to, the nearest node WITH memory (same socket, so the
    # DMA path is unaffected).
    local node; node="$(spdk_mem_node_for "$bdf")"
    local env_args=(PCI_ALLOWED="$bdf" HUGEMEM="$SPDK_HUGEMEM")
    if [ -n "$node" ]; then
        # bdev_nvme + the RDMA transport together want a lot of node-local memory
        # (2 GB is not enough here; the io_channel alloc then races and fails).
        # Floor the per-node allocation at 8 GB (node has 32 GB free); callers can
        # raise it via SPDK_HUGEMEM.
        local nrhuge=$(( SPDK_HUGEMEM / 2 )); [ "$nrhuge" -lt 4096 ] && nrhuge=4096
        echo "   (allocating $nrhuge hugepages on NUMA node $node — nearest RAM to the SSD)"
        env_args+=(HUGENODE="$node" NRHUGE="$nrhuge")
    fi
    env "${env_args[@]}" "$SPDK_DIR/scripts/setup.sh" 2>&1 | sed 's/^/   [setup] /' || true
    # Steer the device's numa_node to that memory node (persists across the vfio
    # bind), saved for restore in spdk_vfio_reset.
    if [ -n "$node" ]; then
        local orig; orig="$(cat "/sys/bus/pci/devices/$bdf/numa_node" 2>/dev/null)"
        if [ "$orig" != "$node" ]; then
            echo "$bdf $orig" > "$SPDK_VFIO_STATE.numa"
            echo "$node" > "/sys/bus/pci/devices/$bdf/numa_node" 2>/dev/null &&
                echo "   (steered $bdf numa_node $orig -> $node for bdev_nvme DMA locality)"
        fi
    fi
}

# Rebind every SPDK-claimed device back to the kernel nvme driver (setup.sh
# reset), then forget them. No-op if none were bound. The driver's 'down' calls
# this so the backend becomes a normal /dev/nvme device again. On a FAILED
# rebind the state files are kept (and 1 returned) so a later 'down' retries —
# deleting them would strand the SSD on vfio-pci with no recovery record.
spdk_vfio_reset() {
    [ -s "$SPDK_VFIO_STATE" ] || return 0
    # A live nvmf_tgt still holds the VFIO group and the rebind would fail;
    # stop it first (spdk_stop waits for the process to exit).
    spdk_stop
    local bdfs; bdfs="$(tr '\n' ' ' < "$SPDK_VFIO_STATE")"
    echo ">> rebinding SPDK VFIO device(s) to the kernel nvme driver: $bdfs"
    local out rc=0
    out="$(PCI_ALLOWED="$bdfs" "$SPDK_DIR/scripts/setup.sh" reset 2>&1)" || rc=$?
    printf '%s\n' "$out" | sed 's/^/   [setup] /'
    if [ "$rc" -ne 0 ]; then
        echo "   setup.sh reset FAILED (rc=$rc); device(s) still on vfio-pci — kept $SPDK_VFIO_STATE, rerun 'down' to retry" >&2
        return 1
    fi
    # Restore any numa_node we steered for bdev_nvme.
    if [ -f "$SPDK_VFIO_STATE.numa" ]; then
        local rb ro; read -r rb ro < "$SPDK_VFIO_STATE.numa"
        { [ -n "${ro:-}" ] && echo "$ro" > "/sys/bus/pci/devices/$rb/numa_node" 2>/dev/null; } || true
        rm -f "$SPDK_VFIO_STATE.numa"
    fi
    rm -f "$SPDK_VFIO_STATE"
}

# Emit the bdev-subsystem config entry for a FILE/RAM SPDK_BDEV (aio/malloc/
# uring) as a JSON object. The bdev name for these types is always `spdk_bdev`
# (runs in a $(...) subshell, so spdk_start hardcodes the same name rather than
# reading it back). The nvme (VFIO) case is handled in spdk_start (it must bind
# the device first). SPDK_BLK = block size (512 default; must match a real
# block device).
_spdk_bdev_json() {
    local backend="$1" blk="${SPDK_BLK:-512}"
    bdev_name=spdk_bdev
    case "$SPDK_BDEV" in
        malloc:*) printf '{"method":"bdev_malloc_create","params":{"name":"%s","num_blocks":%d,"block_size":%d}}' \
                      "$bdev_name" "$(( ${SPDK_BDEV#malloc:} * 1024 * 1024 / blk ))" "$blk" ;;
        malloc)   printf '{"method":"bdev_malloc_create","params":{"name":"%s","num_blocks":%d,"block_size":%d}}' \
                      "$bdev_name" "$(( BACKEND_GB * 1024 * 1024 * 1024 / blk ))" "$blk" ;;
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
    # Backend prep + the bdev config JSON, per SPDK_BDEV type. bdev_nvme exposes
    # its namespace as <ctrl>n1; file/RAM bdevs name the bdev directly.
    local bdev_name=spdk_bdev bdevjson
    case "$SPDK_BDEV" in
        nvme|nvme:*)
            # SPDK's userspace NVMe driver: bind the backend's PCI device to VFIO
            # (removing it from the kernel until 'down' rebinds it). Derive the
            # BDF from the backend unless one is given explicitly (nvme:<BDF>).
            # setup.sh also allocates hugepages.
            local nvme_bdf="${SPDK_BDEV#nvme:}"; [ "$nvme_bdf" = nvme ] && nvme_bdf=""
            [ -n "$nvme_bdf" ] || nvme_bdf="$(spdk_backend_bdf "$backend")"
            [ -n "$nvme_bdf" ] || { echo "   cannot derive a PCI BDF from $backend — is it an NVMe block device?" >&2; exit 1; }
            # The SSD's NUMA node may be memory-less; pin SPDK's reactors to the
            # nearest node WITH memory (unless the caller pinned SPDK_CPUMASK) so
            # bdev_nvme's io_channels allocate local DMA memory rather than ENOMEM.
            if [ -z "$SPDK_CPUMASK" ]; then
                local memnode cl
                memnode="$(spdk_mem_node_for "$nvme_bdf")"
                cl="$([ -n "$memnode" ] && cat "/sys/devices/system/node/node$memnode/cpulist" 2>/dev/null)"
                [ -n "$cl" ] && { SPDK_CPUMASK="[$cl]"; echo ">> pinning SPDK to node $memnode CPUs [$cl] for bdev_nvme locality"; }
            fi
            echo ">> binding $backend (PCI $nvme_bdf) to VFIO for SPDK bdev_nvme"
            spdk_vfio_bind "$nvme_bdf"
            bdev_name=spdk_bdevn1
            bdevjson="$(printf '{"method":"bdev_nvme_attach_controller","params":{"name":"spdk_bdev","trtype":"PCIe","traddr":"%s"}}' "$nvme_bdf")"
            ;;
        *)
            # Hugepages for aio/malloc/uring — directly, NOT via setup.sh (which
            # would rebind the kernel NVMe backing an aio bdev).
            echo "$(( SPDK_HUGEMEM / 2 ))" > /proc/sys/vm/nr_hugepages 2>/dev/null || true
            mkdir -p /dev/hugepages 2>/dev/null || true
            grep -q hugetlbfs /proc/mounts 2>/dev/null || mount -t hugetlbfs nodev /dev/hugepages 2>/dev/null || true
            bdevjson="$(_spdk_bdev_json "$backend")"
            ;;
    esac
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
    # VFIO (bdev_nvme) and the RDMA transport pin memory; the default 8 MB
    # memlock is too low and makes DPDK/MR init fail (host connect then fails).
    ulimit -l unlimited 2>/dev/null || true
    echo ">> starting SPDK nvmf_tgt ($SPDK_TRTYPE, $SPDK_BDEV) on $ip:$port (backend $backend${SPDK_EAL_EXTRA:+, EAL $SPDK_EAL_EXTRA})"
    # Launch, then wait for the config-load VERDICT — the positive "Target
    # Listening on <ip>" signal (subsystem loaded, listener bound) vs "Failed to
    # load subsystems". The RPC socket appearing is NOT proof: with bdev_nvme +
    # the RDMA transport, the async bdev examine can lose a startup race for
    # io_channel DMA memory and fail the load ~400ms AFTER the socket comes up.
    # It's non-deterministic and reliable once it takes, so retry the launch.
    local attempt pid i verdict
    for attempt in $(seq 1 "${SPDK_START_TRIES:-8}"); do
        "${SPDK_NETNS[@]}" "$SPDK_BIN" "${mask[@]}" "${eal[@]}" -r "$SPDK_RPC_SOCK" -c "$cfg" >"$SPDK_LOG" 2>&1 &
        pid=$!; echo "$pid" > "$SPDK_PIDFILE"
        verdict=timeout
        for i in $(seq 1 150); do
            kill -0 "$pid" 2>/dev/null || { verdict=exited; break; }
            grep -q "Target Listening on $ip" "$SPDK_LOG" 2>/dev/null && { verdict=up; break; }
            grep -q "Failed to load subsystems" "$SPDK_LOG" 2>/dev/null && { verdict=loadfail; break; }
            sleep 0.2
        done
        if [ "$verdict" = up ]; then
            echo "   listening on $ip:$port, subsystem $nqn (bdev $bdev_name, transport $SPDK_TRTYPE), pid $pid [attempt $attempt]"
            return 0
        fi
        echo "   start attempt $attempt: $verdict — retrying..." >&2
        kill -9 "$pid" 2>/dev/null; rm -f "$SPDK_RPC_SOCK"; sleep 2
    done
    echo "   nvmf_tgt never loaded its subsystem (last verdict: $verdict); log:" >&2; tail -25 "$SPDK_LOG" >&2; exit 1
}

# spdk_stop — kill the whole nvmf_tgt process and WAIT for it to exit; it owns
# the subsystem + listener, so there is no per-target teardown. The wait
# matters for SPDK_BDEV=nvme: a dying nvmf_tgt must release the VFIO group
# before spdk_vfio_reset's setup.sh reset can rebind the SSD to the kernel.
spdk_stop() {
    local pid="" i
    [ -f "$SPDK_PIDFILE" ] && pid="$(cat "$SPDK_PIDFILE")"
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        for i in $(seq 1 50); do kill -0 "$pid" 2>/dev/null || break; sleep 0.2; done
        kill -0 "$pid" 2>/dev/null && { kill -9 "$pid" 2>/dev/null || true; sleep 1; }
        echo ">> SPDK nvmf_tgt stopped"
    fi
    rm -f "$SPDK_PIDFILE" "$SPDK_RPC_SOCK"
}

