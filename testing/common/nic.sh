# nic.sh — NIC/IRQ perf tuning and affinity helpers.
# Sourced by common.sh (not a standalone script).

# ===== NIC perf tuning (target NIC = $TUNE_NIC, in net namespace $TUNE_NS) =====
# Align a target NIC's RX/TX queue IRQs with ioutgt's io-threads. Reusable by
# any driver: set TUNE_NIC and TUNE_NS ("" = root netns, e.g. a single-NIC box)
# plus IOUTGT_BIN / IOUTGT_SOCK / IOUTGT_PORT. /proc/irq, /proc/interrupts,
# /proc/sys, taskset and `ioutgt ctl` are global; only NIC sysfs/ethtool ops go
# through the namespace (via nic_exec).

# True when an ioutgt control socket + binary are configured — the per-queue
# tuners introspect placement via `ioutgt list`, so they are ioutgt-only and
# no-op for nvmet/SPDK targets (which have no control socket).
ioutgt_ctl_ready() { [ -n "${IOUTGT_SOCK:-}" ] && [ -n "${IOUTGT_BIN:-}" ]; }

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

# Persisted NR_QUEUES, so a driver's separate up/start/connect/status
# invocations agree on the io-thread count. Drivers whose 'up' sizes
# NR_QUEUES from the NIC (the TCP wires) call this once after common.sh:
# it adopts the value persisted by the last 'up' unless the user set
# NR_QUEUES explicitly (NRQ_USER_SET, captured in common.sh).
nrq_state_init() {
    NRQ_STATE="${NRQ_STATE:-/tmp/ioutgt-realwire.nrq}"
    if [ -z "$NRQ_USER_SET" ] && [ -f "$NRQ_STATE" ]; then
        NR_QUEUES="$(cat "$NRQ_STATE")"
    fi
}

# Size NR_QUEUES for a TCP wire and keep NIC $1's Combined channel count
# aligned with it, so the post-connect IRQ<->io-thread mapping (nicq =
# qid-1) stays 1:1; persist the result to $NRQ_STATE for the later
# 'start'/'connect'/'status' invocations (see nrq_state_init). Callers
# run this only under NIC_TUNE=1 — the untuned path persists NR_QUEUES
# as-is instead.
nic_size_queues() {
    local nic="$1"
    if [ -n "$NRQ_USER_SET" ]; then
        # User's NR_QUEUES wins, bounded by what the host/NIC can deliver:
        # nproc and the NIC's hardware-max Combined. Then retune the NIC's
        # Combined channels (up OR down) to match, so every io-thread has its
        # own NIC queue/IRQ. If the NIC has no combined channels, fall back to
        # capping at the current channel count and leave the NIC untouched.
        local want="$NR_QUEUES" ncpu maxc
        ncpu="$(nproc 2>/dev/null || echo 1)"
        maxc="$(nic_max_combined "$nic")"
        [ "$ncpu" -lt "$NR_QUEUES" ] && NR_QUEUES="$ncpu"
        if [ "$maxc" -ge 1 ]; then
            [ "$maxc" -lt "$NR_QUEUES" ] && NR_QUEUES="$maxc"
            # Stale ntuple filters from a prior run pin high RX queues and would
            # reject a Combined reduction; clear them before retuning.
            nic_clear_ntuple "$nic"
            if nic_exec ethtool -L "$nic" combined "$NR_QUEUES" 2>/dev/null; then
                echo "   NR_QUEUES=$NR_QUEUES (requested $want, capped at nproc=$ncpu / max Combined=$maxc); $nic Combined retuned to $NR_QUEUES"
            else
                echo "   note: could not set $nic Combined to $NR_QUEUES; affinity sync may skip unmapped queues"
                echo "   NR_QUEUES=$NR_QUEUES (requested $want, capped at nproc=$ncpu / max Combined=$maxc)"
            fi
        else
            local cur; cur="$(nic_default_queues "$nic")"
            [ "$cur" -lt "$NR_QUEUES" ] && NR_QUEUES="$cur"
            echo "   NR_QUEUES=$NR_QUEUES (requested $want; $nic has no combined channels, capped at current/$cur, NIC not retuned)"
        fi
        echo "$NR_QUEUES" > "$NRQ_STATE"
    else
        # Auto-size from the NIC's current channels so ioutgt's --io-threads
        # matches the NIC channel count (no retune; the NIC drives the count).
        NR_QUEUES="$(nic_default_queues "$nic")"
        echo "$NR_QUEUES" > "$NRQ_STATE"
        echo "   NR_QUEUES defaulted to $NR_QUEUES (min rx/tx of $nic, capped at nproc)"
    fi
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
# The RDMA sibling of tune_target_nic: converge the mlx5 completion-vector
# IRQs with the pinned io-threads. RoCE traffic never touches the netdev RX
# queues, so the TCP tuner's ntuple/XPS/RPS/channel steps are no-ops here;
# what decides locality is the CQ's completion vector — ioutgt-nvme-rdma
# creates each queue's CQ on vector = qid, whose EQ fires the IRQ labeled
# "mlx5_comp<qid>@pci:<bdf>" (nic_queue_irqs' mlx5 fallback). Per connected IO
# queue: push the io-thread's CPU group onto that IRQ, then place the
# io-thread on the IRQ CPU's HT sibling (same policy/helpers as the TCP tuner).
tune_target_rdma() {
    [ -n "${TUNE_NIC:-}" ] || { echo "   (TUNE_NIC unset; skipping IRQ affinity sync)"; return 0; }
    # This tuner reads ioutgt's control socket for per-queue placement, so it only
    # applies to an ioutgt target — skip it for nvmet/SPDK (no control socket).
    ioutgt_ctl_ready || { echo "   (no ioutgt control socket; RDMA IRQ tuning is ioutgt-only, skipping)"; return 0; }
    command -v jq >/dev/null 2>&1 || { echo "   (jq not found; skipping IRQ affinity sync)"; return 0; }
    local json rows
    json="$("$IOUTGT_BIN" ctl --socket "$IOUTGT_SOCK" '{"op":"LIST_CONTROLLER"}' 2>/dev/null || true)"
    rows="$(printf '%s' "$json" \
        | jq -r '.data.controllers[]?.queues[]? | select(.qid >= 1) | "\(.qid) \(.tid) \(.cpus) \(.group_cpus)"' \
            2>/dev/null | sort -n -u || true)"
    if [ -z "$rows" ]; then
        echo "   (no connected IO queues; run 'connect' first)"; return 0
    fi
    systemctl stop irqbalance 2>/dev/null || true
    echo ">> converging $TUNE_NIC comp-vector IRQ affinity <-> ioutgt io-threads"
    local qid tid cpus group irqs irq combo eff pushed irqcpu iocpu
    while read -r qid tid cpus group; do
        [ -n "$qid" ] || continue
        # CQ completion vector = qid (crate build_conn_resources).
        irqs="$(nic_queue_irqs "$TUNE_NIC" "$qid")"
        if [ -z "$irqs" ]; then
            echo "   vec$qid (qid $qid): no mlx5_comp IRQ found; skipped"; continue
        fi
        combo=""; pushed=""
        for irq in $irqs; do
            # 1. push the io-thread's whole CPU group onto the IRQ (valid
            #    cpulist only -- "*"/"?" means unpinned/unknown).
            case "$group" in
                ''|'*'|'?'|*[!0-9,-]*) ;;
                *) if echo "$group" > "/proc/irq/$irq/smp_affinity_list" 2>/dev/null; then
                       pushed="${pushed:+$pushed,}$irq"
                   fi ;;
            esac
            eff="$(cat "/proc/irq/$irq/effective_affinity_list" 2>/dev/null || true)"
            [ -n "$eff" ] && combo="${combo:+$combo,}$eff"
        done
        # 2. place the io-thread on the IRQ CPU's HT sibling.
        irqcpu="${combo%%[,-]*}"
        iocpu="$(iothread_cpu "$group" "$irqcpu")"
        if [ -n "$iocpu" ] && taskset -cp "$iocpu" "$tid" >/dev/null 2>&1; then
            echo "   vec$qid irq[$(echo $irqs | tr '\n' ' ')] eff=$combo group=$group -> io-thread tid $tid cpu $iocpu (off irq cpu $irqcpu) (was cpu $cpus)"
        else
            echo "   vec$qid irq[$(echo $irqs | tr '\n' ' ')] group=$group pushed=[${pushed:-none}]; taskset tid $tid to '${iocpu:-?}' (irq cpu $irqcpu) failed"
        fi
    done <<EOF
$rows
EOF
}

tune_target_nic() {
    [ -n "${TUNE_NIC:-}" ] || { echo "   (TUNE_NIC unset; skipping IRQ affinity sync)"; return 0; }
    # This tuner reads ioutgt's control socket for per-queue placement, so it only
    # applies to an ioutgt target — skip it for nvmet/SPDK (no control socket).
    ioutgt_ctl_ready || { echo "   (no ioutgt control socket; NIC IRQ tuning is ioutgt-only, skipping)"; return 0; }
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

# blk-mq debugfs dir holding a block device's hctx map. With native NVMe
# multipath the map lives under the hidden per-controller node
# (nvmeXnY -> nvmeXcZnY), not the head itself.
mq_debug_dir() {
    local name="${1#/dev/}" cand
    for cand in "$name" $(cd /sys/kernel/debug/block 2>/dev/null &&
            ls -d "${name%n*}"c*n"${name##*n}" 2>/dev/null); do
        [ -d "/sys/kernel/debug/block/$cand/hctx0" ] &&
            { echo "/sys/kernel/debug/block/$cand"; return 0; }
    done
    return 1
}

# Initiator-side twin of tune_target_nic's ntuple steering: deliver each
# connection's inbound flow (the C2H read-data stream) on $TUNE_NIC_INI to
# queue qid-1, with that queue's IRQ pointed INTO the connection's blk-mq
# hctx CPU set. Without this the data flow lands on a per-connect
# RSS-random NIC_I queue -- machine-wide on a multi-node box -- and
# single-job randread swings 10-15% with every reconnect while randwrite
# (already steered on the target side) stays flat. aRFS cannot fix it:
# nvme-tcp consumes its sockets via read_sock from kernel io_work, which
# never records RFS flows, so accelerated RFS never steers ULP traffic.
#
# The IRQ lands on the SECOND-lowest hctx CPU: the lowest is what nvme-tcp
# typically picks as queue->io_cpu, and RX softirq measured best beside the
# io_work CPU, not on it. Needs debugfs for the hctx map; skips otherwise.
tune_initiator_tcp() {
    [ -n "${TUNE_NIC_INI:-}" ] ||
        { echo "   (TUNE_NIC_INI unset; skipping initiator RX steering)"; return 0; }
    # Steers RX by ioutgt's per-queue peer ports (needs its control socket + NQN),
    # so it only applies to an ioutgt target — skip for nvmet/SPDK.
    { ioutgt_ctl_ready && [ -n "${IOUTGT_NQN:-}" ]; } ||
        { echo "   (no ioutgt control socket; initiator RX steering is ioutgt-only, skipping)"; return 0; }
    command -v jq >/dev/null 2>&1 ||
        { echo "   (jq not found; skipping initiator RX steering)"; return 0; }
    local rows dev mqdir
    rows="$("$IOUTGT_BIN" ctl --socket "$IOUTGT_SOCK" '{"op":"LIST_CONTROLLER"}' 2>/dev/null \
        | jq -r '.data.controllers[]?.queues[]? | select(.qid >= 1) | "\(.qid) \(.peer)"' \
            2>/dev/null | sort -n -u || true)"
    [ -n "$rows" ] || { echo "   (no connected IO queues; run 'connect' first)"; return 0; }
    dev="$(find_dev "$IOUTGT_NQN" || true)"
    mqdir="$([ -n "$dev" ] && mq_debug_dir "$dev" || true)"
    [ -n "$mqdir" ] ||
        { echo "   (no blk-mq debugfs map for ${dev:-?}; skipping initiator RX steering)"; return 0; }
    ini_exec ethtool -K "$TUNE_NIC_INI" ntuple on >/dev/null 2>&1 || true
    # Stale rules carry previous connects' ports; subshell so the TUNE_NS
    # override (nic_* helpers act on the initiator netns) does not leak.
    (TUNE_NS="${TUNE_NS_INI:-}" && nic_clear_ntuple "$TUNE_NIC_INI")
    # Same softirq hygiene as the target side: no software RPS/RFS IPIs.
    ini_exec bash -c '
        for q in /sys/class/net/'"$TUNE_NIC_INI"'/queues/rx-*; do
            echo 0 > "$q/rps_flow_cnt" 2>/dev/null
            echo 0 > "$q/rps_cpus" 2>/dev/null
        done' 2>/dev/null || true
    echo ">> steering each flow's C2H RX on $TUNE_NIC_INI into its hctx CPU set"
    local qid peer sport nicq irq hcpus rxcpu
    while read -r qid peer; do
        [ -n "$qid" ] || continue
        nicq=$((qid - 1)); sport="${peer##*:}"
        case "$sport" in ''|*[!0-9]*) echo "   q$nicq: no peer port ($peer); skipped"; continue ;; esac
        irq="$(TUNE_NS="${TUNE_NS_INI:-}" nic_queue_irqs "$TUNE_NIC_INI" "$nicq" | head -1)"
        [ -n "$irq" ] || { echo "   q$nicq: no $TUNE_NIC_INI IRQ found; skipped"; continue; }
        hcpus="$(ls "$mqdir/hctx$nicq" 2>/dev/null | sed -n 's/^cpu//p' | sort -n)"
        rxcpu="$(echo "$hcpus" | sed -n 2p)"
        [ -n "$rxcpu" ] || rxcpu="$(echo "$hcpus" | sed -n 1p)"
        [ -n "$rxcpu" ] || { echo "   q$nicq: empty hctx$nicq CPU set; skipped"; continue; }
        echo "$rxcpu" > "/proc/irq/$irq/smp_affinity_list" 2>/dev/null || true
        if ini_exec ethtool -N "$TUNE_NIC_INI" flow-type tcp4 \
                src-port "$IOUTGT_PORT" dst-port "$sport" action "$nicq" >/dev/null 2>&1; then
            echo "   q$nicq: dst-port $sport -> rx queue $nicq, irq$irq -> cpu $rxcpu (hctx $(echo $hcpus | tr ' ' ','))"
        else
            echo "   q$nicq: ntuple rule (dst-port $sport) rejected"
        fi
    done <<EOF
$rows
EOF
}

# RDMA initiator-side twin of tune_initiator_tcp, for the same reason with a
# different mechanism. On the same-box rig rdma_cm resolves BOTH endpoints
# onto the device owning the target GID (self-loopback), so the kernel
# initiator's per-queue CQs land on $TUNE_NIC's own comp vectors: initiator
# qid n polls on vector n-1, whose IRQ the default spread (or the target
# pass above) points wherever -- measured cross-socket, and worth -35% on
# single-job 4k randread (165K -> 254K once placed). There is no RSS and no
# reconnect lottery here (the vector choice is deterministic), just a
# static misplacement.
#
# Placement: the SMT sibling of the LOWEST hctx CPU. RDMA has no io_work
# consumer (the HCA DMAs the data), so the CQ softirq wants a core that is
# node-local to the submitter without sharing its core: same-CPU measured
# 186K, same-core 228K, same-node-different-core 254K. Runs after
# tune_target_rdma; on shared vectors (target qid n = vector n, initiator
# qid n+1 = vector n) the initiator's immovable blk-mq map wins the vector,
# and only single-job runs keep the two hot vectors disjoint anyway.
tune_initiator_rdma() {
    [ -n "${TUNE_NIC:-}" ] ||
        { echo "   (TUNE_NIC unset; skipping initiator CQ placement)"; return 0; }
    local nqn="$1" dev mqdir h i qid irq hcpus low rxcpu
    dev="$(find_dev "$nqn" || true)"
    [ -n "$dev" ] ||
        { echo "   (no device for $nqn; skipping initiator CQ placement)"; return 0; }
    mqdir="$(mq_debug_dir "$dev" || true)"
    [ -n "$mqdir" ] ||
        { echo "   (no blk-mq debugfs map for $dev; skipping initiator CQ placement)"; return 0; }
    echo ">> placing initiator CQ-vector IRQs ($dev) into their hctx CPU sets"
    for h in "$mqdir"/hctx*; do
        i="${h##*hctx}"; qid=$((i + 1))
        # Initiator qid = hctx index + 1 uses CQ completion vector = hctx index.
        irq="$(nic_queue_irqs "$TUNE_NIC" "$i" | head -1)"
        [ -n "$irq" ] || { echo "   vec$i: no mlx5_comp IRQ found; skipped"; continue; }
        hcpus="$(ls "$h" 2>/dev/null | sed -n 's/^cpu//p' | sort -n)"
        low="$(echo "$hcpus" | sed -n 1p)"
        [ -n "$low" ] || { echo "   vec$i: empty hctx$i CPU set; skipped"; continue; }
        rxcpu="$(tr ',' '\n' <"/sys/devices/system/cpu/cpu$low/topology/thread_siblings_list" \
            2>/dev/null | grep -vx "$low" | head -1)"
        [ -n "$rxcpu" ] || rxcpu="$(echo "$hcpus" | sed -n 2p)"
        [ -n "$rxcpu" ] || rxcpu="$low"
        echo "$rxcpu" > "/proc/irq/$irq/smp_affinity_list" 2>/dev/null || true
        echo "   vec$i (initiator qid $qid): irq$irq -> cpu $rxcpu (hctx $(echo $hcpus | tr ' ' ','))"
    done
}

# Show, per IO queue, the io-thread's LIVE affinity (from `ioutgt list`) beside
# its $TUNE_NIC RX IRQ effective CPU, with the separation verdict (OK = io-thread
# on a different logical CPU than its RX IRQ; SAME-CPU = the capping
# co-location). Reads only globals (/proc/irq, /proc/interrupts, ioutgt ctl).
# With TUNE_COMP_VECTOR=1 (the RDMA driver), a queue's IRQ index is its CQ
# completion vector (= qid, the mlx5_comp<qid> IRQ); default (TCP) is the
# netdev channel (= qid-1).
tune_status() {
    [ -n "${TUNE_NIC:-}" ] || return 0
    # Live IRQ-vs-io-thread affinity readout reads ioutgt's control socket; it
    # only applies to an ioutgt target — skip for nvmet/SPDK.
    ioutgt_ctl_ready || return 0
    local what="queue"
    [ "${TUNE_COMP_VECTOR:-0}" = 1 ] && what="comp-vector"
    echo "== $TUNE_NIC $what IRQ vs ioutgt io-thread (live) affinity =="
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
        if [ "${TUNE_COMP_VECTOR:-0}" = 1 ]; then nicq=$qid; else nicq=$((qid - 1)); fi
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

