#!/bin/bash
# Guest-side CPU-affinity test: run ioutgt (pinning is default-on)
# guest (vmtest.conf: VMTEST_NUMA_NODES > 1) and verify the userspace
# spread_cpus placement against the guest's /sys topology:
#   - one affinity group per IO thread, each group inside ONE NUMA node
#   - groups pairwise disjoint and covering every possible CPU
#   - every node serves at least one IO thread
#   - each IO thread really pinned to the one group CPU from the log
#
# Sourced by the vmtest wrapper (tests/ioutgt_affinity.sh); expects
# lib/common.sh helpers and config already loaded.
set -eu

# Expand a sysfs cpulist ("0-3,8,10-11") to one CPU per line.
ioutgt_expand_cpulist() {
    local -a parts
    local part
    IFS=',' read -ra parts <<<"$1"
    for part in "${parts[@]}"; do
        case "$part" in
        *-*) seq "${part%-*}" "${part#*-}" ;;
        *) echo "$part" ;;
        esac
    done
}

ioutgt_run_affinity() {
    local top
    top=$(cat "${VMTEST_DATA_DIR:-/nonexistent}/tmp/ioutgt_top" 2>/dev/null ||
        echo "${IOUTGT_DIR:-}")
    [ -n "$top" ] ||
        vt_die "no ioutgt checkout (missing ioutgt_top marker and IOUTGT_DIR)"
    local bin="$top/target/release/ioutgt-nvme-tcp"
    [ -x "$bin" ] || vt_die "no ioutgt binary at $bin (run testing/run_affinity.sh)"

    # NUMA layout of this guest.
    local -a node_cpus=()
    local node_dir
    for node_dir in /sys/devices/system/node/node[0-9]*; do
        [ -d "$node_dir" ] || break
        node_cpus+=("$(cat "$node_dir/cpulist")")
    done
    local nodes=${#node_cpus[@]}
    [ "$nodes" -ge 2 ] ||
        vt_skip "guest has $nodes NUMA node(s); set VMTEST_NUMA_NODES>=2 in vmtest.conf"
    local ncpus
    ncpus=$(nproc)
    vt_log "guest: $ncpus CPUs, $nodes nodes (${node_cpus[*]})"

    # More groups than nodes so the per-node ratio allocation runs and
    # every group must come out node-pure.
    local io_threads=$((nodes * 2))
    [ "$io_threads" -le "$ncpus" ] || io_threads=$ncpus

    local log=/tmp/ioutgt-affinity.log
    RUST_LOG=info "$bin" --listen 127.0.0.1:14420 --io-threads "$io_threads" \
        --control-socket /tmp/ioutgt-affinity.sock >"$log.raw" 2>&1 &
    local pid=$!
    vt_atexit "kill $pid 2>/dev/null || true"

    # Wait for all affinity lines (tracing output carries ANSI codes).
    local i
    for i in $(seq 50); do
        sed 's/\x1b\[[0-9;]*m//g' "$log.raw" >"$log"
        [ "$(grep -c 'io queue affinity' "$log")" -eq "$io_threads" ] && break
        kill -0 "$pid" 2>/dev/null || { cat "$log"; vt_die "target died"; }
        sleep 0.2
    done
    [ "$(grep -c 'io queue affinity' "$log")" -eq "$io_threads" ] ||
        { cat "$log"; vt_die "expected $io_threads 'io queue affinity' lines"; }
    grep 'io queue affinity' "$log" >&2

    # The queue-thread pool spawns lazily on the first accept: poke one
    # TCP connection and hold it open so the ioutgt-io* threads exist
    # (and outlive the idle grace period) while we inspect them.
    exec 3<>/dev/tcp/127.0.0.1/14420 || vt_die "cannot connect to 127.0.0.1:14420"
    for i in $(seq 50); do
        grep -qx "ioutgt-io0" /proc/"$pid"/task/*/comm 2>/dev/null && break
        kill -0 "$pid" 2>/dev/null || { cat "$log"; vt_die "target died"; }
        sleep 0.2
    done
    grep -qx "ioutgt-io0" /proc/"$pid"/task/*/comm 2>/dev/null ||
        vt_die "io threads did not spawn after a connection"

    local -a node_threads=()
    for i in $(seq 0 $((nodes - 1))); do node_threads[i]=0; done
    local all_grp_cpus=""

    for i in $(seq 0 $((io_threads - 1))); do
        local line grp cpu
        line=$(grep "io queue affinity thread=$i " "$log") ||
            vt_die "no affinity line for io thread $i"
        grp=$(sed -n 's/.*cpus=\([0-9,-]*\) .*/\1/p' <<<"$line")
        cpu=$(sed -n 's/.*cpu=\([0-9]*\)$/\1/p' <<<"$line")
        [ -n "$grp" ] && [ -n "$cpu" ] || vt_die "unparsable line: $line"

        local grp_cpus
        grp_cpus=$(ioutgt_expand_cpulist "$grp")
        all_grp_cpus="$all_grp_cpus $grp_cpus"

        # The selected CPU is in the group.
        grep -qx "$cpu" <<<"$grp_cpus" || vt_die "thread $i: cpu $cpu not in group $grp"

        # The group lies inside exactly one NUMA node.
        local n in_nodes=0
        for n in $(seq 0 $((nodes - 1))); do
            # comm wants lexicographic order; consistent on both sides
            # is all that set difference needs.
            local outside
            outside=$(comm -23 <(sort <<<"$grp_cpus") \
                <(ioutgt_expand_cpulist "${node_cpus[n]}" | sort))
            if [ -z "$outside" ]; then
                in_nodes=$((in_nodes + 1))
                node_threads[n]=$((node_threads[n] + 1))
            fi
        done
        [ "$in_nodes" -eq 1 ] || vt_die "thread $i: group $grp not confined to one node"

        # The thread is really pinned to that CPU.
        local tid="" comm
        for comm in /proc/$pid/task/*/comm; do
            if [ "$(cat "$comm")" = "ioutgt-io$i" ]; then
                tid=$(basename "$(dirname "$comm")")
                break
            fi
        done
        [ -n "$tid" ] || vt_die "no thread named ioutgt-io$i"
        local allowed
        allowed=$(awk '/Cpus_allowed_list/{print $2}' "/proc/$pid/task/$tid/status")
        [ "$allowed" = "$cpu" ] ||
            vt_die "thread $i (tid $tid): pinned to '$allowed', expected $cpu"
        vt_log "io thread $i: group $grp -> cpu $cpu (tid $tid) OK"
    done

    # Groups are pairwise disjoint and cover every possible CPU.
    local got want
    got=$(tr ' ' '\n' <<<"$all_grp_cpus" | sed '/^$/d' | sort -n)
    [ "$(wc -l <<<"$got")" -eq "$(sort -nu <<<"$got" | wc -l)" ] ||
        vt_die "groups overlap"
    want=$(ioutgt_expand_cpulist "$(cat /sys/devices/system/cpu/possible)" | sort -n)
    [ "$got" = "$want" ] || vt_die "groups do not cover all possible CPUs
got:  $(tr '\n' ' ' <<<"$got")
want: $(tr '\n' ' ' <<<"$want")"

    # Every node serves at least one IO thread.
    for n in $(seq 0 $((nodes - 1))); do
        [ "${node_threads[n]}" -ge 1 ] || vt_die "node $n got no IO thread"
    done

    # End-to-end: `ioutgt list` must report each queue's live affinity
    # exactly as /proc sees it. Controllers exist only while a host is
    # connected, so connect over loopback.
    modprobe nvme-tcp 2>/dev/null || true
    nvme connect -t tcp -a 127.0.0.1 -s 14420 -n nqn.2026-06.io.ioutgt:test \
        --nr-io-queues "$io_threads" || vt_die "nvme connect failed"
    vt_atexit "nvme disconnect -n nqn.2026-06.io.ioutgt:test >/dev/null 2>&1 || true"
    sleep 1

    local listing
    listing=$("$bin" list --socket /tmp/ioutgt-affinity.sock) ||
        vt_die "ioutgt list failed"
    vt_log "$listing"
    local entry qid tid cpus allowed checked=0
    while read -r entry; do
        [ -n "$entry" ] || continue
        qid=${entry%%:*}
        tid=$(sed -n 's/.*@\([0-9]*\) cpus.*/\1/p' <<<"$entry")
        cpus=$(sed -n 's/.*cpus \(.*\)$/\1/p' <<<"$entry")
        [ -n "$tid" ] && [ -n "$cpus" ] || vt_die "unparsable queue entry: $entry"
        if [ "$qid" -eq 0 ]; then
            [ "$cpus" = "*" ] || vt_die "admin queue cpus '$cpus', expected *"
        else
            # `list` renders the whole affinity group with the pinned
            # CPU bracketed (e.g. "[0],1"); /proc carries just the
            # pinned CPU.
            local act
            act=$(sed -n 's/.*\[\([0-9]*\)\].*/\1/p' <<<"$cpus")
            [ -n "$act" ] ||
                vt_die "qid $qid: no bracketed active cpu in '$cpus'"
            allowed=$(awk '/Cpus_allowed_list/{print $2}' "/proc/$pid/task/$tid/status")
            [ "$act" = "$allowed" ] ||
                vt_die "qid $qid tid $tid: list says active '$act' ('$cpus'), /proc says '$allowed'"
        fi
        checked=$((checked + 1))
    done < <(sed -n 's/^  queues: //p' <<<"$listing" | tr '|' '\n' |
        sed 's/^ *//;s/ *$//')
    [ "$checked" -eq $((io_threads + 1)) ] ||
        vt_die "listing showed $checked queues, expected $((io_threads + 1))"

    vt_pass "affinity: $io_threads threads node-pure over $nodes nodes, all CPUs covered, list affinity verified"
}
