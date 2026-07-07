# fio.sh — the fio workload verbs shared by the drivers:
# fio_one (smoke), fio_verify_one (crc32c data-integrity gate),
# fio_perf_one (perf sweep) and their knobs. Sourced by common.sh
# (not a standalone script).

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

