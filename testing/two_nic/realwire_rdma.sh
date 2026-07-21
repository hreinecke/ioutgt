#!/usr/bin/env bash
#
# two_nic/realwire_rdma.sh — the NVMe/RDMA (RoCEv2) sibling of
# two_nic/realwire_tcp.sh: run an NVMe/RDMA target and initiator on ONE host but
# force the traffic across two real RoCE NICs (real hardware offload), for both
# the in-kernel nvmet-rdma target and ioutgt-nvme-rdma, so the two can be
# compared back to back on the same wire.
#
# TOPOLOGY — asymmetric, unlike the TCP sibling
# ---------------------------------------------
# nvmet-rdma's CM listener is hardcoded to the init (root) netns: the kernel
# does rdma_create_id(&init_net, ...) + inet_pton_with_scope(&init_net, ...)
# (drivers/nvme/target/rdma.c), ignoring the configfs writer's netns. So unlike
# nvmet-tcp (sock_create in the writer's netns), nvmet-rdma can ONLY bind in the
# root netns. We therefore keep the TARGET side (NIC_T, IP_T, and its rdma
# device) in the root netns and isolate only the INITIATOR in its own netns:
#
#   root netns (TARGET)                      NS_I (INITIATOR)
#   ┌───────────────────────────┐    ┌────────────────────────────┐
#   │ NIC_T  IP_T  + ibdev_T     │    │ NIC_I  IP_I  + ibdev_I      │
#   │ nvmet-rdma :24420          │    │ nvme connect -t rdma        │
#   │ ioutgt-nvme-rdma :14420    │    │                             │
#   └─────────────┬─────────────┘    └─────────────┬──────────────┘
#                 │      physical cable/switch       │
#                 └──────────────────────────────────┘
#
# The wire is still forced: with the initiator's IP/device in NS_I (and no veth
# to root), root reaches IP_I only out NIC_T → the physical link → NIC_I, and
# vice-versa. The loopback/HCA shortcut only triggers when both endpoints share
# a netns, so isolating just the initiator is enough. A cross-namespace ping
# (NS_I → IP_T) proves the bytes crossed the wire.
#
# !!! netns-exclusive is GLOBAL !!!
#   `rdma system set netns exclusive` is a host-wide mode (revert with
#   `rdma system set netns shared`). It can only be set while no rdma device
#   sits in a non-default netns or is in use. Set it on a box with no other
#   live RDMA users. 'down' leaves the mode exclusive on purpose.
#
# !!! SAFETY !!!  Moving NIC_I into a namespace removes it from root. Do NOT use
#   the NIC that carries your SSH/management link. NIC_T and NIC_I must be two
#   SEPARATE cards (two ports of one card share a single rdma device, which
#   cannot be split across netns).
#
# Two targets, distinct port/NQN/backend, both reachable on the same target IP:
#   ioutgt : 14420  nqn...:ioutgt   IOUTGT_BACKEND   (ioutgt-nvme-rdma)
#   nvmet  : 24420  nqn...:nvmet    NVMET_BACKEND    (in-kernel nvmet-rdma)
#
# USAGE (one env block, then subcommands; selector verbs take nvmet|ioutgt)
#   export NIC_T=mlx5p1 NIC_I=mlx5p2
#   export IOUTGT_BACKEND=/dev/nvme0n1 NVMET_BACKEND=/dev/nvme1n1
#   sudo -E ./two_nic/realwire_rdma.sh up
#   sudo -E ./two_nic/realwire_rdma.sh start                # both targets
#   sudo -E ./two_nic/realwire_rdma.sh connect ioutgt       # or just one
#   sudo -E ./two_nic/realwire_rdma.sh fio                  # both, back to back
#   sudo -E ./two_nic/realwire_rdma.sh fio_perf             # perf sweep, both
#   sudo -E ./two_nic/realwire_rdma.sh disconnect
#   sudo -E ./two_nic/realwire_rdma.sh stop
#   sudo -E ./two_nic/realwire_rdma.sh down
#
# KNOBS (env vars; see also common.sh)
#   IOUTGT_BACKEND / NVMET_BACKEND   each target's file or block device
#   BACKEND_GB=2        size of an auto-created backing file
#   NR_QUEUES=4         IO queues   (ioutgt --io-threads; connect -i)
#   QUEUE_SIZE=128      IO qdepth    (ioutgt --io-queue-size; connect -q)
#   IP_T/IP_I/PREFIX/MTU             addressing (jumbo MTU 9000 by default)
#   FIO_RW/BS/QD/JOBS/SECS           fio / fio_perf parameters
set -euo pipefail

# RDMA fabric: selects ioutgt-nvme-rdma, nvmet-rdma, `nvme -t rdma`, and (in
# common.sh) forces digests + send-zc off. Must be set before common.sh.
export TRANSPORT=rdma

# ---- config (override via environment) -------------------------------
NS_I="${NS_I:-nvmei}"           # initiator network namespace (target stays in root)
IP_T="${IP_T:-192.168.50.1}"    # target IP (on NIC_T, in the root netns)
IP_I="${IP_I:-192.168.50.2}"    # initiator IP (on NIC_I, inside NS_I)
PREFIX="${PREFIX:-24}"
# Jumbo by default; RoCE large-IO benefits from fewer, bigger frames. Both NICs
# (cabled back-to-back) must agree. Override MTU=1500 for a link that can't.
MTU="${MTU:-9000}"
# Ports/NQNs/HOSTNQN come from common.sh's shared identity block (ioutgt
# 14420, nvmet 24420, distinct NQNs, so both run at once on one target IP).

# Transport context consumed by common.sh: the target listens on IP_T in the
# root netns; the initiator's nvme-cli runs inside NS_I (so its RDMA-CM resolve
# egresses NIC_I across the wire).
# shellcheck disable=SC2034  # TARGET_IP consumed by common.sh's verbs
TARGET_IP="$IP_T"
ini_exec() { ip netns exec "$NS_I" "$@"; }

. "$(dirname "$0")/../common/common.sh"

# Per-target backend (file or block device); each target has its OWN, so one env
# block drives both. Validated only when its target is started.
NVMET_BACKEND="${NVMET_BACKEND:-}"
IOUTGT_BACKEND="${IOUTGT_BACKEND:-}"

# Whether to touch IRQ affinity after connect. NIC_TUNE=0 skips the
# comp-vector IRQ <-> io-thread convergence (same knob as the TCP driver;
# there is no channel/ntuple/XPS tuning here — RoCE bypasses those).
NIC_TUNE="${NIC_TUNE:-1}"
# shellcheck disable=SC2034  # consumed by common.sh's tune_target_rdma
TUNE_NIC="${NIC_T:-}"
[ "$NIC_TUNE" = 1 ] || TUNE_NIC=""
# A queue's IRQ index is its CQ completion vector (= qid) here, not the
# netdev channel — steers tune_status's lookup (common.sh).
# shellcheck disable=SC2034
TUNE_COMP_VECTOR=1

# ioutgt target-process knobs.
IOUTGT_SOCK="${IOUTGT_SOCK:-/tmp/ioutgt-realwire-rdma.sock}"
IOUTGT_PIDFILE="${IOUTGT_PIDFILE:-/tmp/ioutgt-realwire-rdma.pid}"
IOUTGT_LOG="${IOUTGT_LOG:-/tmp/ioutgt-realwire-rdma.log}"

usage() {
    cat <<EOF
two_nic/realwire_rdma.sh — NVMe/RDMA target + initiator across two real RoCE
NICs on one host. The target (NIC_T + its rdma device) stays in the ROOT netns
(nvmet-rdma can only listen there); only the initiator (NIC_I) is isolated in
its own netns, which still forces hardware-offloaded RoCE over the wire.
Compares ioutgt-nvme-rdma vs the in-kernel nvmet-rdma target.

Targets (same target IP $IP_T, distinct port/NQN/backend):
  ioutgt   :$IOUTGT_PORT   $IOUTGT_NQN   (IOUTGT_BACKEND)
  nvmet    :$NVMET_PORT   $NVMET_NQN   (NVMET_BACKEND)

Usage: $0 <subcommand> [nvmet|ioutgt]
       (selector verbs act on BOTH targets when the selector is omitted)

  up                            rdma-exclusive; address NIC_T in root, isolate
                                NIC_I (+ its rdma dev) in $NS_I; prove wire
  down                          remove $NS_I (returns NIC_I + its rdma dev to root)
  start         [nvmet|ioutgt]  start the target(s) (nvmet = in-kernel)
  stop          [nvmet|ioutgt]  stop the target(s)
  discover      [nvmet|ioutgt]  nvme discover -t rdma
  connect       [nvmet|ioutgt]  nvme connect -t rdma; wait for the namespace
  disconnect    [nvmet|ioutgt]  nvme disconnect
  fio           [nvmet|ioutgt]  fio on the connected device(s)
  fio_verify    [nvmet|ioutgt]  data-integrity gate: mixed-size (4k-128k) writes
                                + crc32c read-back verify (FIO_VERIFY_MB/job)
  ibperf                        RDMA link baseline over the wire (perftest
                                ib_send/write/read_bw); needs only 'up' — run
                                before 'fio_perf', like the TCP driver's iperf
  fio_perf      [nvmet|ioutgt]  perf sweep: randread/randwrite x bs={4k,64k}
  status                        netns, rdma links, addresses, connected devices
  stat          [args...]       ioutgt per-queue counters (passthrough to the
                                binary's stat client; e.g. 'stat --clear',
                                'stat -i 1'). WR + batch-histogram rows
                                show submission (wr/doorbell) + completion
                                (cqe/poll) batching
  help                          this message

Required env: NIC_T, NIC_I (two SEPARATE RoCE cards cabled back-to-back) and the
started target's backend (IOUTGT_BACKEND / NVMET_BACKEND, a file or bdev).
Knobs: BACKEND_GB=$BACKEND_GB NR_QUEUES=$NR_QUEUES QUEUE_SIZE=$QUEUE_SIZE
  IP_T=$IP_T IP_I=$IP_I PREFIX=$PREFIX MTU=$MTU  FIO_RW/BS/QD/JOBS/SECS
  NIC_TUNE=$NIC_TUNE (0 = skip the post-connect comp-vector IRQ affinity sync)

Example:
  export NIC_T=mlx5p1 NIC_I=mlx5p2 IOUTGT_BACKEND=/dev/nvme0n1 NVMET_BACKEND=/dev/nvme1n1
  sudo -E $0 up && sudo -E $0 start
  sudo -E $0 connect && sudo -E $0 fio_perf && sudo -E $0 disconnect
  sudo -E $0 stop && sudo -E $0 down
EOF
}

# 'help'/'usage' must work without root or NIC_T/NIC_I, so handle it here.
case "${1:-}" in help|usage|-h|--help) usage; exit 0 ;; esac

[ "$(id -u)" -eq 0 ] || { echo "must run as root (use sudo)"; exit 1; }
command -v rdma >/dev/null 2>&1 || { echo "the 'rdma' tool is required (install iproute2/rdma-core)"; exit 1; }

# Target context for common.sh's nvmet_setup/ioutgt_start: BOTH targets run in
# the ROOT netns (nvmet-rdma's listener is pinned to init_net; ioutgt-nvme-rdma
# binds IP_T on NIC_T's rdma device, which also stays in root). So no netns
# wrapper for either — a plain subshell and an empty ioutgt launch prefix.
nvmet_exec() { bash -c "$1"; }
# shellcheck disable=SC2034  # consumed by common.sh's ioutgt_start
IOUTGT_NETNS=()

# =====================================================================
# The whole one-host topology (target stays in root, initiator isolated,
# NM/policy-routing defenses, GID seating, wire proof) is the shared
# realwire_rdma_up/_down in common/rdma_wire.sh.
cmd_up()   { realwire_rdma_up; }
cmd_down() { realwire_rdma_down; }

# 'start'/'stop [SELECTOR]' route to common.sh's shared start_one/stop_one.

cmd_status() {
    echo "== rdma system =="; rdma system show 2>/dev/null || true
    echo "== root rdma link (target) =="; rdma link show 2>/dev/null || echo "(none)"
    echo "== $NS_I rdma link (initiator) =="; ip netns exec "$NS_I" rdma link show 2>/dev/null || echo "(none)"
    echo "== root addr (target) =="; ip -br addr 2>/dev/null | grep -E "${NIC_T:-NoSuchNic}" || echo "($NIC_T not in root?)"
    echo "== $NS_I addr (initiator) =="; ip netns exec "$NS_I" ip -br addr 2>/dev/null || true
    echo "== listeners (root :$IOUTGT_PORT/:$NVMET_PORT) =="; ss -ltn 2>/dev/null | grep -E ":$IOUTGT_PORT|:$NVMET_PORT" || echo "(RDMA listeners are not TCP sockets; check rdma resource)"
    echo "== connected devices =="
    echo "  ioutgt ($IOUTGT_NQN): $(find_dev "$IOUTGT_NQN" || echo none)"
    echo "  nvmet ($NVMET_NQN): $(find_dev "$NVMET_NQN" || echo none)"
    tune_status
}

# The RoCEv2 GID index of `ip` on `dev`'s port 1, looked up through `runner`
# (`run_root` or `ini_exec`, so it works for the netns'd initiator device).
# perftest needs the explicit index (-x): its default picks the link-local v1
# GID, which cannot cross an L3-addressed RoCEv2 link.
run_root() { "$@"; }
roce_v2_gid_index() {
    local runner="$1" dev="$2" ip="$3" i g t want
    # shellcheck disable=SC2046  # word-splitting the octets is the point
    want=$(printf '0000:0000:0000:0000:0000:ffff:%02x%02x:%02x%02x' $(echo "$ip" | tr '.' ' '))
    for i in $(seq 0 15); do
        g=$("$runner" cat "/sys/class/infiniband/$dev/ports/1/gids/$i" 2>/dev/null) || continue
        t=$("$runner" cat "/sys/class/infiniband/$dev/ports/1/gid_attrs/types/$i" 2>/dev/null) || continue
        [ "$g" = "$want" ] && [ "$t" = "RoCE v2" ] && { echo "$i"; return 0; }
    done
    return 1
}

# RDMA link baseline over the physical wire: perftest ib_{send,write,read}_bw
# between the two ports (server on NIC_T in root, client on NIC_I in $NS_I) —
# the same verbs the NVMe data path uses (SEND for capsules/responses, RDMA
# WRITE for read data, RDMA READ for write data). The rdma-link sibling of the
# TCP driver's 'iperf'. Note: unlike the NVMe targets (kernel cm_ids are
# init_net-pinned, so a same-box NVMe session self-loopbacks on one port),
# perftest is netns-exec'd userspace — its client really runs on the isolated
# device, so this is the one RDMA test on this rig whose traffic provably
# crosses the cable. Needs only 'up' (no start/connect); run it before
# 'fio_perf' for a transport baseline.
# Knobs: IBPERF_SECS=5 IBPERF_SIZE=65536 IBPERF_QPS=1 IBPERF_PORT=18515.
cmd_ibperf() {
    require_nics
    for b in ib_send_bw ib_write_bw ib_read_bw; do
        command -v "$b" >/dev/null 2>&1 || { echo "$b not found (install perftest)"; exit 1; }
    done
    local secs="${IBPERF_SECS:-5}" size="${IBPERF_SIZE:-65536}" qps="${IBPERF_QPS:-1}" port="${IBPERF_PORT:-18515}"
    local ibt ibi xt xi
    ibt="$(nic_ibdev "$NIC_T")" || fail "no rdma device for $NIC_T — run 'up' first"
    ibi="$(ini_exec bash -c "basename /sys/class/net/$NIC_I/device/infiniband/*" 2>/dev/null)"
    [ -n "$ibi" ] && [ "$ibi" != "*" ] || fail "no rdma device for $NIC_I in $NS_I — run 'up' first"
    xt="$(roce_v2_gid_index run_root "$ibt" "$IP_T")" || fail "no RoCEv2 GID for $IP_T on $ibt"
    xi="$(roce_v2_gid_index ini_exec "$ibi" "$IP_I")" || fail "no RoCEv2 GID for $IP_I on $ibi in $NS_I"
    echo ">> perftest over the $NIC_I($ibi,gid$xi) -> $NIC_T($ibt,gid$xt) wire (size=$size qps=$qps ${secs}s/verb)"
    # perftest's out-of-band TCP handshake lands on the root netns, where
    # firewalld rejects it ("no route to host") — punch the port for this run
    # only (runtime rule, removed by the RETURN trap). The TCP driver's iperf
    # never hits this: its server lives inside a netns, outside firewalld.
    local fw=0
    if command -v firewall-cmd >/dev/null 2>&1 && firewall-cmd --state >/dev/null 2>&1; then
        firewall-cmd -q --add-port="$port-$((port + 2))/tcp" && fw=1
    fi
    local bw spid=""
    # Single-quoted on purpose: kill the current server + drop the firewall
    # rule however the function exits.
    trap 'kill "$spid" 2>/dev/null || true
          if [ "$fw" = 1 ]; then firewall-cmd -q --remove-port="$port-$((port + 2))/tcp" || true; fi' RETURN
    local i=0 p
    for bw in ib_send_bw ib_write_bw ib_read_bw; do
        # A distinct port per verb: back-to-back runs on one port trip over the
        # previous server's TIME_WAIT/late exit, and a stale server from an
        # aborted earlier run would hijack the handshake — evict those too.
        p=$((port + i)); i=$((i + 1))
        fuser -k -s "$p/tcp" 2>/dev/null || true
        case "$bw" in
            ib_send_bw)  echo "== $bw   (SEND: capsule/response path, client->server)" ;;
            ib_write_bw) echo "== $bw   (RDMA WRITE: read-data path, client->server)" ;;
            ib_read_bw)  echo "== $bw   (RDMA READ: write-data path, server->client)" ;;
        esac
        # exec in the backgrounded subshell so $! is the server itself; the
        # server serves this one client and exits.
        { exec "$bw" -d "$ibt" -x "$xt" -p "$p" -s "$size" -q "$qps" -D "$secs" \
              --report_gbits >/dev/null 2>&1; } &
        spid=$!
        sleep 0.5
        # Print just the results table, minus perftest's "BW peak" column —
        # peak is only measured for short iteration-mode runs (<= 20000 iters,
        # never under -D) and would read a misleading 0.00 here.
        local out
        if out="$(ini_exec "$bw" -d "$ibi" -x "$xi" -p "$p" -s "$size" -q "$qps" -D "$secs" \
            --report_gbits "$IP_T" 2>&1)"; then
            echo "$out" | awk '
                /#bytes/ { printf " %-11s%-15s%-21s%s\n", "#bytes", "#iterations", "BW average[Gb/sec]", "MsgRate[Mpps]"; next }
                /^[[:space:]]*[0-9]+[[:space:]]+[0-9]+[[:space:]]+[0-9.]+[[:space:]]+[0-9.]+[[:space:]]+[0-9.]+[[:space:]]*$/ { printf " %-11s%-15s%-21s%s\n", $1, $2, $4, $5 }'
        else
            echo "   ($bw client failed)"
        fi
        wait "$spid" 2>/dev/null || true
    done
}

# IRQ affinity sync needs the IO queues connected (their pthread tids
# appear in `list`).
post_connect_tune() {
    case "$1" in ioutgt|"") tune_target_rdma ;; esac
    # Initiator CQ vectors are per connected controller; both connections
    # share hctx maps, so idempotent.
    case "$1" in
        ioutgt) tune_initiator_rdma "$IOUTGT_NQN" ;;
        nvmet)  tune_initiator_rdma "$NVMET_NQN" ;;
        "")     tune_initiator_rdma "$IOUTGT_NQN"
                tune_initiator_rdma "$NVMET_NQN" ;;
    esac
}
# Handled verbs exit/exec with their command's status (dispatch contract).
extra_verbs() {
    case "$1" in
        ibperf) cmd_ibperf; exit ;;
        stat)   shift; exec "$IOUTGT_BIN" stat --socket "$IOUTGT_SOCK" "$@" ;;
        *)      return 1 ;;
    esac
}

realwire_dispatch "$@"
