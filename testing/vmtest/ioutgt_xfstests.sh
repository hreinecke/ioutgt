#!/bin/bash
# vmtest-desc: ioutgt NVMe/TCP + NVMe/RDMA xfstests (./check -g quick), file-backed XFS
# vmtest-requires: root
#
# One self-dispatching script that stands up BOTH ioutgt transports inside a
# single vmtest guest and runs the xfstests quick group across them:
#
#   * nvme-rdma namespace -> TEST_DEV    (formatted XFS, mounted at TEST_DIR)
#   * nvme-tcp  namespace -> SCRATCH_DEV  (reformatted per-test by xfstests)
#
# Both namespaces are backed by plain files in the guest's writable data dir
# (the vmtest --rwdir 9p mount, $VMTEST_DATA_DIR).
#
# Modes:
#   HOST  (run with no args): builds the ioutgt-nvme-tcp + ioutgt-nvme-rdma
#         binaries, copies this script into the vmtest tests dir, and launches
#         the guest. vmtest re-invokes it inside the guest as:
#             ioutgt_xfstests --guest <tcp-bin> <rdma-bin> [check args...]
#   GUEST (--guest <tcp-bin> <rdma-bin> [check args...]): does the real work.
#
# Any arguments are passed through verbatim to xfstests `./check` (default
# `-g quick` when none are given), e.g.:
#   testing/vmtest/ioutgt_xfstests.sh                 # ./check -g quick
#   testing/vmtest/ioutgt_xfstests.sh -g auto         # a different group
#   testing/vmtest/ioutgt_xfstests.sh generic/013 generic/020
#   testing/vmtest/ioutgt_xfstests.sh -x dio -g quick # exclude a group
#
# Host knobs (env): VMTEST, VMTEST_CONF (default ~/git/linux-ioutgt/vmtest.conf),
#   IOUTGT_PROFILE (release|debug, default release),
#   IOUTGT_XFSTESTS_TIMEOUT (outer VM wall-clock cap, default 90m).
# Guest knobs (env): IMG_SIZE (default 8G), RUST_LOG (default info).
# Artifacts: xfstests results/ (check.log, per-test .full/.out.bad) is copied
# to $VMTEST_DATA_DIR/tmp/xfstests-results, and the two target logs live at
# $VMTEST_DATA_DIR/tmp/ioutgt-xfstests-{tcp,rdma}.log — all survive VM
# shutdown, including a wedge killed from outside by the run timeout.

set -u

# --------------------------------------------------------------------------
# HOST mode: build, publish this script, launch the guest via vmtest.
# --------------------------------------------------------------------------
if [ "${1:-}" != "--guest" ]; then
	set -euo pipefail
	# Everything on the host command line is forwarded to `./check` (default
	# -g quick). Captured before we touch the positional params below.
	CHECK_ARGS=("$@")
	[ ${#CHECK_ARGS[@]} -eq 0 ] && CHECK_ARGS=(-g quick)
	TOP="$(cd "$(dirname "$0")/../.." && pwd)"
	cd "$TOP"
	VMTEST="${VMTEST:-$HOME/git/utils/vmtest/vmtest}"
	VMTEST_CONF="${VMTEST_CONF:-$HOME/git/linux-ioutgt/vmtest.conf}"
	PROFILE="${IOUTGT_PROFILE:-release}"
	PROFILE_FLAG=""
	[ "$PROFILE" = release ] && PROFILE_FLAG="--release"

	echo "[host] building targets ($PROFILE)"
	cargo build $PROFILE_FLAG -p ioutgt --bin ioutgt-nvme-tcp
	cargo build $PROFILE_FLAG -p ioutgt-nvme-rdma --bin ioutgt-nvme-rdma
	TCP_BIN="$TOP/target/$PROFILE/ioutgt-nvme-tcp"
	RDMA_BIN="$TOP/target/$PROFILE/ioutgt-nvme-rdma"
	for b in "$TCP_BIN" "$RDMA_BIN"; do
		[ -x "$b" ] || { echo "[host] FAIL: missing binary $b"; exit 1; }
	done

	# vmtest runs tests/NAME.sh; publish this entrypoint under that name. The
	# guest sees the built binaries at their host absolute paths via 9p.
	cp "$TOP/testing/vmtest/ioutgt_xfstests.sh" \
		"$(dirname "$VMTEST")/tests/ioutgt_xfstests.sh"
	# Hard wall-clock cap on the whole VM: if a target wedges (e.g. an RDMA
	# error-recovery hang leaves a test in uninterruptible sleep), ./check
	# inside the guest can never be killed, so bound it from OUTSIDE by killing
	# qemu. Without this the run hangs forever.
	RUN_TIMEOUT="${IOUTGT_XFSTESTS_TIMEOUT:-90m}"
	exec timeout --kill-after=30s "$RUN_TIMEOUT" \
		"$VMTEST" -c "$VMTEST_CONF" run ioutgt_xfstests \
		--guest "$TCP_BIN" "$RDMA_BIN" "${CHECK_ARGS[@]}"
fi

# --------------------------------------------------------------------------
# GUEST mode.
# --------------------------------------------------------------------------
shift # drop --guest
TCP_BIN="${1:?guest: need tcp binary path as \$1}"
RDMA_BIN="${2:?guest: need rdma binary path as \$2}"
shift 2
# Remaining args go straight to `./check` (default -g quick).
CHECK_ARGS=("$@")
[ ${#CHECK_ARGS[@]} -eq 0 ] && CHECK_ARGS=(-g quick)

DATA_DIR="${VMTEST_DATA_DIR:?VMTEST_DATA_DIR unset (need writable dir for images)}"
IMG_SIZE="${IMG_SIZE:-8G}"
TCP_IMG="$DATA_DIR/xfstests-tcp.img"
RDMA_IMG="$DATA_DIR/xfstests-rdma.img"
TCP_NQN="nqn.2026-06.io.ioutgt:xfstests-tcp"
RDMA_NQN="nqn.2026-06.io.ioutgt:xfstests-rdma"
TCP_ADDR="127.0.0.1"
TCP_PORT=4420
RDMA_PORT=4421
XFSTESTS_DIR="/var/lib/xfstests"
# Mount points must be on a writable fs. Under vmtest/virtme the guest root is
# a read-only 9p share; only a fixed overlay set (/etc /var /tmp …) is
# writable, and /mnt is NOT among them — so put the mount points under /tmp.
TEST_MNT=/tmp/ioutgt-xfstests/test
SCRATCH_MNT=/tmp/ioutgt-xfstests/scratch
# Target logs go to the 9p data dir, not guest /tmp: if the run wedges and the
# VM is killed from outside (the outer timeout), the guest cleanup never runs
# and a tmpfs log would vanish — but these survive for postmortem.
TCP_LOG="$DATA_DIR/tmp/ioutgt-xfstests-tcp.log"
RDMA_LOG="$DATA_DIR/tmp/ioutgt-xfstests-rdma.log"
RUST_LOG="${RUST_LOG:-info}"

log()  { echo "[xfstests] $*"; }
# Persist the verdict through the 9p data dir: guest console can be lossy, so
# the host runner can assert on tmp/ioutgt_result (same convention as the
# other ioutgt vmtest scripts).
mark() {
	[ -n "${VMTEST_DATA_DIR:-}" ] &&
		mkdir -p "$VMTEST_DATA_DIR/tmp" &&
		echo "$*" >>"$VMTEST_DATA_DIR/tmp/ioutgt_result" || true
}
fail() { log "RESULT: FAIL ($*)"; mark "FAIL $*"; exit 1; }

# Fresh verdict file per run: mark() only appends, so without this a stale
# PASS/FAIL from a previous run would sit next to this run's verdict.
mkdir -p "$DATA_DIR/tmp" && : >"$DATA_DIR/tmp/ioutgt_result" || true

[ "$(id -u)" = 0 ]           || fail "must run as root"
[ -x "$TCP_BIN" ]            || fail "tcp binary not executable: $TCP_BIN"
[ -x "$RDMA_BIN" ]           || fail "rdma binary not executable: $RDMA_BIN"
[ -x "$XFSTESTS_DIR/check" ] || fail "no xfstests ./check under $XFSTESTS_DIR"
# Only kernel-backed tools are mandatory. ibv_devinfo/show_gids (rdma-core /
# libibverbs-utils) are optional diagnostics — the RoCE datapath is the
# rdma_rxe module + in-kernel nvme-rdma host, so RDMA works without them.
for c in nvme mkfs.xfs rdma truncate; do
	command -v "$c" >/dev/null || fail "missing required command: $c"
done

log "loading modules"
for m in nvme_tcp nvme_rdma rdma_rxe; do modprobe "$m" 2>&1 || true; done

log "creating backing images ($IMG_SIZE) under $DATA_DIR"
rm -f "$TCP_IMG" "$RDMA_IMG"
truncate -s "$IMG_SIZE" "$TCP_IMG"  || fail "truncate $TCP_IMG"
truncate -s "$IMG_SIZE" "$RDMA_IMG" || fail "truncate $RDMA_IMG"

# --- soft-RoCE (rxe) bring-up on the guest NIC ---------------------------
# RoCEv2 needs an IP'd Ethernet netdev for a usable GID. rxe populates its GID
# table via async work off the inetaddr notifier: an IP that PRE-dates the rxe
# link races and can silently never produce a CM-usable GID (rdma_bind_addr
# then returns ENODEV — observed intermittently on the boot DHCP address, and
# a sysfs GID entry being present does NOT mean bind will work). An IP added
# AFTER the link exists takes the notifier path cleanly. So instead of fighting
# the DHCP IP, we add a dedicated address post-link and bind RDMA to that —
# deterministic, no re-add heuristics. It's a self-loopback address (target and
# in-kernel host are both in this guest), so the subnet is arbitrary.
RXE_LOCAL_CIDR="${RXE_LOCAL_CIDR:-192.168.234.1/24}"
setup_rxe() {
	local dev
	dev=$(ip -o -4 addr show up scope global 2>/dev/null | awk '{print $2; exit}')
	[ -z "${dev:-}" ] &&
		dev=$(ip -o link show up 2>/dev/null | awk -F': ' '$2!="lo"{print $2; exit}')
	[ -n "${dev:-}" ] || fail "no usable netdev for rxe"
	RXE_IP=${RXE_LOCAL_CIDR%%/*}
	log "rxe on netdev=$dev, dedicated rdma ip=$RXE_IP"
	ip link set "$dev" up 2>/dev/null || true
	rdma link add rxe0 type rxe netdev "$dev" 2>&1 || log "rdma link add note: rc=$?"
	local _
	for _ in $(seq 1 20); do
		rdma link show rxe0 2>/dev/null | grep -qi "state ACTIVE" && break
		sleep 0.5
	done
	# Add the dedicated IP now that the link exists → its GID syncs via the
	# normal notifier path (no pre-existing-IP race).
	ip addr add "$RXE_LOCAL_CIDR" dev "$dev" 2>/dev/null || true
	# RoCEv2 GID for this IPv4 is the v4-mapped form ...:ffff:<ip-in-hex>.
	gid_ready() {
		local o1 o2 o3 o4 tail
		IFS=. read -r o1 o2 o3 o4 <<<"$RXE_IP"
		tail=$(printf 'ffff:%02x%02x:%02x%02x' "$o1" "$o2" "$o3" "$o4")
		grep -qi "$tail" /sys/class/infiniband/rxe0/ports/*/gids/* 2>/dev/null
	}
	for _ in $(seq 1 40); do gid_ready && break; sleep 0.5; done
	gid_ready || log "WARNING: RoCEv2 GID for $RXE_IP still absent; rdma bind may fail"
	log "rxe setup done (gid $(gid_ready && echo ok || echo MISSING))"
	command -v ibv_devinfo >/dev/null &&
		timeout 10 ibv_devinfo 2>&1 | grep -E "hca_id|state:|link_layer" | head -6
	command -v show_gids >/dev/null && show_gids 2>/dev/null | grep -w "$RXE_IP"
}
setup_rxe

# --- start both targets --------------------------------------------------
# Distinct NQNs give the two namespaces distinct UUIDs (ioutgt derives the
# namespace UUID from subsystem-NQN + nsid), so the NVMe host — which dedups
# namespaces by UUID across the whole host — keeps both as separate devices.
log "starting NVMe/TCP  target -> $TCP_ADDR:$TCP_PORT (backend $TCP_IMG)"
RUST_LOG="$RUST_LOG" RUST_BACKTRACE=1 "$TCP_BIN" \
	--listen "$TCP_ADDR:$TCP_PORT" --subsys-nqn "$TCP_NQN" \
	--backend "$TCP_IMG" >"$TCP_LOG" 2>&1 &
TCP_PID=$!
log "starting NVMe/RDMA target -> $RXE_IP:$RDMA_PORT (backend $RDMA_IMG)"
RUST_LOG="$RUST_LOG" RUST_BACKTRACE=1 "$RDMA_BIN" \
	--listen "$RXE_IP:$RDMA_PORT" --subsys-nqn "$RDMA_NQN" \
	--backend "$RDMA_IMG" >"$RDMA_LOG" 2>&1 &
RDMA_PID=$!
# sed -u: line-buffered even with stdout on a pipe — otherwise target log
# lines sit in sed's block buffer and never reach the console live.
tail -f "$TCP_LOG"  | sed -u 's/^/[tcp] /'  & TCP_TAIL=$!
tail -f "$RDMA_LOG" | sed -u 's/^/[rdma] /' & RDMA_TAIL=$!

cleanup() {
	set +e
	umount "$TEST_MNT"    2>/dev/null
	umount "$SCRATCH_MNT" 2>/dev/null
	nvme disconnect -n "$TCP_NQN"  >/dev/null 2>&1
	nvme disconnect -n "$RDMA_NQN" >/dev/null 2>&1
	kill "$TCP_PID" "$RDMA_PID" "$TCP_TAIL" "$RDMA_TAIL" 2>/dev/null
	rdma link del rxe0 2>/dev/null
	log "--- tcp target log tail ---";  tail -n 30 "$TCP_LOG"  2>/dev/null
	log "--- rdma target log tail ---"; tail -n 30 "$RDMA_LOG" 2>/dev/null
	rm -f "$TCP_IMG" "$RDMA_IMG"
}
trap cleanup EXIT

wait_listen() { # $1=log $2=pid $3=name
	local _
	for _ in $(seq 1 100); do
		grep -q "listening on" "$1" && return 0
		kill -0 "$2" 2>/dev/null || { log "$3 target died early:"; cat "$1"; return 1; }
		sleep 0.2
	done
	return 1
}
wait_listen "$TCP_LOG"  "$TCP_PID"  tcp  || fail "tcp target never listened"
wait_listen "$RDMA_LOG" "$RDMA_PID" rdma || fail "rdma target never listened"

# --- connect the in-kernel hosts, resolve each namespace by device diff ---
# The guest already has an emulated PCI NVMe (/dev/nvme0n1); connecting one
# transport at a time and diffing /dev/nvme*n* pins the *new* node without
# guessing controller<->head instance numbers under native multipath.
connect_diff() { # $1=transport $2=addr $3=port $4=nqn -> prints new /dev node
	local before after ns i
	before=$(ls /dev/nvme*n* 2>/dev/null | sort)
	nvme connect -t "$1" -a "$2" -s "$3" -n "$4" --nr-io-queues=4 >/dev/null 2>&1 || return 1
	# The namespace gendisk registers asynchronously after connect returns,
	# so poll the diff rather than sampling it once (otherwise two transports
	# race and one node is missed).
	for i in $(seq 1 50); do
		udevadm settle 2>/dev/null || true
		after=$(ls /dev/nvme*n* 2>/dev/null | sort)
		ns=$(comm -13 <(echo "$before") <(echo "$after") | head -1)
		[ -n "$ns" ] && [ -b "$ns" ] && { echo "$ns"; return 0; }
		sleep 0.2
	done
	return 1
}
RDMA_DEV=$(connect_diff rdma "$RXE_IP"   "$RDMA_PORT" "$RDMA_NQN") || fail "rdma connect/resolve"
TCP_DEV=$( connect_diff tcp  "$TCP_ADDR" "$TCP_PORT"  "$TCP_NQN")  || fail "tcp connect/resolve"
log "TEST_DEV(rdma)=$RDMA_DEV  SCRATCH_DEV(tcp)=$TCP_DEV"

# --- format the RDMA namespace XFS as TEST_DEV, wire up xfstests ----------
mkdir -p "$TEST_MNT" "$SCRATCH_MNT"
log "mkfs.xfs TEST_DEV $RDMA_DEV"
mkfs.xfs -f "$RDMA_DEV" >/dev/null || fail "mkfs.xfs on TEST_DEV"
mount "$RDMA_DEV" "$TEST_MNT"      || fail "mount TEST_DEV"

# xfstests wants an unprivileged user/group for a handful of tests; best
# effort so those don't spuriously fail (the rest still run without it).
getent group fsgqa >/dev/null 2>&1 || groupadd fsgqa 2>/dev/null || true
id fsgqa >/dev/null 2>&1 || useradd -g fsgqa fsgqa 2>/dev/null || true

cat >"$XFSTESTS_DIR/local.config" <<EOF
export FSTYP=xfs
export TEST_DEV=$RDMA_DEV
export TEST_DIR=$TEST_MNT
export SCRATCH_DEV=$TCP_DEV
export SCRATCH_MNT=$SCRATCH_MNT
EOF
log "local.config:"; sed 's/^/[xfstests]   /' "$XFSTESTS_DIR/local.config"

log "=== running xfstests ./check ${CHECK_ARGS[*]} ==="
(cd "$XFSTESTS_DIR" && ./check "${CHECK_ARGS[@]}")
rc=$?

# Preserve the xfstests artifacts (results/: check.log, per-test .full and
# .out.bad) through the 9p data dir — they live in the guest's /var overlay,
# which vanishes at shutdown, and on a failure they are the evidence.
if [ -d "$XFSTESTS_DIR/results" ]; then
	rm -rf "$DATA_DIR/tmp/xfstests-results"
	if cp -r "$XFSTESTS_DIR/results" "$DATA_DIR/tmp/xfstests-results" 2>/dev/null; then
		log "results preserved at \$VMTEST_DATA_DIR/tmp/xfstests-results"
	else
		log "note: failed to copy results/ to the data dir"
	fi
fi

if [ "$rc" = 0 ]; then
	log "RESULT: PASS"
	mark "PASS xfstests ${CHECK_ARGS[*]}"
else
	log "RESULT: FAIL (./check rc=$rc)"
	mark "FAIL xfstests ${CHECK_ARGS[*]} (rc=$rc)"
fi
exit "$rc"
