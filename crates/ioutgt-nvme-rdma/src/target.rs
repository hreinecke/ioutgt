//! Focused-v1 NVMe/RDMA target queue: drive the transport-neutral NVMe model
//! ([`ioutgt_core::dispatch`]) over RDMA capsules on one CM-established RC QP.
//!
//! Per command: RECV the command capsule → parse the [`Sqe`] (and, for Connect,
//! the in-capsule [`ConnectData`]) → run it through the slot pipeline and
//! `dispatch::execute` → if it produced read data, RDMA WRITE
//! `slot.data().segs()` to the host's keyed SGL → SEND the response capsule
//! (the [`Cqe`]) → release the slot and re-arm the RECV. Mirrors the
//! `ioutgt-nvme-tcp` `run_queue`, swapping its PDU staging for verbs.
//!
//! Each connection is single-threaded (its queue thread owns the QP/CQ/MR
//! pool); the harness routes connections to queue threads by qid
//! ([`crate::transport`]). Completions are reaped reactor-driven via
//! [`crate::cq`].

use std::cell::{Cell, RefCell};
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;

use ioutgt_backend::AnyBackend;
use ioutgt_core::controller::Registry;
use ioutgt_core::dispatch::{self, ConnCtx, Outcome, Role};
use ioutgt_core::pool::MAX_SEGS;
use ioutgt_core::queue::{QueueCore, TransportStats};
use ioutgt_core::slotq::SendList;
use ioutgt_core::subsystem::PortConfig;
use ioutgt_nvme::fabrics::ConnectData;
use ioutgt_nvme::spec::{Sqe, io_opcode};
use ioutgt_nvme::status;
use rdma_mummy_sys::ibv_sge;
use sideway::ibverbs::AccessFlags;
use sideway::ibverbs::completion::{
    CompletionChannel, GenericCompletionQueue, WorkCompletionStatus,
};
use sideway::ibverbs::device_context::DeviceContext;
use sideway::ibverbs::memory_region::MemoryRegion;
use sideway::ibverbs::protection_domain::ProtectionDomain;
use sideway::ibverbs::queue_pair::{
    GenericQueuePair, PostSendGuard, QueuePair, QueuePairState, SendOperationFlags,
    SetScatterGatherEntry, WorkRequestFlags,
};
use tokio::sync::Notify;
use tokio::task::JoinSet;
use zerocopy::{FromBytes, IntoBytes};

use rdma_mummy_sys::{
    ibv_post_recv, ibv_qp_ex, ibv_qp_to_qp_ex, ibv_recv_wr, ibv_wr_send_inv, ibv_wr_set_sge,
};

use crate::cm::{CmChannel, EventType, Identifier};
use crate::cmproto::{CM_FMT_1_0, CmRej, CmRep, CmReq, reject_status};
use crate::oerr;

/// Bytes of an NVMe SQE.
const SQE_LEN: usize = 64;
/// Max in-capsule data we accept — one page, matching what IOCCSZ advertises
/// (`ioutgt_core::RDMA_INLINE_DATA_SIZE`, nvmet-rdma's default): write
/// payloads up to this arrive inside the command capsule (no RDMA READ). The
/// fabrics Connect data (1024 B) rides the same allowance.
const ICD_LEN: usize = ioutgt_core::RDMA_INLINE_DATA_SIZE as usize;
/// RECV capsule buffer: SQE + max in-capsule data.
const CAPSULE_LEN: usize = SQE_LEN + ICD_LEN;
/// Bytes of an NVMe CQE (the response capsule).
const CQE_LEN: usize = 16;

// Work-request id encoding: high byte = kind, low 32 bits = slot tag / recv idx.
const WR_RECV: u64 = 1 << 40;
const WR_SEND: u64 = 2 << 40;
const WR_WRITE: u64 = 3 << 40;
const WR_READ: u64 = 4 << 40;
const WR_KIND_MASK: u64 = 0xff << 40;

/// Reap-loop backstop interval: how long the queue may sit on the comp-channel
/// event before it defensively re-arms + re-drains the CQ (recovering a rarely
/// stranded completion). Since the reactor park-probe took over the arming
/// policy (arm + race-drain at the sleep point), this is a pure safety net
/// against event-delivery bugs below us (rxe has priors; userspace has no
/// IB_CQ_REPORT_MISSED_EVENTS to detect a miss) — 1 s keeps it well under the
/// keep-alive timeout so a stranded completion can never starve the controller
/// into a host-side reset, and it also paces the KATO watchdog (every 2nd
/// tick ≈ 2 s).
const BACKSTOP: std::time::Duration = std::time::Duration::from_secs(1);

/// Poll-mode spin-down hysteresis: after the queue drains idle, keep spinning
/// this long before falling back to the event-driven sleep. Covers the
/// inter-command gaps of low-depth workloads (qd1 at ~20k IOPS has ~30 us
/// gaps — sleeping in them would re-pay the event wake on every command and
/// forfeit most of the poll latency win), while a genuinely idle queue stops
/// burning its core within this window.
const SPIN_GRACE: std::time::Duration = std::time::Duration::from_micros(200);

/// SGL descriptor type byte (dptr offset 15). High nibble `0x4` =
/// `NVME_KEY_SGL_FMT_DATA_DESC` (keyed: host-resident, RDMA READ/WRITE); anything
/// else here is an in-capsule data+offset descriptor (inline).
const SGL_TYPE_OFFSET: usize = 24 + 15;
const KEYED_SGL_TYPE_HI: u8 = 0x4;
/// SGL Data Block descriptor with offset addressing (type nibble 0x0, subtype
/// 0x1): the payload is in the capsule at `addr` bytes past ICDOFF (we set
/// ICDOFF = 0). This is what nvme-rdma hosts send for writes that fit the
/// advertised in-capsule size.
const INLINE_SGL_TYPE: u8 = 0x01;

/// The in-capsule data descriptor: `addr` is the offset into the in-capsule
/// region, `len` the payload length (SGL Data Block: addr 8B, len 4B).
fn parse_inline_sgl(sqe: &Sqe) -> (u64, usize) {
    let b = sqe.as_bytes();
    let off = u64::from_le_bytes(b[24..32].try_into().expect("8 bytes"));
    let len = u32::from_le_bytes(b[32..36].try_into().expect("4 bytes"));
    (off, len as usize)
}
/// SGL descriptor sub-type (type byte low nibble) `0xf` = `NVME_SGL_FMT_INVALIDATE`:
/// the host fast-registered an MR for this transfer and wants the target to invalidate
/// its rkey remotely in the response (`nvme_rdma_map_sg_fr`). Honoring it via
/// `IBV_WR_SEND_WITH_INV` spares the host a per-IO local-invalidate WR + completion.
const SGL_SUBTYPE_MASK: u8 = 0x0f;
const SGL_FMT_INVALIDATE: u8 = 0x0f;

/// The host rkey to remotely invalidate in the response SEND, if the command's keyed
/// SGL requested it. Mirrors nvmet's `rsp->invalidate_rkey`.
fn invalidate_rkey_for(cmd: &Sqe) -> Option<u32> {
    let type_byte = cmd.as_bytes()[SGL_TYPE_OFFSET];
    if type_byte >> 4 == KEYED_SGL_TYPE_HI && type_byte & SGL_SUBTYPE_MASK == SGL_FMT_INVALIDATE {
        Some(parse_keyed_sgl(cmd).rkey)
    } else {
        None
    }
}

fn wr(kind: u64, low: u32) -> u64 {
    kind | u64::from(low)
}
fn wr_kind(id: u64) -> u64 {
    id & WR_KIND_MASK
}
fn wr_low(id: u64) -> u32 {
    (id & 0xffff_ffff) as u32
}

/// A host RDMA target region from a command SQE's keyed SGL data block
/// descriptor (NVMe-oF RDMA). Lives in the SQE `dptr` at offset 24:
/// `addr`(le64) `length`(24-bit le) `key`(le32 rkey) `type`.
struct KeyedSgl {
    addr: u64,
    len: u32,
    rkey: u32,
}

/// Whether `opcode` on this queue carries host→controller data the target must
/// pull (RDMA READ) into a pool lease before dispatch. Admin commands in the
/// connect/discovery path carry no host data.
fn host_data_in(role: &Role<AnyBackend>, opcode: u8) -> bool {
    matches!(role, Role::Io(_)) && matches!(opcode, io_opcode::WRITE | io_opcode::DSM)
}

fn parse_keyed_sgl(sqe: &Sqe) -> KeyedSgl {
    let b = sqe.as_bytes();
    let d = &b[24..40];
    let addr = u64::from_le_bytes(d[0..8].try_into().expect("8 bytes"));
    // length is a 24-bit little-endian field at descriptor offset 8.
    let len = u32::from(d[8]) | u32::from(d[9]) << 8 | u32::from(d[10]) << 16;
    let rkey = u32::from_le_bytes(d[11..15].try_into().expect("4 bytes"));
    KeyedSgl { addr, len, rkey }
}

/// The staged transfer length of a validated host-data-in SQE: its keyed-SGL
/// length clamped to MDTS. Recomputed when a pool-deferred command retries.
fn staged_len(sqe: &Sqe) -> usize {
    (parse_keyed_sgl(sqe).len as usize).min(ioutgt_core::MDTS_BYTES as usize)
}

/// Fill `sges` with the slot data's pool-lease segments covering its first
/// `len` bytes, tagged with `lkey`; returns the sge count. A pool lease spans
/// at most [`MAX_SEGS`] runs, so the fixed array always suffices, and the
/// extended post guard copies the list into the WQE at `setup_sge_list`, so
/// one stack array can be reused across a batch.
// A run never exceeds the lease length <= MDTS (128 KiB) < u32::MAX.
#[allow(clippy::cast_possible_truncation)]
fn fill_sges(
    data: &ioutgt_core::pool::SlotData,
    len: usize,
    lkey: u32,
    sges: &mut [ibv_sge; MAX_SEGS],
) -> usize {
    let mut n = 0usize;
    let mut remaining = len;
    for seg in data.segs() {
        if remaining == 0 {
            break;
        }
        let take = remaining.min(seg.len);
        sges[n] = ibv_sge {
            addr: seg.ptr as u64,
            length: take as u32,
            lkey,
        };
        n += 1;
        remaining -= take;
    }
    n
}

/// The extended-verbs handle of `qp`, for work-request calls sideway does not
/// wrap. All target QPs are built with `build_ex` (asserted), so the handle is
/// valid whenever a post-send guard session is open on `qp`.
fn qp_ex_of(qp: &GenericQueuePair) -> io::Result<std::ptr::NonNull<ibv_qp_ex>> {
    debug_assert!(matches!(qp, GenericQueuePair::Extended(_)));
    // SAFETY: the raw qp handle is valid for qp's lifetime; ibv_qp_to_qp_ex is
    // pointer arithmetic recovering the ibv_qp_ex an extended QP embeds.
    std::ptr::NonNull::new(unsafe { ibv_qp_to_qp_ex(qp.qp().as_ptr()) })
        .ok_or_else(|| io::Error::other("not an extended QP"))
}

/// Emit a `SEND_WITH_INV` work request into the extended-QP work-request
/// session the surrounding sideway post guard opened (`ibv_wr_start` ..
/// `ibv_wr_complete`). sideway 0.4.3 has no `setup_send_with_inv`; until the
/// upstream PR lands, this makes the two raw calls the missing method would
/// (same shape, so the call sites survive a switch-back unchanged).
///
/// # Safety
///
/// A post guard must be live on the (extended) QP `qp_ex` belongs to, with the
/// current work request's id/flags already set via `construct_wr` and no
/// opcode issued for it yet; the sge must reference registered memory that
/// stays valid until the send completes.
unsafe fn wr_send_with_inv(
    qp_ex: std::ptr::NonNull<ibv_qp_ex>,
    invalidate_rkey: u32,
    lkey: u32,
    addr: u64,
    len: u32,
) {
    // SAFETY: caller contract above; these are the extended-verbs calls
    // `setup_send_with_inv` + `setup_sge` would make.
    unsafe {
        ibv_wr_send_inv(qp_ex.as_ptr(), invalidate_rkey);
        ibv_wr_set_sge(qp_ex.as_ptr(), lkey, addr, len);
    }
}

/// A finished command's response, handed from a slot task (or the reap loop's
/// validation-failure path) to the reap loop, which owns the QP and posts it.
struct RdmaResp {
    tag: u16,
    cmd: Sqe,
    outcome: Outcome,
}

/// A write command's host-data pull deferred from `handle_recv` so all the
/// RDMA READs of one CQ-poll's RECV batch flush on a single doorbell. The
/// slot's pool lease (the READ's local landing segments) is already set up;
/// this carries the remote side (`rkey`/`addr`) and length.
#[derive(Clone, Copy)]
struct PendingRead {
    tag: u16,
    len: u32,
    rkey: u32,
    addr: u64,
}

/// Per-queue RDMA work-request counters, one class each for READ (host
/// write-data pulls), WRITE (read-data pushes), SEND (CQE capsules), and RECV
/// (command capsules). `posted`/`done` are cumulative (reset by GET_STATS
/// `clear`); `inflight` is a live gauge (posted−done), never reset, so it stays
/// accurate across a clear. Reported under `"wr"` in GET_STATS via
/// [`TransportStats`]. All access is on the owning queue thread (`Cell`).
#[derive(Debug, Default)]
struct WrClass {
    posted: Cell<u64>,
    done: Cell<u64>,
    inflight: Cell<i64>,
}

impl WrClass {
    /// Count `n` posted WRs: bump the cumulative `posted` and the live gauge.
    #[inline]
    fn post_n(&self, n: u64) {
        self.posted.set(self.posted.get() + n);
        self.inflight.set(self.inflight.get() + n as i64);
    }

    #[inline]
    fn post(&self) {
        self.post_n(1);
    }

    /// Count a completed WR: bump cumulative `done`, drop the live gauge.
    #[inline]
    fn complete(&self) {
        self.done.set(self.done.get() + 1);
        self.inflight.set(self.inflight.get() - 1);
    }
}

/// Log2-bucketed batch-size histogram — buckets for 1, 2, 3-4, 5-8, 9-16 and
/// 17+ items — cheap enough for the hot path (one branch + one Cell bump).
/// Exposed through GET_STATS so `stat` can show the *distribution* of
/// submission and completion batch sizes, not just their averages.
#[derive(Debug, Default)]
struct BatchHist([Cell<u64>; 6]);

impl BatchHist {
    #[inline]
    fn record(&self, n: usize) {
        let idx = match n {
            0 => return,
            1 => 0,
            2 => 1,
            3..=4 => 2,
            5..=8 => 3,
            9..=16 => 4,
            _ => 5,
        };
        self.0[idx].set(self.0[idx].get() + 1);
    }
}

/// State shared between the reap loop and its reactor park-probe: completions
/// the probe drained while the loop was between wakes, and the waker that
/// tells the loop they exist. Single-threaded (`Rc` + `RefCell`).
#[derive(Default)]
struct ProbeShared {
    /// `(wr_id, success)` pairs the probe pulled off the CQ; consumed by
    /// `drain_into` ahead of the live CQ on the next `process_cqes`.
    staged: RefCell<Vec<(u64, bool)>>,
    /// Reap-loop waker, registered by `staged_ready` when `staged` is empty.
    waker: RefCell<Option<std::task::Waker>>,
}

impl ProbeShared {
    /// Resolve once `staged` is non-empty (the probe parked completions).
    async fn staged_ready(self: &Rc<Self>) {
        std::future::poll_fn(|cx| {
            if self.staged.borrow().is_empty() {
                *self.waker.borrow_mut() = Some(cx.waker().clone());
                std::task::Poll::Pending
            } else {
                std::task::Poll::Ready(())
            }
        })
        .await
    }
}

/// GET_STATS key names for the three batch histograms (wire-format stable):
/// WRs per read-batch doorbell, WRs per response-batch doorbell, and CQEs per
/// non-empty poll.
const HIST_KEYS: [[&str; 6]; 4] = [
    [
        "read_db_b1",
        "read_db_b2",
        "read_db_b4",
        "read_db_b8",
        "read_db_b16",
        "read_db_b32",
    ],
    [
        "resp_db_b1",
        "resp_db_b2",
        "resp_db_b4",
        "resp_db_b8",
        "resp_db_b16",
        "resp_db_b32",
    ],
    [
        "recv_db_b1",
        "recv_db_b2",
        "recv_db_b4",
        "recv_db_b8",
        "recv_db_b16",
        "recv_db_b32",
    ],
    [
        "poll_b1", "poll_b2", "poll_b4", "poll_b8", "poll_b16", "poll_b32",
    ],
];

#[derive(Debug, Default)]
struct RdmaWrStats {
    /// Host write-data pulls (RDMA READ), read-data pushes (RDMA WRITE),
    /// CQE capsules (SEND), and command capsules (RECV).
    read: WrClass,
    write: WrClass,
    send: WrClass,
    recv: WrClass,
    /// Non-empty CQ polls (completion batches). `*_done / poll_batches` is the
    /// average number of each WR class reaped per batch.
    poll_batches: Cell<u64>,
    /// Send-queue doorbells rung (`ibv_post_send` calls for READ/WRITE/SEND).
    /// `(read+write+send)_posted / sq_doorbells` is the submission batch size —
    /// 1.0 with one WR per post, higher once WRs are chained per doorbell.
    sq_doorbells: Cell<u64>,
    /// WRs chained per read-batch doorbell (`post_reads_batch`).
    read_db: BatchHist,
    /// WRs chained per response-batch doorbell (`post_responses_batch`;
    /// a response is 1 SEND, or WRITE+SEND when it carries read data).
    resp_db: BatchHist,
    /// RECV WRs per repost doorbell (the recv queue's doorbell, not counted in
    /// `sq_doorbells`). Always singletons BY DESIGN — see the RNR note in
    /// `handle_recv`; this column is the canary that keeps it that way.
    recv_db: BatchHist,
    /// CQEs reaped per non-empty CQ poll.
    poll: BatchHist,
}

impl RdmaWrStats {
    /// Count a send-queue doorbell (`ibv_post_send`), whatever it batched.
    #[inline]
    fn doorbell(&self) {
        self.sq_doorbells.set(self.sq_doorbells.get() + 1);
    }

    /// Count a single-WR send-queue post plus the doorbell it rings.
    #[inline]
    fn sq_post(&self, class: &WrClass) {
        class.post();
        self.doorbell();
    }

    /// The classes with their GET_STATS key names (wire-format stable).
    fn classes(&self) -> [([&'static str; 3], &WrClass); 4] {
        [
            (["read_posted", "read_done", "read_inflight"], &self.read),
            (
                ["write_posted", "write_done", "write_inflight"],
                &self.write,
            ),
            (["send_posted", "send_done", "send_inflight"], &self.send),
            (["recv_posted", "recv_done", "recv_inflight"], &self.recv),
        ]
    }
}

impl TransportStats for RdmaWrStats {
    fn snapshot(&self) -> Vec<(&'static str, u64)> {
        let mut out = Vec::with_capacity(14);
        for (names, class) in self.classes() {
            let gauge = u64::try_from(class.inflight.get().max(0)).unwrap_or(0);
            out.extend([
                (names[0], class.posted.get()),
                (names[1], class.done.get()),
                (names[2], gauge),
            ]);
        }
        out.push(("poll_batches", self.poll_batches.get()));
        out.push(("sq_doorbells", self.sq_doorbells.get()));
        for (keys, hist) in
            HIST_KEYS
                .iter()
                .zip([&self.read_db, &self.resp_db, &self.recv_db, &self.poll])
        {
            for (key, cell) in keys.iter().zip(&hist.0) {
                out.push((key, cell.get()));
            }
        }
        out
    }

    fn reset(&self) {
        for (_, class) in self.classes() {
            class.posted.set(0);
            class.done.set(0);
        }
        self.poll_batches.set(0);
        self.sq_doorbells.set(0);
        for hist in [&self.read_db, &self.resp_db, &self.recv_db, &self.poll] {
            for cell in &hist.0 {
                cell.set(0);
            }
        }
    }
}

/// One RC connection's RDMA resources + the NVMe slot engine. Owns the receive
/// capsule buffers and a send/response staging buffer, all registered as MRs.
///
/// Field order is the drop order and is load-bearing: the QP is destroyed first
/// (no in-flight DMA, and it frees the CQ for destroy), then each MR is
/// deregistered *before* the memory it pins is freed (an MR outliving its backing
/// buffer would `ibv_dereg_mr` an already-freed region). [`Drop`] additionally
/// drains the comp channel so destroying the CQ cannot block.
pub struct RdmaQueue {
    qp: GenericQueuePair,
    cq: Rc<GenericCompletionQueue>,
    channel: Arc<CompletionChannel>,
    /// MR over the data pool arena (local key for RDMA WRITE sges); dropped
    /// before `nvme`, which owns the arena memory.
    _pool_mr: Arc<MemoryRegion>,
    pool_lkey: u32,
    nvme: Rc<QueueCore<Sqe>>,
    /// MR over the `nslots` receive capsule buffers; dropped before `recv_buf`.
    recv_mr: Arc<MemoryRegion>,
    recv_buf: Vec<u8>,
    /// MR over the per-slot response (CQE) staging; dropped before `resp_buf`.
    resp_mr: Arc<MemoryRegion>,
    resp_buf: Vec<u8>,
    /// MR over the connect-data RDMA-READ landing buffer; before `cdata_buf`.
    cdata_mr: Arc<MemoryRegion>,
    /// Destination for the admin-queue fabrics Connect data (host-resident via
    /// keyed SGL, RDMA READ here before bootstrap parses it).
    cdata_buf: Vec<u8>,
    nslots: u32,
    /// Outstanding response WRs (WRITE + SEND) per tag; release the tag at 0.
    inflight: Vec<u8>,
    /// Finished responses, pushed by the per-tag slot tasks (and the reap loop's
    /// validation-failure path) and drained + posted by the reap loop.
    responses: Rc<SendList<RdmaResp>>,
    /// For a deferred write command: the SQE stashed at RECV, submitted to its
    /// slot only once its host write-data RDMA READ (`WR_READ`) completes.
    pending_read: Vec<Sqe>,
    /// Per-class RDMA WR counters (READ/WRITE/SEND/RECV posted/done/inflight),
    /// shared with `nvme.stats` so GET_STATS can snapshot them on this thread.
    wr: Rc<RdmaWrStats>,
    /// Write-command host-data READs deferred from `handle_recv`, flushed on one
    /// doorbell after each CQ-poll's RECV batch. Reused; sized to the queue depth.
    read_batch: Vec<PendingRead>,
    /// Park-probe rendezvous (see [`ProbeShared`]); the probe itself is
    /// registered by [`Self::run`] and removed at its exits.
    probe: Rc<ProbeShared>,
    /// Commands parked because every slot tag is held. RDMA-only ordering
    /// window: the response SEND delivers the CQE to the host — freeing its SQ
    /// slot — before our own SEND completion is reaped and the tag released, so
    /// a conforming host can deliver the next command while all tags are busy.
    /// Park it and drain oldest-first as tags free (nvmet parks the same way on
    /// `rsp_wr_wait_list`); the old treat-as-fatal behavior tore down healthy
    /// queues under full-depth 64k bursts. Preallocated to the queue depth.
    parked: std::collections::VecDeque<(Sqe, Option<u32>)>,
    /// Write commands (tag claimed) whose pool lease could not be satisfied.
    /// The RDMA READ of the host data must land in the registered pool arena,
    /// so `lease_or_owned`'s private-heap fallback is unusable here; failing
    /// the command instead (the old behavior) returned DATA_XFER_ERROR with
    /// DNR — an instant EIO to the host under full-depth write bursts, since
    /// the pool is deliberately smaller than depth x MDTS. Defer instead
    /// (SPDK's pending_buf_queue shape) and retry front-only as completions
    /// release leases. Bounded by the slot count (each entry holds a tag).
    pool_wait: std::collections::VecDeque<(u16, Sqe, Option<u32>)>,
    /// Backstop ticks since start; rate-gates the keep-alive watchdog.
    watchdog_tick: u32,
}

impl Drop for RdmaQueue {
    fn drop(&mut self) {
        // `ibv_destroy_cq` (when `cq` drops next) blocks until every *delivered*
        // comp-channel event is acked. A completion arriving after the last
        // `cq::wait` re-arm leaves an unacked event; consume it here so the CQ
        // destroy cannot hang this (single) reactor thread.
        let _ = crate::cq::drain_events(&self.channel, &self.cq);
    }
}

// Capsule/CQE lengths are small compile-time constants and slot tags fit u16
// (sqsize <= MAX_QUEUE_ENTRIES); the casts here are provably lossless.
#[allow(clippy::cast_possible_truncation, clippy::too_many_arguments)]
impl RdmaQueue {
    /// Build the per-connection queue on the connection's PD/CQ/QP. `qid==0` is
    /// the admin queue (its pool is sized so leases never block).
    pub fn new(
        qid: u16,
        sqsize: u16,
        sqhd_disabled: bool,
        pd: Arc<ProtectionDomain>,
        channel: Arc<CompletionChannel>,
        cq: Rc<GenericCompletionQueue>,
        qp: GenericQueuePair,
        queue_buf_bytes: usize,
    ) -> io::Result<RdmaQueue> {
        let pool_bytes = if qid == 0 {
            usize::from(sqsize).max(1) * ioutgt_core::ADMIN_DATA_MAX
        } else {
            queue_buf_bytes
        };
        let nvme = QueueCore::new(qid, sqsize, pool_bytes, sqhd_disabled, Sqe::zeroed());
        // Register the data pool arena as one MR — the local source for RDMA
        // WRITEs of read responses.
        let (ptr, len) = nvme.slots.pool().arena();
        // SAFETY: the pool arena is owned by `nvme` (kept in this struct) and
        // outlives the MR; LocalWrite lets read handlers fill it.
        let pool_mr =
            unsafe { pd.reg_mr(ptr as usize, len, AccessFlags::LocalWrite) }.map_err(oerr)?;
        let pool_lkey = pool_mr.lkey();
        // Also register the arena as an io_uring fixed buffer (TCP parity), so
        // disk IO from pooled slots uses READV_FIXED/WRITEV_FIXED — the kernel
        // reuses the pre-pinned mapping instead of get_user_pages + IOMMU-
        // mapping the pages on every IO (measured at ~7% of the io-thread on
        // 4k randwrite). Best-effort: None (no kernel support / table full)
        // keeps the plain readv/writev path. Released at run() teardown.
        if let Some(idx) = ioutgt_uring::register_pool_buffer(ptr, len) {
            nvme.slots.pool().set_buf_index(idx);
        }

        let nslots = u32::from(sqsize);
        let mut recv_buf = vec![0u8; sqsize as usize * CAPSULE_LEN];
        // SAFETY: recv_buf outlives recv_mr (both held in this struct); the NIC
        // writes received capsules into it.
        let recv_mr = unsafe {
            pd.reg_mr(
                recv_buf.as_mut_ptr() as usize,
                recv_buf.len(),
                AccessFlags::LocalWrite,
            )
        }
        .map_err(oerr)?;
        let mut resp_buf = vec![0u8; sqsize as usize * CQE_LEN];
        // SAFETY: resp_buf outlives resp_mr; a SEND source is locally read.
        let resp_mr = unsafe {
            pd.reg_mr(
                resp_buf.as_mut_ptr() as usize,
                resp_buf.len(),
                AccessFlags::none(),
            )
        }
        .map_err(oerr)?;
        let mut cdata_buf = vec![0u8; ICD_LEN];
        // SAFETY: cdata_buf outlives cdata_mr; the NIC writes the RDMA-READ data.
        let cdata_mr = unsafe {
            pd.reg_mr(
                cdata_buf.as_mut_ptr() as usize,
                cdata_buf.len(),
                AccessFlags::LocalWrite,
            )
        }
        .map_err(oerr)?;

        let wr = Rc::new(RdmaWrStats::default());
        // Report the RDMA WR counters under "wr" in GET_STATS; snapshotted on
        // this queue thread (via the mailbox), same as the core QueueStats.
        nvme.stats.set_transport(wr.clone());

        Ok(RdmaQueue {
            qp,
            cq,
            channel,
            nvme,
            pool_lkey,
            _pool_mr: pool_mr,
            recv_buf,
            recv_mr,
            resp_buf,
            resp_mr,
            cdata_buf,
            cdata_mr,
            nslots,
            inflight: vec![0u8; sqsize as usize],
            responses: Rc::new(SendList::new(sqsize)),
            pending_read: vec![Sqe::zeroed(); sqsize as usize],
            wr,
            read_batch: Vec::with_capacity(sqsize as usize),
            probe: Rc::new(ProbeShared::default()),
            parked: std::collections::VecDeque::with_capacity(sqsize as usize),
            pool_wait: std::collections::VecDeque::with_capacity(sqsize as usize),
            watchdog_tick: 0,
        })
    }

    fn recv_slice(&self, idx: u32) -> &[u8] {
        let off = idx as usize * CAPSULE_LEN;
        &self.recv_buf[off..off + CAPSULE_LEN]
    }

    /// (Re-)post the RECV for capsule buffer `idx`.
    fn post_recv(&mut self, idx: u32) -> io::Result<()> {
        let off = idx as usize * CAPSULE_LEN;
        let mut sge = ibv_sge {
            addr: self.recv_buf.as_ptr() as u64 + off as u64,
            length: CAPSULE_LEN as u32,
            lkey: self.recv_mr.lkey(),
        };
        let mut rwr = ibv_recv_wr {
            wr_id: wr(WR_RECV, idx),
            next: std::ptr::null_mut(),
            sg_list: &mut sge,
            num_sge: 1,
        };
        let mut bad: *mut ibv_recv_wr = std::ptr::null_mut();
        // Raw ibv_post_recv instead of sideway's PostRecvGuard: the guard heap-
        // allocates two Vecs per call, and this runs once per command on the
        // hot path (measured ~2.4% of a saturated io-thread — also a quiet
        // violation of the zero-steady-state-allocation invariant).
        // SAFETY: the QP is live; `rwr`/`sge` are valid across the call (the
        // provider copies them into the RQ before returning); the region is
        // registered (recv_mr) and stays valid — the NIC writes the next
        // capsule here.
        let rc = unsafe { ibv_post_recv(self.qp.qp().as_ptr(), &mut rwr, &mut bad) };
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
        self.wr.recv.post();
        self.wr.recv_db.record(1);
        Ok(())
    }

    /// RDMA READ `len` bytes from the host's keyed-SGL region into `cdata_buf`
    /// (the fabrics Connect data, which on the admin queue is host-resident, not
    /// in-capsule). Completes with a `WR_READ` work completion.
    ///
    /// This shares the `wr(WR_READ, _)` encoding with the write-data path's
    /// per-tag READs, with `0` in the low bits — but it is bootstrap-only and is
    /// awaited to completion in [`Self::await_read`] *before* the steady reap loop
    /// (and thus the `WR_READ => submit_pending` arm) exists, so the two never
    /// coexist and the shared low value is unambiguous.
    fn post_read_cdata(&mut self, src: &KeyedSgl, len: usize) -> io::Result<()> {
        let lkey = self.cdata_mr.lkey();
        let addr = self.cdata_buf.as_ptr() as u64;
        let mut g = self.qp.start_post_send();
        let h = g
            .construct_wr(wr(WR_READ, 0), WorkRequestFlags::Signaled)
            .setup_read(src.rkey, src.addr);
        // SAFETY: cdata_buf is registered (cdata_mr, LocalWrite) and lives in
        // this struct; len is clamped to its size by the caller.
        unsafe { h.setup_sge(lkey, addr, len as u32) };
        g.post().map_err(oerr)?;
        self.wr.sq_post(&self.wr.read);
        Ok(())
    }

    /// Post every write-command host-data READ collected in `read_batch` on ONE
    /// doorbell: for each, an RDMA READ scattering the host's keyed-SGL region
    /// into the slot's leased pool segments, all constructed on a single guard
    /// and flushed with one `ibv_post_send`. Each READ completes independently
    /// (`WR_READ` → submit_pending). Empties `read_batch` on success.
    fn post_reads_batch(&mut self) -> io::Result<()> {
        if self.read_batch.is_empty() {
            return Ok(());
        }
        // Split-borrow so the send guard (&mut qp) coexists with the sge
        // construction reading from the slot pool.
        let RdmaQueue {
            qp,
            nvme,
            pool_lkey,
            wr: stats,
            read_batch,
            ..
        } = self;
        let pool_lkey = *pool_lkey;
        let mut reads = 0u64;
        let mut g = qp.start_post_send();
        let mut sges = [ibv_sge {
            addr: 0,
            length: 0,
            lkey: 0,
        }; MAX_SEGS];
        for pr in read_batch.iter().copied() {
            let n = fill_sges(
                &nvme.slots.slot(pr.tag).data(),
                pr.len as usize,
                pool_lkey,
                &mut sges,
            );
            let h = g
                .construct_wr(wr(WR_READ, u32::from(pr.tag)), WorkRequestFlags::Signaled)
                .setup_read(pr.rkey, pr.addr);
            // SAFETY: the sges reference the registered pool arena (pool_lkey,
            // is_pool-checked in handle_recv); the slot stays leased until its
            // response SEND completes (tag not released until then).
            unsafe { h.setup_sge_list(&sges[..n]) };
            reads += 1;
        }
        // One atomic ibv_post_send for the batch (extended guard's
        // ibv_wr_complete); a post error is fatal and tears the queue down, so
        // the not-yet-cleared read_batch residue is harmless.
        g.post().map_err(oerr)?;
        stats.read.post_n(reads);
        stats.doorbell();
        stats
            .read_db
            .record(usize::try_from(reads).unwrap_or(usize::MAX));
        read_batch.clear();
        Ok(())
    }

    /// Drain all currently-available completions as `(wr_id, success)` into the
    /// reused `out` buffer (cleared first), so the steady IO path allocates none.
    fn drain_into(&self, out: &mut Vec<(u64, bool)>) {
        out.clear();
        // Completions the park-probe already pulled off the CQ come first
        // (they are older than anything still queued).
        out.append(&mut self.probe.staged.borrow_mut());
        if let Ok(poller) = self.cq.start_poll() {
            for wc in poller {
                out.push((
                    wc.wr_id(),
                    wc.status() == WorkCompletionStatus::Success as u32,
                ));
            }
        }
    }

    /// Drain the CQ and dispatch each completion. Returns `Ok(true)` when a
    /// flushed completion is seen (peer gone → stop the reap loop), `Ok(false)`
    /// to keep going, `Err` on a fatal handler error. Reused by both the
    /// event-driven reap and the periodic backstop in [`Self::run`].
    fn process_cqes(
        &mut self,
        ctx: &Rc<ConnCtx<AnyBackend>>,
        comps: &mut Vec<(u64, bool)>,
    ) -> io::Result<bool> {
        self.drain_into(comps);
        if !comps.is_empty() {
            self.wr.poll_batches.set(self.wr.poll_batches.get() + 1);
            self.wr.poll.record(comps.len());
        }
        // `comps` is a local buffer (disjoint from `self`), so iterating it while
        // calling `&mut self` handlers is sound.
        for &(id, ok) in comps.iter() {
            if !ok {
                // Flushed completion: the peer is gone.
                return Ok(true);
            }
            match wr_kind(id) {
                WR_RECV => {
                    self.wr.recv.complete();
                    self.handle_recv(ctx, wr_low(id))?;
                }
                // A write-data RDMA READ finished: the slot is filled, so submit
                // it to wake its slot task.
                WR_READ => {
                    self.wr.read.complete();
                    self.submit_pending(wr_low(id) as u16);
                }
                WR_SEND => {
                    self.wr.send.complete();
                    self.on_response_done(wr_low(id));
                }
                WR_WRITE => {
                    self.wr.write.complete();
                    self.on_response_done(wr_low(id));
                }
                _ => {}
            }
        }
        // Pool leases freed by this poll's response completions un-block
        // deferred write stages, oldest first (head-of-line, like SPDK's
        // pending queues — a big front request is not starved by smaller
        // later ones). Before the parked drain so freed pages go to commands
        // that already hold tags.
        while let Some(&(tag, sqe, inline_idx)) = self.pool_wait.front() {
            let staged = match inline_idx {
                Some(idx) => {
                    let (off, len) = parse_inline_sgl(&sqe);
                    // Bounds were validated before parking.
                    self.try_stage_inline(tag, sqe, idx, off as usize, len)?
                }
                None => self.try_stage_write(tag, sqe, staged_len(&sqe)),
            };
            if !staged {
                break;
            }
            self.pool_wait.pop_front();
        }
        // Tags freed by this poll's response completions un-park waiting
        // commands (oldest first) — before the read-batch flush so a parked
        // write command's host-data READ rides the same doorbell.
        while !self.parked.is_empty() {
            let Some(tag) = self.nvme.claim_tag() else {
                break;
            };
            let (sqe, inline_idx) = self.parked.pop_front().expect("parked is non-empty");
            self.route_cmd(ctx, tag, sqe, inline_idx)?;
        }
        // Flush the write-command host-data READs `handle_recv` collected from
        // this poll's RECV batch — all on one doorbell.
        self.post_reads_batch()?;
        Ok(false)
    }

    /// Keep-alive / controller-liveness watchdog, run on the backstop cadence
    /// (checked every 2nd tick ≈ 2 s). The RDMA path has no socket death to
    /// unwind a vanished host, so without this a dead host leaks every QP it
    /// had (observed: an aborted connect left 17 QPs in RTS). Returns `true`
    /// when the queue must tear down:
    ///  - admin queue: the host has been silent past KATO×2 + grace — mirrors
    ///    nvmet's keep-alive timer and the TCP path's watchdog (`last_heard`
    ///    is bumped by every dispatched command; kato 0 = disabled, e.g. a
    ///    persistent discovery controller);
    ///  - IO queue: its controller is gone from the registry (the admin
    ///    queue's teardown removed it), so it follows the controller down.
    fn watchdog(&mut self, ctx: &Rc<ConnCtx<AnyBackend>>) -> bool {
        self.watchdog_tick = self.watchdog_tick.wrapping_add(1);
        if self.watchdog_tick % 2 != 0 {
            return false;
        }
        match &ctx.role {
            Role::Admin(admin) => {
                let kato = u64::from(admin.kato_ms.get());
                if kato == 0 {
                    return false;
                }
                let silent =
                    u64::try_from(admin.last_heard.get().elapsed().as_millis()).unwrap_or(u64::MAX);
                if silent > kato * 2 + 5_000 {
                    tracing::info!(
                        cntlid = admin.cntlid.get(),
                        silent_ms = silent,
                        "nvme-rdma: keep-alive expired; tearing down controller"
                    );
                    return true;
                }
            }
            Role::Io(io) => {
                let cntlid = io.cntlid.get();
                if cntlid != 0 && !ctx.registry.contains(cntlid) {
                    tracing::info!(
                        cntlid,
                        qid = self.nvme.qid,
                        "nvme-rdma: controller gone; tearing down io queue"
                    );
                    return true;
                }
            }
        }
        false
    }

    /// On a response WR (WRITE/SEND) completion, decrement the tag's in-flight
    /// count and release the slot once both its responses have completed.
    fn on_response_done(&mut self, tag: u32) {
        let n = &mut self.inflight[tag as usize];
        *n = n.saturating_sub(1);
        if *n == 0 {
            self.nvme.slots.release_tag(tag as u16);
        }
    }

    /// Bootstrap-only: park on the completion channel until a completion of
    /// `kind` (`WR_RECV` for the Connect capsule, `WR_READ` for its keyed-SGL
    /// connect data) arrives, returning its low bits (the recv buffer index /
    /// tag). Response completions are serviced (slots released) meanwhile; the
    /// steady reap loop takes over once [`Self::bootstrap`] returns.
    async fn await_bootstrap(&mut self, kind: u64) -> io::Result<u32> {
        // One Connect per queue, so this one-time buffer is fine.
        let mut comps: Vec<(u64, bool)> = Vec::with_capacity(8);
        loop {
            crate::cq::wait(&self.channel, &self.cq).await?;
            self.drain_into(&mut comps);
            for &(id, ok) in &comps {
                if !ok {
                    return Err(io::Error::other("RDMA completion error (peer gone?)"));
                }
                match wr_kind(id) {
                    k if k == kind => {
                        match kind {
                            WR_RECV => &self.wr.recv,
                            _ => &self.wr.read,
                        }
                        .complete();
                        return Ok(wr_low(id));
                    }
                    WR_SEND => {
                        self.wr.send.complete();
                        self.on_response_done(wr_low(id));
                    }
                    WR_WRITE => {
                        self.wr.write.complete();
                        self.on_response_done(wr_low(id));
                    }
                    _ => {}
                }
            }
        }
    }

    /// Take a command capsule at buffer `idx`, re-arm its RECV, claim a slot, and
    /// route it. A host-to-controller-data command (IO write / DSM) leases a pool
    /// buffer and RDMA-READs the host's keyed-SGL data into it; the slot is
    /// submitted — waking its slot task to dispatch — only when that READ
    /// completes (`WR_READ` → [`Self::submit_pending`]). Everything else submits
    /// at once. Commands the transport cannot satisfy are failed without dispatch.
    fn handle_recv(&mut self, ctx: &Rc<ConnCtx<AnyBackend>>, idx: u32) -> io::Result<()> {
        let sqe = Sqe::read_from_bytes(&self.recv_slice(idx)[..SQE_LEN])
            .map_err(|_| io::Error::other("short command capsule"))?;
        // The capsule buffer is free once the SQE is parsed — re-arm it NOW,
        // one doorbell per capsule. Deferring reposts to the cycle tail (to
        // chain them on one doorbell, SPDK-style) was tried and REVERTED: a
        // reproducible 168k-vs-174k (-3.4%) on 4k randwrite qd128, mlx5 +
        // null_blk, four runs each. NOT an RNR problem (host-side
        // rnr_nak_retry_err delta was 0 under both builds — the ring never
        // went empty); the mechanism is unestablished, plausibly the later
        // repost delaying the host's next capsule down a zero-slack pipeline.
        // nvmet also re-posts per op (before each response, rdma.c); SPDK
        // batches but couples each requeue to its own request's response and
        // flushes recvs before sends — if batching is ever retried, start
        // from that shape plus explicit ring slack, and A/B with recv/db.
        //
        // Exception: a write whose payload is IN the capsule (non-keyed SGL)
        // must hold its buffer until the payload is copied into the pool
        // lease — `try_stage_inline` re-posts it then. Holding is safe for
        // the ring: the command has no response yet, so the host cannot send
        // a replacement capsule for its slot (nvmet holds its recv through
        // processing for the same reason).
        let inline_idx = (host_data_in(&ctx.role, sqe.opcode)
            && sqe.as_bytes()[SGL_TYPE_OFFSET] >> 4 != KEYED_SGL_TYPE_HI)
            .then_some(idx);
        if inline_idx.is_none() {
            self.post_recv(idx)?;
        }
        let Some(tag) = self.nvme.claim_tag() else {
            // All tags held: park, don't kill (see the `parked` field). A host
            // can only overrun the parking lot by exceeding the negotiated
            // queue depth outright — that stays fatal.
            if self.parked.len() >= self.nslots as usize {
                return Err(io::Error::other(
                    "command overflow: host exceeded negotiated queue depth",
                ));
            }
            self.parked.push_back((sqe, inline_idx));
            return Ok(());
        };
        self.route_cmd(ctx, tag, sqe, inline_idx)
    }

    /// Route a claimed command: submit it to its slot task, or (host-data-in)
    /// lease a pool buffer and post the host-data RDMA READ first. Split from
    /// [`Self::handle_recv`] so parked commands re-enter here when a tag frees.
    fn route_cmd(
        &mut self,
        ctx: &Rc<ConnCtx<AnyBackend>>,
        tag: u16,
        sqe: Sqe,
        inline_idx: Option<u32>,
    ) -> io::Result<()> {
        if !host_data_in(&ctx.role, sqe.opcode) {
            self.nvme.submit(tag, sqe);
            return Ok(());
        }

        if let Some(idx) = inline_idx {
            // In-capsule write data (IOCCSZ advertises one page): the payload
            // is in the capsule buffer at `idx`, which handle_recv left
            // un-reposted for us. Anything other than a data-block/offset
            // descriptor within the in-capsule bounds is rejected (mirrors
            // nvmet_rdma_map_sgl_inline); the reject paths re-post the recv
            // since the payload is not used.
            let (off, len) = parse_inline_sgl(&sqe);
            if sqe.as_bytes()[SGL_TYPE_OFFSET] != INLINE_SGL_TYPE {
                self.fail_recv(ctx, tag, &sqe, status::SGL_INVALID_TYPE | status::DNR);
                return self.post_recv(idx);
            }
            if len == 0 || off.saturating_add(len as u64) > ICD_LEN as u64 {
                self.fail_recv(ctx, tag, &sqe, status::DATA_SGL_LEN_INVALID | status::DNR);
                return self.post_recv(idx);
            }
            if !self.try_stage_inline(tag, sqe, idx, off as usize, len)? {
                self.pool_wait.push_back((tag, sqe, Some(idx)));
            }
            return Ok(());
        }

        let len = staged_len(&sqe);
        if len == 0 {
            self.fail_recv(ctx, tag, &sqe, status::DATA_SGL_LEN_INVALID | status::DNR);
            return Ok(());
        }
        if !self.try_stage_write(tag, sqe, len) {
            // Pool momentarily full: the lease must come from the registered
            // arena (it is the RDMA READ's local target), so defer — do NOT
            // fail. Retried by the reap loop as completions release leases
            // (see `pool_wait`).
            self.pool_wait.push_back((tag, sqe, None));
        }
        Ok(())
    }

    /// Stage an in-capsule write: lease the pool buffer, copy the payload out
    /// of the capsule, submit the slot (the data is already here — no RDMA
    /// READ), and re-post the capsule RECV its caller held back. `false` when
    /// the pool cannot satisfy the lease right now — the caller parks the
    /// command on `pool_wait` (capsule still held) and the reap loop retries.
    fn try_stage_inline(
        &mut self,
        tag: u16,
        sqe: Sqe,
        idx: u32,
        off: usize,
        len: usize,
    ) -> io::Result<bool> {
        if !self.nvme.slots.try_lease(tag, len) {
            return Ok(false);
        }
        let slot = self.nvme.slots.slot(tag);
        slot.set_data_len(len as u32);
        // Copy the payload from the capsule into the lease's segments. ~100 ns
        // for a 4k page — three orders cheaper than the RDMA READ round trip
        // it replaces.
        let src = &self.recv_slice(idx)[SQE_LEN + off..SQE_LEN + off + len];
        let mut copied = 0usize;
        for seg in slot.data().segs() {
            if copied == len {
                break;
            }
            let take = (len - copied).min(seg.len);
            // SAFETY: `seg` is a live pool-lease segment owned by this slot
            // (leased just above, released only after the response completes);
            // `src` is the registered capsule buffer, disjoint from the pool.
            unsafe {
                std::ptr::copy_nonoverlapping(src.as_ptr().add(copied), seg.ptr, take);
            }
            copied += take;
        }
        self.nvme.submit(tag, sqe);
        self.post_recv(idx)?;
        Ok(true)
    }

    /// Stage a validated write command: lease its pool buffer, stash the SQE,
    /// and queue the host-data RDMA READ. `false` (nothing staged) when the
    /// pool cannot satisfy the lease right now — the caller parks the command
    /// on `pool_wait` and the reap loop retries it as leases free.
    fn try_stage_write(&mut self, tag: u16, sqe: Sqe, len: usize) -> bool {
        if !self.nvme.slots.try_lease(tag, len) {
            return false;
        }
        // io::write/dsm check `data_len()` (the *received* length), so set it
        // to the SGL-advertised length the RDMA READ will fill.
        self.nvme.slots.slot(tag).set_data_len(len as u32);
        // Stash the SQE and defer the host-data RDMA READ into read_batch, so all
        // of this CQ poll's write-command reads flush on one doorbell; the slot
        // is submitted when the READ completes (`WR_READ` → submit_pending).
        let sgl = parse_keyed_sgl(&sqe);
        self.pending_read[tag as usize] = sqe;
        self.read_batch.push(PendingRead {
            tag,
            len: len as u32,
            rkey: sgl.rkey,
            addr: sgl.addr,
        });
        true
    }

    /// Submit a deferred write command once its host-data RDMA READ completed —
    /// the slot is now filled, so its slot task can dispatch.
    fn submit_pending(&self, tag: u16) {
        self.nvme.submit(tag, self.pending_read[tag as usize]);
    }

    /// Fail a still-`Receiving` command without dispatching it (the slot task
    /// never sees it): step the slot to `Responding` and queue an error CQE.
    fn fail_recv(&self, ctx: &Rc<ConnCtx<AnyBackend>>, tag: u16, sqe: &Sqe, status: u16) {
        self.nvme.slots.respond_receiving(tag);
        let cqe = ctx.cqe(0, sqe.cid.get(), status);
        self.responses.push(RdmaResp {
            tag,
            cmd: *sqe,
            outcome: Outcome::status(cqe),
        });
    }

    /// Post every finished response in `batch` on ONE doorbell: for each, the
    /// read-data RDMA WRITE (if any) then the CQE SEND, all constructed on a
    /// single send guard and flushed with one `ibv_post_send`. Extends B1's
    /// per-command WRITE+SEND chain across all responses drained in one reactor
    /// wake — N responses collapse from up to 2N doorbells to one. Each WR still
    /// completes separately and decrements its tag's `inflight` (set here to 1
    /// for a bare CQE, 2 when a read-data WRITE precedes it).
    fn post_responses_batch(&mut self, batch: &[RdmaResp]) -> io::Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        // Split `self` into disjoint field borrows: the send guard borrows
        // `&mut qp`, while CQE staging (`resp_buf`) and sge construction
        // (`nvme`/`pool_lkey`) touch other fields concurrently.
        let RdmaQueue {
            qp,
            resp_buf,
            resp_mr,
            nvme,
            pool_lkey,
            inflight,
            wr: stats,
            ..
        } = self;
        let resp_lkey = resp_mr.lkey();
        let pool_lkey = *pool_lkey;
        let mut writes = 0u64;
        let mut sends = 0u64;
        let qp_ex = qp_ex_of(qp)?;
        let mut g = qp.start_post_send();
        let mut sges = [ibv_sge {
            addr: 0,
            length: 0,
            lkey: 0,
        }; MAX_SEGS];
        for resp in batch {
            let tag = resp.tag;
            let mut pending = 1u8;
            if resp.outcome.data_len > 0 {
                let n = fill_sges(
                    &nvme.slots.slot(tag).data(),
                    resp.outcome.data_len as usize,
                    pool_lkey,
                    &mut sges,
                );
                let dst = parse_keyed_sgl(&resp.cmd);
                let hw = g
                    .construct_wr(wr(WR_WRITE, u32::from(tag)), WorkRequestFlags::Signaled)
                    .setup_write(dst.rkey, dst.addr);
                // SAFETY: the sges reference the registered pool arena (pool_lkey);
                // the slot stays leased until both WRs complete (tag not released).
                unsafe { hw.setup_sge_list(&sges[..n]) };
                pending = 2;
                writes += 1;
            }
            let off = tag as usize * CQE_LEN;
            resp_buf[off..off + CQE_LEN].copy_from_slice(resp.outcome.cqe.as_bytes());
            let cqe_addr = resp_buf.as_ptr() as u64 + off as u64;
            // Solicited: the host's nvme-rdma CQ is armed for solicited events
            // only, so the response CQE SEND must set IBV_SEND_SOLICITED or the
            // host never takes a completion interrupt — it sleeps with the CQE
            // unreaped and the IO hangs (mirrors nvmet-rdma, which marks its
            // responses solicited).
            let ws = g.construct_wr(
                wr(WR_SEND, u32::from(tag)),
                WorkRequestFlags::Signaled | WorkRequestFlags::Solicited,
            );
            match invalidate_rkey_for(&resp.cmd) {
                // SAFETY: guard live on this (extended) QP, wr id/flags set just
                // above; the staging region is registered and stays valid until
                // the SEND completes (tag not released until then).
                Some(rkey) => unsafe {
                    wr_send_with_inv(qp_ex, rkey, resp_lkey, cqe_addr, CQE_LEN as u32)
                },
                None => {
                    let hs = ws.setup_send();
                    // SAFETY: staging region registered + valid as above.
                    unsafe { hs.setup_sge(resp_lkey, cqe_addr, CQE_LEN as u32) };
                }
            }
            sends += 1;
            inflight[tag as usize] = pending;
        }
        // One atomic ibv_post_send for the whole batch (extended guard's
        // ibv_wr_complete): all WRs post or none do, so no partial batch can
        // generate stray completions. `inflight[tag]` was set in the loop above;
        // on a post error it stays non-zero with no completion to clear it, but
        // that is harmless — a post error is fatal and tears the queue down,
        // discarding the residue. The stats below run only on the Ok path.
        g.post().map_err(oerr)?;
        stats.write.post_n(writes);
        stats.send.post_n(sends);
        stats.doorbell();
        stats
            .resp_db
            .record(usize::try_from(writes + sends).unwrap_or(usize::MAX));
        Ok(())
    }

    /// Bootstrap the controller from the first capsule (the fabrics Connect),
    /// dispatch it, and return the dispatch context.
    ///
    /// The connect data lands two ways depending on the queue: the **admin**
    /// queue's host sends it via a keyed SGL (host-resident), so we RDMA READ it
    /// into `cdata_buf`; an **IO** queue's host sends it inline (in-capsule). We
    /// branch on the SGL descriptor type byte.
    async fn bootstrap(
        &mut self,
        port: &Arc<PortConfig<AnyBackend>>,
        registry: &Arc<Registry>,
        peer: &str,
    ) -> io::Result<Rc<ConnCtx<AnyBackend>>> {
        let idx = self.await_bootstrap(WR_RECV).await?;
        // `inline_cd` is Some for an in-capsule (IO-queue) connect, None for a
        // keyed-SGL (admin-queue) connect that we must RDMA READ below.
        let (sqe, inline_cd) = {
            let s = self.recv_slice(idx);
            let sqe = Sqe::read_from_bytes(&s[..SQE_LEN])
                .map_err(|_| io::Error::other("short connect capsule"))?;
            let keyed = s[SGL_TYPE_OFFSET] >> 4 == KEYED_SGL_TYPE_HI;
            // Parse inline connect data out of the capsule before re-arming.
            let inline_cd = if keyed {
                None
            } else {
                Some(
                    ConnectData::read_from_bytes(&s[SQE_LEN..SQE_LEN + size_of::<ConnectData>()])
                        .map_err(|_| io::Error::other("short connect data"))?,
                )
            };
            (sqe, inline_cd)
        };
        self.post_recv(idx)?;

        let connect_data = if let Some(cd) = inline_cd {
            Box::new(cd)
        } else {
            // Keyed SGL: RDMA READ the host-resident connect data, then parse it.
            let sgl = parse_keyed_sgl(&sqe);
            let len = (sgl.len as usize).min(ICD_LEN);
            self.post_read_cdata(&sgl, len)?;
            self.await_bootstrap(WR_READ).await?;
            Box::new(
                ConnectData::read_from_bytes(&self.cdata_buf[..size_of::<ConnectData>()])
                    .map_err(|_| io::Error::other("short connect data"))?,
            )
        };

        // qid 0 bootstraps a new controller (new_admin); qid n attaches to the
        // controller the admin Connect created, by cntlid (new_io).
        let ctx = if self.nvme.qid == 0 {
            ConnCtx::new_admin(
                Rc::clone(&self.nvme),
                Arc::clone(port),
                Arc::clone(registry),
                connect_data,
                peer.to_string(),
            )
        } else {
            ConnCtx::new_io(
                Rc::clone(&self.nvme),
                Arc::clone(port),
                Arc::clone(registry),
                connect_data,
                peer.to_string(),
            )
        };

        let nvme = Rc::clone(&self.nvme);
        let tag = nvme
            .claim_tag()
            .ok_or_else(|| io::Error::other("no tag for connect"))?;
        nvme.submit(tag, sqe);
        let cmd = nvme.await_command(tag).await;
        let outcome = dispatch::execute(&ctx, tag, &cmd).await;
        nvme.begin_respond(tag);
        self.post_responses_batch(&[RdmaResp { tag, cmd, outcome }])?;
        Ok(ctx)
    }

    /// The QP number to hand back to the host in the CM accept reply.
    pub fn qp_number(&self) -> u32 {
        self.qp.qp_number()
    }

    /// Post all RECV WRs and arm the CQ. Must be called once, after the QP is at
    /// least INIT and *before* `accept` (so the host's first capsule, which it
    /// may send the instant the connection establishes, is never dropped — the
    /// kernel `nvmet-rdma` posts its RECVs before accepting too).
    pub fn prime(&mut self) -> io::Result<()> {
        for idx in 0..self.nslots {
            self.post_recv(idx)?;
        }
        crate::cq::arm(&self.cq)
    }

    /// Drive this connection: bootstrap the controller from the Connect capsule,
    /// then process commands until the QP errors (peer disconnect) or a fatal
    /// error. Requires [`prime`](Self::prime) to have been called first. `on_ctx`
    /// is invoked once with the dispatch context after bootstrap — the harness
    /// uses it to register the controller (AER nudges) and its per-queue stats.
    pub async fn run(
        mut self,
        port: Arc<PortConfig<AnyBackend>>,
        registry: Arc<Registry>,
        peer: String,
        stop: Arc<Notify>,
        on_ctx: impl FnOnce(&Rc<ConnCtx<AnyBackend>>),
    ) -> io::Result<()> {
        let ctx = self.bootstrap(&port, &registry, &peer).await?;
        on_ctx(&ctx);

        // One persistent task per slot (preallocated at queue install — zero
        // per-command allocation): each loops await_command → dispatch →
        // begin_respond → push the response for the reap loop to post. Dispatch
        // off the reap loop means a parked Async Event Request (held until an
        // async event) cannot stall the queue. The JoinSet aborts every task when
        // it drops at `run` exit, so a parked AER task is torn down cleanly.
        let mut slot_tasks: JoinSet<()> = JoinSet::new();
        for tag in 0..u16::try_from(self.nslots).unwrap_or(u16::MAX) {
            let nvme = Rc::clone(&self.nvme);
            let ctx = Rc::clone(&ctx);
            let responses = Rc::clone(&self.responses);
            slot_tasks.spawn_local(async move {
                loop {
                    let cmd = nvme.await_command(tag).await;
                    let outcome = dispatch::execute(&ctx, tag, &cmd).await;
                    nvme.begin_respond(tag);
                    responses.push(RdmaResp { tag, cmd, outcome });
                }
            });
        }

        let responses = Rc::clone(&self.responses);
        // Reused across iterations: the steady IO path allocates nothing.
        let mut comps: Vec<(u64, bool)> = Vec::with_capacity(64);
        // Drained-response batch, posted on one doorbell; reused, so no steady alloc.
        let mut resp_batch: Vec<RdmaResp> = Vec::with_capacity(self.nslots as usize);
        // Persistent multishot poll on the comp-channel fd: one SQE, a CQE per
        // readiness edge (IORING_POLL_ADD_MULTI). Registered once, so the reap arm
        // never re-arms a one-shot poll every wake — no drop/re-submit churn.
        let mut comp_poll = ioutgt_uring::ops::poll_add_multi(
            std::os::fd::AsRawFd::as_raw_fd(&self.channel),
            crate::cq::pollin(),
        )?;
        // Persistent backstop timer. A completion whose comp-channel notification
        // was lost (userspace ibverbs has no IB_CQ_REPORT_MISSED_EVENTS) sits in
        // the CQ with no fd event, so the multishot poll never fires for it; this
        // defensively re-arms + re-drains the CQ. Created ONCE and reset only when
        // it fires, so a busy select! cannot starve it — the previous per-iteration
        // `sleep` rebuilt its timer op every wake and never survived to elapse
        // under load, which is exactly how a stranded completion wedged the queue.
        let mut backstop = std::pin::pin!(ioutgt_uring::ops::sleep(BACKSTOP)?);
        // Reactor park-probe: drain this queue's CQ at the reactor's sleep
        // point — under load completions are reaped with no comp-channel
        // event, no read(2) and no poll round-trip per batch (measured ~16%
        // of a saturated io-thread). Going to sleep, the probe arms the CQ
        // and race-drains once (arm-before-drain), so the sleep always has a
        // wake source; while awake it never arms, so no events are generated.
        // The probe only stages + wakes; processing stays in this loop.
        let probe_id = {
            let shared = Rc::clone(&self.probe);
            let cq = Rc::clone(&self.cq);
            // Poll mode (`--poll`): while commands are in flight (any tag
            // claimed), the spin predicate keeps the reactor from sleeping —
            // the CQ is busy-polled at park cadence and completions never
            // wait for an event. The moment the queue drains idle (all tags
            // free — the admin queue between keep-alives, an idle
            // connection), the probe arms the CQ and the thread sleeps
            // event-driven; the next capsule's comp event resumes the spin.
            // One core per IO thread only while it is doing IO. The admin
            // queue (qid 0) never spins: its parked Async Event Request holds
            // a slot for the controller lifetime (so `idle()` is never true
            // there), and keep-alive latency does not merit a core.
            let poll = port.poll && self.nvme.qid != 0;
            let last_active = Rc::new(std::cell::Cell::new(std::time::Instant::now()));
            let spin_nvme = Rc::clone(&self.nvme);
            let spin_last = Rc::clone(&last_active);
            let spin = poll.then(|| {
                Box::new(move || {
                    !spin_nvme.slots.idle() || spin_last.get().elapsed() < SPIN_GRACE
                }) as Box<dyn Fn() -> bool>
            });
            let probe_nvme = Rc::clone(&self.nvme);
            ioutgt_uring::add_park_probe(Box::new(move || {
                let mut staged = shared.staged.borrow_mut();
                let drain = |staged: &mut Vec<(u64, bool)>| {
                    if let Ok(poller) = cq.start_poll() {
                        for wc in poller {
                            staged.push((
                                wc.wr_id(),
                                wc.status() == WorkCompletionStatus::Success as u32,
                            ));
                        }
                    }
                };
                drain(&mut staged);
                if poll && !staged.is_empty() {
                    last_active.set(std::time::Instant::now());
                }
                // Arm whenever this pass may end in a sleep: event mode
                // always; poll mode whenever the queue is idle — deliberately
                // IGNORING the spin grace here. The spin predicate is
                // re-evaluated after this probe with a later timestamp, so
                // gating the arm on the same grace check could disagree
                // across the 200 us boundary and let the pass sleep with the
                // CQ unarmed (a ~1 s backstop stall on the next capsule —
                // review finding). Arming strictly more often than the
                // predicate sleeps closes the race; the cost is at most one
                // spurious comp event per idle transition while grace-
                // spinning, which the fd poll simply drains.
                if staged.is_empty() && (!poll || probe_nvme.slots.idle()) {
                    // Nothing pending: arm so a completion during the coming
                    // sleep raises an event for the multishot poll, then
                    // re-check the race window.
                    if crate::cq::arm(&cq).is_err() {
                        return true; // never sleep on a broken CQ
                    }
                    drain(&mut staged);
                }
                if !staged.is_empty() {
                    if let Some(w) = shared.waker.borrow_mut().take() {
                        w.wake();
                    }
                    true
                } else {
                    false
                }
            }), spin)?
        };
        // Reap until peer-gone (a flushed completion), a CM Disconnected (`stop`),
        // or a fatal error; then drain and tear down. Each select arm yields
        // Ok(false) to keep going, Ok(true) to stop, or Err for a fatal error.
        let result: io::Result<()> = loop {
            let step: io::Result<bool> = tokio::select! {
                ev = comp_poll.next() => match ev {
                    // Multishot ended (fd closed/cancelled): peer gone, tear down.
                    None => Ok(true),
                    Some(Err(e)) => Err(e),
                    // Acknowledge without re-arming: the park-probe arms the
                    // CQ exactly when the thread goes to sleep, so an event
                    // (and its read(2)) happens at most once per idle
                    // transition instead of once per completion batch.
                    Some(Ok(revents)) => match crate::cq::acknowledge(revents, &self.channel, &self.cq) {
                        Err(e) => Err(e),
                        Ok(()) => self.process_cqes(&ctx, &mut comps),
                    },
                },
                // The park-probe staged completions while we were between
                // wakes: process them (drain_into consumes `staged` first).
                () = self.probe.staged_ready() => self.process_cqes(&ctx, &mut comps),
                Some(resp) = responses.next() => {
                    // Collect this wake's response and any siblings, then post
                    // them all on one doorbell. The first push wakes this task
                    // before its sibling slot tasks (woken by the same
                    // completion batch) have run, so yield once to let them
                    // dispatch and push before collecting — without it ~95% of
                    // response doorbells carry a single SEND (stat batch row).
                    resp_batch.clear();
                    resp_batch.push(resp);
                    tokio::task::yield_now().await;
                    while let Some(rr) = responses.try_next() {
                        resp_batch.push(rr);
                    }
                    self.post_responses_batch(&resp_batch).map(|()| false)
                }
                // CM Disconnected for this connection (the QP isn't cm_id-bound,
                // so this is the only prompt teardown signal).
                () = stop.notified() => Ok(true),
                // Backstop re-drain (persistent timer, above). The multishot reap
                // arm only fires on a comp-channel event; userspace ibverbs has no
                // `IB_CQ_REPORT_MISSED_EVENTS`, so a completion that races the
                // re-arm can be left in the CQ with no event delivered, and the
                // reactor's `PARK_SAFETY` only re-checks io_uring (not the RDMA
                // CQ) — which wedges the queue under sustained load. This timer
                // fires every `BACKSTOP` regardless (it is reset only after firing,
                // so a busy select! cannot starve it) and re-arms + re-drains, so a
                // stranded completion is recovered within one interval (the
                // userspace analog of nvmet-rdma's missed-events re-poll).
                // NB: no `?` in this arm — it would return from run() and skip
                // the remove_park_probe below, leaving a stale probe arming a
                // dead CQ on this (shared, long-lived) queue thread. Errors
                // must flow through `step` so every exit passes the removal.
                res = backstop.as_mut() => match res {
                    Err(e) => Err(e),
                    Ok(()) => {
                        let r = match crate::cq::arm(&self.cq) {
                            Err(e) => Err(e),
                            Ok(()) => self.process_cqes(&ctx, &mut comps),
                        };
                        match ioutgt_uring::ops::sleep(BACKSTOP) {
                            Err(e) => Err(e),
                            Ok(t) => {
                                backstop.set(t);
                                // Piggyback the keep-alive / controller-liveness
                                // watchdog on the backstop cadence (see
                                // [`Self::watchdog`]).
                                match r {
                                    Ok(false) => Ok(self.watchdog(&ctx)),
                                    other => other,
                                }
                            }
                        }
                    }
                }
            };
            match step {
                Ok(false) => {}
                Ok(true) => break Ok(()),
                Err(e) => break Err(e),
            }
        };

        // The probe polls the CQ from the reactor; remove it before any
        // teardown of the resources it touches (both the normal drop and the
        // wedged-backend leak path below).
        ioutgt_uring::remove_park_probe(probe_id);

        // Teardown: resolve parked AERs, then drain in-flight dispatches before
        // returning (returning drops `self` → the QP and the pool arena). A slot
        // task mid-dispatch may have a backend op in flight into the arena; the
        // in-flight RDMA WRs are handled by the QP destroy + Drop's CQ drain.
        // `ctx.close()` lets executing() reach 0. Bounded; the memory backend
        // dispatches synchronously, so this is ~instant there.
        tracing::debug!(qid = self.nvme.qid, "nvme-rdma: queue teardown");
        ctx.close();
        // Tear down the controller when its admin queue dies (TCP parity).
        // Removed before the drain so the IO queues' watchdogs see it gone and
        // follow promptly — an abruptly-vanished host sends no per-queue DREQs.
        if let Role::Admin(admin) = &ctx.role {
            let cntlid = admin.cntlid.get();
            if cntlid != 0 && ctx.registry.remove(cntlid).is_some() {
                tracing::info!(cntlid, "nvme-rdma: controller removed");
            }
        }
        let mut waited = 0u32;
        while self.nvme.slots.executing() > 0 && waited < 10_000 {
            match ioutgt_uring::ops::sleep(std::time::Duration::from_millis(2)) {
                Ok(s) => {
                    let _ = s.await;
                }
                Err(_) => break,
            }
            waited += 2;
        }
        if self.nvme.slots.executing() > 0 {
            // A wedged backend op (file backend) still references the pool arena.
            // Returning would drop `self` (dereg the MRs, free the arena) and the
            // slot tasks while the kernel may still write into the arena — a UAF.
            // Leak both instead, keeping the arena + the op's buffer alive for the
            // process's remaining lifetime (mirrors the TCP teardown). v1's memory
            // backend dispatches synchronously, so this is never reached.
            tracing::warn!(
                qid = self.nvme.qid,
                executing = self.nvme.slots.executing(),
                "nvme-rdma: teardown drain timed out; leaking queue + tasks"
            );
            std::mem::forget(slot_tasks);
            std::mem::forget(self);
            return result;
        }
        // All ops have drained: release the pool's fixed-buffer slot so the
        // index is reusable before the queue (and its arena) is freed on
        // return. The leak branch above intentionally keeps it pinned.
        if let Some(idx) = self.nvme.slots.pool().take_buf_index() {
            ioutgt_uring::unregister_pool_buffer(idx);
        }
        result
    }
}

/// Build the per-connection RDMA resources on the cm_id's *own* device context
/// (the QP must live on the exact `ibv_context` the connection landed on): a
/// non-blocking completion channel + a CQ bound to it (so completions are reaped
/// reactor-driven, [`crate::cq`]), a fresh PD, and an RC QP sized for `sqsize`
/// in-flight commands (one RECV + up to a WRITE and a SEND each).
#[allow(clippy::type_complexity)]
fn build_conn_resources(
    ctx: &Arc<DeviceContext>,
    sqsize: u16,
    qid: u16,
) -> io::Result<(
    Arc<ProtectionDomain>,
    Arc<CompletionChannel>,
    GenericCompletionQueue,
    GenericQueuePair,
)> {
    let channel = ctx.create_comp_channel().map_err(oerr)?;
    channel.set_nonblocking(true)?;
    let depth = u32::from(sqsize) * 3 + 16;
    // Spread queues across completion vectors (admin on 0, IO queues on 1, 2,
    // …) like nvmet-rdma (`comp_vector = idx % num_comp_vectors`). Sharing one
    // vector funnels every queue's CQ interrupts through a single MSI-X
    // EQ/CPU, so under sustained IO load the admin queue's keep-alive event is
    // starved behind the IO completions and the host times the controller out.
    // Devices with few vectors (rxe has one) reject a high index, so fall back
    // to vector 0.
    let make_cq = |vector: u32| {
        let mut cqb = ctx.create_cq_builder();
        cqb.setup_cqe(depth).setup_comp_channel(&channel, vector);
        cqb.build_ex()
    };
    let cq_ex = match make_cq(u32::from(qid)) {
        Ok(cq) => cq,
        Err(_) => make_cq(0).map_err(oerr)?,
    };
    let cq = GenericCompletionQueue::from(cq_ex);

    let pd = ctx.alloc_pd().map_err(oerr)?;
    let mut b = pd.create_qp_builder();
    b.setup_max_send_wr(u32::from(sqsize) * 2 + 8)
        .setup_max_recv_wr(u32::from(sqsize) + 8)
        // Up to MAX_DATA_SGE pool segments per RDMA WRITE of a read response.
        .setup_max_send_sge(MAX_DATA_SGE)
        .setup_max_recv_sge(1)
        .setup_send_cq(cq.clone())
        .setup_recv_cq(cq.clone())
        .setup_send_ops_flags(
            SendOperationFlags::Send
                | SendOperationFlags::SendWithInvalidate
                | SendOperationFlags::Write
                | SendOperationFlags::Read,
        );
    let qp: GenericQueuePair = b.build_ex().map_err(oerr)?.into();
    Ok((pd, channel, cq, qp))
}

/// QP send-SGE cap. Must equal the pool's max segments per lease so a fragmented
/// read response's RDMA WRITE (one sge per run) never exceeds `max_send_sge` —
/// otherwise `ibv_post_send` returns EINVAL and kills the connection.
#[allow(clippy::cast_possible_truncation)] // MAX_SEGS (32) trivially fits u32
const MAX_DATA_SGE: u32 = MAX_SEGS as u32;

/// Admin (qid 0) queue-depth cap (entries). The fabrics admin queue is small;
/// clamp a host's request to this regardless of what it asks for.
const ADMIN_QUEUE_DEPTH: u32 = 32;

/// An accepted connection handed off to be driven to completion. Every field is
/// `Send` — the cm_id is `Send`/`Sync` (librdmacm cm_id ops
/// are thread-safe), the rest are `Arc`s — so this can cross a mailbox to a queue
/// thread. This is the shape the harness `Transport::Conn` will take: the CM
/// listener produces it, and [`run_conn`] (the reactor-bound work) consumes it.
pub struct RdmaConn {
    /// The accepted CM identifier: its device context builds the QP, its
    /// CM-derived attrs drive INIT→RTS, and `rdma_accept` replies on it.
    pub id: Identifier,
    /// NVMe-oF queue id (0 = admin); routes the connection to a queue thread.
    pub qid: u16,
    /// Host SQ size, 0-based (the queue holds `hsqsize + 1`, clamped).
    pub hsqsize: u16,
    /// The served port model (subsystems/namespaces, advertised limits).
    pub port: Arc<PortConfig<AnyBackend>>,
    /// The controller registry (shared across this port's queues).
    pub registry: Arc<Registry>,
    /// Live-connection accounting permit; held for the connection's lifetime
    /// and dropped when its queue ends, so the harness's active count +
    /// idle-teardown track it.
    pub permit: ioutgt_core::permit::ConnPermit,
    /// Fired by the CM listener on this connection's `Disconnected` event; the
    /// reap loop ([`RdmaQueue::run`]) selects on it and ends the queue (our
    /// manually-built QP isn't cm_id-associated, so `rdma_disconnect` doesn't
    /// flush it — this is how a graceful host disconnect tears the queue down).
    pub stop: Arc<Notify>,
}

/// Build the queue for an accepted [`RdmaConn`] and drive it to completion: build
/// the QP on the cm_id's own device context, drive it to RTS via the CM-derived
/// attributes, prime the RECVs, `accept` with a [`CmRep`], then run
/// [`RdmaQueue::run`]. This is the reactor-bound half of accepting — it must run
/// on the thread whose io_uring reaps this queue's completions (the same reactor
/// thread today; a queue thread once wired into the harness). `on_ctx` is invoked
/// with the dispatch context after bootstrap (see [`RdmaQueue::run`]).
pub async fn run_conn(
    conn: RdmaConn,
    on_ctx: impl FnOnce(&Rc<ConnCtx<AnyBackend>>),
) -> io::Result<()> {
    let dev = conn
        .id
        .get_device_context()
        .ok_or_else(|| io::Error::other("connect request without device context"))?;

    // hsqsize is 0-based; clamp the queue depth to what we advertise (admin to the
    // fabrics AQ depth) so a buggy/hostile host can't over-size the QP/recv_buf.
    let cap = if conn.qid == 0 {
        ADMIN_QUEUE_DEPTH
    } else {
        u32::from(conn.port.io_queue_size).max(1)
    };
    let sqsize = u16::try_from((u32::from(conn.hsqsize) + 1).clamp(1, cap)).unwrap_or(1);
    let (pd, channel, cq, qp) = build_conn_resources(&dev, sqsize, conn.qid)?;
    conn.id.get_qp_attr(QueuePairState::Init)?.apply(&qp)?;
    conn.id
        .get_qp_attr(QueuePairState::ReadyToReceive)?
        .apply(&qp)?;
    // librdmacm has already computed this QP's `max_rd_atomic` (the max
    // write-data RDMA READs we can have outstanding) into the RTS attr as
    // min(the request's initiator_depth, device `max_qp_init_rd_atom`). Capture
    // it so the CM accept reply advertises the *same* value as the QP holds —
    // see the `accept` call below.
    let mut rts_attr = conn.id.get_qp_attr(QueuePairState::ReadyToSend)?;
    let initiator_depth = rts_attr.max_read_atomic();
    // Widen the IO queues' RC ACK timeout to 4.096us * 2^20 (~4.3s).
    // librdmacm derives a short timeout from the CM path; under sustained large
    // writes the host can retransmit before the target's batched RDMA READs
    // drain, tripping local_ack_timeout_err -> transport-retry exhaustion. But
    // NEVER widen the admin queue (qid 0): it carries the keep-alive, and a 4.3s
    // retransmit on a lost admin ACK would exceed the host KATO (which the harness
    // does not bump) and wedge QID 0 — the very stall we are avoiding. Admin keeps
    // the CM-default (short) timeout so keep-alive recovers fast. Diagnostic
    // mitigation, not a final design choice.
    if conn.qid != 0 {
        rts_attr.setup_timeout(20);
        tracing::debug!(
            qid = conn.qid,
            ack_timeout = 20,
            "widened IO-queue RC ACK timeout"
        );
    }
    rts_attr.apply(&qp)?;

    let mut queue = RdmaQueue::new(
        conn.qid,
        sqsize,
        false,
        pd,
        channel,
        Rc::new(cq),
        qp,
        conn.port.queue_buf_bytes,
    )?;
    let qp_num = queue.qp_number();
    // Post RECVs + arm before accepting, so the host's first capsule is caught.
    queue.prime()?;
    // crqsize is 0-based: report the (possibly clamped) queue size we built so
    // the host sizes its queue to match.
    let rep = CmRep {
        recfmt: CM_FMT_1_0,
        crqsize: sqsize - 1,
    }
    .to_bytes();
    // Mirror nvmet-rdma (drivers/nvme/target/rdma.c): the reply's
    // initiator_depth must equal the QP's `max_rd_atomic` so the host sets its
    // `max_dest_rd_atomic` to match and can service every concurrent write-data
    // RDMA READ we initiate. The old hardcoded `1` capped the host at one
    // outstanding read while the QP allowed the device max, so under write load
    // (qd > 1) the host NAK'd the 2nd+ read → transport retry exhaustion → QP
    // error → connection reset (reconnect storm). responder_resources is 0: an
    // nvme-rdma host never issues RDMA reads against the target.
    conn.id.accept(qp_num, &rep, 0, initiator_depth)?;

    let peer = format!("rdma:qid{}", conn.qid);
    queue
        .run(conn.port, conn.registry, peer, conn.stop, on_ctx)
        .await
}

/// A freshly accepted connection request, pre-QP-build: the cm_id plus the
/// host's [`CmReq`] routing fields. The CM-thread half of accepting (what the
/// harness `Transport::Raw` will be); [`RdmaListener::accept`] produces it and a
/// caller turns it into an [`RdmaConn`] (adding port/registry) for [`run_conn`].
pub struct RdmaRaw {
    /// The accepted CM identifier (see [`RdmaConn::id`]).
    pub id: Identifier,
    /// NVMe-oF queue id (0 = admin).
    pub qid: u16,
    /// Host SQ size, 0-based.
    pub hsqsize: u16,
    /// Fired by the listener when this connection's `Disconnected` arrives; the
    /// queue's reap loop ends on it (see [`RdmaConn::stop`]).
    pub stop: Arc<Notify>,
}

/// A live accepted connection the listener tracks: its cm_id (kept alive +
/// matched against later CM events) and the stop signal to end its queue.
struct ConnSlot {
    id: Identifier,
    stop: Arc<Notify>,
}

/// The NVMe/RDMA listener: a bound CM event channel that yields one accepted
/// connection per [`accept`](Self::accept), pumping (acking) the lifecycle events
/// of already-accepted connections in between. This is the connection-source seam
/// (the harness `Transport::bind`/`accept`); it owns the CM channel and holds
/// accepted cm_ids alive (best-effort teardown — see `docs/nvme-rdma.md`).
pub struct RdmaListener {
    ch: CmChannel,
    /// The listening cm_id — kept alive for the channel's lifetime.
    _listen_id: Identifier,
    /// Live accepted connections; entries are pruned on `Disconnected` (which
    /// also fires the connection's stop signal), bounding this across reconnects.
    conns: Vec<ConnSlot>,
}

impl RdmaListener {
    /// Bind a CM event channel + listen cm_id to `listen` and start listening.
    pub async fn bind(listen: SocketAddr) -> io::Result<RdmaListener> {
        let ch = CmChannel::new()?;
        let listen_id = ch.create_id()?;
        // The RDMA device's GID/IP association is populated asynchronously after a
        // soft-RoCE (rxe) netdev is added, so `rdma_bind_addr` on the concrete IP
        // can transiently fail with ENODEV even once the port is ACTIVE. Retry.
        // (Binding the unspecified address would skip GID resolution but does not
        // receive connects on rxe, so we bind the concrete IP.)
        let mut attempt = 0;
        loop {
            match listen_id.bind_addr(listen) {
                Ok(()) => break,
                Err(e) if attempt < 120 => {
                    attempt += 1;
                    if attempt % 8 == 0 {
                        tracing::info!(
                            "nvme-rdma bind {listen} not ready (attempt {attempt}): {e:?}"
                        );
                    }
                    ioutgt_uring::ops::sleep(std::time::Duration::from_millis(250))?.await?;
                }
                Err(e) => return Err(e),
            }
        }
        listen_id.listen(128)?;
        tracing::info!("nvme-rdma listening on {listen}");
        Ok(RdmaListener {
            ch,
            _listen_id: listen_id,
            conns: Vec::new(),
        })
    }

    /// Await the next accepted connection. The CM channel multiplexes all cm_ids,
    /// so this also acks the lifecycle events of already-accepted connections
    /// (Established, Disconnected, …) and rejects malformed connect requests,
    /// returning only on a valid CONNECT_REQUEST.
    pub async fn accept(&mut self) -> io::Result<RdmaRaw> {
        loop {
            let event = self.ch.next_event().await?;
            match event.event_type() {
                EventType::ConnectRequest => {
                    // Reject with the proper `nvme_rdma_cm_rej` status so the
                    // host logs the reason instead of a bare reject (nvmet
                    // parity). Adopt even when rejecting: ownership of the
                    // child cm_id is ours either way; dropping it destroys it.
                    let sts = match CmReq::parse(&event.private_data()) {
                        Ok(req) if req.recfmt != CM_FMT_1_0 => Err(reject_status::INVALID_RECFMT),
                        Ok(req) => Ok(req),
                        Err(_) => Err(reject_status::INVALID_LEN),
                    };
                    match (sts, self.ch.adopt(&event)) {
                        (Ok(req), Ok(id)) => {
                            let stop = Arc::new(Notify::new());
                            self.conns.push(ConnSlot {
                                id: id.clone(),
                                stop: Arc::clone(&stop),
                            });
                            event.ack().map_err(oerr)?;
                            return Ok(RdmaRaw {
                                id,
                                qid: req.qid,
                                hsqsize: req.hsqsize,
                                stop,
                            });
                        }
                        (Err(sts), id) => {
                            tracing::warn!(sts, "nvme-rdma rejecting connect request");
                            if let Ok(id) = id {
                                let _ = id.reject(&CmRej::new(sts).to_bytes());
                            }
                            event.ack().map_err(oerr)?;
                        }
                        (Ok(_), Err(e)) => {
                            tracing::warn!("nvme-rdma connect request without cm_id: {e}");
                            event.ack().map_err(oerr)?;
                        }
                    }
                }
                EventType::Established => event.ack().map_err(oerr)?,
                // The host tore the connection down: drop our keep-alive cm_id
                // clone so it isn't retained for the process lifetime (bounds
                // `conns` across reconnect churn — a reconnect-soak leak fix). The
                // queue's own clone (in its RdmaConn) drops when its reap loop ends
                // on the flushed completions, so the cm_id is destroyed then.
                EventType::Disconnected => {
                    // Match by raw pointer (never dereferenced): only a cm_id we
                    // still hold alive can compare equal. Send the DREP, fire the
                    // connection's stop signal so its reap loop ends (our
                    // manually-built QP isn't cm_id-associated, so
                    // rdma_disconnect doesn't flush it), and drop the slot —
                    // bounding `conns` across reconnects.
                    let raw = event.raw_id();
                    if let Some(pos) = self.conns.iter().position(|c| c.id.is_raw(raw)) {
                        let slot = self.conns.swap_remove(pos);
                        let _ = slot.id.disconnect();
                        slot.stop.notify_one();
                    }
                    event.ack().map_err(oerr)?;
                }
                other => {
                    tracing::debug!("nvme-rdma CM event {other:?}");
                    event.ack().map_err(oerr)?;
                }
            }
        }
    }
}
