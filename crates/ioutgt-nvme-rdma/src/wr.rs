//! Low-level RDMA work-request primitives: the wr_id encoding ([`WrKind`] /
//! [`WrId`]) and the single-SGE-per-run WR posting helpers ([`fill_sges`],
//! [`post_sge_runs`], [`qp_ex_of`], [`wr_send_with_inv`]) that
//! [`crate::target::RdmaQueue`] drives to post RECV/SEND/READ/WRITE work
//! requests.

use std::io;

use ioutgt_core::pool::MAX_SEGS;
use rdma_mummy_sys::{ibv_qp_ex, ibv_qp_to_qp_ex, ibv_sge, ibv_wr_send_inv, ibv_wr_set_sge};
use sideway::ibverbs::queue_pair::{
    GenericQueuePair, PostSendGuard, QueuePair, SetScatterGatherEntry, WorkRequestFlags,
};

/// The class of an RDMA work request, encoded in a wr_id's high byte (bits
/// 40..48); the low 32 bits carry the slot tag or recv-buffer index. Every
/// wr_id is built with [`WrId::encode`] and every completion decoded with
/// [`WrId::decode`], so the completion dispatch match stays exhaustive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WrKind {
    /// Command-capsule RECV (low bits = recv-buffer index).
    Recv,
    /// Response-capsule SEND (low bits = slot tag).
    Send,
    /// Read-data RDMA WRITE, target → host (low bits = slot tag).
    Write,
    /// Write-data RDMA READ, host → target (low bits = slot tag).
    Read,
}

impl WrKind {
    /// High-byte code — kept identical to the original `WR_*` constants (1..=4)
    /// so no posted/completed wr_id value changes.
    const fn code(self) -> u64 {
        match self {
            WrKind::Recv => 1,
            WrKind::Send => 2,
            WrKind::Write => 3,
            WrKind::Read => 4,
        }
    }

    /// The class named by a raw wr_id's high byte, or `None` if it is unknown
    /// (impossible for an id we posted — a defensive guard, not a wire case).
    pub(crate) fn from_id(id: u64) -> Option<WrKind> {
        match (id >> 40) & 0xff {
            1 => Some(WrKind::Recv),
            2 => Some(WrKind::Send),
            3 => Some(WrKind::Write),
            4 => Some(WrKind::Read),
            _ => None,
        }
    }

    /// Short label for completion-error diagnostics.
    pub(crate) fn name(self) -> &'static str {
        match self {
            WrKind::Recv => "RECV",
            WrKind::Send => "SEND",
            WrKind::Write => "WRITE",
            WrKind::Read => "READ",
        }
    }
}

/// A decoded work-request id: its [`WrKind`] class and the low-32-bit tag/index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WrId {
    pub(crate) kind: WrKind,
    pub(crate) low: u32,
}

impl WrId {
    pub(crate) fn new(kind: WrKind, low: u32) -> WrId {
        WrId { kind, low }
    }

    /// Pack into the raw `wr_id` posted on a work request.
    pub(crate) fn encode(self) -> u64 {
        (self.kind.code() << 40) | u64::from(self.low)
    }

    /// Unpack a completion's raw `wr_id`, or `None` if its class byte is unknown.
    pub(crate) fn decode(id: u64) -> Option<WrId> {
        WrKind::from_id(id).map(|kind| WrId {
            kind,
            low: (id & 0xffff_ffff) as u32,
        })
    }
}

/// Fill `sges` with the slot data's pool-lease segments covering its first
/// `len` bytes, tagged with `lkey`; returns the run count. A pool lease spans
/// at most [`MAX_SEGS`] runs, so the fixed array always suffices. The callers
/// emit one single-SGE work request per run (see `post_reads_batch` for why —
/// rdma-core's rxe provider corrupts multi-SGE WR lengths), so the array is
/// only a scratch enumeration of the lease's contiguous runs.
// A run never exceeds the lease length <= MDTS (128 KiB) < u32::MAX.
#[allow(clippy::cast_possible_truncation)]
pub(crate) fn fill_sges(
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

/// The one-sided op a batched data-transfer WR issues over a pool run.
#[derive(Clone, Copy)]
pub(crate) enum SgeOp {
    /// RDMA READ — pull host write-data into the slot.
    Read,
    /// RDMA WRITE — push slot read-data to the host.
    Write,
}

/// Post one single-SGE work request per contiguous pool run in `sges`, all on
/// the open guard `g`, advancing the remote address run by run; only the last
/// run is signaled (so one completion per command, RC-ordered). Returns the WR
/// count. Shared by [`RdmaQueue::post_reads_batch`] (READ) and
/// [`RdmaQueue::post_responses_batch`]'s read-data WRITE.
///
/// Single-SGE per run on purpose, not just simplicity: rdma-core's rxe provider
/// (≤ v61) computes a multi-SGE WR's total length as `num_sge × sge[0].length`
/// (`wr_set_sge_list` never advances the list pointer), inflating the wire RETH
/// length of a fragmented lease — the host responder then NAKs Remote Access and
/// kills the QP (found by xfstests generic/113). Single-SGE WRs are immune on
/// every provider, and the common unfragmented lease still posts exactly one WR.
pub(crate) fn post_sge_runs<G: PostSendGuard>(
    g: &mut G,
    wr_id: u64,
    op: SgeOp,
    rkey: u32,
    base: u64,
    lkey: u32,
    sges: &[ibv_sge],
) -> u64 {
    let mut remote = base;
    for (i, sge) in sges.iter().enumerate() {
        let flags = if i + 1 == sges.len() {
            WorkRequestFlags::Signaled
        } else {
            WorkRequestFlags::none()
        };
        let wr = g.construct_wr(wr_id, flags);
        let h = match op {
            SgeOp::Read => wr.setup_read(rkey, remote),
            SgeOp::Write => wr.setup_write(rkey, remote),
        };
        // SAFETY: the sge references the registered pool arena (`lkey`); the
        // slot stays leased until its response WRs complete (its tag is not
        // released until then), so the memory outlives the transfer.
        unsafe { h.setup_sge(lkey, sge.addr, sge.length) };
        remote += u64::from(sge.length);
    }
    sges.len() as u64
}

/// The extended-verbs handle of `qp`, for work-request calls sideway does not
/// wrap. All target QPs are built with `build_ex` (asserted), so the handle is
/// valid whenever a post-send guard session is open on `qp`.
pub(crate) fn qp_ex_of(qp: &GenericQueuePair) -> io::Result<std::ptr::NonNull<ibv_qp_ex>> {
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
pub(crate) unsafe fn wr_send_with_inv(
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

#[cfg(test)]
mod tests {
    use super::*;

    /// `WrId` must round-trip and keep the exact legacy bit layout (high byte
    /// 1..=4 << 40, low 32 = tag/idx), because posted and completed wr_ids are
    /// matched by these bits — an off-by-one would misroute completions.
    #[test]
    fn wr_id_round_trips_with_legacy_layout() {
        for kind in [WrKind::Recv, WrKind::Send, WrKind::Write, WrKind::Read] {
            for low in [0u32, 1, 7, 0xffff_ffff] {
                let decoded = WrId::decode(WrId::new(kind, low).encode()).expect("known kind");
                assert_eq!(decoded, WrId { kind, low });
            }
        }
        // Exact values the old `WR_* | low` encoding produced.
        assert_eq!(WrId::new(WrKind::Recv, 7).encode(), (1 << 40) | 7);
        assert_eq!(WrId::new(WrKind::Send, 7).encode(), (2 << 40) | 7);
        assert_eq!(WrId::new(WrKind::Write, 7).encode(), (3 << 40) | 7);
        assert_eq!(WrId::new(WrKind::Read, 7).encode(), (4 << 40) | 7);
        // An unknown class byte decodes to None (the defensive guard).
        assert_eq!(WrId::decode(9 << 40), None);
    }
}
