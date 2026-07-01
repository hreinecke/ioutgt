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
//! Single-threaded (one reactor thread owns the QP/CQ/MR pool); not yet wired
//! into the harness pool. Completions are reaped reactor-driven via [`crate::cq`].

use std::cell::Cell;
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
use ioutgt_nvme::spec::{Cqe, Sqe, io_opcode};
use ioutgt_nvme::status;
use rdma_mummy_sys::ibv_sge;
use sideway::ibverbs::AccessFlags;
use sideway::ibverbs::completion::{CompletionChannel, GenericCompletionQueue, WorkCompletionStatus};
use sideway::ibverbs::device_context::DeviceContext;
use sideway::ibverbs::memory_region::MemoryRegion;
use sideway::ibverbs::protection_domain::ProtectionDomain;
use sideway::ibverbs::queue_pair::{
    GenericQueuePair, PostSendGuard, QueuePair, QueuePairState, SendOperationFlags,
    SetScatterGatherEntry, WorkRequestFlags,
};
use sideway::rdmacm::communication_manager::{EventType, Identifier};
use tokio::sync::Notify;
use tokio::task::JoinSet;
use zerocopy::{FromBytes, IntoBytes};

use crate::cm::{CmChannel, accept, private_data, reject};
use crate::cmproto::{CM_FMT_1_0, CmRep, CmReq};

/// Bytes of an NVMe SQE.
const SQE_LEN: usize = 64;
/// Max in-capsule data we accept (the fabrics Connect carries 1024 B).
const ICD_LEN: usize = 1024;
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
/// stranded completion). Well under the keep-alive timeout so a stranded
/// completion can never starve the controller into a host-side reset; only
/// fires when the queue is otherwise idle, so it costs nothing under load.
const BACKSTOP: std::time::Duration = std::time::Duration::from_millis(200);

/// SGL descriptor type byte (dptr offset 15). High nibble `0x4` =
/// `NVME_KEY_SGL_FMT_DATA_DESC` (keyed: host-resident, RDMA READ/WRITE); anything
/// else here is an in-capsule data+offset descriptor (inline).
const SGL_TYPE_OFFSET: usize = 24 + 15;
const KEYED_SGL_TYPE_HI: u8 = 0x4;
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

fn oerr<E: std::error::Error>(e: E) -> io::Error {
    io::Error::other(format!("{e:?}"))
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
/// pull (RDMA READ) before dispatch. v1 has no write-data path, so these are
/// failed; admin commands in the connect/discovery path carry no host data.
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

/// A finished command's response, handed from a slot task (or the reap loop's
/// validation-failure path) to the reap loop, which owns the QP and posts it.
struct RdmaResp {
    tag: u16,
    cmd: Sqe,
    outcome: Outcome,
}

/// Per-queue RDMA work-request counters, one class each for READ (host
/// write-data pulls), WRITE (read-data pushes), SEND (CQE capsules), and RECV
/// (command capsules). `posted`/`done` are cumulative (reset by GET_STATS
/// `clear`); `inflight` is a live gauge (posted−done), never reset, so it stays
/// accurate across a clear. Reported under `"wr"` in GET_STATS via
/// [`TransportStats`]. All access is on the owning queue thread (`Cell`).
#[derive(Debug, Default)]
struct RdmaWrStats {
    read_posted: Cell<u64>,
    read_done: Cell<u64>,
    read_inflight: Cell<i64>,
    write_posted: Cell<u64>,
    write_done: Cell<u64>,
    write_inflight: Cell<i64>,
    send_posted: Cell<u64>,
    send_done: Cell<u64>,
    send_inflight: Cell<i64>,
    recv_posted: Cell<u64>,
    recv_done: Cell<u64>,
    recv_inflight: Cell<i64>,
    /// Non-empty CQ polls (completion batches). `*_done / poll_batches` is the
    /// average number of each WR class reaped per batch.
    poll_batches: Cell<u64>,
    /// Send-queue doorbells rung (`ibv_post_send` calls for READ/WRITE/SEND).
    /// `(read+write+send)_posted / sq_doorbells` is the submission batch size —
    /// 1.0 with one WR per post, higher once WRs are chained per doorbell.
    sq_doorbells: Cell<u64>,
}

impl RdmaWrStats {
    /// Count a posted WR: bump the cumulative `posted` and the live `inflight`.
    #[inline]
    fn post(posted: &Cell<u64>, inflight: &Cell<i64>) {
        posted.set(posted.get() + 1);
        inflight.set(inflight.get() + 1);
    }

    /// Count a send-queue WR post plus the doorbell it rings (one WR per
    /// `ibv_post_send` today). Once WRs are chained, the doorbell count moves to
    /// the batched flush and this reverts to [`post`].
    #[inline]
    fn sq_post(posted: &Cell<u64>, inflight: &Cell<i64>, doorbells: &Cell<u64>) {
        Self::post(posted, inflight);
        doorbells.set(doorbells.get() + 1);
    }

    /// Count a completed WR: bump cumulative `done`, drop the live `inflight`.
    #[inline]
    fn complete(done: &Cell<u64>, inflight: &Cell<i64>) {
        done.set(done.get() + 1);
        inflight.set(inflight.get() - 1);
    }
}

impl TransportStats for RdmaWrStats {
    fn snapshot(&self) -> Vec<(&'static str, u64)> {
        let gauge = |c: &Cell<i64>| u64::try_from(c.get().max(0)).unwrap_or(0);
        vec![
            ("read_posted", self.read_posted.get()),
            ("read_done", self.read_done.get()),
            ("read_inflight", gauge(&self.read_inflight)),
            ("write_posted", self.write_posted.get()),
            ("write_done", self.write_done.get()),
            ("write_inflight", gauge(&self.write_inflight)),
            ("send_posted", self.send_posted.get()),
            ("send_done", self.send_done.get()),
            ("send_inflight", gauge(&self.send_inflight)),
            ("recv_posted", self.recv_posted.get()),
            ("recv_done", self.recv_done.get()),
            ("recv_inflight", gauge(&self.recv_inflight)),
            ("poll_batches", self.poll_batches.get()),
            ("sq_doorbells", self.sq_doorbells.get()),
        ]
    }

    fn reset(&self) {
        for c in [
            &self.read_posted,
            &self.read_done,
            &self.write_posted,
            &self.write_done,
            &self.send_posted,
            &self.send_done,
            &self.recv_posted,
            &self.recv_done,
            &self.poll_batches,
            &self.sq_doorbells,
        ] {
            c.set(0);
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
    cq: GenericCompletionQueue,
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
        cq: GenericCompletionQueue,
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
        let pool_mr = unsafe { pd.reg_mr(ptr as usize, len, AccessFlags::LocalWrite) }.map_err(oerr)?;
        let pool_lkey = pool_mr.lkey();

        let nslots = u32::from(sqsize);
        let mut recv_buf = vec![0u8; sqsize as usize * CAPSULE_LEN];
        // SAFETY: recv_buf outlives recv_mr (both held in this struct); the NIC
        // writes received capsules into it.
        let recv_mr =
            unsafe { pd.reg_mr(recv_buf.as_mut_ptr() as usize, recv_buf.len(), AccessFlags::LocalWrite) }
                .map_err(oerr)?;
        let mut resp_buf = vec![0u8; sqsize as usize * CQE_LEN];
        // SAFETY: resp_buf outlives resp_mr; a SEND source is locally read.
        let resp_mr =
            unsafe { pd.reg_mr(resp_buf.as_mut_ptr() as usize, resp_buf.len(), AccessFlags::none()) }
                .map_err(oerr)?;
        let mut cdata_buf = vec![0u8; ICD_LEN];
        // SAFETY: cdata_buf outlives cdata_mr; the NIC writes the RDMA-READ data.
        let cdata_mr =
            unsafe { pd.reg_mr(cdata_buf.as_mut_ptr() as usize, cdata_buf.len(), AccessFlags::LocalWrite) }
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
        })
    }

    fn recv_slice(&self, idx: u32) -> &[u8] {
        let off = idx as usize * CAPSULE_LEN;
        &self.recv_buf[off..off + CAPSULE_LEN]
    }

    /// (Re-)post the RECV for capsule buffer `idx`.
    fn post_recv(&mut self, idx: u32) -> io::Result<()> {
        let off = idx as usize * CAPSULE_LEN;
        let addr = self.recv_buf.as_ptr() as u64 + off as u64;
        let lkey = self.recv_mr.lkey();
        let mut g = self.qp.start_post_recv();
        let h = g.construct_wr(wr(WR_RECV, idx));
        // SAFETY: the region is registered (recv_mr) and stays valid; the NIC
        // writes the next capsule here.
        unsafe { h.setup_sge(lkey, addr, CAPSULE_LEN as u32) };
        g.post().map_err(oerr)?;
        RdmaWrStats::post(&self.wr.recv_posted, &self.wr.recv_inflight);
        Ok(())
    }

    /// SEND the 16-byte CQE response capsule for `tag` from the staging buffer. When
    /// `invalidate_rkey` is `Some` (the command's keyed SGL requested it), use
    /// `SEND_WITH_INV` so the host's rkey is invalidated remotely instead of the host
    /// posting a per-IO local-invalidate WR (whose extra completion, on the comp vector
    /// the host shares between admin + IO, otherwise overloads the host's CQ softirq).
    fn send_cqe(&mut self, tag: u32, cqe: Cqe, invalidate_rkey: Option<u32>) -> io::Result<()> {
        let off = tag as usize * CQE_LEN;
        self.resp_buf[off..off + CQE_LEN].copy_from_slice(cqe.as_bytes());
        let addr = self.resp_buf.as_ptr() as u64 + off as u64;
        let lkey = self.resp_mr.lkey();
        let mut g = self.qp.start_post_send();
        let wrh = g.construct_wr(wr(WR_SEND, tag), WorkRequestFlags::Signaled);
        let h = match invalidate_rkey {
            Some(rkey) => wrh.setup_send_with_inv(rkey),
            None => wrh.setup_send(),
        };
        // SAFETY: the staging region is registered and stays valid until the
        // send completes (tag not released until then).
        unsafe { h.setup_sge(lkey, addr, CQE_LEN as u32) };
        g.post().map_err(oerr)?;
        RdmaWrStats::sq_post(&self.wr.send_posted, &self.wr.send_inflight, &self.wr.sq_doorbells);
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
        RdmaWrStats::sq_post(&self.wr.read_posted, &self.wr.read_inflight, &self.wr.sq_doorbells);
        Ok(())
    }

    /// RDMA READ `len` bytes from the host's keyed-SGL region (`src`) into
    /// `slot(tag)`'s leased pool-backed segments, so the deferred dispatch task
    /// (spawned on the `WR_READ` completion) sees the host write-data already in
    /// the slot. The caller must have confirmed the lease is pool-backed.
    fn post_read_data(&mut self, tag: u32, len: usize, src: &KeyedSgl) -> io::Result<()> {
        let mut sges = [ibv_sge {
            addr: 0,
            length: 0,
            lkey: 0,
        }; MAX_SEGS];
        let mut n = 0usize;
        let mut remaining = len;
        {
            let data = self.nvme.slots.slot(tag as u16).data();
            for seg in data.segs() {
                if remaining == 0 {
                    break;
                }
                let take = remaining.min(seg.len);
                sges[n] = ibv_sge {
                    addr: seg.ptr as u64,
                    length: take as u32,
                    lkey: self.pool_lkey,
                };
                n += 1;
                remaining -= take;
            }
        }
        let mut g = self.qp.start_post_send();
        let h = g
            .construct_wr(wr(WR_READ, tag), WorkRequestFlags::Signaled)
            .setup_read(src.rkey, src.addr);
        // SAFETY: the sges reference the registered pool arena (pool_lkey,
        // is_pool-checked by the caller); the slot stays leased until the slot's
        // SEND completes (tag not released until then).
        unsafe { h.setup_sge_list(&sges[..n]) };
        g.post().map_err(oerr)?;
        RdmaWrStats::sq_post(&self.wr.read_posted, &self.wr.read_inflight, &self.wr.sq_doorbells);
        Ok(())
    }

    /// Drain all currently-available completions as `(wr_id, success)` into the
    /// reused `out` buffer (cleared first), so the steady IO path allocates none.
    fn drain_into(&self, out: &mut Vec<(u64, bool)>) {
        out.clear();
        if let Ok(poller) = self.cq.start_poll() {
            for wc in poller {
                out.push((wc.wr_id(), wc.status() == WorkCompletionStatus::Success as u32));
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
        }
        // `comps` is a local buffer (disjoint from `self`), so iterating it while
        // calling `&mut self` handlers is sound.
        for &(id, ok) in comps.iter() {
            if !ok {
                // Flushed completion: the peer is gone.
                return Ok(true);
            }
            match wr_kind(id) {
                WR_RECV => RdmaWrStats::complete(&self.wr.recv_done, &self.wr.recv_inflight),
                WR_READ => RdmaWrStats::complete(&self.wr.read_done, &self.wr.read_inflight),
                WR_SEND => RdmaWrStats::complete(&self.wr.send_done, &self.wr.send_inflight),
                WR_WRITE => RdmaWrStats::complete(&self.wr.write_done, &self.wr.write_inflight),
                _ => {}
            }
            match wr_kind(id) {
                WR_RECV => self.handle_recv(ctx, wr_low(id))?,
                // A write-data RDMA READ finished: the slot is filled, so submit
                // it to wake its slot task.
                WR_READ => self.submit_pending(wr_low(id) as u16),
                WR_SEND | WR_WRITE => self.on_response_done(wr_low(id)),
                _ => {}
            }
        }
        Ok(false)
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

    /// Park on the completion channel until the next RECV (command capsule)
    /// completes, returning its buffer index; service response completions
    /// (release slots) in the meantime.
    async fn next_recv(&mut self) -> io::Result<u32> {
        // Bootstrap-only (one Connect capsule), so this one-time buffer is fine.
        let mut comps: Vec<(u64, bool)> = Vec::with_capacity(8);
        loop {
            crate::cq::wait(&self.channel, &self.cq).await?;
            self.drain_into(&mut comps);
            for &(id, ok) in &comps {
                if !ok {
                    return Err(io::Error::other("RDMA completion error (peer gone?)"));
                }
                match wr_kind(id) {
                    WR_RECV => {
                        RdmaWrStats::complete(&self.wr.recv_done, &self.wr.recv_inflight);
                        return Ok(wr_low(id));
                    }
                    WR_SEND => {
                        RdmaWrStats::complete(&self.wr.send_done, &self.wr.send_inflight);
                        self.on_response_done(wr_low(id));
                    }
                    WR_WRITE => {
                        RdmaWrStats::complete(&self.wr.write_done, &self.wr.write_inflight);
                        self.on_response_done(wr_low(id));
                    }
                    _ => {}
                }
            }
        }
    }

    /// Park until the pending connect-data RDMA READ (`WR_READ`) completes,
    /// servicing any response completions meanwhile. Bootstrap-only.
    async fn await_read(&mut self) -> io::Result<()> {
        let mut comps: Vec<(u64, bool)> = Vec::with_capacity(4);
        loop {
            crate::cq::wait(&self.channel, &self.cq).await?;
            self.drain_into(&mut comps);
            for &(id, ok) in &comps {
                if !ok {
                    return Err(io::Error::other("RDMA READ completion error"));
                }
                match wr_kind(id) {
                    WR_READ => {
                        RdmaWrStats::complete(&self.wr.read_done, &self.wr.read_inflight);
                        return Ok(());
                    }
                    WR_SEND => {
                        RdmaWrStats::complete(&self.wr.send_done, &self.wr.send_inflight);
                        self.on_response_done(wr_low(id));
                    }
                    WR_WRITE => {
                        RdmaWrStats::complete(&self.wr.write_done, &self.wr.write_inflight);
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
        // The capsule buffer is free once the SQE is parsed — re-arm it.
        self.post_recv(idx)?;
        let Some(tag) = self.nvme.claim_tag() else {
            return Err(io::Error::other("no free tag for command"));
        };

        if !host_data_in(&ctx.role, sqe.opcode) {
            self.nvme.submit(tag, sqe);
            return Ok(());
        }

        // RDMA v1 advertises no in-capsule data (ioccsz=4), so a conformant host
        // always sends a keyed SGL here. Reject anything else cleanly rather than
        // misparse an in-capsule descriptor into a bogus rkey (which would fail
        // the RDMA READ and tear the queue down).
        if sqe.as_bytes()[SGL_TYPE_OFFSET] >> 4 != KEYED_SGL_TYPE_HI {
            self.fail_recv(ctx, tag, &sqe, status::SGL_INVALID_TYPE | status::DNR);
            return Ok(());
        }
        let sgl = parse_keyed_sgl(&sqe);
        let len = (sgl.len as usize).min(ioutgt_core::MDTS_BYTES as usize);
        if len == 0 {
            self.fail_recv(ctx, tag, &sqe, status::DATA_SGL_LEN_INVALID | status::DNR);
            return Ok(());
        }
        // Lease a pool buffer for the host write-data. `lease_or_owned` sets the
        // capacity; io::write/dsm check `data_len()` (the *received* length), so
        // set it to the SGL-advertised length the RDMA READ will fill.
        self.nvme.lease_or_owned(tag, len);
        self.nvme.slots.slot(tag).set_data_len(len as u32);
        if !self.nvme.slots.slot(tag).data().is_pool() {
            // Pool momentarily full → the owned fallback is not in the RDMA MR, so
            // we cannot RDMA into it. Fail cleanly (the host retries).
            self.fail_recv(ctx, tag, &sqe, status::DATA_XFER_ERROR | status::DNR);
            return Ok(());
        }
        // Stash the SQE; RDMA READ the host data and submit on its completion.
        self.pending_read[tag as usize] = sqe;
        self.post_read_data(u32::from(tag), len, &sgl)
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
        let mut g = qp.start_post_send();
        for resp in batch {
            let tag = resp.tag;
            let mut pending = 1u8;
            if resp.outcome.data_len > 0 {
                // A pool lease spans at most MAX_SEGS runs; a fixed stack array
                // holds every sge with no allocation. The extended guard copies
                // the sges into the WQE at `setup_sge_list`, so reusing this
                // array across iterations is sound.
                let mut sges = [ibv_sge {
                    addr: 0,
                    length: 0,
                    lkey: 0,
                }; MAX_SEGS];
                let mut n = 0usize;
                let mut remaining = resp.outcome.data_len as usize;
                {
                    let data = nvme.slots.slot(tag).data();
                    for seg in data.segs() {
                        if remaining == 0 {
                            break;
                        }
                        let take = remaining.min(seg.len);
                        sges[n] = ibv_sge {
                            addr: seg.ptr as u64,
                            length: take as u32,
                            lkey: pool_lkey,
                        };
                        n += 1;
                        remaining -= take;
                    }
                }
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
            let ws = g.construct_wr(wr(WR_SEND, u32::from(tag)), WorkRequestFlags::Signaled);
            let hs = match invalidate_rkey_for(&resp.cmd) {
                Some(rkey) => ws.setup_send_with_inv(rkey),
                None => ws.setup_send(),
            };
            // SAFETY: the staging region is registered and stays valid until the
            // SEND completes (tag not released until then).
            unsafe { hs.setup_sge(resp_lkey, cqe_addr, CQE_LEN as u32) };
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
        for _ in 0..writes {
            RdmaWrStats::post(&stats.write_posted, &stats.write_inflight);
        }
        for _ in 0..sends {
            RdmaWrStats::post(&stats.send_posted, &stats.send_inflight);
        }
        stats.sq_doorbells.set(stats.sq_doorbells.get() + 1);
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
        let idx = self.next_recv().await?;
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
                    ConnectData::read_from_bytes(&s[SQE_LEN..SQE_LEN + ICD_LEN])
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
            self.await_read().await?;
            Box::new(
                ConnectData::read_from_bytes(&self.cdata_buf[..ICD_LEN])
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
        self.send_cqe(u32::from(tag), outcome.cqe, invalidate_rkey_for(&sqe))?;
        self.inflight[tag as usize] = 1;
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
        // Reap until peer-gone (a flushed completion), a CM Disconnected (`stop`),
        // or a fatal error; then drain and tear down. Each select arm yields
        // Ok(false) to keep going, Ok(true) to stop, or Err for a fatal error.
        let result: io::Result<()> = loop {
            let step: io::Result<bool> = tokio::select! {
                res = crate::cq::wait(&self.channel, &self.cq) => match res {
                    Err(e) => Err(e),
                    Ok(()) => self.process_cqes(&ctx, &mut comps),
                },
                Some(resp) = responses.next() => {
                    // Collect this wake's response and any siblings queued in the
                    // same wake, then post them all on one doorbell.
                    resp_batch.clear();
                    resp_batch.push(resp);
                    while let Some(rr) = responses.try_next() {
                        resp_batch.push(rr);
                    }
                    self.post_responses_batch(&resp_batch).map(|()| false)
                }
                // CM Disconnected for this connection (the QP isn't cm_id-bound,
                // so this is the only prompt teardown signal).
                () = stop.notified() => Ok(true),
                // Backstop re-drain. The reap above sleeps on the comp-channel
                // event (`cq::wait`'s `POLL_ADD`); userspace ibverbs has no
                // `IB_CQ_REPORT_MISSED_EVENTS`, so a completion that races the
                // re-arm can be left in the CQ with no event delivered, and the
                // reactor's `PARK_SAFETY` only re-checks io_uring (not the RDMA
                // CQ) — wedging the queue under sustained load. This timer fires
                // ONLY when no completion/response woke us for `BACKSTOP`, then
                // re-arms + drains so any stranded completion is recovered (the
                // userspace analog of nvmet-rdma's missed-events re-poll). When
                // the queue is busy it is re-created every iteration and never
                // elapses, so steady-state cost is nil.
                () = async {
                    if let Ok(s) = ioutgt_uring::ops::sleep(BACKSTOP) {
                        let _ = s.await;
                    }
                } => match crate::cq::arm(&self.cq) {
                    Err(e) => Err(e),
                    Ok(()) => self.process_cqes(&ctx, &mut comps),
                },
            };
            match step {
                Ok(false) => {}
                Ok(true) => break Ok(()),
                Err(e) => break Err(e),
            }
        };

        // Teardown: resolve parked AERs, then drain in-flight dispatches before
        // returning (returning drops `self` → the QP and the pool arena). A slot
        // task mid-dispatch may have a backend op in flight into the arena; the
        // in-flight RDMA WRs are handled by the QP destroy + Drop's CQ drain.
        // `ctx.close()` lets executing() reach 0. Bounded; the memory backend
        // dispatches synchronously, so this is ~instant there.
        tracing::debug!(qid = self.nvme.qid, "nvme-rdma: queue teardown");
        ctx.close();
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
/// `Send` — the cm_id is `Send`/`Sync` (sideway declares it; librdmacm cm_id ops
/// are thread-safe), the rest are `Arc`s — so this can cross a mailbox to a queue
/// thread. This is the shape the harness `Transport::Conn` will take: the CM
/// listener produces it, and [`run_conn`] (the reactor-bound work) consumes it.
pub struct RdmaConn {
    /// The accepted CM identifier: its device context builds the QP, its
    /// CM-derived attrs drive INIT→RTS, and `rdma_accept` replies on it.
    pub id: Arc<Identifier>,
    /// NVMe-oF queue id (0 = admin); routes the connection to a queue thread.
    pub qid: u16,
    /// Host SQ size, 0-based (the queue holds `hsqsize + 1`, clamped).
    pub hsqsize: u16,
    /// The served port model (subsystems/namespaces, advertised limits).
    pub port: Arc<PortConfig<AnyBackend>>,
    /// The controller registry (shared across this port's queues).
    pub registry: Arc<Registry>,
    /// Live-connection accounting permit (harness path); held for the
    /// connection's lifetime and dropped when its queue ends, so the active
    /// count + idle-teardown track it. `None` on the bare `serve()` path.
    pub permit: Option<ioutgt_core::permit::ConnPermit>,
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
    let (pd, channel, cq, mut qp) = build_conn_resources(&dev, sqsize, conn.qid)?;
    qp.modify(&conn.id.get_qp_attr(QueuePairState::Init).map_err(oerr)?)
        .map_err(oerr)?;
    qp.modify(&conn.id.get_qp_attr(QueuePairState::ReadyToReceive).map_err(oerr)?)
        .map_err(oerr)?;
    // librdmacm has already computed this QP's `max_rd_atomic` (the max
    // write-data RDMA READs we can have outstanding) into the RTS attr as
    // min(the request's initiator_depth, device `max_qp_init_rd_atom`). Capture
    // it so the CM accept reply advertises the *same* value as the QP holds —
    // see the `accept` call below.
    let rts_attr = conn.id.get_qp_attr(QueuePairState::ReadyToSend).map_err(oerr)?;
    let initiator_depth = rts_attr.max_read_atomic();
    qp.modify(&rts_attr).map_err(oerr)?;

    let mut queue = RdmaQueue::new(
        conn.qid,
        sqsize,
        false,
        pd,
        channel,
        cq,
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
    accept(&conn.id, qp_num, &rep, 0, initiator_depth)?;

    let peer = format!("rdma:qid{}", conn.qid);
    queue.run(conn.port, conn.registry, peer, conn.stop, on_ctx).await
}

/// A freshly accepted connection request, pre-QP-build: the cm_id plus the
/// host's [`CmReq`] routing fields. The CM-thread half of accepting (what the
/// harness `Transport::Raw` will be); [`RdmaListener::accept`] produces it and a
/// caller turns it into an [`RdmaConn`] (adding port/registry) for [`run_conn`].
pub struct RdmaRaw {
    /// The accepted CM identifier (see [`RdmaConn::id`]).
    pub id: Arc<Identifier>,
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
    id: Arc<Identifier>,
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
    _listen_id: Arc<Identifier>,
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
                Err(e) => return Err(oerr(e)),
            }
        }
        listen_id.listen(128).map_err(oerr)?;
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
                EventType::ConnectRequest => match CmReq::parse(&private_data(&event)) {
                    Ok(req) => {
                        let Some(id) = event.cm_id() else {
                            tracing::warn!("nvme-rdma connect request without cm_id");
                            event.ack().map_err(oerr)?;
                            continue;
                        };
                        let stop = Arc::new(Notify::new());
                        self.conns.push(ConnSlot {
                            id: Arc::clone(&id),
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
                    Err(e) => {
                        tracing::warn!("nvme-rdma rejecting connect: {e}");
                        if let Some(id) = event.cm_id() {
                            let _ = reject(&id, &[]);
                        }
                        event.ack().map_err(oerr)?;
                    }
                },
                EventType::Established => event.ack().map_err(oerr)?,
                // The host tore the connection down: drop our keep-alive cm_id
                // clone so it isn't retained for the process lifetime (bounds
                // `conns` across reconnect churn — a reconnect-soak leak fix). The
                // queue's own clone (in its RdmaConn) drops when its reap loop ends
                // on the flushed completions, so the cm_id is destroyed then.
                EventType::Disconnected => {
                    if let Some(id) = event.cm_id() {
                        // Send the DREP, then fire this connection's stop signal so
                        // its reap loop ends (our manually-built QP isn't
                        // cm_id-associated, so rdma_disconnect doesn't flush it),
                        // and drop the slot — bounding `conns` across reconnects.
                        let _ = id.disconnect();
                        if let Some(pos) = self.conns.iter().position(|c| Arc::ptr_eq(&c.id, &id)) {
                            self.conns.swap_remove(pos).stop.notify_one();
                        }
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

/// Listen for NVMe/RDMA connections on `listen` and drive each to a controller.
///
/// Focused-v1: a single reactor thread owns the listener and every queue. Each
/// accepted connection ([`RdmaListener::accept`]) becomes an [`RdmaConn`] and is
/// spawned on the same thread via [`run_conn`] (which builds the QP, accepts, and
/// runs the queue). The CM channel multiplexes all cm_ids; data completions are
/// reaped per queue via its completion channel. Teardown is best-effort — see
/// `docs/nvme-rdma.md`. This is the seam the harness `Transport` will split:
/// `bind`/`accept` on the listener, `run_conn` on a queue thread.
pub async fn serve(
    listen: SocketAddr,
    port: Arc<PortConfig<AnyBackend>>,
    registry: Arc<Registry>,
) -> io::Result<()> {
    let mut listener = RdmaListener::bind(listen).await?;
    loop {
        let raw = listener.accept().await?;
        let conn = RdmaConn {
            id: raw.id,
            qid: raw.qid,
            hsqsize: raw.hsqsize,
            port: Arc::clone(&port),
            registry: Arc::clone(&registry),
            permit: None,
            stop: raw.stop,
        };
        tokio::task::spawn_local(async move {
            // A failure inside run_conn (QP build / accept / run) only logs — the
            // host times out rather than getting a CM reject. Rare resource paths;
            // the common reject (CmReq parse) happens in `RdmaListener::accept`,
            // and the queue-thread model cannot reject post-handshake either.
            // serve() has no stats/AER registry to hook; the harness passes a
            // real on_ctx in run_queue.
            if let Err(e) = run_conn(conn, |_| {}).await {
                tracing::warn!("nvme-rdma queue ended: {e}");
            }
        });
    }
}
