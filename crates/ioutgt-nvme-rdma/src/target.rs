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

use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;

use ioutgt_backend::AnyBackend;
use ioutgt_core::controller::Registry;
use ioutgt_core::dispatch::{self, ConnCtx, Outcome, Role};
use ioutgt_core::pool::MAX_SEGS;
use ioutgt_core::queue::QueueCore;
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

/// SGL descriptor type byte (dptr offset 15). High nibble `0x4` =
/// `NVME_KEY_SGL_FMT_DATA_DESC` (keyed: host-resident, RDMA READ/WRITE); anything
/// else here is an in-capsule data+offset descriptor (inline).
const SGL_TYPE_OFFSET: usize = 24 + 15;
const KEYED_SGL_TYPE_HI: u8 = 0x4;

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
        g.post().map_err(oerr)
    }

    /// SEND the 16-byte CQE response capsule for `tag` from the staging buffer.
    fn send_cqe(&mut self, tag: u32, cqe: Cqe) -> io::Result<()> {
        let off = tag as usize * CQE_LEN;
        self.resp_buf[off..off + CQE_LEN].copy_from_slice(cqe.as_bytes());
        let addr = self.resp_buf.as_ptr() as u64 + off as u64;
        let lkey = self.resp_mr.lkey();
        let mut g = self.qp.start_post_send();
        let h = g.construct_wr(wr(WR_SEND, tag), WorkRequestFlags::Signaled).setup_send();
        // SAFETY: the staging region is registered and stays valid until the
        // send completes (tag not released until then).
        unsafe { h.setup_sge(lkey, addr, CQE_LEN as u32) };
        g.post().map_err(oerr)
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
        g.post().map_err(oerr)
    }

    /// RDMA WRITE the first `data_len` bytes of `slot(tag).data().segs()` to the
    /// host's keyed-SGL region.
    fn write_read_data(&mut self, tag: u32, data_len: u32, dst: &KeyedSgl) -> io::Result<()> {
        // A pool lease spans at most MAX_SEGS runs (and the QP's max_send_sge is
        // built to match), so a fixed array holds every sge with no allocation.
        let mut sges = [ibv_sge {
            addr: 0,
            length: 0,
            lkey: 0,
        }; MAX_SEGS];
        let mut n = 0usize;
        let mut remaining = data_len as usize;
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
            .construct_wr(wr(WR_WRITE, tag), WorkRequestFlags::Signaled)
            .setup_write(dst.rkey, dst.addr);
        // SAFETY: the sges reference the registered pool arena (pool_lkey); the
        // slot stays leased until the WRITE completes (tag not released).
        unsafe { h.setup_sge_list(&sges[..n]) };
        g.post().map_err(oerr)
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
        g.post().map_err(oerr)
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
                    WR_RECV => return Ok(wr_low(id)),
                    WR_SEND | WR_WRITE => self.on_response_done(wr_low(id)),
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
                    WR_READ => return Ok(()),
                    WR_SEND | WR_WRITE => self.on_response_done(wr_low(id)),
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

    /// Post a finished command's response (its slot is already `Responding`): the
    /// RDMA WRITE of any read-data to the host's keyed SGL, then the CQE capsule.
    /// The slot is released once both response WRs complete (tracked by `inflight`).
    fn post_response(&mut self, tag: u16, cmd: &Sqe, outcome: Outcome) -> io::Result<()> {
        let mut pending = 0u8;
        if outcome.data_len > 0 {
            let dst = parse_keyed_sgl(cmd);
            self.write_read_data(u32::from(tag), outcome.data_len, &dst)?;
            pending += 1;
        }
        self.send_cqe(u32::from(tag), outcome.cqe)?;
        pending += 1;
        self.inflight[tag as usize] = pending;
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
        self.send_cqe(u32::from(tag), outcome.cqe)?;
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
        // Reap until peer-gone (a flushed completion), a CM Disconnected (`stop`),
        // or a fatal error; then drain and tear down. Each select arm yields
        // Ok(false) to keep going, Ok(true) to stop, or Err for a fatal error.
        let result: io::Result<()> = loop {
            let step: io::Result<bool> = tokio::select! {
                res = crate::cq::wait(&self.channel, &self.cq) => match res {
                    Err(e) => Err(e),
                    Ok(()) => {
                        self.drain_into(&mut comps);
                        let mut step = Ok(false);
                        for &(id, ok) in &comps {
                            if !ok {
                                // Flushed completion: the peer is gone.
                                step = Ok(true);
                                break;
                            }
                            let r = match wr_kind(id) {
                                WR_RECV => self.handle_recv(&ctx, wr_low(id)),
                                // A write-data RDMA READ finished: the slot is
                                // filled, so submit it to wake its slot task.
                                WR_READ => {
                                    self.submit_pending(wr_low(id) as u16);
                                    Ok(())
                                }
                                WR_SEND | WR_WRITE => {
                                    self.on_response_done(wr_low(id));
                                    Ok(())
                                }
                                _ => Ok(()),
                            };
                            if let Err(e) = r {
                                step = Err(e);
                                break;
                            }
                        }
                        step
                    }
                },
                Some(resp) = responses.next() => {
                    let mut r = self.post_response(resp.tag, &resp.cmd, resp.outcome);
                    // Drain any siblings queued in the same wake without re-parking.
                    while r.is_ok() {
                        match responses.try_next() {
                            Some(rr) => r = self.post_response(rr.tag, &rr.cmd, rr.outcome),
                            None => break,
                        }
                    }
                    r.map(|()| false)
                }
                // CM Disconnected for this connection (the QP isn't cm_id-bound,
                // so this is the only prompt teardown signal).
                () = stop.notified() => Ok(true),
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
) -> io::Result<(
    Arc<ProtectionDomain>,
    Arc<CompletionChannel>,
    GenericCompletionQueue,
    GenericQueuePair,
)> {
    let channel = ctx.create_comp_channel().map_err(oerr)?;
    channel.set_nonblocking(true)?;
    let depth = u32::from(sqsize) * 3 + 16;
    let mut cqb = ctx.create_cq_builder();
    cqb.setup_cqe(depth).setup_comp_channel(&channel, 0);
    let cq = GenericCompletionQueue::from(cqb.build_ex().map_err(oerr)?);

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
            SendOperationFlags::Send | SendOperationFlags::Write | SendOperationFlags::Read,
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
    let (pd, channel, cq, mut qp) = build_conn_resources(&dev, sqsize)?;
    qp.modify(&conn.id.get_qp_attr(QueuePairState::Init).map_err(oerr)?)
        .map_err(oerr)?;
    qp.modify(&conn.id.get_qp_attr(QueuePairState::ReadyToReceive).map_err(oerr)?)
        .map_err(oerr)?;
    qp.modify(&conn.id.get_qp_attr(QueuePairState::ReadyToSend).map_err(oerr)?)
        .map_err(oerr)?;

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
    accept(&conn.id, qp_num, &rep, 1, 1)?;

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
