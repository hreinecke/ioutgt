//! Receive path: PDU decode, payload reassembly (in-capsule and
//! R2T-solicited H2C), and data-digest verification. Owns the recv-side
//! phase machine; `connection.rs` only calls [`recv_loop`] and gets back
//! an `io::Result<()>` — the phase types never escape this module.

use std::rc::Rc;

use crate::queue::NvmeTcpQueue;
use ioutgt_core::pool::{RingProvider, SlotData};
use ioutgt_nvme::pdu::{self, PduDecoder, PduError, PduKind};
use ioutgt_nvme::spec::{Cqe, Sqe, sgl};
use ioutgt_nvme::{digest, status};
use ioutgt_stream::StreamReader;
use ioutgt_uring::bufring::BufRing;
use ioutgt_uring::ops;
use tracing::{debug, warn};

/// Transport-side [`RingProvider`]: the bridge that lets a core
/// [`SlotData::ring`] lease carry the recv-ring sub-buffer's fixed-buffer
/// index and run the deferred re-provide on drop, without `ioutgt-core`
/// depending on `ioutgt-uring`. One handle per connection (in ring mode); a
/// retained write holds a refcount clone of it, so retention allocates nothing
/// beyond the first `Rc`.
struct RingHandle {
    ring: Rc<BufRing>,
}

impl RingProvider for RingHandle {
    fn buf_index(&self, bid: u16) -> Option<u16> {
        self.ring.buf_index(bid)
    }
    fn release(&self, bid: u16) {
        self.ring.release(bid);
    }
}

/// Per-window ring context handed to the PDU handlers so a write payload can
/// be retained zero-copy in place. `bid` is the sub-buffer backing the current
/// recv window; `provider`/`ring` are the connection's long-lived handles.
struct RingCtx<'a> {
    provider: &'a Rc<dyn RingProvider>,
    ring: &'a Rc<BufRing>,
    bid: u16,
}

/// Try to retain a write payload in place in the recv ring (zero-copy).
///
/// `payload_ptr` is the start of the payload on the wire — the current recv
/// window's unconsumed pointer, which is exactly where the payload's bytes
/// land (those already arrived are there; under the incremental ring the rest
/// fill the same sub-buffer contiguously). Retains only when the whole payload
/// fits in this sub-buffer's remaining capacity (so it never straddles into
/// the next sub-buffer — `SlotData::ring` is a single contiguous segment);
/// returns `false` to fall back to the lease-and-copy path otherwise. On
/// success it takes the ring borrow and installs the ring-backed `SlotData` on
/// the slot, so the bytes land directly in the slot buffer.
fn try_retain_payload(
    queue: &Rc<NvmeTcpQueue>,
    rc: &RingCtx<'_>,
    tag: u16,
    payload_ptr: *const u8,
    payload_len: usize,
) -> bool {
    let cap = rc.ring.buf_size() as usize;
    let off = (payload_ptr as usize).wrapping_sub(rc.ring.buf(rc.bid) as usize);
    // `off > cap` guards the header-ends-exactly-at-buffer-end case (the
    // payload is in the *next* sub-buffer, so `payload_ptr` is past this one).
    if off > cap || payload_len > cap - off {
        return false;
    }
    rc.ring.borrow(rc.bid);
    // SAFETY: off + payload_len <= buf_size (checked above), so
    // [payload_ptr, payload_ptr + payload_len) lies within sub-buffer `bid`;
    // the borrow keeps that sub-buffer pinned (not re-provided to the kernel)
    // until this SlotData drops at release_tag, which releases the borrow.
    let data = unsafe {
        SlotData::ring(
            Rc::clone(rc.provider),
            rc.bid,
            payload_ptr.cast_mut(),
            payload_len,
        )
    };
    queue.slot(tag).set_data(data);
    true
}

/// Where the payload currently streaming in belongs.
#[derive(Clone, Copy)]
enum PayloadKind {
    /// In-capsule data: submit on completion.
    InCapsule,
    /// One H2CData PDU of an R2T-solicited transfer.
    H2c { last: bool, length: u32 },
}

/// Receive-side state across recv() completions.
enum RecvPhase {
    /// Assembling a PDU header in the decoder.
    Header,
    /// Copying payload bytes into the slot buffer.
    Data(DataPhase),
    /// Consuming the 4-byte data digest.
    Ddgst(DdgstPhase),
}

impl RecvPhase {
    /// Payload fully received: consume its 4-byte digest next.
    fn ddgst(tag: u16, expected: u32, kind: PayloadKind) -> RecvPhase {
        RecvPhase::Ddgst(DdgstPhase {
            tag,
            expected,
            have: [0; 4],
            have_len: 0,
            kind,
        })
    }
}

/// Mid-payload state: copying bytes from the recv buffer into the slot.
/// Every field is `Copy` (`Crc32c` included), so snapshotting the phase
/// for the direct-recv tail path is cheap.
#[derive(Clone, Copy)]
struct DataPhase {
    tag: u16,
    /// Slot offset where this PDU's payload begins.
    base: u32,
    remaining: u32,
    crc: digest::Crc32c,
    ddgst: bool,
    kind: PayloadKind,
    /// Zero-copy retention (ring mode): the slot's `SlotData::ring` already
    /// points at this payload's region in the recv ring, so `advance` must NOT
    /// copy — the bytes land directly in the slot buffer. It only folds the
    /// DDGST over the window slice and advances the cursor.
    retained: bool,
}

impl DataPhase {
    /// Copy payload bytes from the recv buffer into the slot; returns
    /// the next phase once this PDU's payload has fully arrived.
    fn advance(
        &mut self,
        queue: &Rc<NvmeTcpQueue>,
        slice: &mut &[u8],
    ) -> Result<Option<RecvPhase>, RecvEnd> {
        let take = (self.remaining as usize).min(slice.len());
        // Retained payloads already live in the slot buffer (it is the recv
        // ring sub-buffer): skip the copy. Otherwise copy the window into the
        // slot's leased buffer at the reassembly offset.
        if !self.retained {
            let slot = queue.slot(self.tag);
            let total = match self.kind {
                PayloadKind::InCapsule => slot.data_len(),
                PayloadKind::H2c { length, .. } => length,
            };
            let dest = (self.base + (total - self.remaining)) as usize;
            slot.data().write_at(dest, &slice[..take]);
        }
        // Only fold bytes into the digest when one was negotiated; with the
        // data digest off the result is discarded, so the CRC pass is pure
        // waste over every received byte.
        if self.ddgst {
            self.crc.update(&slice[..take]);
        }
        *slice = &slice[take..];
        self.remaining -= u32::try_from(take).expect("take <= remaining: u32");
        if self.remaining > 0 {
            return Ok(None);
        }
        if self.ddgst {
            Ok(Some(RecvPhase::ddgst(
                self.tag,
                self.crc.finalize(),
                self.kind,
            )))
        } else {
            finish_payload(queue, self.tag, self.kind)?;
            Ok(Some(RecvPhase::Header))
        }
    }
}

/// Consuming the 4-byte data digest that trails a payload.
#[derive(Clone, Copy)]
struct DdgstPhase {
    tag: u16,
    expected: u32,
    have: [u8; 4],
    have_len: u8,
    kind: PayloadKind,
}

impl DdgstPhase {
    /// Accumulate digest bytes; on the 4th, verify and finish the
    /// payload. A mismatch fails the command but keeps the connection.
    fn advance(
        &mut self,
        queue: &Rc<NvmeTcpQueue>,
        slice: &mut &[u8],
    ) -> Result<Option<RecvPhase>, RecvEnd> {
        let take = (4 - self.have_len as usize).min(slice.len());
        self.have[self.have_len as usize..self.have_len as usize + take]
            .copy_from_slice(&slice[..take]);
        self.have_len += u8::try_from(take).expect("take <= 4");
        *slice = &slice[take..];
        if self.have_len < 4 {
            return Ok(None);
        }
        let wire = u32::from_le_bytes(self.have);
        if wire != self.expected {
            // There is no NVMe/TCP "data digest error" FES; nvmet
            // completes the offending command with
            // NVME_SC_DATA_XFER_ERROR and keeps the connection.
            // Executing the write is skipped so corrupt data never
            // reaches the backend.
            let cid = queue.slot(self.tag).cmd().cid.get();
            warn!(qid = queue.qid, cid, "DDGST mismatch; failing command");
            let cqe = Cqe::new(
                0,
                queue.advance_sqhd(),
                queue.qid,
                cid,
                status::DATA_XFER_ERROR | status::DNR,
            );
            queue.complete_receiving(self.tag, cqe);
            return Ok(Some(RecvPhase::Header));
        }
        finish_payload(queue, self.tag, self.kind)?;
        Ok(Some(RecvPhase::Header))
    }
}

/// Why the receive path is finished with the connection. `recv_loop`
/// maps each ending onto the connection contract exactly once instead
/// of every protocol-check site hand-rolling `send_term` + return.
enum RecvEnd {
    /// Transport error: surfaces to the connection-closed log line.
    Io(std::io::Error),
    /// Protocol violation: send a C2HTermReq, then close.
    Term(PduError),
    /// The host sent an H2CTermReq; close without replying.
    HostTerm { fes: u16, fei: u32 },
    /// Orderly EOF mid-payload (direct-recv tail path).
    Closed,
}

impl RecvEnd {
    /// Protocol violation at `fes` with no field-error information.
    fn term(fes: u16) -> RecvEnd {
        RecvEnd::Term(PduError { fes, fei: 0 })
    }
}

impl From<std::io::Error> for RecvEnd {
    fn from(err: std::io::Error) -> RecvEnd {
        RecvEnd::Io(err)
    }
}

impl From<PduError> for RecvEnd {
    fn from(err: PduError) -> RecvEnd {
        RecvEnd::Term(err)
    }
}

async fn send_term(fd: i32, error: PduError) {
    let mut buf = vec![0u8; 24].into_boxed_slice();
    let n = pdu::encode_c2h_term(&mut buf, error);
    debug_assert_eq!(n, 24);
    if let Ok(op) = ops::send(fd, buf) {
        let _ = op.await;
    }
}

/// Payload for this PDU fully received (and digest-verified): advance
/// the slot; submit the command once the whole transfer is present.
fn finish_payload(queue: &Rc<NvmeTcpQueue>, tag: u16, kind: PayloadKind) -> Result<(), PduError> {
    let slot = queue.slot(tag);
    match kind {
        PayloadKind::InCapsule => {
            queue.submit(tag, slot.cmd());
            Ok(())
        }
        PayloadKind::H2c { last, length } => {
            let done = slot.recv_offset() + length;
            slot.set_recv_offset(done);
            if done == slot.data_len() {
                queue.submit(tag, slot.cmd());
                Ok(())
            } else if last {
                // Host claims the transfer is over but bytes are missing.
                Err(PduError {
                    fes: pdu::fes::DATA_OUT_OF_RANGE,
                    fei: 0,
                })
            } else {
                Ok(())
            }
        }
    }
}

/// H2C payload tails at least this large — bytes not yet arrived when
/// the connection buffer drains mid-payload — are received straight
/// into the slot buffer, skipping the recv-buffer→slot copy. Below it
/// the op-issue cost outweighs the copy. `u32::MAX` disables the path
/// entirely (A/B measurement). Public so the threshold-edge tests pin
/// their segmentation to the real gate value.
pub const H2C_DIRECT_MIN: u32 = 16 * 1024;

/// True when this command moves data host→controller (opcode bits 1:0
/// = 01b) and the host kept it resident (transport SGL): solicit it.
fn needs_r2t(sqe: &Sqe) -> bool {
    sqe.opcode & 0x3 == 0x1
        && sqe.dptr.sgl_type == sgl::TYPE_TRANSPORT_DATA_BLOCK
        && sqe.dptr.length.get() > 0
}

/// Receive loop: bytes → decoder → slot pipeline. Maps every recv-path
/// ending onto the connection contract in one place: protocol
/// violations send a C2HTermReq and close cleanly; transport errors
/// propagate to the connection-closed log line.
pub(crate) async fn recv_loop(
    queue: &Rc<NvmeTcpQueue>,
    fd: i32,
    hdr_digest: bool,
    data_digest: bool,
    recv_buf_bytes: usize,
) -> std::io::Result<()> {
    match drive_recv(queue, fd, hdr_digest, data_digest, recv_buf_bytes).await {
        Ok(()) | Err(RecvEnd::Closed) => Ok(()),
        Err(RecvEnd::Io(err)) => Err(err),
        Err(RecvEnd::Term(err)) => {
            warn!(qid = queue.qid, "protocol error: {err}");
            send_term(fd, err).await;
            Ok(())
        }
        Err(RecvEnd::HostTerm { fes, fei }) => {
            warn!(qid = queue.qid, fes, fei, "host terminated connection");
            Ok(())
        }
    }
}

/// Receive loop body: each recv buffer steps the phase machine; large
/// H2C tails switch to direct-into-slot receives between buffers.
async fn drive_recv(
    queue: &Rc<NvmeTcpQueue>,
    fd: i32,
    hdr_digest: bool,
    data_digest: bool,
    recv_buf_bytes: usize,
) -> Result<(), RecvEnd> {
    let mut decoder = PduDecoder::new(hdr_digest);
    let mut phase = RecvPhase::Header;
    // Ring mode (recv_buf_bytes > 0 and the kernel supports a buf_ring) draws
    // recv chunks from this connection's own provided-buffer ring; otherwise a
    // 64 KiB scratch buffer, allocated once per connection. The 64 KiB size is
    // the small-IO batching unit (~15 × 4 KiB write capsules per wakeup) and
    // matches the kernel's max GRO/loopback burst (64 KiB), so one recv drains
    // a full coalesced burst; only headers, in-capsule payloads, and payload
    // prefixes flow through it (large H2C tails bypass into the slot).
    let mut reader = match (recv_buf_bytes > 0)
        .then(|| ioutgt_uring::bufring::BufRing::for_connection(recv_buf_bytes))
        .flatten()
    {
        Some(ring) => {
            debug!(qid = queue.qid, "recv ring engaged: zero-copy receive");
            StreamReader::new_ring(fd, ring)
        }
        None => {
            if recv_buf_bytes > 0 {
                debug!(
                    qid = queue.qid,
                    "recv ring unavailable (kernel gap); classic recv"
                );
            }
            StreamReader::new(fd, 64 * 1024)
        }
    };

    // Connection-scoped ring handles (ring mode only), built once: every recv
    // window draws from this connection's own ring, and a retained write holds
    // a refcount clone of `provider` — so retention adds no per-write allocation.
    let ring = reader.ring().map(Rc::clone);
    let provider: Option<Rc<dyn RingProvider>> = ring
        .as_ref()
        .map(|r| Rc::new(RingHandle { ring: Rc::clone(r) }) as Rc<dyn RingProvider>);

    loop {
        // `fill_with_bid` bundles the sub-buffer id with the window so a
        // zero-copy consumer records the in-place borrow against the right
        // buffer without a second `&reader` query racing the window's borrow.
        let (window, window_bid) = reader.fill_with_bid().await?;
        if window.is_empty() {
            return Ok(()); // orderly shutdown
        }
        let window_len = window.len();
        let mut slice = window;

        // Retention context for this window (None in classic mode): all of
        // `provider`, `ring`, and a bid are present exactly in ring mode.
        let ring_ctx = match (provider.as_ref(), ring.as_ref(), window_bid) {
            (Some(provider), Some(ring), Some(bid)) => Some(RingCtx {
                provider,
                ring,
                bid,
            }),
            _ => None,
        };

        while !slice.is_empty() {
            let next = match &mut phase {
                RecvPhase::Header => {
                    feed_header(
                        queue,
                        &mut decoder,
                        &mut slice,
                        data_digest,
                        ring_ctx.as_ref(),
                    )
                    .await?
                }
                RecvPhase::Data(data) => data.advance(queue, &mut slice)?,
                RecvPhase::Ddgst(ddgst) => ddgst.advance(queue, &mut slice)?,
            };
            if let Some(next) = next {
                phase = next;
            }
        }
        // The inner loop drains the window to empty; mark it consumed
        // before the next fill. (Computed up front, not inline in the
        // call, so the window borrow ends before this `&mut reader`.)
        reader.consume(window_len);

        // Buffer exhausted mid-payload with a large H2C tail still to
        // come: pull it straight into the slot, skipping the
        // buffer→slot copy. Copy-snapshot the phase (cheap — DataPhase
        // is Copy): a failed guard leaves phase untouched for the normal
        // copy path.
        // The direct path receives straight into the slot's pool segments —
        // contiguous or scattered — so no contiguity gate is needed. Ring mode
        // owns the fd with a multishot recv, so the classic single-recv direct
        // tail is gated off there; ring payloads flow through the phase machine
        // (and, in P5b, can be retained in place zero-copy).
        if let &RecvPhase::Data(data) = &phase
            && !reader.is_ring()
            && matches!(data.kind, PayloadKind::H2c { .. })
            && data.remaining >= H2C_DIRECT_MIN
        {
            phase = recv_tail_direct(queue, &mut reader, data).await?;
        }
    }
}

/// Header phase: feed the decoder; once a header is complete, route
/// the PDU. Returns the next phase if payload follows on the stream.
async fn feed_header(
    queue: &Rc<NvmeTcpQueue>,
    decoder: &mut PduDecoder,
    slice: &mut &[u8],
    data_digest: bool,
    ring_ctx: Option<&RingCtx<'_>>,
) -> Result<Option<RecvPhase>, RecvEnd> {
    let consumed = decoder.feed(slice)?;
    *slice = &slice[consumed..];
    if !decoder.is_complete() {
        return Ok(None);
    }
    let decoded = decoder.take()?;
    // The header is fully consumed: the unconsumed window pointer is now the
    // start of this PDU's payload on the wire — exactly where a retained
    // zero-copy write payload lands in the recv ring.
    let payload_ptr = slice.as_ptr();
    handle_pdu(queue, decoded, data_digest, ring_ctx, payload_ptr).await
}

/// Split the slot-buffer segments covering the logical range
/// `[off, off+len)` into `(ptr, len)` sub-segments, in order. One entry
/// for a contiguous lease; several for a scattered one. Returns the count.
fn fill_subsegs(
    out: &mut [(*mut u8, usize)],
    segs: &[ioutgt_core::pool::Seg],
    mut off: usize,
    mut len: usize,
) -> usize {
    let mut n = 0;
    for seg in segs {
        if len == 0 {
            break;
        }
        if off >= seg.len {
            off -= seg.len;
            continue;
        }
        let take = (seg.len - off).min(len);
        // SAFETY: off < seg.len, so ptr.add(off) is within the segment.
        out[n] = (unsafe { seg.ptr.add(off) }, take);
        n += 1;
        len -= take;
        off = 0;
    }
    n
}

/// Receive a payload tail straight into the slot buffer at the reassembly
/// offset, via [`StreamReader::read_direct_vectored`] (the scratch buffer is
/// bypassed — true zero-copy receive into the pooled slot, DIO- or
/// buffered-written later per the buffer's alignment). The tail is by
/// definition the next bytes on the stream, so no buffered recv is
/// re-armed until it lands — never more than one outstanding recv on the
/// socket. The slot lease may be contiguous (one sub-segment) or scattered
/// across pool pages (several); either way it is received with a single
/// scatter `recvmsg` (`MSG_WAITALL`), the kernel filling all segments in one
/// syscall. A total shorter than `remaining` means an orderly close
/// mid-payload. For a digested transfer the landed segments are folded into
/// the CRC in logical order once received; with the data digest off no CRC
/// is computed.
async fn recv_tail_direct(
    queue: &Rc<NvmeTcpQueue>,
    reader: &mut StreamReader,
    mut data: DataPhase,
) -> Result<RecvPhase, RecvEnd> {
    let total = match data.kind {
        PayloadKind::InCapsule => queue.slot(data.tag).data_len(),
        PayloadKind::H2c { length, .. } => length,
    };
    let dest = (data.base + (total - data.remaining)) as usize;
    let remaining = data.remaining as usize;

    // Snapshot the destination sub-segments up front so the RefCell borrow
    // ends before the awaits below (the raw ptrs stay valid: pool memory is
    // stable and the slot is `Receiving`).
    let mut subs = [(std::ptr::null_mut::<u8>(), 0usize); ioutgt_core::pool::MAX_SEGS];
    let nsub = {
        let slot_data = queue.slot(data.tag).data();
        fill_subsegs(&mut subs, slot_data.segs(), dest, remaining)
    };

    // One iovec per destination sub-segment: the whole tail is received
    // with a single scatter `recvmsg` (`MSG_WAITALL`) instead of a recv per
    // segment — the kernel scatters the payload across the segments.
    let mut iovs = [libc::iovec {
        iov_base: std::ptr::null_mut(),
        iov_len: 0,
    }; ioutgt_core::pool::MAX_SEGS];
    for (iov, &(ptr, len)) in iovs.iter_mut().zip(&subs[..nsub]) {
        iov.iov_base = ptr.cast();
        iov.iov_len = len;
    }
    // SAFETY: every (ptr, len) is a sub-range of the slot's data buffer,
    // owned by NvmeTcpQueue (this recv task holds an Rc); the slot is
    // `Receiving`, so nothing else touches it. read_direct_vectored awaits
    // the recv inline; on whole-future drop the reactor's orphan protocol
    // holds the op to its terminal CQE — the same envelope as backend raw
    // ops on slot memory.
    let got = unsafe { reader.read_direct_vectored(&mut iovs[..nsub]).await? } as usize;
    if got < remaining {
        return Err(RecvEnd::Closed);
    }
    if data.ddgst {
        // The segments are now full (a short transfer returned above); fold
        // them into the digest in logical order for the warm-cache CRC pass.
        for &(ptr, len) in &subs[..nsub] {
            // SAFETY: ptr..ptr+len is the just-filled slot sub-segment,
            // held exclusively for this transfer.
            data.crc
                .update(unsafe { std::slice::from_raw_parts(ptr, len) });
        }
        // Next recv starts at the 4-byte digest.
        Ok(RecvPhase::ddgst(data.tag, data.crc.finalize(), data.kind))
    } else {
        finish_payload(queue, data.tag, data.kind)?;
        // Next recv starts at the next PDU header.
        Ok(RecvPhase::Header)
    }
}

/// Route one decoded PDU header. Returns the next recv phase if payload
/// follows on the stream. Async for the command-slot tag wait (recv
/// backpressure when the freelist is momentarily empty); every other path
/// resolves immediately.
async fn handle_pdu(
    queue: &Rc<NvmeTcpQueue>,
    decoded: pdu::DecodedPdu,
    data_digest: bool,
    ring_ctx: Option<&RingCtx<'_>>,
    payload_ptr: *const u8,
) -> Result<Option<RecvPhase>, RecvEnd> {
    let ddgst = decoded.ddgst && data_digest;
    match decoded.kind {
        PduKind::CapsuleCmd(sqe) => {
            handle_capsule_cmd(queue, sqe, decoded.data_len, ddgst, ring_ctx, payload_ptr).await
        }
        PduKind::H2CData {
            cid,
            ttag,
            offset,
            length,
            last,
        } => {
            let tag = validate_h2c(queue, cid, ttag, offset, length, decoded.data_len)?;
            // Retain zero-copy only when this single H2CData PDU carries the
            // *whole* solicited transfer (offset 0, length == data_len) and it
            // lands contiguously in the current sub-buffer: `SlotData::ring` is
            // one contiguous segment, so a split/partial transfer can't use it
            // and falls through to the lease-and-copy path set up at R2T time.
            let retained = match ring_ctx {
                Some(rc) if offset == 0 && length == queue.slot(tag).data_len() => {
                    try_retain_payload(queue, rc, tag, payload_ptr, length as usize)
                }
                _ => false,
            };
            Ok(Some(RecvPhase::Data(DataPhase {
                tag,
                base: offset,
                remaining: length,
                crc: digest::Crc32c::new(),
                ddgst,
                kind: PayloadKind::H2c { last, length },
                retained,
            })))
        }
        PduKind::H2CTerm { fes, fei } => Err(RecvEnd::HostTerm { fes, fei }),
        PduKind::IcReq(_)
        | PduKind::IcResp(_)
        | PduKind::CapsuleResp(_)
        | PduKind::C2HData { .. }
        | PduKind::R2T { .. } => Err(RecvEnd::term(pdu::fes::PDU_SEQ_ERR)),
    }
}

/// A new command capsule: claim a slot, then route by payload
/// residency — in-capsule data, host-resident via R2T, or no data.
async fn handle_capsule_cmd(
    queue: &Rc<NvmeTcpQueue>,
    sqe: Sqe,
    data_len: u32,
    ddgst: bool,
    ring_ctx: Option<&RingCtx<'_>>,
    payload_ptr: *const u8,
) -> Result<Option<RecvPhase>, RecvEnd> {
    // An empty freelist is a transient, NOT a depth violation, so park
    // (TCP backpressure) rather than terminating. A tag releases only when
    // its response's send *batch* completes (retire -> release_tag), so on
    // a real NIC the host — receiving responses as the batch streams out —
    // frees SQ slots and submits new commands faster than we release the
    // batched tags, briefly emptying the freelist while the host is still
    // within the negotiated depth (max outstanding <= sqsize < tag count).
    // await_tag cannot deadlock (release never depends on the recv path);
    // even a genuinely over-submitting host is correctly flow-controlled,
    // not killed. This mirrors kernel nvmet, which never terms on depth.
    let tag = queue.await_tag().await;
    let slot = queue.slot(tag);
    if data_len > 0 {
        // In-capsule payload follows the capsule on the wire.
        if data_len > ioutgt_core::MDTS_BYTES {
            return Err(RecvEnd::term(pdu::fes::DATA_LIMIT_EXCEEDED));
        }
        // In ring mode, try to retain the in-capsule payload in place
        // (zero-copy); otherwise lease the landing buffer for the copy path
        // (deadlock-free: the serial recv loop never blocks, falling back to a
        // private buffer under pressure).
        let retained = match ring_ctx {
            Some(rc) => try_retain_payload(queue, rc, tag, payload_ptr, data_len as usize),
            None => false,
        };
        if !retained {
            queue.lease_or_owned(tag, data_len as usize);
        }
        slot.set_data_len(data_len);
        slot.stash_cmd(sqe);
        Ok(Some(RecvPhase::Data(DataPhase {
            tag,
            base: 0,
            remaining: data_len,
            crc: digest::Crc32c::new(),
            ddgst,
            kind: PayloadKind::InCapsule,
            retained,
        })))
    } else if needs_r2t(&sqe) {
        let length = sqe.dptr.length.get();
        if length > ioutgt_core::MDTS_BYTES {
            return Err(RecvEnd::term(pdu::fes::DATA_LIMIT_EXCEEDED));
        }
        queue.lease_or_owned(tag, length as usize);
        slot.set_data_len(length);
        slot.set_recv_offset(0);
        slot.stash_cmd(sqe);
        // Solicit the whole transfer with one R2T; the host may
        // split it into several H2CData PDUs.
        queue.solicit(tag, sqe.cid.get(), 0, length);
        Ok(None)
    } else {
        queue.submit(tag, sqe);
        Ok(None)
    }
}

/// Validate one H2CData header against its slot's expected reassembly
/// state; returns the target tag.
fn validate_h2c(
    queue: &Rc<NvmeTcpQueue>,
    cid: u16,
    ttag: u16,
    offset: u32,
    length: u32,
    data_len: u32,
) -> Result<u16, RecvEnd> {
    if usize::from(ttag) >= usize::from(queue.sqsize) {
        return Err(RecvEnd::Term(PduError {
            fes: pdu::fes::INVALID_PDU_HDR,
            fei: 10, // ttag field offset
        }));
    }
    let slot = queue.slot(ttag);
    let valid = slot.state() == ioutgt_core::queue::SlotState::Receiving
        && slot.cmd().cid.get() == cid
        && offset == slot.recv_offset()
        && offset
            .checked_add(length)
            .is_some_and(|end| end <= slot.data_len())
        && length > 0;
    if !valid {
        return Err(RecvEnd::term(pdu::fes::DATA_OUT_OF_RANGE));
    }
    if data_len != length {
        return Err(RecvEnd::Term(PduError {
            fes: pdu::fes::INVALID_PDU_HDR,
            fei: 16, // data_length field offset
        }));
    }
    Ok(ttag)
}

#[cfg(test)]
mod subseg_tests {
    use super::*;
    use ioutgt_core::pool::Seg;

    #[test]
    fn fill_subsegs_contiguous_and_scattered() {
        let mut a = [0u8; 100];
        let mut b = [0u8; 100];
        let (pa, pb) = (a.as_mut_ptr(), b.as_mut_ptr());
        let segs = [Seg { ptr: pa, len: 100 }, Seg { ptr: pb, len: 100 }];
        let mut out = [(std::ptr::null_mut::<u8>(), 0usize); ioutgt_core::pool::MAX_SEGS];

        // Range fully inside the first segment → one sub-seg.
        let n = fill_subsegs(&mut out, &segs, 10, 50);
        assert_eq!(n, 1);
        // SAFETY: in-bounds offset for the assertion's expected pointer.
        assert_eq!(out[0], (unsafe { pa.add(10) }, 50));

        // Range straddling the seam → tail of seg0 + head of seg1.
        let n = fill_subsegs(&mut out, &segs, 80, 40);
        assert_eq!(n, 2);
        // SAFETY: in-bounds offset for the assertion's expected pointer.
        assert_eq!(out[0], (unsafe { pa.add(80) }, 20));
        assert_eq!(out[1], (pb, 20));
    }
}
