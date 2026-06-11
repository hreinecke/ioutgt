//! Queue-thread connection driver: recv state machine, slot tasks, send
//! path. All IO goes through the thread's io_uring reactor.
//!
//! Receive side: capsule commands carry data in-capsule (≤ IOCCSZ) or
//! host-resident via R2T — we solicit the whole transfer with a single
//! R2T (TTAG = slot index) and reassemble however many H2CData PDUs the
//! host sends, verifying a data digest per PDU. Send side: single-PDU
//! C2HData for reads (with the SUCCESS elision when SQ flow control is
//! off), R2Ts, response capsules — all serialized on one send task.

use std::os::fd::{AsRawFd, OwnedFd};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use ioutgt_core::backend::Backend;
use ioutgt_core::controller::Registry;
use ioutgt_core::dispatch::{self, ConnCtx, Role};
use ioutgt_core::queue::{QueueCore, SendWork};
use ioutgt_core::subsystem::PortConfig;
use ioutgt_nvme::fabrics::ConnectData;
use ioutgt_nvme::pdu::{self, PduDecoder, PduError, PduKind};
use ioutgt_nvme::spec::{Cqe, Sqe, sgl};
use ioutgt_nvme::{digest, status};
use ioutgt_uring::ops;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

/// Admin-queue slot buffers: identify/log pages (4 KiB) plus margin.
pub const ADMIN_SLOT_BUF: usize = 8 * 1024;
/// IO-queue slot buffers: MDTS.
pub const IO_SLOT_BUF: usize = 128 * 1024;

/// RAII guard for the active-connection counter: the count is
/// incremented by the acceptor before the permit is built, and
/// decremented here when the connection's `run_queue` returns. This is
/// how the control thread bounds concurrent connections (and thus total
/// preallocated queue memory) across queue threads.
pub struct ConnPermit(Arc<std::sync::atomic::AtomicUsize>);

impl ConnPermit {
    /// Wrap an already-incremented counter; drop decrements it.
    pub fn new(counter: Arc<std::sync::atomic::AtomicUsize>) -> Self {
        ConnPermit(counter)
    }
}

impl Drop for ConnPermit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Everything a queue thread receives to run one connection.
#[allow(missing_docs)]
pub struct QueueConn<B> {
    pub fd: OwnedFd,
    pub hdr_digest: bool,
    pub data_digest: bool,
    pub qid: u16,
    /// Queue depth in entries (Connect sqsize + 1).
    pub sqsize: u16,
    /// SQ flow control disabled (Connect CATTR bit 2).
    pub sqhd_disabled: bool,
    /// The already-consumed Connect command, to be executed as this
    /// queue's first command.
    pub connect_sqe: Sqe,
    /// The Connect command's 1024-byte data payload.
    pub connect_data: Box<ConnectData>,
    /// Port configuration (subsystems reachable here).
    pub port: Arc<PortConfig<B>>,
    /// Cross-thread controller registry.
    pub registry: Arc<Registry>,
    /// Active-connection accounting; dropped when this connection ends.
    pub permit: ConnPermit,
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
    Data {
        tag: u16,
        base: u32,
        remaining: u32,
        crc: digest::Crc32c,
        ddgst: bool,
        kind: PayloadKind,
    },
    /// Consuming the 4-byte data digest.
    Ddgst {
        tag: u16,
        expected: u32,
        have: [u8; 4],
        have_len: u8,
        kind: PayloadKind,
    },
}

/// Drive one queue connection to completion (EOF, error, or term).
///
/// `on_ctx` runs once the dispatch context exists — the binary's admin
/// thread uses it to register live controllers for AER nudges.
pub async fn run_queue<B: Backend>(conn: QueueConn<B>, on_ctx: impl FnOnce(&Rc<ConnCtx<B>>)) {
    let slot_buf = if conn.qid == 0 {
        ADMIN_SLOT_BUF
    } else {
        IO_SLOT_BUF
    };
    let queue = QueueCore::new(conn.qid, conn.sqsize, slot_buf, conn.sqhd_disabled);
    let fd = conn.fd.as_raw_fd();
    let ctx = if conn.qid == 0 {
        ConnCtx::new_admin(
            Rc::clone(&queue),
            Arc::clone(&conn.port),
            Arc::clone(&conn.registry),
            conn.connect_data,
        )
    } else {
        ConnCtx::new_io(
            Rc::clone(&queue),
            Arc::clone(&conn.port),
            Arc::clone(&conn.registry),
            conn.connect_data,
        )
    };

    on_ctx(&ctx);

    // Persistent task per tag.
    let mut tasks: Vec<JoinHandle<()>> = (0..conn.sqsize)
        .map(|tag| {
            let queue = Rc::clone(&queue);
            let ctx = Rc::clone(&ctx);
            tokio::task::spawn_local(async move {
                loop {
                    let sqe = queue.await_command(tag).await;
                    let outcome = dispatch::execute(&ctx, tag, &sqe).await;
                    queue.complete(tag, outcome.cqe, outcome.data_len);
                }
            })
        })
        .collect();

    // Keep-alive watchdog (admin queues): close the socket when the host
    // goes silent past KATO + grace, which unwinds the whole connection.
    if let Role::Admin(_) = &ctx.role {
        let ctx = Rc::clone(&ctx);
        tasks.push(tokio::task::spawn_local(async move {
            loop {
                let Ok(sleep) = ops::sleep(Duration::from_secs(5)) else {
                    return;
                };
                if sleep.await.is_err() {
                    return;
                }
                let Role::Admin(admin) = &ctx.role else {
                    return;
                };
                let kato = u64::from(admin.kato_ms.get());
                if kato == 0 {
                    continue;
                }
                let silent =
                    u64::try_from(admin.last_heard.get().elapsed().as_millis()).unwrap_or(u64::MAX);
                if silent > kato * 2 + 5_000 {
                    info!(
                        cntlid = admin.cntlid.get(),
                        silent_ms = silent,
                        "keep-alive expired; closing connection"
                    );
                    // SAFETY: fd is valid for the connection's lifetime;
                    // shutdown only signals, never frees.
                    unsafe { libc::shutdown(fd, libc::SHUT_RDWR) };
                    return;
                }
            }
        }));
    }

    // Send path.
    {
        let queue = Rc::clone(&queue);
        let hdr_digest = conn.hdr_digest;
        let data_digest = conn.data_digest;
        tasks.push(tokio::task::spawn_local(async move {
            if let Err(err) = send_loop(&queue, fd, hdr_digest, data_digest).await {
                debug!(qid = queue.qid, "send loop ended: {err}");
            }
        }));
    }

    // The Connect command was consumed on the control thread; run it
    // through the normal slot pipeline as this queue's first command.
    let tag = queue.claim_tag().expect("fresh queue has free tags");
    queue.submit(tag, conn.connect_sqe);

    // Receive path (this task).
    if let Err(err) = recv_loop(&queue, fd, conn.hdr_digest, conn.data_digest).await {
        debug!(qid = conn.qid, "connection closed: {err}");
    }

    // Resolve parked AERs (their slots count as executing but reference
    // no kernel-visible memory) so the drain below terminates promptly.
    ctx.close();

    // Backend ops in flight reference slot memory: wait for executing
    // slots to finish before aborting tasks and freeing the queue.
    let mut waited = 0u32;
    while queue.executing() > 0 && waited < 10_000 {
        match ops::sleep(Duration::from_millis(2)) {
            Ok(sleep) => {
                let _ = sleep.await;
            }
            Err(_) => break,
        }
        waited += 2;
    }
    if queue.executing() > 0 {
        // A wedged backend op: leak the queue AND the slot tasks rather
        // than free memory the kernel may still write to. A suspended
        // backend future can own a private buffer (e.g. the write-zeroes
        // fallback chunk) referenced by an in-flight raw kernel op;
        // aborting the task would drop and free that buffer mid-DMA.
        // Leaking the tasks keeps every such future — and its buffer —
        // alive for the process's remaining lifetime.
        warn!(
            qid = conn.qid,
            executing = queue.executing(),
            "teardown timeout; leaking queue and tasks"
        );
        std::mem::forget(Rc::clone(&queue));
        for task in tasks {
            std::mem::forget(task);
        }
    } else {
        for task in &tasks {
            task.abort();
        }
    }
    // Tear down the controller when its admin queue dies.
    if let Role::Admin(admin) = &ctx.role {
        let cntlid = admin.cntlid.get();
        if cntlid != 0 {
            ctx.registry.remove(cntlid);
            info!(cntlid, "controller removed");
        }
    }
    // conn.fd drops here, closing the socket; in-flight ops orphan and
    // drain through the reactor.
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
fn finish_payload(queue: &Rc<QueueCore>, tag: u16, kind: PayloadKind) -> Result<(), PduError> {
    let slot = queue.slot(tag);
    match kind {
        PayloadKind::InCapsule => {
            queue.submit(tag, slot.stashed_sqe());
            Ok(())
        }
        PayloadKind::H2c { last, length } => {
            let done = slot.recv_offset() + length;
            slot.set_recv_offset(done);
            if done == slot.data_len() {
                queue.submit(tag, slot.stashed_sqe());
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

/// True when this command moves data host→controller (opcode bits 1:0
/// = 01b) and the host kept it resident (transport SGL): solicit it.
fn needs_r2t(sqe: &Sqe) -> bool {
    sqe.opcode & 0x3 == 0x1
        && sqe.dptr.sgl_type == sgl::TYPE_TRANSPORT_DATA_BLOCK
        && sqe.dptr.length.get() > 0
}

/// Receive loop: bytes → decoder → slot pipeline.
async fn recv_loop(
    queue: &Rc<QueueCore>,
    fd: i32,
    hdr_digest: bool,
    data_digest: bool,
) -> std::io::Result<()> {
    let mut decoder = PduDecoder::new(hdr_digest);
    let mut phase = RecvPhase::Header;
    let mut buf = vec![0u8; 64 * 1024].into_boxed_slice();

    loop {
        let (res, b) = ops::recv(fd, buf)?.await;
        buf = b;
        let n = res? as usize;
        if n == 0 {
            return Ok(()); // orderly shutdown
        }
        let mut slice = &buf[..n];

        while !slice.is_empty() {
            match &mut phase {
                RecvPhase::Header => {
                    let consumed = match decoder.feed(slice) {
                        Ok(consumed) => consumed,
                        Err(err) => {
                            warn!(qid = queue.qid, "PDU error: {err}");
                            send_term(fd, err).await;
                            return Ok(());
                        }
                    };
                    slice = &slice[consumed..];
                    if !decoder.is_complete() {
                        continue;
                    }
                    let decoded = match decoder.take() {
                        Ok(decoded) => decoded,
                        Err(err) => {
                            warn!(qid = queue.qid, "PDU error: {err}");
                            send_term(fd, err).await;
                            return Ok(());
                        }
                    };
                    match handle_pdu(queue, decoded, data_digest) {
                        Ok(Some(next)) => phase = next,
                        Ok(None) => {}
                        Err(HandleError::Term(err)) => {
                            warn!(qid = queue.qid, "protocol error: {err}");
                            send_term(fd, err).await;
                            return Ok(());
                        }
                        Err(HandleError::HostTerm { fes, fei }) => {
                            warn!(qid = queue.qid, fes, fei, "host terminated connection");
                            return Ok(());
                        }
                    }
                }
                RecvPhase::Data {
                    tag,
                    base,
                    remaining,
                    crc,
                    ddgst,
                    kind,
                } => {
                    let take = (*remaining as usize).min(slice.len());
                    {
                        let slot = queue.slot(*tag);
                        let total = match kind {
                            PayloadKind::InCapsule => slot.data_len(),
                            PayloadKind::H2c { length, .. } => *length,
                        };
                        let dest = (*base + (total - *remaining)) as usize;
                        slot.data()[dest..dest + take].copy_from_slice(&slice[..take]);
                    }
                    crc.update(&slice[..take]);
                    slice = &slice[take..];
                    *remaining -= u32::try_from(take).expect("take <= remaining: u32");
                    if *remaining == 0 {
                        if *ddgst {
                            phase = RecvPhase::Ddgst {
                                tag: *tag,
                                expected: crc.finalize(),
                                have: [0; 4],
                                have_len: 0,
                                kind: *kind,
                            };
                        } else {
                            let kind = *kind;
                            let tag = *tag;
                            if let Err(err) = finish_payload(queue, tag, kind) {
                                send_term(fd, err).await;
                                return Ok(());
                            }
                            phase = RecvPhase::Header;
                        }
                    }
                }
                RecvPhase::Ddgst {
                    tag,
                    expected,
                    have,
                    have_len,
                    kind,
                } => {
                    let take = (4 - *have_len as usize).min(slice.len());
                    have[*have_len as usize..*have_len as usize + take]
                        .copy_from_slice(&slice[..take]);
                    *have_len += u8::try_from(take).expect("take <= 4");
                    slice = &slice[take..];
                    if *have_len == 4 {
                        let wire = u32::from_le_bytes(*have);
                        if wire != *expected {
                            // There is no NVMe/TCP "data digest error" FES;
                            // nvmet completes the offending command with
                            // NVME_SC_DATA_XFER_ERROR and keeps the
                            // connection. Executing the write is skipped so
                            // corrupt data never reaches the backend.
                            let cid = queue.slot(*tag).stashed_sqe().cid.get();
                            warn!(qid = queue.qid, cid, "DDGST mismatch; failing command");
                            let cqe = Cqe::new(
                                0,
                                queue.advance_sqhd(),
                                queue.qid,
                                cid,
                                status::DATA_XFER_ERROR | status::DNR,
                            );
                            queue.complete_receiving(*tag, cqe);
                            phase = RecvPhase::Header;
                            continue;
                        }
                        let kind = *kind;
                        let tag = *tag;
                        if let Err(err) = finish_payload(queue, tag, kind) {
                            send_term(fd, err).await;
                            return Ok(());
                        }
                        phase = RecvPhase::Header;
                    }
                }
            }
        }
    }
}

enum HandleError {
    Term(PduError),
    HostTerm { fes: u16, fei: u32 },
}

/// Route one decoded PDU header. Returns the next recv phase if payload
/// follows on the stream.
fn handle_pdu(
    queue: &Rc<QueueCore>,
    decoded: pdu::DecodedPdu,
    data_digest: bool,
) -> Result<Option<RecvPhase>, HandleError> {
    match decoded.kind {
        PduKind::CapsuleCmd(sqe) => {
            let Some(tag) = queue.claim_tag() else {
                // Host exceeded the negotiated queue depth.
                return Err(HandleError::Term(PduError {
                    fes: pdu::fes::PDU_SEQ_ERR,
                    fei: 0,
                }));
            };
            let slot = queue.slot(tag);
            if decoded.data_len > 0 {
                // In-capsule payload follows the capsule on the wire.
                if decoded.data_len as usize > slot.data().len() {
                    return Err(HandleError::Term(PduError {
                        fes: pdu::fes::DATA_LIMIT_EXCEEDED,
                        fei: 0,
                    }));
                }
                slot.set_data_len(decoded.data_len);
                slot.stash_sqe(sqe);
                Ok(Some(RecvPhase::Data {
                    tag,
                    base: 0,
                    remaining: decoded.data_len,
                    crc: digest::Crc32c::new(),
                    ddgst: decoded.ddgst && data_digest,
                    kind: PayloadKind::InCapsule,
                }))
            } else if needs_r2t(&sqe) {
                let length = sqe.dptr.length.get();
                if length as usize > slot.data().len() {
                    return Err(HandleError::Term(PduError {
                        fes: pdu::fes::DATA_LIMIT_EXCEEDED,
                        fei: 0,
                    }));
                }
                slot.set_data_len(length);
                slot.set_recv_offset(0);
                slot.stash_sqe(sqe);
                // Solicit the whole transfer with one R2T; the host may
                // split it into several H2CData PDUs.
                queue.solicit(tag, sqe.cid.get(), 0, length);
                Ok(None)
            } else {
                queue.submit(tag, sqe);
                Ok(None)
            }
        }
        PduKind::H2CData {
            cid,
            ttag,
            offset,
            length,
            last,
        } => {
            let tag = ttag;
            if usize::from(tag) >= usize::from(queue.sqsize) {
                return Err(HandleError::Term(PduError {
                    fes: pdu::fes::INVALID_PDU_HDR,
                    fei: 10, // ttag field offset
                }));
            }
            let slot = queue.slot(tag);
            let valid = slot.state() == ioutgt_core::queue::SlotState::Receiving
                && slot.stashed_sqe().cid.get() == cid
                && offset == slot.recv_offset()
                && offset
                    .checked_add(length)
                    .is_some_and(|end| end <= slot.data_len())
                && length > 0;
            if !valid {
                return Err(HandleError::Term(PduError {
                    fes: pdu::fes::DATA_OUT_OF_RANGE,
                    fei: 0,
                }));
            }
            if decoded.data_len != length {
                return Err(HandleError::Term(PduError {
                    fes: pdu::fes::INVALID_PDU_HDR,
                    fei: 16, // data_length field offset
                }));
            }
            Ok(Some(RecvPhase::Data {
                tag,
                base: offset,
                remaining: length,
                crc: digest::Crc32c::new(),
                ddgst: decoded.ddgst && data_digest,
                kind: PayloadKind::H2c { last, length },
            }))
        }
        PduKind::H2CTerm { fes, fei } => Err(HandleError::HostTerm { fes, fei }),
        PduKind::IcReq(_)
        | PduKind::IcResp(_)
        | PduKind::CapsuleResp(_)
        | PduKind::C2HData { .. }
        | PduKind::R2T { .. } => Err(HandleError::Term(PduError {
            fes: pdu::fes::PDU_SEQ_ERR,
            fei: 0,
        })),
    }
}

/// Send loop: drain ALL pending completions/R2Ts into one staging
/// buffer and ship them as a single send op.
///
/// Independent send SQEs on one socket carry no ordering guarantee, so
/// pipelining ops is not an option; batching into one op is — and it
/// fixes the worst sleep pattern this design can exhibit: with
/// one-op-per-response, the thread parks (one io_uring_enter round
/// trip) for every response even when dozens are queued. One park per
/// batch instead of one per IO.
async fn send_loop(
    queue: &Rc<QueueCore>,
    fd: i32,
    hdr_digest: bool,
    data_digest: bool,
) -> std::io::Result<()> {
    let slot_capacity = queue.slot(0).data().len();
    // Room for several full responses; a single one always fits.
    let staging_size = (slot_capacity + 256).max(512 * 1024);
    let mut staging: Option<Box<[u8]>> = Some(vec![0u8; staging_size].into_boxed_slice());
    let mut done_tags: Vec<u16> = Vec::with_capacity(usize::from(queue.sqsize));
    let mut carry: Option<SendWork> = None;

    loop {
        let first = match carry.take() {
            Some(work) => work,
            None => queue.next_send_work().await,
        };
        let mut buf = staging.take().expect("staged");
        let mut offset = 0usize;
        done_tags.clear();
        let mut work = Some(first);
        while let Some(item) = work {
            let worst_case = match &item {
                SendWork::R2t { .. } => 28,
                SendWork::Response(completion) => 64 + completion.data_len as usize + 4,
            };
            if offset + worst_case > buf.len() {
                carry = Some(item); // flush first, encode next round
                break;
            }
            offset += encode_send_work(queue, &mut buf[offset..], &item, hdr_digest, data_digest);
            if let SendWork::Response(completion) = item {
                done_tags.push(completion.tag);
            }
            work = queue.try_next_send_work();
        }

        let len = u32::try_from(offset).expect("staging < 4G");
        let (res, returned) = ops::send_partial(fd, buf, len)?.await;
        staging = Some(returned);
        let mut sent = res? as usize;
        // Short send: finish the remainder before anything else may hit
        // the wire (ordering).
        while sent < offset {
            let mut tail = staging.take().expect("staged");
            let remaining = offset - sent;
            tail.copy_within(sent..offset, 0);
            let (res, returned) =
                ops::send_partial(fd, tail, u32::try_from(remaining).expect("fits"))?.await;
            staging = Some(returned);
            let n = res? as usize;
            if n == 0 {
                return Err(std::io::ErrorKind::WriteZero.into());
            }
            offset = remaining;
            sent = n;
        }
        for tag in done_tags.drain(..) {
            queue.release_tag(tag);
        }
    }
}

/// Encode one work item at `out[0..]`; returns bytes written.
fn encode_send_work(
    queue: &Rc<QueueCore>,
    out: &mut [u8],
    work: &SendWork,
    hdr_digest: bool,
    data_digest: bool,
) -> usize {
    match *work {
        SendWork::R2t {
            tag,
            cid,
            offset,
            length,
        } => pdu::encode_r2t(out, cid, tag, offset, length, hdr_digest),
        SendWork::Response(completion) => {
            let mut offset = 0usize;
            let success_elide =
                completion.data_len > 0 && queue.sqhd_disabled && completion.cqe.status.get() == 0;
            if completion.data_len > 0 {
                let data_len = completion.data_len as usize;
                let hdr_len = pdu::encode_c2h_data(
                    &mut out[offset..],
                    completion.cqe.cid.get(),
                    0,
                    completion.data_len,
                    true,
                    success_elide,
                    hdr_digest,
                    data_digest,
                );
                offset += hdr_len;
                {
                    let slot_data = queue.slot(completion.tag).data();
                    out[offset..offset + data_len].copy_from_slice(&slot_data[..data_len]);
                }
                if data_digest {
                    let crc = digest::crc32c(&out[offset..offset + data_len]);
                    out[offset + data_len..offset + data_len + 4]
                        .copy_from_slice(&crc.to_le_bytes());
                    offset += 4;
                }
                offset += data_len;
            }
            if !success_elide {
                offset += pdu::encode_capsule_resp(&mut out[offset..], &completion.cqe, hdr_digest);
            }
            offset
        }
    }
}
