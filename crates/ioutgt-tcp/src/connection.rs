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
use ioutgt_nvme::spec::{Sqe, sgl};
use ioutgt_nvme::{digest, status};
use ioutgt_uring::ops;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

/// Admin-queue slot buffers: identify/log pages (4 KiB) plus margin.
pub const ADMIN_SLOT_BUF: usize = 8 * 1024;
/// IO-queue slot buffers: MDTS.
pub const IO_SLOT_BUF: usize = 128 * 1024;

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
pub async fn run_queue<B: Backend>(conn: QueueConn<B>) {
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

    for task in &tasks {
        task.abort();
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
                            let sqe = queue.slot(*tag).stashed_sqe();
                            warn!(qid = queue.qid, cid = sqe.cid.get(), "DDGST mismatch");
                            let _ = status::DATA_XFER_ERROR;
                            send_term(
                                fd,
                                PduError {
                                    fes: pdu::fes::HDR_DIGEST_ERR,
                                    fei: 0,
                                },
                            )
                            .await;
                            return Ok(());
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

/// Send loop: R2Ts and completions → wire, in order.
async fn send_loop(
    queue: &Rc<QueueCore>,
    fd: i32,
    hdr_digest: bool,
    data_digest: bool,
) -> std::io::Result<()> {
    loop {
        match queue.next_send_work().await {
            SendWork::R2t {
                tag,
                cid,
                offset,
                length,
            } => {
                let mut buf = vec![0u8; 28].into_boxed_slice();
                let n = pdu::encode_r2t(&mut buf, cid, tag, offset, length, hdr_digest);
                send_all(fd, &buf[..n]).await?;
                // No tag release: the slot is waiting for H2CData.
            }
            SendWork::Response(completion) => {
                // SUCCESS elision: when SQ flow control is off and the
                // read succeeded, the final C2HData carries the
                // completion (no response capsule), as nvmet does.
                let success_elide = completion.data_len > 0
                    && queue.sqhd_disabled
                    && completion.cqe.status.get() == 0;

                if completion.data_len > 0 {
                    let mut header = vec![0u8; 32].into_boxed_slice();
                    let hdr_len = pdu::encode_c2h_data(
                        &mut header,
                        completion.cqe.cid.get(),
                        0,
                        completion.data_len,
                        true,
                        success_elide,
                        hdr_digest,
                        data_digest,
                    );
                    let mut payload = Vec::with_capacity(completion.data_len as usize + 4);
                    payload.extend_from_slice(
                        &queue.slot(completion.tag).data()[..completion.data_len as usize],
                    );
                    if data_digest {
                        let crc = digest::crc32c(&payload);
                        payload.extend_from_slice(&crc.to_le_bytes());
                    }
                    let header = header[..hdr_len].to_vec().into_boxed_slice();
                    let total = header.len() + payload.len();
                    let (res, _bufs) =
                        ops::send_vectored(fd, header, payload.into_boxed_slice())?.await;
                    let sent = res? as usize;
                    if sent != total {
                        // Vectored short send: finish flat (rare slow path).
                        return Err(std::io::Error::other("short C2HData send"));
                    }
                }

                if !success_elide {
                    let mut rsp = vec![0u8; 28].into_boxed_slice();
                    let n = pdu::encode_capsule_resp(&mut rsp, &completion.cqe, hdr_digest);
                    send_all(fd, &rsp[..n]).await?;
                }

                queue.release_tag(completion.tag);
            }
        }
    }
}

/// Send a small fully-buffered frame, retrying short sends.
async fn send_all(fd: i32, frame: &[u8]) -> std::io::Result<()> {
    let mut offset = 0;
    while offset < frame.len() {
        let chunk = frame[offset..].to_vec().into_boxed_slice();
        let (res, _) = ops::send(fd, chunk)?.await;
        let sent = res? as usize;
        if sent == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        offset += sent;
    }
    Ok(())
}
