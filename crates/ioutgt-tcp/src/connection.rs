//! Queue-thread connection driver: recv state machine, slot tasks, send
//! path. All IO goes through the thread's io_uring reactor.
//!
//! M3 scope: capsule commands with optional in-capsule data, response
//! capsules, and single-PDU C2HData on the send side. H2CData/R2T and
//! data digests on the receive payload path land with the IO milestone;
//! until then those PDUs draw a C2HTermReq.

use std::os::fd::{AsRawFd, OwnedFd};
use std::rc::Rc;

use ioutgt_core::dispatch;
use ioutgt_core::queue::QueueCore;
use ioutgt_nvme::pdu::{self, PduDecoder, PduError, PduKind};
use ioutgt_nvme::spec::Sqe;
use ioutgt_nvme::{digest, status};
use ioutgt_uring::ops;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

/// Admin-queue slot buffers: identify/log pages (4 KiB) plus margin.
pub const ADMIN_SLOT_BUF: usize = 8 * 1024;
/// IO-queue slot buffers: MDTS.
pub const IO_SLOT_BUF: usize = 128 * 1024;

/// Everything a queue thread receives to run one connection.
#[allow(missing_docs)]
pub struct QueueConn {
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
}

/// Receive-side state across recv() completions.
enum RecvPhase {
    /// Assembling a PDU header in the decoder.
    Header,
    /// Copying in-capsule payload into the slot buffer.
    Data {
        tag: u16,
        remaining: u32,
        crc: digest::Crc32c,
        ddgst: bool,
    },
    /// Consuming the 4-byte data digest.
    Ddgst {
        tag: u16,
        expected: u32,
        have: [u8; 4],
        have_len: u8,
    },
}

/// Drive one queue connection to completion (EOF, error, or term).
pub async fn run_queue(conn: QueueConn) {
    let slot_buf = if conn.qid == 0 {
        ADMIN_SLOT_BUF
    } else {
        IO_SLOT_BUF
    };
    let queue = QueueCore::new(conn.qid, conn.sqsize, slot_buf, conn.sqhd_disabled);
    let fd = conn.fd.as_raw_fd();

    // Persistent task per tag.
    let mut tasks: Vec<JoinHandle<()>> = (0..conn.sqsize)
        .map(|tag| {
            let queue = Rc::clone(&queue);
            tokio::task::spawn_local(async move {
                loop {
                    let sqe = queue.await_command(tag).await;
                    let (cqe, data_len) = if queue.qid == 0 {
                        dispatch::execute_admin(&queue, tag, &sqe).await
                    } else {
                        dispatch::execute_io(&queue, tag, &sqe).await
                    };
                    queue.complete(tag, cqe, data_len);
                }
            })
        })
        .collect();

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
                    match decoded.kind {
                        PduKind::CapsuleCmd(sqe) => {
                            let Some(tag) = queue.claim_tag() else {
                                // Host exceeded the negotiated queue depth.
                                send_term(
                                    fd,
                                    PduError {
                                        fes: pdu::fes::PDU_SEQ_ERR,
                                        fei: 0,
                                    },
                                )
                                .await;
                                return Ok(());
                            };
                            if decoded.data_len > 0 {
                                if decoded.data_len as usize > queue.slot(tag).data().len() {
                                    send_term(
                                        fd,
                                        PduError {
                                            fes: pdu::fes::DATA_LIMIT_EXCEEDED,
                                            fei: 0,
                                        },
                                    )
                                    .await;
                                    return Ok(());
                                }
                                queue.slot(tag).set_data_len(decoded.data_len);
                                phase = RecvPhase::Data {
                                    tag,
                                    remaining: decoded.data_len,
                                    crc: digest::Crc32c::new(),
                                    ddgst: decoded.ddgst && data_digest,
                                };
                                // Store the SQE; submitted once payload
                                // completes.
                                queue.slot(tag).stash_sqe(sqe);
                            } else {
                                queue.submit(tag, sqe);
                            }
                        }
                        PduKind::H2CData { .. } => {
                            // R2T flow lands in M5.
                            send_term(
                                fd,
                                PduError {
                                    fes: pdu::fes::PDU_SEQ_ERR,
                                    fei: 0,
                                },
                            )
                            .await;
                            return Ok(());
                        }
                        PduKind::H2CTerm { fes, fei } => {
                            warn!(qid = queue.qid, fes, fei, "host terminated connection");
                            return Ok(());
                        }
                        PduKind::IcReq(_)
                        | PduKind::IcResp(_)
                        | PduKind::CapsuleResp(_)
                        | PduKind::C2HData { .. }
                        | PduKind::R2T { .. } => {
                            send_term(
                                fd,
                                PduError {
                                    fes: pdu::fes::PDU_SEQ_ERR,
                                    fei: 0,
                                },
                            )
                            .await;
                            return Ok(());
                        }
                    }
                }
                RecvPhase::Data {
                    tag,
                    remaining,
                    crc,
                    ddgst,
                } => {
                    let take = (*remaining as usize).min(slice.len());
                    let offset = queue.slot(*tag).data_len() as usize - *remaining as usize;
                    queue.slot(*tag).data()[offset..offset + take].copy_from_slice(&slice[..take]);
                    crc.update(&slice[..take]);
                    slice = &slice[take..];
                    *remaining -= u32::try_from(take).expect("take <= remaining: u32");
                    if *remaining == 0 {
                        let sqe = queue.slot(*tag).stashed_sqe();
                        if *ddgst {
                            phase = RecvPhase::Ddgst {
                                tag: *tag,
                                expected: crc.finalize(),
                                have: [0; 4],
                                have_len: 0,
                            };
                        } else {
                            queue.submit(*tag, sqe);
                            phase = RecvPhase::Header;
                        }
                    }
                }
                RecvPhase::Ddgst {
                    tag,
                    expected,
                    have,
                    have_len,
                } => {
                    let take = (4 - *have_len as usize).min(slice.len());
                    have[*have_len as usize..*have_len as usize + take]
                        .copy_from_slice(&slice[..take]);
                    *have_len += u8::try_from(take).expect("take <= 4");
                    slice = &slice[take..];
                    if *have_len == 4 {
                        let wire = u32::from_le_bytes(*have);
                        if wire != *expected {
                            // Data digest mismatch: complete the command
                            // with a transient error, per nvmet.
                            let sqe = queue.slot(*tag).stashed_sqe();
                            warn!(qid = queue.qid, cid = sqe.cid.get(), "DDGST mismatch");
                            send_term(
                                fd,
                                PduError {
                                    fes: pdu::fes::HDR_DIGEST_ERR,
                                    fei: 0,
                                },
                            )
                            .await;
                            let _ = status::DATA_XFER_ERROR;
                            return Ok(());
                        }
                        let sqe = queue.slot(*tag).stashed_sqe();
                        queue.submit(*tag, sqe);
                        phase = RecvPhase::Header;
                    }
                }
            }
        }
    }
}

/// Send loop: completions → response capsules (+ C2HData when the slot
/// holds read payload).
async fn send_loop(
    queue: &Rc<QueueCore>,
    fd: i32,
    hdr_digest: bool,
    data_digest: bool,
) -> std::io::Result<()> {
    loop {
        let completion = queue.next_completion().await;

        if completion.data_len > 0 {
            // Single-PDU C2HData (split per MAXDATA arrives with M5),
            // then the response capsule.
            let mut header = vec![0u8; 32].into_boxed_slice();
            let hdr_len = pdu::encode_c2h_data(
                &mut header,
                completion.cqe.cid.get(),
                0,
                completion.data_len,
                true,
                false,
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
            let (res, _bufs) = ops::send_vectored(fd, header, payload.into_boxed_slice())?.await;
            let sent = res? as usize;
            if sent != total {
                // Short send handling is part of the M5 hardening pass;
                // treat as fatal for now.
                return Err(std::io::Error::other("short C2HData send"));
            }
        }

        let mut rsp = vec![0u8; 28].into_boxed_slice();
        let n = pdu::encode_capsule_resp(&mut rsp, &completion.cqe, hdr_digest);
        let rsp = rsp[..n].to_vec().into_boxed_slice();
        let (res, _) = ops::send(fd, rsp)?.await;
        let sent = res? as usize;
        if sent != n {
            return Err(std::io::Error::other("short response send"));
        }

        queue.release_tag(completion.tag);
    }
}
