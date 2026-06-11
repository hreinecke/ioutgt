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

    // Send path. Held separately: teardown must join it (the gather
    // send references slot buffers) before freeing the queue.
    let send_task = {
        let queue = Rc::clone(&queue);
        let hdr_digest = conn.hdr_digest;
        let data_digest = conn.data_digest;
        tokio::task::spawn_local(async move {
            if let Err(err) = send_loop(&queue, fd, hdr_digest, data_digest).await {
                debug!(qid = queue.qid, "send loop ended: {err}");
            }
        })
    };

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
    // Stop the send task and wait for any in-flight send op before
    // anything it references is freed. shutdown() unwedges a send
    // parked on a full socket buffer; close_send() unparks an idle
    // send loop.
    queue.close_send();
    // SAFETY: fd is valid for the connection's lifetime; shutdown only
    // signals, never frees.
    unsafe { libc::shutdown(fd, libc::SHUT_RDWR) };
    // Own budget: the executing drain may have spent all of `waited`,
    // and the send task needs at least one poll cycle to observe
    // close_send/shutdown.
    let mut send_waited = 0u32;
    while !send_task.is_finished() && send_waited < 10_000 {
        match ops::sleep(Duration::from_millis(2)) {
            Ok(sleep) => {
                let _ = sleep.await;
            }
            Err(_) => break,
        }
        send_waited += 2;
    }
    if queue.executing() > 0 || !send_task.is_finished() {
        // A wedged backend op: leak the queue AND the slot tasks rather
        // than free memory the kernel may still write to. A suspended
        // backend future can own a private buffer (e.g. the write-zeroes
        // fallback chunk) referenced by an in-flight raw kernel op;
        // aborting the task would drop and free that buffer mid-DMA.
        // Leaking the tasks keeps every such future — and its buffer —
        // alive for the process's remaining lifetime. The same applies
        // to the send task: its in-flight gather op references slot
        // buffers and the batch arena.
        warn!(
            qid = conn.qid,
            executing = queue.executing(),
            "teardown timeout; leaking queue and tasks"
        );
        std::mem::forget(Rc::clone(&queue));
        std::mem::forget(send_task);
        for task in tasks {
            std::mem::forget(task);
        }
    } else {
        send_task.abort();
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

/// Worst-case arena bytes per staged item: C2HData header (24+4
/// HDGST) + DDGST trailer (4) + response capsule (24+4).
const ARENA_PER_ITEM: usize = 64;
/// Worst-case iovec entries per staged item (header, payload, digest,
/// capsule). Adjacent arena chunks merge; this is the unmerged bound.
const IOVS_PER_ITEM: usize = 4;
/// Kernel cap on msg_iovlen.
const UIO_MAXIOV: usize = libc::UIO_MAXIOV as usize;

/// One batch's gather state: headers and digests packed into a small
/// arena, payloads referenced in place from slot buffers. Exactly one
/// batch is in flight at a time; `reset()` recycles everything. All
/// memory is preallocated at queue install.
struct SendBatch {
    arena: Box<[u8]>,
    arena_used: usize,
    iovs: Vec<libc::iovec>,
    /// Hard entry cap (≤ UIO_MAXIOV). `Vec::with_capacity` may
    /// over-allocate, so the fit check must not use `capacity()`.
    iov_cap: usize,
    /// First entry not yet fully sent (short-send resume point).
    live: usize,
    msghdr: Box<libc::msghdr>,
}

impl SendBatch {
    fn new(sqsize: u16) -> SendBatch {
        let n = usize::from(sqsize);
        let iov_cap = (n * IOVS_PER_ITEM + IOVS_PER_ITEM).min(UIO_MAXIOV);
        SendBatch {
            arena: vec![0u8; (n * ARENA_PER_ITEM).max(4096)].into_boxed_slice(),
            arena_used: 0,
            iovs: Vec::with_capacity(iov_cap),
            iov_cap,
            live: 0,
            // SAFETY: a zeroed msghdr is a valid value; msg_iov[len]
            // are set in msghdr() before every submit.
            msghdr: Box::new(unsafe { std::mem::zeroed() }),
        }
    }

    fn reset(&mut self) {
        self.arena_used = 0;
        self.iovs.clear();
        self.live = 0;
    }

    /// Headroom for one more worst-case item?
    fn fits(&self) -> bool {
        self.arena_used + ARENA_PER_ITEM <= self.arena.len()
            && self.iovs.len() + IOVS_PER_ITEM <= self.iov_cap
    }

    /// Unused arena to encode the next header piece into.
    fn arena_tail(&mut self) -> &mut [u8] {
        &mut self.arena[self.arena_used..]
    }

    /// Publish `len` bytes just written at the arena tail.
    fn push_arena(&mut self, len: usize) {
        let start = self.arena_used;
        self.arena_used += len;
        let ptr = self.arena[start..].as_ptr();
        self.push_raw(ptr, len);
    }

    /// Append a wire chunk; merges with the previous entry when
    /// byte-contiguous (consecutive arena pieces collapse, so pure
    /// header batches degenerate to a single entry).
    fn push_raw(&mut self, ptr: *const u8, len: usize) {
        if len == 0 {
            return;
        }
        if let Some(last) = self.iovs.last_mut() {
            // SAFETY: one-past-the-end pointer, used only for equality.
            let end = unsafe { last.iov_base.cast::<u8>().add(last.iov_len) };
            if std::ptr::eq(end, ptr) {
                last.iov_len += len;
                return;
            }
        }
        self.iovs.push(libc::iovec {
            iov_base: ptr.cast_mut().cast(),
            iov_len: len,
        });
    }

    /// msghdr describing the unsent suffix; call before each submit.
    fn msghdr(&mut self) -> *const libc::msghdr {
        self.msghdr.msg_iov = self.iovs[self.live..].as_mut_ptr();
        self.msghdr.msg_iovlen = self.iovs.len() - self.live;
        &raw const *self.msghdr
    }

    /// Consume `sent` bytes; true when the whole batch hit the socket.
    fn advance(&mut self, sent: usize) -> bool {
        advance_iovecs(&mut self.iovs, &mut self.live, sent)
    }
}

/// Skip fully-sent entries, bump the partial one in place. Returns
/// true when `sent` consumed everything from `live` onward.
fn advance_iovecs(iovs: &mut [libc::iovec], live: &mut usize, mut sent: usize) -> bool {
    while *live < iovs.len() {
        let e = &mut iovs[*live];
        if sent < e.iov_len {
            // SAFETY: stays within the entry's own chunk.
            e.iov_base = unsafe { e.iov_base.cast::<u8>().add(sent).cast() };
            e.iov_len -= sent;
            return false;
        }
        sent -= e.iov_len;
        *live += 1;
    }
    true
}

/// Stage one work item: header pieces into the arena (sans-IO encoders
/// unchanged), payload referenced in place from the slot buffer, DDGST
/// computed over the slot and trailed in the arena.
fn stage_send_work(
    queue: &Rc<QueueCore>,
    batch: &mut SendBatch,
    work: &SendWork,
    hdr_digest: bool,
    data_digest: bool,
) {
    match *work {
        SendWork::R2t {
            tag,
            cid,
            offset,
            length,
        } => {
            let n = pdu::encode_r2t(batch.arena_tail(), cid, tag, offset, length, hdr_digest);
            batch.push_arena(n);
        }
        SendWork::Response(completion) => {
            let success_elide =
                completion.data_len > 0 && queue.sqhd_disabled && completion.cqe.status.get() == 0;
            if completion.data_len > 0 {
                let data_len = completion.data_len as usize;
                let n = pdu::encode_c2h_data(
                    batch.arena_tail(),
                    completion.cqe.cid.get(),
                    0,
                    completion.data_len,
                    true,
                    success_elide,
                    hdr_digest,
                    data_digest,
                );
                batch.push_arena(n);
                let slot_data = queue.slot(completion.tag).data();
                // The payload rides in place: the slot stays claimed
                // until release_tag after the batch send completes.
                batch.push_raw(slot_data.as_ptr(), data_len);
                if data_digest {
                    let crc = digest::crc32c(&slot_data[..data_len]);
                    batch.arena_tail()[..4].copy_from_slice(&crc.to_le_bytes());
                    batch.push_arena(4);
                }
            }
            if !success_elide {
                let n = pdu::encode_capsule_resp(batch.arena_tail(), &completion.cqe, hdr_digest);
                batch.push_arena(n);
            }
        }
    }
}

/// Send loop: drain ALL pending completions/R2Ts into one iovec
/// gather list and ship it as a single SENDMSG.
///
/// Independent send SQEs on one socket carry no ordering guarantee, so
/// pipelining ops is not an option; one op per batch is — and gather
/// keeps that while the payload entries point straight into slot
/// buffers (no staging copy). One park per batch, zero payload memcpy.
async fn send_loop(
    queue: &Rc<QueueCore>,
    fd: i32,
    hdr_digest: bool,
    data_digest: bool,
) -> std::io::Result<()> {
    let mut batch = SendBatch::new(queue.sqsize);
    let mut done_tags: Vec<u16> = Vec::with_capacity(usize::from(queue.sqsize));
    let mut carry: Option<SendWork> = None;

    loop {
        let first = match carry.take() {
            Some(work) => work,
            None => match queue.next_send_work().await {
                Some(work) => work,
                None => return Ok(()), // close_send(): teardown
            },
        };
        batch.reset();
        done_tags.clear();
        let mut work = Some(first);
        while let Some(item) = work {
            if !batch.fits() {
                carry = Some(item); // flush first, stage next round
                break;
            }
            stage_send_work(queue, &mut batch, &item, hdr_digest, data_digest);
            if let SendWork::Response(completion) = item {
                done_tags.push(completion.tag);
            }
            work = queue.try_next_send_work();
        }

        // Ship; on short send advance the iovecs and re-issue so
        // nothing else can interleave on the wire (ordering).
        loop {
            // SAFETY: the msghdr, iovec array, arena, and referenced
            // slot buffers all outlive the await — the batch is owned
            // by this task, slots release only after the batch
            // completes, and run_queue joins this task (or leaks the
            // queue) before freeing anything.
            let op = unsafe { ops::sendmsg_raw(fd, batch.msghdr()) }?;
            let n = op.await? as usize;
            if n == 0 {
                return Err(std::io::ErrorKind::WriteZero.into());
            }
            if batch.advance(n) {
                break;
            }
        }
        for tag in done_tags.drain(..) {
            queue.release_tag(tag);
        }
    }
}

#[cfg(test)]
mod gather_tests {
    use ioutgt_core::queue::Completion;

    use super::*;

    /// Linearize a batch's iovecs (what the kernel would put on the wire).
    fn gather(batch: &SendBatch) -> Vec<u8> {
        let mut out = Vec::new();
        for e in &batch.iovs {
            // SAFETY: entries reference the batch arena and slot
            // buffers owned by the test, sized by construction.
            let s = unsafe { std::slice::from_raw_parts(e.iov_base.cast::<u8>(), e.iov_len) };
            out.extend_from_slice(s);
        }
        out
    }

    #[test]
    fn batch_matches_linear_encoding() {
        let queue = QueueCore::new(1, 4, 4096, false);
        #[allow(clippy::cast_possible_truncation)]
        let payload: Vec<u8> = (0..1000u32).map(|i| i as u8).collect();
        queue.slot(2).data()[..1000].copy_from_slice(&payload);

        let mut batch = SendBatch::new(queue.sqsize);
        let cqe = Cqe::new(0, 1, 1, 7, 0);
        let items = [
            SendWork::R2t {
                tag: 3,
                cid: 9,
                offset: 0,
                length: 4096,
            },
            SendWork::Response(Completion {
                tag: 2,
                cqe,
                data_len: 1000,
            }),
        ];
        for item in &items {
            assert!(batch.fits());
            stage_send_work(&queue, &mut batch, item, true, true);
        }

        // Reference: the same PDUs encoded linearly (the old staging
        // layout): R2T | C2HData hdr | payload | DDGST | resp capsule.
        let mut expect = vec![0u8; 8192];
        let mut off = pdu::encode_r2t(&mut expect, 9, 3, 0, 4096, true);
        off += pdu::encode_c2h_data(&mut expect[off..], 7, 0, 1000, true, false, true, true);
        expect[off..off + 1000].copy_from_slice(&payload);
        off += 1000;
        let crc = digest::crc32c(&payload);
        expect[off..off + 4].copy_from_slice(&crc.to_le_bytes());
        off += 4;
        off += pdu::encode_capsule_resp(&mut expect[off..], &cqe, true);
        expect.truncate(off);

        assert_eq!(gather(&batch), expect);
        // Arena-contiguous chunks merge: [R2T+C2H hdr][payload][DDGST+capsule].
        assert_eq!(batch.iovs.len(), 3);
    }

    #[test]
    fn batch_elides_and_merges_without_digests() {
        // sqhd_disabled queue: a successful read elides the response
        // capsule; digests off exercises the bare-header layout.
        let queue = QueueCore::new(1, 4, 4096, true);
        let payload = [0xa5u8; 512];
        queue.slot(1).data()[..512].copy_from_slice(&payload);

        let mut batch = SendBatch::new(queue.sqsize);
        let read_cqe = Cqe::new(0, 1, 1, 5, 0);
        let flush_cqe = Cqe::new(0, 2, 1, 6, 0);
        let items = [
            // Elided: C2HData header + payload, no capsule.
            SendWork::Response(Completion {
                tag: 1,
                cqe: read_cqe,
                data_len: 512,
            }),
            // Data-less response: capsule only.
            SendWork::Response(Completion {
                tag: 3,
                cqe: flush_cqe,
                data_len: 0,
            }),
        ];
        for item in &items {
            assert!(batch.fits());
            stage_send_work(&queue, &mut batch, item, false, false);
        }

        let mut expect = vec![0u8; 4096];
        let mut off = pdu::encode_c2h_data(&mut expect, 5, 0, 512, true, true, false, false);
        expect[off..off + 512].copy_from_slice(&payload);
        off += 512;
        off += pdu::encode_capsule_resp(&mut expect[off..], &flush_cqe, false);
        expect.truncate(off);

        assert_eq!(gather(&batch), expect);
        // [C2H hdr][payload][capsule]: capsule can't merge across the
        // slot-payload entry.
        assert_eq!(batch.iovs.len(), 3);
    }

    #[test]
    fn advance_iovecs_walks_short_sends() {
        let a = [1u8, 2, 3, 4];
        let b = [5u8, 6, 7];
        let mut iovs = vec![
            libc::iovec {
                iov_base: a.as_ptr().cast_mut().cast(),
                iov_len: 4,
            },
            libc::iovec {
                iov_base: b.as_ptr().cast_mut().cast(),
                iov_len: 3,
            },
        ];
        let mut live = 0;
        assert!(!advance_iovecs(&mut iovs, &mut live, 5)); // all of a + 1 byte of b
        assert_eq!(live, 1);
        assert_eq!(iovs[1].iov_len, 2);
        assert!(!advance_iovecs(&mut iovs, &mut live, 1));
        assert_eq!(iovs[1].iov_len, 1);
        assert!(advance_iovecs(&mut iovs, &mut live, 1));
        assert_eq!(live, 2);
    }
}
