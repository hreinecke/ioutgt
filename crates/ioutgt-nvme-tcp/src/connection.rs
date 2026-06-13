//! Queue-thread connection driver: recv state machine, slot tasks, send
//! path. All IO goes through the thread's io_uring reactor.
//!
//! Receive side: capsule commands carry data in-capsule (≤ IOCCSZ) or
//! host-resident via R2T — we solicit the whole transfer with a single
//! R2T (TTAG = slot index) and reassemble however many H2CData PDUs the
//! host sends, verifying a data digest per PDU. Send side: single-PDU
//! C2HData for reads (with the SUCCESS elision when SQ flow control is
//! off), R2Ts, response capsules — all serialized on one send task.

use std::collections::VecDeque;
use std::future::Future;
use std::os::fd::{AsRawFd, OwnedFd};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use ioutgt_core::backend::Backend;
use ioutgt_core::controller::Registry;
use ioutgt_core::dispatch::{self, ConnCtx, Role};
use crate::queue::{SendWork, TcpQueue};
use ioutgt_core::subsystem::PortConfig;
use ioutgt_nvme::fabrics::ConnectData;
use ioutgt_nvme::pdu::{self, PduDecoder, PduError, PduKind};
use ioutgt_nvme::spec::{Cqe, Sqe, sgl};
use ioutgt_nvme::{digest, status};
use ioutgt_uring::ops;
use ioutgt_uring::sendbatch::GatherBatch;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

/// Admin-queue slot buffers: identify/log pages (4 KiB) plus margin.
pub const ADMIN_SLOT_BUF: usize = 8 * 1024;
/// IO-queue slot buffers: MDTS.
pub const IO_SLOT_BUF: usize = 128 * 1024;

pub use ioutgt_core::permit::ConnPermit;

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
    /// Ship payload-carrying batches as SENDMSG_ZC, gating slot reuse
    /// on the zero-copy notification (--send-zc).
    pub send_zc: bool,
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
}

impl DataPhase {
    /// Copy payload bytes from the recv buffer into the slot; returns
    /// the next phase once this PDU's payload has fully arrived.
    fn advance(
        &mut self,
        queue: &Rc<TcpQueue>,
        slice: &mut &[u8],
    ) -> Result<Option<RecvPhase>, RecvEnd> {
        let take = (self.remaining as usize).min(slice.len());
        {
            let slot = queue.slot(self.tag);
            let total = match self.kind {
                PayloadKind::InCapsule => slot.data_len(),
                PayloadKind::H2c { length, .. } => length,
            };
            let dest = (self.base + (total - self.remaining)) as usize;
            slot.data()[dest..dest + take].copy_from_slice(&slice[..take]);
        }
        self.crc.update(&slice[..take]);
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
        queue: &Rc<TcpQueue>,
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
            let cid = queue.slot(self.tag).stashed_sqe().cid.get();
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
    let queue = TcpQueue::new(conn.qid, conn.sqsize, slot_buf, conn.sqhd_disabled);
    let fd = conn.fd.as_raw_fd();
    let ctx = if conn.qid == 0 {
        ConnCtx::new_admin(
            Rc::clone(&queue.nvme),
            Arc::clone(&conn.port),
            Arc::clone(&conn.registry),
            conn.connect_data,
        )
    } else {
        ConnCtx::new_io(
            Rc::clone(&queue.nvme),
            Arc::clone(&conn.port),
            Arc::clone(&conn.registry),
            conn.connect_data,
        )
    };

    on_ctx(&ctx);

    let mut tasks = spawn_slot_tasks(&queue, &ctx);
    if let Role::Admin(_) = &ctx.role {
        tasks.push(spawn_keepalive_watchdog(Rc::clone(&ctx), fd));
    }
    let send_task = spawn_send_task(
        Rc::clone(&queue),
        fd,
        conn.hdr_digest,
        conn.data_digest,
        conn.send_zc,
    );

    // The Connect command was consumed on the control thread; run it
    // through the normal slot pipeline as this queue's first command.
    let tag = queue.claim_tag().expect("fresh queue has free tags");
    queue.submit(tag, conn.connect_sqe);

    // Receive path (this task).
    if let Err(err) = recv_loop(&queue, fd, conn.hdr_digest, conn.data_digest, conn.send_zc).await {
        debug!(qid = conn.qid, "connection closed: {err}");
    }

    teardown(&queue, &ctx, fd, send_task, tasks).await;
    // conn.fd drops here, closing the socket; in-flight ops orphan and
    // drain through the reactor.
}

/// One persistent task per command slot: each waits for its tag's next
/// command, executes it, and posts the completion.
fn spawn_slot_tasks<B: Backend>(
    queue: &Rc<TcpQueue>,
    ctx: &Rc<ConnCtx<B>>,
) -> Vec<JoinHandle<()>> {
    (0..queue.sqsize)
        .map(|tag| {
            let queue = Rc::clone(queue);
            let ctx = Rc::clone(ctx);
            tokio::task::spawn_local(async move {
                loop {
                    let sqe = queue.await_command(tag).await;
                    let outcome = dispatch::execute(&ctx, tag, &sqe).await;
                    queue.complete(tag, outcome.cqe, outcome.data_len);
                }
            })
        })
        .collect()
}

/// Keep-alive watchdog (admin queues): close the socket when the host
/// goes silent past KATO + grace, which unwinds the whole connection.
fn spawn_keepalive_watchdog<B: Backend>(ctx: Rc<ConnCtx<B>>, fd: i32) -> JoinHandle<()> {
    tokio::task::spawn_local(async move {
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
    })
}

/// Send path. Held separately from the slot tasks: teardown must join
/// it (the gather send references slot buffers) before freeing the
/// queue.
fn spawn_send_task(
    queue: Rc<TcpQueue>,
    fd: i32,
    hdr_digest: bool,
    data_digest: bool,
    send_zc: bool,
) -> JoinHandle<()> {
    tokio::task::spawn_local(async move {
        if let Err(err) = send_loop(&queue, fd, hdr_digest, data_digest, send_zc).await {
            debug!(qid = queue.qid, "send loop ended: {err}");
            // A dead send path leaves the connection half-alive:
            // the recv loop keeps accepting commands whose
            // responses can never ship, and the host only notices
            // at its IO timeout (~30 s). Shut the socket down so
            // the recv loop sees EOF and teardown runs now.
            // SAFETY: fd is valid for the connection's lifetime;
            // shutdown only signals, never frees.
            unsafe { libc::shutdown(fd, libc::SHUT_RDWR) };
        }
    })
}

/// Poll `done` every 2 ms for up to 10 s; each call gets its own
/// budget. The teardown quiesce primitive.
async fn quiesce(mut done: impl FnMut() -> bool) {
    let mut waited = 0u32;
    while !done() && waited < 10_000 {
        match ops::sleep(Duration::from_millis(2)) {
            Ok(sleep) => {
                let _ = sleep.await;
            }
            Err(_) => break,
        }
        waited += 2;
    }
}

/// Post-recv teardown: quiesce executing slots and the send task, then
/// abort the per-tag tasks — or leak everything on timeout.
async fn teardown<B: Backend>(
    queue: &Rc<TcpQueue>,
    ctx: &Rc<ConnCtx<B>>,
    fd: i32,
    send_task: JoinHandle<()>,
    tasks: Vec<JoinHandle<()>>,
) {
    // Resolve parked AERs (their slots count as executing but reference
    // no kernel-visible memory) so the drain below terminates promptly.
    ctx.close();

    // Backend ops in flight reference slot memory: wait for executing
    // slots to finish before aborting tasks and freeing the queue.
    quiesce(|| queue.executing() == 0).await;
    // Stop the send task and wait for any in-flight send op before
    // anything it references is freed. shutdown() unwedges a send
    // parked on a full socket buffer; close_send() unparks an idle
    // send loop.
    queue.close_send();
    // SAFETY: fd is valid for the connection's lifetime; shutdown only
    // signals, never frees.
    unsafe { libc::shutdown(fd, libc::SHUT_RDWR) };
    // Own budget: the executing drain may have spent all of its wait,
    // and the send task needs at least one poll cycle to observe
    // close_send/shutdown.
    quiesce(|| send_task.is_finished()).await;
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
            qid = queue.qid,
            executing = queue.executing(),
            "teardown timeout; leaking queue and tasks"
        );
        std::mem::forget(Rc::clone(queue));
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
fn finish_payload(queue: &Rc<TcpQueue>, tag: u16, kind: PayloadKind) -> Result<(), PduError> {
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
async fn recv_loop(
    queue: &Rc<TcpQueue>,
    fd: i32,
    hdr_digest: bool,
    data_digest: bool,
    send_zc: bool,
) -> std::io::Result<()> {
    match drive_recv(queue, fd, hdr_digest, data_digest, send_zc).await {
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
    queue: &Rc<TcpQueue>,
    fd: i32,
    hdr_digest: bool,
    data_digest: bool,
    send_zc: bool,
) -> Result<(), RecvEnd> {
    let mut decoder = PduDecoder::new(hdr_digest);
    let mut phase = RecvPhase::Header;
    // 64 KiB recv buffer, allocated once per connection: only headers,
    // in-capsule payloads (≤ 16 KiB inline limit), and payload prefixes
    // pass through it — H2C tails ≥ H2C_DIRECT_MIN bypass it into the
    // slot, and read data never touches it. The size is the small-IO
    // batching unit (~15 × 4 KiB write capsules per wakeup) and matches
    // the kernel's max GRO/loopback burst (64 KiB), so one recv drains
    // the largest coalesced unit the stack delivers per softirq pass.
    // Larger buys nothing (big payloads are routed around it); smaller
    // splits one burst into several wakeup + state-machine passes.
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
            let next = match &mut phase {
                RecvPhase::Header => {
                    feed_header(queue, &mut decoder, &mut slice, data_digest, send_zc).await?
                }
                RecvPhase::Data(data) => data.advance(queue, &mut slice)?,
                RecvPhase::Ddgst(ddgst) => ddgst.advance(queue, &mut slice)?,
            };
            if let Some(next) = next {
                phase = next;
            }
        }

        // Buffer exhausted mid-payload with a large H2C tail still to
        // come: pull it straight into the slot, skipping the
        // buffer→slot copy. Copy-snapshot of the phase (every field is
        // Copy — Crc32c included — so this is cheap): a failed guard
        // falls through with `phase`, including its
        // partially-accumulated CRC, untouched for the normal copy
        // path.
        if let &RecvPhase::Data(data) = &phase
            && matches!(data.kind, PayloadKind::H2c { .. })
            && data.remaining >= H2C_DIRECT_MIN
        {
            phase = recv_tail_direct(queue, fd, data).await?;
        }
    }
}

/// Header phase: feed the decoder; once a header is complete, route
/// the PDU. Returns the next phase if payload follows on the stream.
async fn feed_header(
    queue: &Rc<TcpQueue>,
    decoder: &mut PduDecoder,
    slice: &mut &[u8],
    data_digest: bool,
    send_zc: bool,
) -> Result<Option<RecvPhase>, RecvEnd> {
    let consumed = decoder.feed(slice)?;
    *slice = &slice[consumed..];
    if !decoder.is_complete() {
        return Ok(None);
    }
    let decoded = decoder.take()?;
    handle_pdu(queue, decoded, data_digest, send_zc).await
}

/// Receive a payload tail straight into the slot buffer at the
/// reassembly offset. The tail is by definition the next bytes on the
/// TCP stream, so the buffer recv is NOT re-armed until the tail
/// lands — this serializes nothing that could have proceeded, and
/// there is never more than one outstanding recv on the socket.
/// MSG_WAITALL is best-effort: short-but-nonzero returns resume IN
/// PLACE at the advanced slot offset (never staged elsewhere, never
/// restarted); 0 is an orderly close mid-payload, as on the buffered
/// path.
async fn recv_tail_direct(
    queue: &Rc<TcpQueue>,
    fd: i32,
    mut data: DataPhase,
) -> Result<RecvPhase, RecvEnd> {
    let total = match data.kind {
        PayloadKind::InCapsule => queue.slot(data.tag).data_len(),
        PayloadKind::H2c { length, .. } => length,
    };
    let mut dest = (data.base + (total - data.remaining)) as usize;
    let tail_start = dest;
    while data.remaining > 0 {
        let ptr = {
            // Scoped: the RefCell borrow must end before the
            // await below.
            let mut slot_data = queue.slot(data.tag).data();
            slot_data[dest..dest + data.remaining as usize].as_mut_ptr()
        };
        // SAFETY: ptr..ptr+remaining is slot-buffer memory
        // (bounds-checked by the slicing above) owned by
        // TcpQueue, to which this function holds an Rc, and
        // the op is awaited inline — the recv path cannot return
        // while it is in flight, so the memory outlives the
        // terminal CQE. If the whole run_queue future is
        // instead dropped mid-await (LocalSet teardown), the
        // reactor's orphan protocol holds the op entry until
        // its terminal CQE — the same accepted envelope as
        // backend raw ops on slot memory (queue threads run
        // for the process lifetime; reactor drain/leak is the
        // backstop before anything is freed). Nothing else
        // touches this slot's data while its state is
        // Receiving.
        let n = unsafe { ops::recv_raw_waitall(fd, ptr, data.remaining) }?.await?;
        if n == 0 {
            return Err(RecvEnd::Closed); // orderly close mid-payload
        }
        dest += n as usize;
        data.remaining -= n; // recv returns at most `remaining`
    }
    if data.ddgst {
        {
            // Re-borrowed after the await: warm pass over the
            // tail the kernel just wrote.
            let slot_data = queue.slot(data.tag).data();
            data.crc.update(&slot_data[tail_start..dest]);
        }
        // Next ops::recv starts at the 4-byte digest.
        Ok(RecvPhase::ddgst(data.tag, data.crc.finalize(), data.kind))
    } else {
        finish_payload(queue, data.tag, data.kind)?;
        // Next ops::recv starts at the next PDU header.
        Ok(RecvPhase::Header)
    }
}

/// Route one decoded PDU header. Returns the next recv phase if payload
/// follows on the stream. Async only for the ZC-mode tag wait; every
/// other path resolves immediately.
async fn handle_pdu(
    queue: &Rc<TcpQueue>,
    decoded: pdu::DecodedPdu,
    data_digest: bool,
    send_zc: bool,
) -> Result<Option<RecvPhase>, RecvEnd> {
    let ddgst = decoded.ddgst && data_digest;
    match decoded.kind {
        PduKind::CapsuleCmd(sqe) => {
            handle_capsule_cmd(queue, sqe, decoded.data_len, ddgst, send_zc).await
        }
        PduKind::H2CData {
            cid,
            ttag,
            offset,
            length,
            last,
        } => {
            let tag = validate_h2c(queue, cid, ttag, offset, length, decoded.data_len)?;
            Ok(Some(RecvPhase::Data(DataPhase {
                tag,
                base: offset,
                remaining: length,
                crc: digest::Crc32c::new(),
                ddgst,
                kind: PayloadKind::H2c { last, length },
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
    queue: &Rc<TcpQueue>,
    sqe: Sqe,
    data_len: u32,
    ddgst: bool,
    send_zc: bool,
) -> Result<Option<RecvPhase>, RecvEnd> {
    // ZC mode: payload tags release on the send notification
    // (≈ the peer's ACK), racing the host's next command — an
    // empty freelist is an expected transient; park (TCP
    // backpressure) instead of terminating. Non-ZC keeps the
    // strict depth-violation term.
    let tag = if send_zc {
        queue.await_tag().await
    } else {
        // An empty freelist means the host exceeded the negotiated
        // queue depth.
        queue
            .claim_tag()
            .ok_or_else(|| RecvEnd::term(pdu::fes::PDU_SEQ_ERR))?
    };
    let slot = queue.slot(tag);
    if data_len > 0 {
        // In-capsule payload follows the capsule on the wire.
        if data_len as usize > slot.data().len() {
            return Err(RecvEnd::term(pdu::fes::DATA_LIMIT_EXCEEDED));
        }
        slot.set_data_len(data_len);
        slot.stash_sqe(sqe);
        Ok(Some(RecvPhase::Data(DataPhase {
            tag,
            base: 0,
            remaining: data_len,
            crc: digest::Crc32c::new(),
            ddgst,
            kind: PayloadKind::InCapsule,
        })))
    } else if needs_r2t(&sqe) {
        let length = sqe.dptr.length.get();
        if length as usize > slot.data().len() {
            return Err(RecvEnd::term(pdu::fes::DATA_LIMIT_EXCEEDED));
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

/// Validate one H2CData header against its slot's expected reassembly
/// state; returns the target tag.
fn validate_h2c(
    queue: &Rc<TcpQueue>,
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
        && slot.stashed_sqe().cid.get() == cid
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

/// Worst-case arena bytes per staged item: C2HData header (24+4
/// HDGST) + DDGST trailer (4) + response capsule (24+4).
const ARENA_PER_ITEM: usize = 64;
/// Worst-case iovec entries per staged item (header, payload, digest,
/// capsule). Adjacent arena chunks merge; this is the unmerged bound.
const IOVS_PER_ITEM: usize = 4;

/// One batch's gather state: a [`GatherBatch`] for the
/// protocol-free arena/iovec/msghdr mechanics, plus NVMe-specific
/// tag-accounting and ZC notification vectors. Exactly one batch is
/// in flight at a time; `reset()` recycles everything. All memory is
/// preallocated at queue install.
struct SendBatch {
    gather: GatherBatch,
    /// Response tags safe to release at the send CQE: capsule-only
    /// responses, whose slot memory the op never references.
    tags_at_cqe: Vec<u16>,
    /// Response tags whose slot buffers ride in the iovecs: released
    /// only after every notification for this batch is reaped.
    tags_at_notif: Vec<u16>,
    /// One notification per ZC op issued for this batch; >1 only
    /// after short-send re-issues (the cold path).
    pending_notifs: Vec<ops::ZcNotif>,
}

impl SendBatch {
    fn new(sqsize: u16) -> SendBatch {
        let n = usize::from(sqsize);
        SendBatch {
            gather: GatherBatch::new(n * ARENA_PER_ITEM, n * IOVS_PER_ITEM + IOVS_PER_ITEM),
            tags_at_cqe: Vec::with_capacity(n),
            tags_at_notif: Vec::with_capacity(n),
            pending_notifs: Vec::with_capacity(8),
        }
    }

    fn reset(&mut self) {
        self.gather.reset();
        self.tags_at_cqe.clear();
        self.tags_at_notif.clear();
        debug_assert!(
            self.pending_notifs.is_empty(),
            "batch reset with notifications outstanding"
        );
    }

    /// Headroom for one more worst-case item?
    fn fits(&self) -> bool {
        self.gather.fits(ARENA_PER_ITEM, IOVS_PER_ITEM)
    }

    /// Unused arena to encode the next header piece into.
    fn arena_tail(&mut self) -> &mut [u8] {
        self.gather.arena_tail()
    }

    /// Publish `len` bytes just written at the arena tail.
    fn push_arena(&mut self, len: usize) {
        self.gather.push_arena(len);
    }

    /// Append a wire chunk; merges with the previous entry when
    /// byte-contiguous (consecutive arena pieces collapse, so pure
    /// header batches degenerate to a single entry).
    fn push_raw(&mut self, ptr: *const u8, len: usize) {
        self.gather.push_raw(ptr, len);
    }

    /// msghdr describing the unsent suffix; call before each submit.
    fn msghdr(&mut self) -> *const libc::msghdr {
        self.gather.msghdr()
    }

    /// Consume `sent` bytes; true when the whole batch hit the socket.
    fn advance(&mut self, sent: usize) -> bool {
        self.gather.advance(sent)
    }

    /// Release the notif-gated tags and recycle the batch for staging.
    /// Callers must have reaped every pending notification first.
    fn recycle(&mut self, queue: &Rc<TcpQueue>) {
        for tag in self.tags_at_notif.drain(..) {
            queue.release_tag(tag);
        }
        self.reset();
    }
}

/// Stage one work item: header pieces into the arena (sans-IO encoders
/// unchanged), payload referenced in place from the slot buffer, DDGST
/// computed over the slot and trailed in the arena.
fn stage_send_work(
    queue: &Rc<TcpQueue>,
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

/// Stage one item and record its tag-release class: payload-carrying
/// responses gate on the batch's send notification, capsule-only
/// responses release at the send CQE (their slot memory is not
/// referenced by the op), R2Ts release nothing.
fn stage_and_account(
    queue: &Rc<TcpQueue>,
    batch: &mut SendBatch,
    work: &SendWork,
    hdr_digest: bool,
    data_digest: bool,
) {
    stage_send_work(queue, batch, work, hdr_digest, data_digest);
    if let SendWork::Response(completion) = work {
        if completion.data_len > 0 {
            batch.tags_at_notif.push(completion.tag);
        } else {
            batch.tags_at_cqe.push(completion.tag);
        }
    }
}

/// Double-buffered send state for ZC mode: while one batch's
/// notifications are outstanding (≈ one RTT — the peer's ACK frees
/// the skbs), the other batch stages and ships. At most one *send*
/// op is ever in flight, preserving wire ordering; only
/// notifications overlap. Non-ZC batches recycle immediately, so the
/// flag-off path degenerates to the previous single-batch behavior.
struct SendState {
    batches: [SendBatch; 2],
    /// Indices into `batches` with outstanding notifications, oldest
    /// first (len ≤ 2).
    inflight: VecDeque<usize>,
    zc_batches: u64,
    zc_copied: u64,
    /// ZC sends refused by the kernel's pinned-page accounting
    /// (RLIMIT_MEMLOCK) and shipped by copy instead.
    zc_fallbacks: u64,
}

impl SendState {
    fn new(sqsize: u16) -> SendState {
        SendState {
            batches: [SendBatch::new(sqsize), SendBatch::new(sqsize)],
            inflight: VecDeque::with_capacity(2),
            zc_batches: 0,
            zc_copied: 0,
            zc_fallbacks: 0,
        }
    }

    /// Index of a batch free for staging, reaping the oldest
    /// in-flight batch first when both are awaiting notifications.
    async fn acquire(&mut self, queue: &Rc<TcpQueue>) -> usize {
        if self.inflight.len() == self.batches.len() {
            self.reap_oldest(queue).await;
        }
        (0..self.batches.len())
            .find(|i| !self.inflight.contains(i))
            .expect("two batches, at most one in flight here")
    }

    /// Await every notification of the oldest in-flight batch, then
    /// release its notif-gated tags and recycle it.
    async fn reap_oldest(&mut self, queue: &Rc<TcpQueue>) {
        if let Some(idx) = self.inflight.pop_front() {
            self.reap(queue, idx).await;
        }
    }

    /// Await every notification of one batch, then recycle it.
    async fn reap(&mut self, queue: &Rc<TcpQueue>, idx: usize) {
        while let Some(notif) = self.batches[idx].pending_notifs.pop() {
            if notif.await {
                self.zc_copied += 1;
            }
        }
        self.batches[idx].recycle(queue);
    }

    /// Drain every outstanding notification. Teardown MUST run this
    /// on every send-task exit path: the kernel may still hold page
    /// references (arena + slot buffers) that `run_queue` frees right
    /// after joining the task. Covers the in-flight list AND the
    /// batch a send error abandoned mid-ship — that one is not on
    /// the list but may hold notifs from its earlier ZC ops.
    async fn drain(&mut self, queue: &Rc<TcpQueue>) {
        while let Some(idx) = self.inflight.pop_front() {
            self.reap(queue, idx).await;
        }
        for idx in 0..self.batches.len() {
            self.reap(queue, idx).await;
        }
    }

    /// Park on send work while reaping the oldest in-flight batch's
    /// notifications — the anti-deadlock invariant: tag release must
    /// never depend on new send work arriving (with all tags
    /// notif-gated and the host idle, work can only be *produced*
    /// once a notif frees a tag).
    async fn next_work_reaping(&mut self, queue: &Rc<TcpQueue>) -> Option<SendWork> {
        loop {
            let Some(&front) = self.inflight.front() else {
                return queue.next_send_work().await;
            };
            enum Wait {
                Work(Option<SendWork>),
                Reaped,
            }
            let zc_copied = &mut self.zc_copied;
            let batch = &mut self.batches[front];
            let outcome = std::future::poll_fn(|cx| {
                if let Poll::Ready(work) = queue.poll_send_work(cx) {
                    return Poll::Ready(Wait::Work(work));
                }
                if poll_notifs(&mut batch.pending_notifs, zc_copied, cx) {
                    return Poll::Ready(Wait::Reaped);
                }
                Poll::Pending
            })
            .await;
            match outcome {
                Wait::Work(work) => return work,
                Wait::Reaped => {
                    self.inflight.pop_front();
                    self.batches[front].recycle(queue);
                }
            }
        }
    }

    /// A batch just hit the socket: release what the send CQE allows
    /// and classify the batch. Notif-gated iff it actually holds
    /// notifications — a batch whose every ZC attempt fell back has
    /// none and is immediately reusable, like the plain path.
    fn retire(&mut self, queue: &Rc<TcpQueue>, idx: usize) {
        let batch = &mut self.batches[idx];
        for tag in batch.tags_at_cqe.drain(..) {
            queue.release_tag(tag);
        }
        if batch.pending_notifs.is_empty() {
            batch.recycle(queue);
        } else {
            self.zc_batches += 1;
            self.inflight.push_back(idx);
        }
    }
}

/// Poll every pending notification once; completed ones are recorded
/// in `zc_copied` and removed. No future is ever dropped unfinished,
/// so completion is never silently lost. True when none remain.
fn poll_notifs(notifs: &mut Vec<ops::ZcNotif>, zc_copied: &mut u64, cx: &mut Context<'_>) -> bool {
    let mut i = 0;
    while i < notifs.len() {
        match Pin::new(&mut notifs[i]).poll(cx) {
            Poll::Ready(copied) => {
                notifs.swap_remove(i);
                if copied {
                    *zc_copied += 1;
                }
            }
            Poll::Pending => i += 1,
        }
    }
    notifs.is_empty()
}

/// Send loop: drain ALL pending completions/R2Ts into one iovec
/// gather list and ship it as a single SENDMSG — or SENDMSG_ZC when
/// `--send-zc` is on, gating slot reuse on the zero-copy
/// notification (pin-budget failures fall back to the copying path).
///
/// Independent send SQEs on one socket carry no ordering guarantee, so
/// pipelining ops is not an option; one op per batch is — and gather
/// keeps that while the payload entries point straight into slot
/// buffers (no staging copy). One park per batch, zero payload memcpy.
async fn send_loop(
    queue: &Rc<TcpQueue>,
    fd: i32,
    hdr_digest: bool,
    data_digest: bool,
    send_zc: bool,
) -> std::io::Result<()> {
    let mut state = SendState::new(queue.sqsize);
    let result = send_batches(queue, fd, hdr_digest, data_digest, send_zc, &mut state).await;
    // Every exit path — orderly close, send error, WriteZero — drains
    // outstanding notifications before the queue's memory can be
    // freed by run_queue's join.
    state.drain(queue).await;
    if send_zc {
        debug!(
            qid = queue.qid,
            zc_batches = state.zc_batches,
            zc_copied = state.zc_copied,
            zc_fallbacks = state.zc_fallbacks,
            "send loop ZC stats"
        );
    }
    result
}

async fn send_batches(
    queue: &Rc<TcpQueue>,
    fd: i32,
    hdr_digest: bool,
    data_digest: bool,
    send_zc: bool,
    state: &mut SendState,
) -> std::io::Result<()> {
    let mut carry: Option<SendWork> = None;
    loop {
        let first = match carry.take() {
            Some(work) => work,
            None => match state.next_work_reaping(queue).await {
                Some(work) => work,
                None => return Ok(()), // close_send(): teardown
            },
        };
        let idx = state.acquire(queue).await;
        let batch = &mut state.batches[idx];
        carry = stage_batch(queue, batch, first, hdr_digest, data_digest);
        ship_batch(fd, batch, send_zc, &mut state.zc_fallbacks).await?;
        state.retire(queue, idx);
    }
}

/// Stage `first` and every immediately-available work item into the
/// batch, stopping at its headroom. Returns the item that didn't fit
/// (it is staged first next round).
fn stage_batch(
    queue: &Rc<TcpQueue>,
    batch: &mut SendBatch,
    first: SendWork,
    hdr_digest: bool,
    data_digest: bool,
) -> Option<SendWork> {
    let mut work = Some(first);
    while let Some(item) = work {
        if !batch.fits() {
            return Some(item); // flush first, stage next round
        }
        stage_and_account(queue, batch, &item, hdr_digest, data_digest);
        work = queue.try_next_send_work();
    }
    None
}

/// Ship one staged batch; on short send advance the iovecs and
/// re-issue so nothing else can interleave on the wire (ordering).
/// The re-issue stays ZC: the batch's pages are pinned regardless.
async fn ship_batch(
    fd: i32,
    batch: &mut SendBatch,
    send_zc: bool,
    zc_fallbacks: &mut u64,
) -> std::io::Result<()> {
    let mut use_zc = send_zc;
    loop {
        let n = if use_zc {
            // SAFETY: msghdr, iovecs, arena, and referenced slot
            // buffers stay allocated until this batch's notifs
            // are reaped (reap_oldest/drain precede reset, and
            // run_queue joins this task — or leaks the queue —
            // before freeing anything). The kernel snapshots the
            // iovec array at issue, so advance() after the send
            // CQE never races it.
            let mut op = unsafe { ops::sendmsg_zc_raw(fd, batch.msghdr()) }?;
            let res = op.sent().await;
            // Stash the notif BEFORE examining the result: a
            // failed ZC send can still have pinned pages
            // (F_MORE), and only the stashed handle makes the
            // teardown drain wait for them.
            batch.pending_notifs.push(op.into_notif());
            match res {
                Ok(n) => n as usize,
                // ZC pins pages against the per-user
                // RLIMIT_MEMLOCK (ENOMEM past it; ENOBUFS for
                // the optmem variant); two full batches alone
                // can exceed the default 8 MiB, and the budget
                // is shared across connections. An expected
                // operational condition, not a connection
                // error: ship the rest of this batch by copy.
                Err(err) if matches!(err.raw_os_error(), Some(libc::ENOMEM | libc::ENOBUFS)) => {
                    *zc_fallbacks += 1;
                    use_zc = false;
                    continue;
                }
                Err(err) => return Err(err),
            }
        } else {
            // SAFETY: the msghdr, iovec array, arena, and
            // referenced slot buffers all outlive the await — the
            // batch is owned by this task, slots release only
            // after the batch completes, and run_queue joins this
            // task (or leaks the queue) before freeing anything.
            let op = unsafe { ops::sendmsg_raw(fd, batch.msghdr()) }?;
            op.await? as usize
        };
        if n == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        if batch.advance(n) {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod gather_tests {
    use crate::queue::Completion;

    use super::*;

    /// Linearize a batch's iovecs (what the kernel would put on the wire).
    fn gather(batch: &SendBatch) -> Vec<u8> {
        let mut out = Vec::new();
        for e in batch.gather.iovs() {
            // SAFETY: entries reference the batch arena and slot
            // buffers owned by the test, sized by construction.
            let s = unsafe { std::slice::from_raw_parts(e.iov_base.cast::<u8>(), e.iov_len) };
            out.extend_from_slice(s);
        }
        out
    }

    #[test]
    fn batch_matches_linear_encoding() {
        let queue = TcpQueue::new(1, 4, 4096, false);
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
        assert_eq!(batch.gather.iovs().len(), 3);
    }

    #[test]
    fn batch_elides_and_merges_without_digests() {
        // sqhd_disabled queue: a successful read elides the response
        // capsule; digests off exercises the bare-header layout.
        let queue = TcpQueue::new(1, 4, 4096, true);
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
        assert_eq!(batch.gather.iovs().len(), 3);
    }

    #[test]
    fn staging_splits_tag_release_classes() {
        let queue = TcpQueue::new(1, 4, 4096, false);
        let mut batch = SendBatch::new(queue.sqsize);
        let read_cqe = Cqe::new(0, 1, 1, 5, 0);
        let flush_cqe = Cqe::new(0, 2, 1, 6, 0);
        let items = [
            // Payload-carrying: slot referenced by the op → notif-gated.
            SendWork::Response(Completion {
                tag: 1,
                cqe: read_cqe,
                data_len: 4096,
            }),
            // Capsule-only: arena bytes only → released at the send CQE.
            SendWork::Response(Completion {
                tag: 2,
                cqe: flush_cqe,
                data_len: 0,
            }),
            // R2T: no tag to release at all.
            SendWork::R2t {
                tag: 3,
                cid: 9,
                offset: 0,
                length: 4096,
            },
        ];
        for item in &items {
            stage_and_account(&queue, &mut batch, item, false, false);
        }
        assert_eq!(batch.tags_at_notif, vec![1]);
        assert_eq!(batch.tags_at_cqe, vec![2]);

        batch.reset();
        assert!(batch.tags_at_notif.is_empty() && batch.tags_at_cqe.is_empty());
    }
}
