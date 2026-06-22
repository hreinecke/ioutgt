//! Operation constructors and their futures.
//!
//! Owned-buffer ops move a `Box<[u8]>` into the reactor for the op's
//! lifetime and hand it back on completion — safe under arbitrary future
//! cancellation, used by the control path and tests. Raw ops carry only a
//! pointer and are the hot-path variants for queue-slot buffers; see their
//! safety contracts.

use std::future::Future;
use std::io;
use std::os::fd::{FromRawFd, OwnedFd, RawFd};
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use io_uring::{opcode, types};

use crate::op::{MsgResources, MultiOp, Op, Resources};
use crate::reactor::SqeClass;

fn buf_len(buf: &[u8]) -> io::Result<u32> {
    u32::try_from(buf.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "buffer too large"))
}

/// Future of an op that owns one buffer; resolves to the syscall result
/// plus the buffer handed back.
pub struct BufOp {
    op: Op,
}

impl Future for BufOp {
    type Output = (io::Result<u32>, Box<[u8]>);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (result, resources) = ready!(self.op.poll_single(cx));
        Poll::Ready((result.io(), resources.into_buffer()))
    }
}

/// Future of an op without resources; resolves to the syscall result.
pub struct RawOp {
    op: Op,
}

impl Future for RawOp {
    type Output = io::Result<u32>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (result, _) = ready!(self.op.poll_single(cx));
        Poll::Ready(result.io())
    }
}

/// Read from `fd` at `offset` into an owned buffer.
///
/// For non-seekable fds (sockets via [`recv`], pipes, eventfds) pass
/// offset 0.
pub fn read_at(fd: RawFd, mut buf: Box<[u8]>, offset: u64) -> io::Result<BufOp> {
    let len = buf_len(&buf)?;
    let ptr = buf.as_mut_ptr();
    let op = Op::submit(
        |key| {
            opcode::Read::new(types::Fd(fd), ptr, len)
                .offset(offset)
                .build()
                .user_data(key)
        },
        Resources::Buffer(buf),
    )?;
    Ok(BufOp { op })
}

/// Write `buf` to `fd` at `offset`.
pub fn write_at(fd: RawFd, buf: Box<[u8]>, offset: u64) -> io::Result<BufOp> {
    let len = buf_len(&buf)?;
    let ptr = buf.as_ptr();
    let op = Op::submit(
        |key| {
            opcode::Write::new(types::Fd(fd), ptr, len)
                .offset(offset)
                .build()
                .user_data(key)
        },
        Resources::Buffer(buf),
    )?;
    Ok(BufOp { op })
}

/// Receive from a socket into an owned buffer.
pub fn recv(fd: RawFd, mut buf: Box<[u8]>) -> io::Result<BufOp> {
    let len = buf_len(&buf)?;
    let ptr = buf.as_mut_ptr();
    let op = Op::submit_classed(
        |key| {
            opcode::Recv::new(types::Fd(fd), ptr, len)
                .build()
                .user_data(key)
        },
        Resources::Buffer(buf),
        SqeClass::Recv,
    )?;
    Ok(BufOp { op })
}

/// Send an owned buffer on a socket.
pub fn send(fd: RawFd, buf: Box<[u8]>) -> io::Result<BufOp> {
    let len = buf_len(&buf)?;
    let ptr = buf.as_ptr();
    let op = Op::submit_classed(
        |key| {
            opcode::Send::new(types::Fd(fd), ptr, len)
                .build()
                .user_data(key)
        },
        Resources::Buffer(buf),
        SqeClass::Send,
    )?;
    Ok(BufOp { op })
}

/// Future of a two-segment vectored send; hands both buffers back.
pub struct SendVectored {
    op: Op,
}

impl Future for SendVectored {
    type Output = (io::Result<u32>, [Box<[u8]>; 2]);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (result, resources) = ready!(self.op.poll_single(cx));
        Poll::Ready((result.io(), resources.into_msg().bufs))
    }
}

/// Vectored send of the first `hlen` bytes of `header` then `plen`
/// bytes of `payload` in one SENDMSG; both buffers come back whole.
pub fn send_vectored_partial(
    fd: RawFd,
    header: Box<[u8]>,
    hlen: usize,
    payload: Box<[u8]>,
    plen: usize,
) -> io::Result<SendVectored> {
    if hlen > header.len() || plen > payload.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "len exceeds buffer",
        ));
    }
    let mut msg = MsgResources::new_send(header, payload);
    msg.iovecs[0].iov_len = hlen;
    msg.iovecs[1].iov_len = plen;
    let msghdr_ptr: *const libc::msghdr = &msg.msghdr;
    let op = Op::submit_classed(
        |key| {
            opcode::SendMsg::new(types::Fd(fd), msghdr_ptr)
                .build()
                .user_data(key)
        },
        Resources::Msg(msg),
        SqeClass::Send,
    )?;
    Ok(SendVectored { op })
}

/// Vectored send of `header` then `payload` in a single SENDMSG — the
/// phase-1 "PDU header + data in one op" primitive.
pub fn send_vectored(fd: RawFd, header: Box<[u8]>, payload: Box<[u8]>) -> io::Result<SendVectored> {
    let msg = MsgResources::new_send(header, payload);
    let msghdr_ptr: *const libc::msghdr = &msg.msghdr;
    let op = Op::submit_classed(
        |key| {
            opcode::SendMsg::new(types::Fd(fd), msghdr_ptr)
                .build()
                .user_data(key)
        },
        Resources::Msg(msg),
        SqeClass::Send,
    )?;
    Ok(SendVectored { op })
}

/// Future resolving to one accepted connection.
pub struct Accept {
    op: Op,
}

impl Future for Accept {
    type Output = io::Result<OwnedFd>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (result, _) = ready!(self.op.poll_single(cx));
        // SAFETY: a successful accept CQE carries a fresh fd owned by us.
        Poll::Ready(
            result
                .io()
                .map(|fd| unsafe { OwnedFd::from_raw_fd(fd as RawFd) }),
        )
    }
}

/// Accept one connection on a listening socket.
pub fn accept(fd: RawFd) -> io::Result<Accept> {
    let op = Op::submit(
        |key| {
            opcode::Accept::new(types::Fd(fd), std::ptr::null_mut(), std::ptr::null_mut())
                .build()
                .user_data(key)
        },
        Resources::None,
    )?;
    Ok(Accept { op })
}

/// Stream of accepted connections from a multishot accept.
pub struct AcceptMulti {
    op: MultiOp,
}

impl AcceptMulti {
    /// Next accepted connection; `None` once the multishot terminates
    /// (listener closed or cancelled).
    pub async fn next(&mut self) -> Option<io::Result<OwnedFd>> {
        let result = std::future::poll_fn(|cx| self.op.poll_next(cx)).await?;
        // SAFETY: as for `Accept`, the CQE result is a fresh owned fd.
        Some(
            result
                .io()
                .map(|fd| unsafe { OwnedFd::from_raw_fd(fd as RawFd) }),
        )
    }
}

/// Multishot accept: one SQE, a CQE per incoming connection.
pub fn accept_multi(fd: RawFd) -> io::Result<AcceptMulti> {
    let op = MultiOp::submit(
        |key| {
            opcode::AcceptMulti::new(types::Fd(fd))
                .build()
                .user_data(key)
        },
        Resources::None,
    )?;
    Ok(AcceptMulti { op })
}

/// Future of a ring timer.
pub struct Sleep {
    op: Op,
}

impl Future for Sleep {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (result, _) = ready!(self.op.poll_single(cx));
        match result.result {
            // -ETIME is the normal "timer fired" completion.
            0 => Poll::Ready(Ok(())),
            err if err == -libc::ETIME => Poll::Ready(Ok(())),
            err => Poll::Ready(Err(io::Error::from_raw_os_error(-err))),
        }
    }
}

/// Sleep via `IORING_OP_TIMEOUT` — the only timer primitive on queue
/// threads (Tokio's time driver is disabled there).
pub fn sleep(duration: Duration) -> io::Result<Sleep> {
    let timespec = Box::new(
        types::Timespec::new()
            .sec(duration.as_secs())
            .nsec(duration.subsec_nanos()),
    );
    let timespec_ptr: *const types::Timespec = &*timespec;
    let op = Op::submit(
        |key| opcode::Timeout::new(timespec_ptr).build().user_data(key),
        Resources::Timespec(timespec),
    )?;
    Ok(Sleep { op })
}

/// `fsync(2)` / `fdatasync(2)` via the ring.
pub fn fsync(fd: RawFd, datasync: bool) -> io::Result<RawOp> {
    let flags = if datasync {
        types::FsyncFlags::DATASYNC
    } else {
        types::FsyncFlags::empty()
    };
    let op = Op::submit(
        |key| {
            opcode::Fsync::new(types::Fd(fd))
                .flags(flags)
                .build()
                .user_data(key)
        },
        Resources::None,
    )?;
    Ok(RawOp { op })
}

/// `fallocate(2)` via the ring (`FALLOC_FL_*` modes: punch-hole,
/// zero-range, ... — the file backend's discard/write-zeroes primitive).
pub fn fallocate(fd: RawFd, mode: i32, offset: u64, len: u64) -> io::Result<RawOp> {
    let op = Op::submit(
        |key| {
            opcode::Fallocate::new(types::Fd(fd), len)
                .offset(offset)
                .mode(mode)
                .build()
                .user_data(key)
        },
        Resources::None,
    )?;
    Ok(RawOp { op })
}

/// Receive into caller-managed memory (queue-slot buffers).
///
/// # Safety
///
/// `ptr..ptr+len` must remain valid and unaliased for writes until this
/// op's terminal CQE has been reaped — which, if the returned future is
/// dropped before completion, is *later* than the drop: the caller must
/// keep the memory alive until [`crate::Reactor::drain`] (or queue
/// teardown) confirms no ops are pending.
pub unsafe fn recv_raw(fd: RawFd, ptr: *mut u8, len: u32) -> io::Result<RawOp> {
    let op = Op::submit_classed(
        |key| {
            opcode::Recv::new(types::Fd(fd), ptr, len)
                .build()
                .user_data(key)
        },
        Resources::None,
        SqeClass::Recv,
    )?;
    Ok(RawOp { op })
}

/// Receive into caller-managed memory, requesting `MSG_WAITALL`: the
/// kernel holds the op until `len` bytes arrive (best-effort — may still
/// return short on EOF/error; callers must loop on progress).
///
/// # Safety
///
/// Same contract as [`recv_raw`]: `ptr..ptr+len` must remain valid and
/// unaliased for writes until this op's terminal CQE has been reaped.
pub unsafe fn recv_raw_waitall(fd: RawFd, ptr: *mut u8, len: u32) -> io::Result<RawOp> {
    let op = Op::submit_classed(
        |key| {
            opcode::Recv::new(types::Fd(fd), ptr, len)
                .flags(libc::MSG_WAITALL)
                .build()
                .user_data(key)
        },
        Resources::None,
        SqeClass::Recv,
    )?;
    Ok(RawOp { op })
}

/// Send from caller-managed memory.
///
/// # Safety
///
/// Same contract as [`recv_raw`] (valid until terminal CQE; reads only).
pub unsafe fn send_raw(fd: RawFd, ptr: *const u8, len: u32) -> io::Result<RawOp> {
    let op = Op::submit_classed(
        |key| {
            opcode::Send::new(types::Fd(fd), ptr, len)
                .build()
                .user_data(key)
        },
        Resources::None,
        SqeClass::Send,
    )?;
    Ok(RawOp { op })
}

/// Vectored send described by a caller-managed `msghdr` — the batched
/// gather-send primitive (header arena + slot-payload iovecs).
///
/// # Safety
///
/// `msg`, its iovec array, and every buffer the iovecs reference must
/// remain valid (reads only) until this op's terminal CQE has been
/// reaped — same contract as [`recv_raw`].
pub unsafe fn sendmsg_raw(fd: RawFd, msg: *const libc::msghdr) -> io::Result<RawOp> {
    let op = Op::submit_classed(
        |key| {
            opcode::SendMsg::new(types::Fd(fd), msg)
                .build()
                .user_data(key)
        },
        Resources::None,
        SqeClass::Send,
    )?;
    Ok(RawOp { op })
}

/// `IORING_SEND_ZC_REPORT_USAGE`: ask the notification CQE to report
/// whether the kernel copied (the loopback fallback). Rides the SQE
/// ioprio field; the io-uring crate does not re-export the constant.
const SEND_ZC_REPORT_USAGE: u16 = 8;
/// Notification-CQE `res` bit set when a "zero-copy" send actually
/// copied. Bit 31 — notif results are raw flags, never an errno.
const NOTIF_ZC_COPIED: u32 = 1 << 31;

/// Handle to an in-flight zero-copy vectored send.
///
/// ZC sends complete in two CQEs: the send result first (awaited via
/// [`SendZcOp::sent`]), then a notification once the kernel drops its
/// last page reference ([`SendZcOp::into_notif`]). The notif handle
/// must be taken and awaited (or deliberately orphaned) before any
/// referenced memory is reused.
pub struct SendZcOp {
    op: MultiOp,
}

impl SendZcOp {
    /// Await the send CQE: bytes accepted into the socket, as
    /// `sendmsg(2)`. Call exactly once, before [`Self::into_notif`].
    pub async fn sent(&mut self) -> io::Result<u32> {
        let result = std::future::poll_fn(|cx| self.op.poll_next(cx))
            .await
            .expect("ZC send: result CQE precedes termination");
        result.io()
    }

    /// The notification future gating buffer reuse. Take it even on
    /// the error path: a failed ZC send may still have pinned pages
    /// (`F_MORE` on the result CQE) and post a notif; if it did not,
    /// the future resolves immediately.
    pub fn into_notif(self) -> ZcNotif {
        ZcNotif { op: self.op }
    }
}

/// Future of a ZC send's notification CQE; yields `true` when the
/// kernel reported the data was copied rather than sent zero-copy
/// (REPORT_USAGE — always the case on loopback).
pub struct ZcNotif {
    op: MultiOp,
}

impl Future for ZcNotif {
    type Output = bool;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut copied = false;
        // The notif is the terminal CQE: one Some (the notif itself,
        // or nothing at all if the errored send never pinned pages),
        // then None. Raw bit test — bit 31 is not an errno.
        while let Some(result) = ready!(self.op.poll_next(cx)) {
            copied = result.result as u32 & NOTIF_ZC_COPIED != 0;
        }
        Poll::Ready(copied)
    }
}

/// Zero-copy vectored send described by a caller-managed `msghdr`
/// (`IORING_OP_SENDMSG_ZC`); completes in two CQEs — see [`SendZcOp`].
/// REPORT_USAGE is always requested.
///
/// # Safety
///
/// `msg`, its iovec array, and every buffer the iovecs reference must
/// remain valid (reads only) until this op's **terminal** CQE — the
/// notification, not the send result. The kernel snapshots the iovec
/// array at issue, so its *contents* may be rewritten once the send
/// result has been reaped (short-send resume), but all memory must
/// stay allocated until the notification is reaped,
/// [`crate::Reactor::drain`] returns, or queue teardown completes.
pub unsafe fn sendmsg_zc_raw(fd: RawFd, msg: *const libc::msghdr) -> io::Result<SendZcOp> {
    let op = MultiOp::submit_classed(
        |key| {
            opcode::SendMsgZc::new(types::Fd(fd), msg)
                .ioprio(SEND_ZC_REPORT_USAGE)
                .build()
                .user_data(key)
        },
        Resources::None,
        SqeClass::Send,
    )?;
    Ok(SendZcOp { op })
}

/// Positional read into caller-managed memory.
///
/// # Safety
///
/// Same contract as [`recv_raw`].
pub unsafe fn read_at_raw(fd: RawFd, ptr: *mut u8, len: u32, offset: u64) -> io::Result<RawOp> {
    let op = Op::submit_classed(
        |key| {
            opcode::Read::new(types::Fd(fd), ptr, len)
                .offset(offset)
                .build()
                .user_data(key)
        },
        Resources::None,
        SqeClass::Read,
    )?;
    Ok(RawOp { op })
}

/// Positional write from caller-managed memory.
///
/// # Safety
///
/// Same contract as [`recv_raw`] (reads only).
pub unsafe fn write_at_raw(fd: RawFd, ptr: *const u8, len: u32, offset: u64) -> io::Result<RawOp> {
    let op = Op::submit_classed(
        |key| {
            opcode::Write::new(types::Fd(fd), ptr, len)
                .offset(offset)
                .build()
                .user_data(key)
        },
        Resources::None,
        SqeClass::Write,
    )?;
    Ok(RawOp { op })
}
