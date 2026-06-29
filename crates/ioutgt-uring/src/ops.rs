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

/// A disk op's file target: a raw fd, or a registered fixed-file table index.
///
/// Mirrors the fixed-buffer pattern: the backend best-effort registers each
/// fd once per thread ([`crate::fixed_file_index`]) and addresses it by index
/// (`types::Fixed`) so the kernel skips the per-IO fd lookup; `Raw` is the
/// plain non-registered fallback.
#[derive(Clone, Copy)]
pub enum BackendFd {
    /// A plain (non-registered) file descriptor.
    Raw(RawFd),
    /// An index into the ring's registered fixed-file table.
    Fixed(u16),
}

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

/// Wait for `fd` to become ready for the given `poll(2)` event mask (e.g.
/// `libc::POLLIN`) via `IORING_OP_POLL_ADD` (one-shot). On readiness the op
/// completes with the returned events mask; re-issue to wait again. Lets a
/// queue thread's reactor wake on an arbitrary external fd — e.g. an RDMA
/// completion-channel fd — through the same `submit_and_wait` park as its IO,
/// instead of busy-polling that fd.
pub fn poll_add(fd: RawFd, events: u32) -> io::Result<RawOp> {
    let op = Op::submit(
        |key| {
            opcode::PollAdd::new(types::Fd(fd), events)
                .build()
                .user_data(key)
        },
        Resources::None,
    )?;
    Ok(RawOp { op })
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
pub fn fsync(file: BackendFd, datasync: bool) -> io::Result<RawOp> {
    let flags = if datasync {
        types::FsyncFlags::DATASYNC
    } else {
        types::FsyncFlags::empty()
    };
    let op = Op::submit(
        |key| {
            let e = match file {
                BackendFd::Raw(fd) => opcode::Fsync::new(types::Fd(fd)).flags(flags).build(),
                BackendFd::Fixed(idx) => opcode::Fsync::new(types::Fixed(idx.into()))
                    .flags(flags)
                    .build(),
            };
            e.user_data(key)
        },
        Resources::None,
    )?;
    Ok(RawOp { op })
}

/// `fallocate(2)` via the ring (`FALLOC_FL_*` modes: punch-hole,
/// zero-range, ... — the file backend's discard/write-zeroes primitive).
pub fn fallocate(file: BackendFd, mode: i32, offset: u64, len: u64) -> io::Result<RawOp> {
    let op = Op::submit(
        |key| {
            let e = match file {
                BackendFd::Raw(fd) => opcode::Fallocate::new(types::Fd(fd), len)
                    .offset(offset)
                    .mode(mode)
                    .build(),
                BackendFd::Fixed(idx) => opcode::Fallocate::new(types::Fixed(idx.into()), len)
                    .offset(offset)
                    .mode(mode)
                    .build(),
            };
            e.user_data(key)
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

/// Vectored receive into caller-managed iovecs described by a `msghdr`,
/// requesting `MSG_WAITALL`: the kernel scatters the arriving bytes across
/// the iovecs and holds the op until they are full (best-effort — may
/// still return short on EOF/error; callers handle the short return).
///
/// # Safety
///
/// `msg`, its iovec array, and every buffer the iovecs reference must
/// remain valid and unaliased for writes until this op's terminal CQE has
/// been reaped — same contract as [`recv_raw`].
pub unsafe fn recvmsg_raw(fd: RawFd, msg: *mut libc::msghdr) -> io::Result<RawOp> {
    let op = Op::submit_classed(
        |key| {
            opcode::RecvMsg::new(types::Fd(fd), msg)
                .flags(libc::MSG_WAITALL as u32)
                .build()
                .user_data(key)
        },
        Resources::None,
        SqeClass::Recv,
    )?;
    Ok(RawOp { op })
}

/// Stream of received chunks from a multishot recv backed by a
/// provided-buffer ring ([`crate::bufring::BufRing`]).
pub struct RecvMultiOp {
    op: MultiOp,
}

impl RecvMultiOp {
    /// Next received chunk. `None` on orderly EOF (peer closed) or once the
    /// multishot has fully ended; `Some(Err(ENOBUFS))` when the buffer
    /// group ran dry — in both terminal cases the caller replenishes
    /// buffers and re-arms with a fresh [`recv_multi`].
    pub async fn next(&mut self) -> Option<io::Result<crate::bufring::RecvChunk>> {
        let result = std::future::poll_fn(|cx| self.op.poll_next(cx)).await?;
        let more = result.more();
        let buf_more = io_uring::cqueue::buffer_more(result.flags);
        match result.io() {
            Ok(0) => None, // EOF
            Ok(len) => match io_uring::cqueue::buffer_select(result.flags) {
                Some(bid) => Some(Ok(crate::bufring::RecvChunk {
                    bid,
                    len,
                    more,
                    buf_more,
                })),
                None => Some(Err(io::Error::other("recv CQE carried no buffer"))),
            },
            Err(e) => Some(Err(e)),
        }
    }
}

/// Multishot recv drawing buffers from the `bgid` provided-buffer ring.
/// One SQE; a CQE per received chunk until the group drains or EOF.
pub fn recv_multi(fd: RawFd, bgid: u16) -> io::Result<RecvMultiOp> {
    let op = MultiOp::submit_classed(
        |key| {
            opcode::RecvMulti::new(types::Fd(fd), bgid)
                .build()
                .user_data(key)
        },
        Resources::None,
        SqeClass::Recv,
    )?;
    Ok(RecvMultiOp { op })
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
/// `IORING_SEND_VECTORIZED` (kernel ABI bit 5): make `SEND_ZC` read `addr`
/// as an iovec array of `len` segments instead of one buffer. With
/// `FIXED_BUF` set, every segment must lie inside the registered buffer
/// `buf_index` — a vectored zero-copy send that reuses one registration
/// (no per-send page-pin/IOMMU map). Rides the SQE ioprio; not in the crate.
const SEND_VECTORIZED: u16 = 1 << 5;
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

/// Vectored zero-copy send (`SEND_ZC` + `VECTORIZED` + `FIXED_BUF`) whose
/// every `iov` segment lies inside the single registered buffer `buf_index`.
/// The kernel reuses that buffer's registration — no per-send page-pin or
/// IOMMU map, unlike [`sendmsg_zc_raw`]. Two CQEs (send result, then notif),
/// same as [`SendZcOp`]; REPORT_USAGE is requested. Requires kernel
/// `IORING_SEND_VECTORIZED` support — probe with
/// [`crate::send_vectored_fixed_supported`] before use.
///
/// # Safety
///
/// `iov` (an array of `nr_segs` entries) and the registered buffer must stay
/// valid (reads only) until this op's **terminal** (notification) CQE — same
/// contract as [`sendmsg_zc_raw`]. Every segment must fall within the bytes
/// registered under `buf_index`, or the kernel rejects the import (`EFAULT`).
pub unsafe fn send_zc_vec_fixed_raw(
    fd: RawFd,
    iov: *const libc::iovec,
    nr_segs: u32,
    buf_index: u16,
) -> io::Result<SendZcOp> {
    let op = MultiOp::submit_classed(
        |key| {
            opcode::SendZc::new(types::Fd(fd), iov.cast(), nr_segs)
                .buf_index(Some(buf_index))
                .zc_flags(SEND_ZC_REPORT_USAGE | SEND_VECTORIZED)
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
pub unsafe fn read_at_raw(
    file: BackendFd,
    ptr: *mut u8,
    len: u32,
    offset: u64,
) -> io::Result<RawOp> {
    let op = Op::submit_classed(
        |key| {
            let e = match file {
                BackendFd::Raw(fd) => opcode::Read::new(types::Fd(fd), ptr, len)
                    .offset(offset)
                    .build(),
                BackendFd::Fixed(idx) => opcode::Read::new(types::Fixed(idx.into()), ptr, len)
                    .offset(offset)
                    .build(),
            };
            e.user_data(key)
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
pub unsafe fn write_at_raw(
    file: BackendFd,
    ptr: *const u8,
    len: u32,
    offset: u64,
) -> io::Result<RawOp> {
    let op = Op::submit_classed(
        |key| {
            let e = match file {
                BackendFd::Raw(fd) => opcode::Write::new(types::Fd(fd), ptr, len)
                    .offset(offset)
                    .build(),
                BackendFd::Fixed(idx) => opcode::Write::new(types::Fixed(idx.into()), ptr, len)
                    .offset(offset)
                    .build(),
            };
            e.user_data(key)
        },
        Resources::None,
        SqeClass::Write,
    )?;
    Ok(RawOp { op })
}

/// Positional vectored read into caller-managed iovecs. `rw_flags` is a
/// `preadv2(2)` flag bitset (e.g. `RWF_DONTCACHE`).
///
/// # Safety
///
/// Same contract as [`recv_raw`]: the `iovec` array *and* every buffer it
/// points at must stay valid and exclusively borrowed until the terminal
/// CQE is reaped.
pub unsafe fn readv_at_raw(
    file: BackendFd,
    iov: *const libc::iovec,
    n: u32,
    offset: u64,
    rw_flags: i32,
) -> io::Result<RawOp> {
    let op = Op::submit_classed(
        |key| {
            let e = match file {
                BackendFd::Raw(fd) => opcode::Readv::new(types::Fd(fd), iov, n)
                    .offset(offset)
                    .rw_flags(rw_flags)
                    .build(),
                BackendFd::Fixed(idx) => opcode::Readv::new(types::Fixed(idx.into()), iov, n)
                    .offset(offset)
                    .rw_flags(rw_flags)
                    .build(),
            };
            e.user_data(key)
        },
        Resources::None,
        SqeClass::Read,
    )?;
    Ok(RawOp { op })
}

/// Positional vectored write from caller-managed iovecs. `rw_flags` is a
/// `pwritev2(2)` flag bitset (e.g. `RWF_DONTCACHE`).
///
/// # Safety
///
/// Same contract as [`readv_at_raw`] (reads the buffers only).
pub unsafe fn writev_at_raw(
    file: BackendFd,
    iov: *const libc::iovec,
    n: u32,
    offset: u64,
    rw_flags: i32,
) -> io::Result<RawOp> {
    let op = Op::submit_classed(
        |key| {
            let e = match file {
                BackendFd::Raw(fd) => opcode::Writev::new(types::Fd(fd), iov, n)
                    .offset(offset)
                    .rw_flags(rw_flags)
                    .build(),
                BackendFd::Fixed(idx) => opcode::Writev::new(types::Fixed(idx.into()), iov, n)
                    .offset(offset)
                    .rw_flags(rw_flags)
                    .build(),
            };
            e.user_data(key)
        },
        Resources::None,
        SqeClass::Write,
    )?;
    Ok(RawOp { op })
}

/// Vectored read (`READV_FIXED`) whose iovecs point into the registered
/// buffer `buf_index` — the kernel uses the pre-pinned mapping instead of
/// mapping the pages per IO. `rw_flags` is a `preadv2(2)` flag bitset.
///
/// # Safety
///
/// Same contract as [`readv_at_raw`], and every `iov_base` must fall within
/// the region registered at `buf_index` (the connection's pool arena).
pub unsafe fn readv_fixed_at_raw(
    file: BackendFd,
    iov: *const libc::iovec,
    n: u32,
    offset: u64,
    buf_index: u16,
    rw_flags: i32,
) -> io::Result<RawOp> {
    let op = Op::submit_classed(
        |key| {
            let e = match file {
                BackendFd::Raw(fd) => opcode::ReadvFixed::new(types::Fd(fd), iov, n, buf_index)
                    .offset(offset)
                    .rw_flags(rw_flags)
                    .build(),
                BackendFd::Fixed(idx) => {
                    opcode::ReadvFixed::new(types::Fixed(idx.into()), iov, n, buf_index)
                        .offset(offset)
                        .rw_flags(rw_flags)
                        .build()
                }
            };
            e.user_data(key)
        },
        Resources::None,
        SqeClass::Read,
    )?;
    Ok(RawOp { op })
}

/// Vectored write (`WRITEV_FIXED`) from the registered buffer `buf_index`.
///
/// # Safety
///
/// Same contract as [`readv_fixed_at_raw`] (reads the buffers only).
pub unsafe fn writev_fixed_at_raw(
    file: BackendFd,
    iov: *const libc::iovec,
    n: u32,
    offset: u64,
    buf_index: u16,
    rw_flags: i32,
) -> io::Result<RawOp> {
    let op = Op::submit_classed(
        |key| {
            let e = match file {
                BackendFd::Raw(fd) => opcode::WritevFixed::new(types::Fd(fd), iov, n, buf_index)
                    .offset(offset)
                    .rw_flags(rw_flags)
                    .build(),
                BackendFd::Fixed(idx) => {
                    opcode::WritevFixed::new(types::Fixed(idx.into()), iov, n, buf_index)
                        .offset(offset)
                        .rw_flags(rw_flags)
                        .build()
                }
            };
            e.user_data(key)
        },
        Resources::None,
        SqeClass::Write,
    )?;
    Ok(RawOp { op })
}
