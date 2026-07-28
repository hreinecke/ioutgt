//! In-flight operation state: the slab entry, single-shot and multishot
//! op futures, and the resources kept alive on behalf of the kernel.

use std::collections::VecDeque;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use crate::cqe::CqeResult;
use crate::reactor::{Reactor, SqeClass};

/// `user_data` sentinel for SQEs whose CQE carries no op state
/// (e.g. ASYNC_CANCEL issued on orphaning).
pub(crate) const IGNORE_USER_DATA: u64 = u64::MAX;

/// Resources owned by the slab entry while the kernel may still reference
/// them. They are released (or handed back to the caller) only once the
/// terminal CQE has been reaped.
pub(crate) enum Resources {
    /// Op references no caller memory (raw ops, fsync, accept, ...).
    None,
    /// One owned data buffer (read/write/recv/send).
    Buffer(Box<[u8]>),
    /// Timespec referenced by a TIMEOUT SQE.
    Timespec(#[allow(dead_code)] Box<io_uring::types::Timespec>),
    /// Socket address referenced by a CONNECT SQE.
    SockAddr(#[allow(dead_code)] Box<libc::sockaddr_storage>),
    /// msghdr + iovecs + buffers referenced by a SENDMSG SQE.
    Msg(Box<MsgResources>),
}

impl Resources {
    pub(crate) fn into_buffer(self) -> Box<[u8]> {
        match self {
            Resources::Buffer(b) => b,
            _ => unreachable!("op resources are not a buffer"),
        }
    }

    pub(crate) fn into_msg(self) -> Box<MsgResources> {
        match self {
            Resources::Msg(m) => m,
            _ => unreachable!("op resources are not a msghdr"),
        }
    }
}

/// Stable storage for a vectored send: the msghdr and iovec array must
/// stay at a fixed address from SQE submission until the CQE arrives,
/// which the enclosing `Box` guarantees.
pub(crate) struct MsgResources {
    pub(crate) bufs: [Box<[u8]>; 2],
    pub(crate) iovecs: [libc::iovec; 2],
    pub(crate) msghdr: libc::msghdr,
}

impl MsgResources {
    /// Build a boxed two-segment send msghdr; pointers are wired up after
    /// boxing so they refer to the final, stable addresses.
    pub(crate) fn new_send(header: Box<[u8]>, payload: Box<[u8]>) -> Box<Self> {
        // SAFETY: msghdr is a plain C struct for which all-zeroes is a
        // valid (empty) value.
        let msghdr: libc::msghdr = unsafe { std::mem::zeroed() };
        let mut res = Box::new(MsgResources {
            bufs: [header, payload],
            iovecs: [libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            }; 2],
            msghdr,
        });
        for i in 0..2 {
            res.iovecs[i] = libc::iovec {
                iov_base: res.bufs[i].as_ptr() as *mut libc::c_void,
                iov_len: res.bufs[i].len(),
            };
        }
        res.msghdr.msg_iov = res.iovecs.as_mut_ptr();
        res.msghdr.msg_iovlen = 2;
        res
    }
}

/// Per-op slab entry. Owns everything the kernel may still look at.
pub(crate) struct OpEntry {
    /// First pending CQE, inline: single-shot ops (the vast majority)
    /// complete without touching the heap-backed overflow queue.
    pub(crate) first: Option<CqeResult>,
    /// Overflow for multishot bursts only.
    pub(crate) overflow: VecDeque<CqeResult>,
    pub(crate) waker: Option<Waker>,
    /// Terminal CQE seen (single-shot completion, or multishot CQE
    /// without `IORING_CQE_F_MORE`). No further CQEs will arrive.
    pub(crate) terminated: bool,
    /// The owning future was dropped; reactor frees the entry on the
    /// terminal CQE.
    pub(crate) orphaned: bool,
    pub(crate) resources: Resources,
}

impl OpEntry {
    pub(crate) fn new(resources: Resources) -> Self {
        OpEntry {
            first: None,
            overflow: VecDeque::new(),
            waker: None,
            terminated: false,
            orphaned: false,
            resources,
        }
    }

    pub(crate) fn push_result(&mut self, result: CqeResult) {
        if self.first.is_none() && self.overflow.is_empty() {
            self.first = Some(result);
        } else {
            self.overflow.push_back(result);
        }
    }

    pub(crate) fn pop_result(&mut self) -> Option<CqeResult> {
        self.first.take().or_else(|| self.overflow.pop_front())
    }

    pub(crate) fn has_result(&self) -> bool {
        self.first.is_some() || !self.overflow.is_empty()
    }
}

/// Handle to one submitted single-shot operation.
///
/// Dropping it before completion orphans the slab entry (resources stay
/// alive until the terminal CQE) and issues a best-effort ASYNC_CANCEL.
pub(crate) struct Op {
    reactor: Rc<Reactor>,
    key: usize,
    done: bool,
}

impl Op {
    /// Submit an SQE built by `build` (which receives the `user_data` key)
    /// with `resources` kept alive in the slab entry.
    pub(crate) fn submit(
        build: impl FnOnce(u64) -> io_uring::squeue::Entry,
        resources: Resources,
    ) -> std::io::Result<Op> {
        Self::submit_classed(build, resources, SqeClass::Other)
    }

    /// As [`Op::submit`], tagging the SQE for the send/recv counters.
    pub(crate) fn submit_classed(
        build: impl FnOnce(u64) -> io_uring::squeue::Entry,
        resources: Resources,
        class: SqeClass,
    ) -> std::io::Result<Op> {
        let reactor = Reactor::current()?;
        let key = reactor.submit_op(build, resources, class)?;
        Ok(Op {
            reactor,
            key,
            done: false,
        })
    }

    /// Poll for the single terminal completion, handing back the kept
    /// resources.
    pub(crate) fn poll_single(&mut self, cx: &mut Context<'_>) -> Poll<(CqeResult, Resources)> {
        assert!(!self.done, "op polled after completion");
        let mut slab = self.reactor.slab_mut();
        let entry = slab.get_mut(self.key).expect("op entry vanished");
        if !entry.has_result() {
            entry.waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        debug_assert!(entry.terminated, "single-shot op got non-terminal CQE");
        let mut entry = slab.remove(self.key);
        self.done = true;
        let result = entry.pop_result().expect("checked non-empty");
        Poll::Ready((result, entry.resources))
    }
}

impl Drop for Op {
    fn drop(&mut self) {
        if !self.done {
            self.reactor.orphan(self.key);
        }
    }
}

/// Handle to one submitted multishot operation (multishot accept, ...).
pub(crate) struct MultiOp {
    reactor: Rc<Reactor>,
    key: usize,
    done: bool,
}

impl MultiOp {
    pub(crate) fn submit(
        build: impl FnOnce(u64) -> io_uring::squeue::Entry,
        resources: Resources,
    ) -> std::io::Result<MultiOp> {
        Self::submit_classed(build, resources, SqeClass::Other)
    }

    /// As [`MultiOp::submit`], tagging the SQE for the send/recv counters.
    pub(crate) fn submit_classed(
        build: impl FnOnce(u64) -> io_uring::squeue::Entry,
        resources: Resources,
        class: SqeClass,
    ) -> std::io::Result<MultiOp> {
        let reactor = Reactor::current()?;
        let key = reactor.submit_op(build, resources, class)?;
        Ok(MultiOp {
            reactor,
            key,
            done: false,
        })
    }

    /// Poll for the next completion; `Ready(None)` after the terminal CQE
    /// has been consumed.
    pub(crate) fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<CqeResult>> {
        if self.done {
            return Poll::Ready(None);
        }
        let mut slab = self.reactor.slab_mut();
        let entry = slab.get_mut(self.key).expect("op entry vanished");
        if let Some(result) = entry.pop_result() {
            if entry.terminated && !entry.has_result() {
                slab.remove(self.key);
                self.done = true;
            }
            return Poll::Ready(Some(result));
        }
        if entry.terminated {
            slab.remove(self.key);
            self.done = true;
            return Poll::Ready(None);
        }
        entry.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for MultiOp {
    fn drop(&mut self) {
        if !self.done {
            self.reactor.orphan(self.key);
        }
    }
}
