//! Protocol-neutral buffered byte-source for stream transports.
//!
//! A stream transport (NVMe/TCP today, NBD next) frames inbound bytes by
//! reading a header, then a payload of the length that header announces.
//! The framing and decoding are protocol-specific and stay in the
//! transport; the *byte plumbing* underneath is not. [`StreamReader`]
//! owns the socket fd and a single scratch buffer and exposes exactly two
//! mechanics:
//!
//! - [`fill`](StreamReader::fill)/[`consume`](StreamReader::consume): a
//!   buffered window the transport decodes headers and small payloads out
//!   of. One `recv` refills it; `consume` advances past processed bytes.
//! - [`read_direct_vectored`](StreamReader::read_direct_vectored): receive a
//!   large payload straight into caller memory (one or more slot segments),
//!   skipping the scratch buffer — the bug-prone bit (raw pointers, scatter
//!   `recvmsg`/`MSG_WAITALL` short-read resume, cancellation/orphan safety)
//!   lives here once.
//!
//! The reader holds no protocol or slot state: it deals in raw byte
//! windows and a caller-supplied destination pointer only. It never
//! closes `fd` — the connection's `OwnedFd` stays the sole owner, so the
//! teardown contract (the fd drops last, orphaning any in-flight op) is
//! unchanged. Sits in `ioutgt-stream` beside [`StreamSender`](crate::StreamSender),
//! above `ioutgt-uring`.

use ioutgt_uring::ops;

/// Buffered byte-source over a socket `fd`: a refillable window plus a
/// direct-into-caller-memory path for large payloads. See the module
/// docs for the protocol/slot boundary.
pub struct StreamReader {
    fd: i32,
    /// Scratch buffer. `None` only across the `recv` await in [`fill`](Self::fill),
    /// which takes it (the op owns it) and restores it on completion.
    buf: Option<Box<[u8]>>,
    /// Bytes valid in `buf` after the last `recv`.
    filled: usize,
    /// Consumed prefix; the live window is `buf[pos..filled]`.
    pos: usize,
}

impl StreamReader {
    /// Reader over `fd` with a `cap`-byte scratch buffer (nvme-tcp passes
    /// 64 KiB). Allocated once; reused for the connection's lifetime.
    /// Does not take ownership of `fd` and never closes it.
    pub fn new(fd: i32, cap: usize) -> StreamReader {
        StreamReader {
            fd,
            buf: Some(vec![0u8; cap].into_boxed_slice()),
            filled: 0,
            pos: 0,
        }
    }

    /// Return the current buffered window, issuing one `recv` first if it
    /// is empty. An empty returned slice means orderly EOF (the peer
    /// closed). The window stays valid until the next
    /// [`fill`](Self::fill)/[`read_direct_vectored`](Self::read_direct_vectored);
    /// [`consume`](Self::consume) advances past bytes already processed.
    pub async fn fill(&mut self) -> std::io::Result<&[u8]> {
        if self.pos == self.filled {
            // Window drained: issue one recv. `take` moves the buffer into
            // the op; on completion BufOp hands it back, so we restore it
            // before propagating the recv result (an await-completion error
            // still returns the buffer). A *submit* failure instead consumes
            // the buffer via `?`, leaving the reader bufferless — but that
            // error propagates out of the recv loop and tears the connection
            // down, so the reader is dropped, not reused.
            let buf = self.buf.take().expect("buffer present between recvs");
            let (res, buf) = ops::recv(self.fd, buf)?.await;
            self.buf = Some(buf);
            let n = res? as usize;
            self.pos = 0;
            self.filled = n;
        }
        let buf = self.buf.as_ref().expect("buffer present after recv");
        Ok(&buf[self.pos..self.filled])
    }

    /// Mark `n` bytes of the current window consumed; `n` must not exceed
    /// the last [`fill`](Self::fill) window length.
    pub fn consume(&mut self, n: usize) {
        debug_assert!(self.pos + n <= self.filled, "consume past window");
        self.pos += n;
    }

    /// Receive the iovecs' total length straight into the caller's
    /// (possibly scattered)
    /// segments with a single `recvmsg`/`MSG_WAITALL` — the kernel scatters
    /// the payload across `iovs`, one syscall instead of one `recv` per
    /// segment. Returns the bytes received (short only on EOF; the caller
    /// maps a short return to an orderly mid-payload close). `iovs` is left
    /// advanced past the received bytes.
    ///
    /// # Safety
    ///
    /// Every buffer the iovecs reference must stay valid and exclusively
    /// borrowed for writes until this future resolves — the op is awaited
    /// inline; on whole-future drop the reactor holds it to its terminal
    /// CQE. The reader's scratch window must be empty.
    #[allow(clippy::cast_possible_truncation)] // total is caller-bounded to MDTS
    pub async unsafe fn read_direct_vectored(
        &mut self,
        iovs: &mut [libc::iovec],
    ) -> std::io::Result<u32> {
        debug_assert!(
            self.pos == self.filled,
            "read_direct_vectored with buffered bytes pending"
        );
        let total: usize = iovs.iter().map(|v| v.iov_len).sum();
        let mut done = 0usize;
        let mut idx = 0usize; // first iovec not yet fully filled
        while done < total {
            // A msghdr over the iovecs still awaiting bytes, rebuilt each
            // iteration so a short (non-EOF) return resumes from the gap.
            // SAFETY: all-zero is a valid `msghdr`.
            let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
            msg.msg_iov = iovs[idx..].as_mut_ptr();
            msg.msg_iovlen = iovs.len() - idx;
            // SAFETY: `msg`, the iovec array, and every buffer they point at
            // outlive this awaited op (held in this frame); the caller
            // guarantees the buffers are valid and unaliased for writes.
            let n = unsafe { ops::recvmsg_raw(self.fd, &raw mut msg) }?.await?;
            if n == 0 {
                break; // EOF mid-transfer; caller maps the short return.
            }
            done += n as usize;
            // Advance `idx`/iovecs past the `n` landed bytes for a resume.
            let mut adv = n as usize;
            while adv > 0 {
                let v = &mut iovs[idx];
                if v.iov_len <= adv {
                    adv -= v.iov_len;
                    idx += 1;
                } else {
                    // SAFETY: advancing within the current iovec's buffer.
                    v.iov_base = unsafe { v.iov_base.cast::<u8>().add(adv).cast() };
                    v.iov_len -= adv;
                    adv = 0;
                }
            }
        }
        Ok(done as u32)
    }
}
