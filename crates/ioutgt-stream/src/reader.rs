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
//! - [`read_direct`](StreamReader::read_direct): receive a large payload
//!   straight into caller memory (a slot buffer), skipping the scratch
//!   buffer — the bug-prone bit (raw pointer, `MSG_WAITALL` short-read
//!   resume loop, cancellation/orphan safety) lives here once.
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
    /// [`fill`](Self::fill)/[`read_direct`](Self::read_direct);
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

    /// Receive exactly `len` bytes straight into caller memory at `dst`,
    /// bypassing the scratch buffer (`MSG_WAITALL` with a short-read
    /// resume loop). `on_chunk` runs once per completed `recv` over the
    /// bytes just written — the digest seam; pass `|_| {}` to skip it at
    /// zero cost. Returns the byte count actually received; a value
    /// `< len` means EOF arrived mid-transfer.
    ///
    /// The buffered window must be empty (the direct path is only entered
    /// once the scratch buffer drained mid-payload).
    ///
    /// # Safety
    ///
    /// `dst` must be valid for `len` writable bytes and must remain
    /// allocated and unaliased for writes until this future resolves. The
    /// op is awaited inline, so the recv cannot still be in flight once
    /// this returns; if the whole future is dropped mid-await, the
    /// reactor's orphan protocol holds the op entry until its terminal
    /// CQE — the caller must keep `dst` alive until then (queue teardown's
    /// drain/leak backstop covers this).
    pub async unsafe fn read_direct(
        &mut self,
        dst: *mut u8,
        len: u32,
        mut on_chunk: impl FnMut(&[u8]),
    ) -> std::io::Result<u32> {
        debug_assert!(
            self.pos == self.filled,
            "read_direct with buffered bytes pending"
        );
        let mut done: u32 = 0;
        while done < len {
            // SAFETY: done < len and the caller guarantees `dst` is valid
            // for `len` bytes, so `dst + done` is in-bounds and the
            // `len - done` bytes from there stay within the allocation.
            let ptr = unsafe { dst.add(done as usize) };
            let want = len - done;
            // SAFETY: `ptr..ptr+want` is within `dst..dst+len`, which the
            // caller guarantees is valid for writes until this future
            // resolves; the op is awaited inline on the next line.
            let n = unsafe { ops::recv_raw_waitall(self.fd, ptr, want) }?.await?;
            if n == 0 {
                break; // EOF mid-transfer; caller maps the short return.
            }
            // SAFETY: the kernel just wrote `n` bytes at `ptr`; they are
            // within the caller's valid `dst` region and nothing else
            // touches them while this borrow lives (the slot is the
            // caller's, held exclusively for this transfer).
            let chunk = unsafe { std::slice::from_raw_parts(ptr, n as usize) };
            on_chunk(chunk);
            done += n;
        }
        Ok(done)
    }
}
