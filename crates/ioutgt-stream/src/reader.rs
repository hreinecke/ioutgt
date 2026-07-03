//! Protocol-neutral buffered byte-source for stream transports.
//!
//! A stream transport (NVMe/TCP today, NBD next) frames inbound bytes by
//! reading a header, then a payload of the length that header announces.
//! The framing and decoding are protocol-specific and stay in the
//! transport; the *byte plumbing* underneath is not. [`StreamReader`]
//! owns the socket fd and exposes a small mechanic surface:
//!
//! - [`fill`](StreamReader::fill)/[`consume`](StreamReader::consume): a
//!   buffered window the transport decodes headers and small payloads out
//!   of. One `recv` (or ring chunk) refills it; `consume` advances past
//!   processed bytes.
//! - [`read_direct_vectored`](StreamReader::read_direct_vectored): receive a
//!   large payload straight into caller memory (one or more slot segments),
//!   skipping the scratch buffer — the bug-prone bit (raw pointers, scatter
//!   `recvmsg`/`MSG_WAITALL` short-read resume, cancellation/orphan safety)
//!   lives here once. Classic mode only.
//!
//! Two backing modes share that surface:
//!
//! - **Classic**: one scratch buffer refilled by a plain `recv`; the
//!   direct-into-caller `recvmsg` path lives here.
//! - **Ring**: a multishot `recv` drawing from the thread's shared
//!   [`BufRing`]; each [`fill`](StreamReader::fill) hands back the next
//!   ring chunk and recycles the previous one. No per-recv submission and
//!   (a later phase) the chunk can be retained zero-copy for write payloads.
//!
//! The reader holds no protocol or slot state: it deals in raw byte
//! windows and a caller-supplied destination pointer only. It never
//! closes `fd` — the connection's `OwnedFd` stays the sole owner, so the
//! teardown contract (the fd drops last, orphaning any in-flight op) is
//! unchanged. Sits in `ioutgt-stream` beside [`StreamSender`](crate::StreamSender),
//! above `ioutgt-uring`.

use std::rc::Rc;

use ioutgt_uring::bufring::BufRing;
use ioutgt_uring::ops::{self, RecvMultiOp};

/// Classic scratch-buffer backing.
struct ClassicMode {
    /// `None` only across the `recv` await in [`StreamReader::fill`].
    buf: Option<Box<[u8]>>,
    filled: usize,
    pos: usize,
}

/// The ring chunk currently being consumed.
struct Cur {
    bid: u16,
    /// Byte offset within the buffer where this chunk's data begins — the
    /// ring's running `recv_off(bid)` snapshotted when the CQE arrived. The
    /// window is `[off, off+len)`; `off+pos` is the unconsumed start.
    off: usize,
    len: usize,
    pos: usize,
    /// `IORING_CQE_F_BUF_MORE` for the CQE that produced this chunk: the
    /// buffer is only partially consumed and more CQEs (same bid, advancing
    /// offset) will follow. When clear, draining this chunk fully consumes
    /// the buffer and it must be re-provided.
    buf_more: bool,
}

/// Provided-buffer-ring backing: a multishot recv plus the chunk in hand.
///
/// The per-buffer running offset under the incremental ring lives on the
/// shared [`BufRing`], not here: every connection on this reactor thread
/// draws from the same `bgid`, and the kernel advances one offset per buffer
/// shared by all of them — a per-reader offset would desync the moment two
/// connections touch the same buffer.
struct RingMode {
    ring: Rc<BufRing>,
    /// Armed multishot recv; `None` between (re-)arms.
    op: Option<RecvMultiOp>,
    cur: Option<Cur>,
    /// Ring provide generation captured when `op` was last armed. On ENOBUFS,
    /// the park is conditioned on this so a buffer re-provided after the kernel
    /// queued ENOBUFS (but before we observe it) is not missed — see
    /// [`BufRing::wait_for_provide`].
    armed_gen: u64,
}

enum Mode {
    Classic(ClassicMode),
    Ring(RingMode),
}

/// Buffered byte-source over a socket `fd`: a refillable window plus a
/// direct-into-caller-memory path for large payloads. See the module
/// docs for the protocol/slot boundary and the classic/ring split.
pub struct StreamReader {
    fd: i32,
    mode: Mode,
}

impl StreamReader {
    /// Classic reader over `fd` with a `cap`-byte scratch buffer (nvme-tcp
    /// passes 64 KiB). Allocated once; reused for the connection's lifetime.
    /// Does not take ownership of `fd` and never closes it.
    pub fn new(fd: i32, cap: usize) -> StreamReader {
        StreamReader {
            fd,
            mode: Mode::Classic(ClassicMode {
                buf: Some(vec![0u8; cap].into_boxed_slice()),
                filled: 0,
                pos: 0,
            }),
        }
    }

    /// Ring-backed reader drawing from the connection's own [`BufRing`].
    pub fn new_ring(fd: i32, ring: Rc<BufRing>) -> StreamReader {
        StreamReader {
            fd,
            mode: Mode::Ring(RingMode {
                ring,
                op: None,
                cur: None,
                armed_gen: 0,
            }),
        }
    }

    /// True for a ring-backed reader (the direct-tail path is classic-only).
    pub fn is_ring(&self) -> bool {
        matches!(self.mode, Mode::Ring(_))
    }

    /// The thread's ring (ring mode only) — for re-providing a retained
    /// chunk and querying geometry.
    pub fn ring(&self) -> Option<&Rc<BufRing>> {
        match &self.mode {
            Mode::Ring(r) => Some(&r.ring),
            Mode::Classic(_) => None,
        }
    }

    /// Return the current buffered window, receiving more first if it is
    /// empty. An empty slice means orderly EOF (the peer closed). The window
    /// stays valid until the next
    /// [`fill`](Self::fill)/[`read_direct_vectored`](Self::read_direct_vectored);
    /// [`consume`](Self::consume) advances past bytes already processed.
    pub async fn fill(&mut self) -> std::io::Result<&[u8]> {
        Ok(self.fill_with_bid().await?.0)
    }

    /// Like [`fill`](Self::fill), but also returns the ring buffer id backing
    /// the window (`Some` in ring mode, `None` in classic). Bundling the bid
    /// with the window lets a zero-copy consumer record an in-place borrow
    /// without a second `&self` query racing the window's `&mut self` borrow.
    pub async fn fill_with_bid(&mut self) -> std::io::Result<(&[u8], Option<u16>)> {
        let fd = self.fd;
        match &mut self.mode {
            Mode::Classic(c) => Ok((c.fill(fd).await?, None)),
            Mode::Ring(r) => {
                let (win, bid) = r.fill_bid(fd).await?;
                Ok((win, Some(bid)))
            }
        }
    }

    /// Mark `n` bytes of the current window consumed; `n` must not exceed
    /// the last [`fill`](Self::fill) window length.
    pub fn consume(&mut self, n: usize) {
        match &mut self.mode {
            Mode::Classic(c) => {
                debug_assert!(c.pos + n <= c.filled, "consume past window");
                c.pos += n;
            }
            Mode::Ring(r) => {
                let cur = r.cur.as_mut().expect("consume with no ring chunk");
                debug_assert!(cur.pos + n <= cur.len, "consume past chunk");
                cur.pos += n;
            }
        }
    }

    /// Receive the iovecs' total length straight into the caller's
    /// (possibly scattered)
    /// segments with a single `recvmsg`/`MSG_WAITALL` — the kernel scatters
    /// the payload across `iovs`, one syscall instead of one `recv` per
    /// segment. Returns the bytes received (short only on EOF; the caller
    /// maps a short return to an orderly mid-payload close). `iovs` is left
    /// advanced past the received bytes. Classic mode only.
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
            matches!(&self.mode, Mode::Classic(c) if c.pos == c.filled),
            "read_direct_vectored requires classic mode with a drained window"
        );
        let fd = self.fd;
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
            let n = unsafe { ops::recvmsg_raw(fd, &raw mut msg) }?.await?;
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

impl ClassicMode {
    async fn fill(&mut self, fd: i32) -> std::io::Result<&[u8]> {
        if self.pos == self.filled {
            // Window drained: one recv. `take` moves the buffer into the op;
            // BufOp hands it back on completion (even on error). A *submit*
            // failure consumes it via `?`, but that error tears the
            // connection down and the reader is dropped, not reused.
            let buf = self.buf.take().expect("buffer present between recvs");
            let (res, buf) = ops::recv(fd, buf)?.await;
            self.buf = Some(buf);
            let n = res? as usize;
            self.pos = 0;
            self.filled = n;
        }
        let buf = self.buf.as_ref().expect("buffer present after recv");
        Ok(&buf[self.pos..self.filled])
    }
}

impl RingMode {
    /// Return the next window together with the bid of
    /// the buffer backing it. The bid is read before the returned slice is
    /// constructed, so both leave the `&mut self` borrow at once. On EOF the
    /// window is empty and the bid is a placeholder the caller ignores.
    async fn fill_bid(&mut self, fd: i32) -> std::io::Result<(&[u8], u16)> {
        // Bytes still unconsumed in the current chunk: hand them back. The
        // chunk's data lives at `buf(bid) + off[bid]` (the running per-buffer
        // offset under an incremental ring); `cur.pos` is the consumed prefix
        // within this chunk.
        if let Some(cur) = &self.cur {
            if cur.pos < cur.len {
                let start = cur.off + cur.pos;
                let len = cur.len - cur.pos;
                // SAFETY: [start, start+len) lies within buffer `bid` (the
                // kernel filled it at the shared running offset), alive until
                // we re-provide it (only once fully consumed).
                let win =
                    unsafe { std::slice::from_raw_parts(self.ring.buf(cur.bid).add(start), len) };
                return Ok((win, cur.bid));
            }
            // Chunk fully consumed; the offset was already advanced at observe
            // time, so here we only re-provide a fully-consumed buffer.
            if !cur.buf_more {
                // Buffer fully consumed: the kernel advanced `head`. Tell the
                // ring the recv loop is done with it (returns to the kernel
                // now if unborrowed, else on the last BufRing::release); the
                // running offset resets to 0 when the buffer is re-provided.
                self.ring.recv_done(cur.bid);
            }
            self.cur = None;
        }
        loop {
            if self.op.is_none() {
                // Snapshot the provide gen before arming (lost-wakeup guard —
                // see BufRing::wait_for_provide).
                self.armed_gen = self.ring.provide_gen();
                self.op = Some(ops::recv_multi(fd, self.ring.bgid())?);
            }
            match self.op.as_mut().unwrap().next().await {
                Some(Ok(chunk)) => {
                    if !chunk.more {
                        // Terminal CQE that still carried data: re-arm next.
                        self.op = None;
                    }
                    let len = chunk.len as usize;
                    // This chunk begins at the buffer's running offset; advance it
                    // now (while BUF_MORE keeps the buffer filling) so a sibling
                    // drawing the next chunk sees the right offset. A retired
                    // buffer (BUF_MORE clear) isn't advanced — recv_done
                    // re-provides it and the offset resets.
                    let off = self.ring.recv_off(chunk.bid);
                    if chunk.buf_more {
                        self.ring.recv_advance(chunk.bid, len);
                    }
                    self.cur = Some(Cur {
                        bid: chunk.bid,
                        off,
                        len,
                        pos: 0,
                        buf_more: chunk.buf_more,
                    });
                    // SAFETY: the kernel filled `len` bytes at `buf(bid) + off`
                    // (the running offset for this incremental buffer), valid
                    // until we re-provide it.
                    let win = unsafe {
                        std::slice::from_raw_parts(self.ring.buf(chunk.bid).add(off), len)
                    };
                    return Ok((win, chunk.bid));
                }
                // Group drained: all buffers are out (recv-side or borrowed by
                // in-flight writes). Drop the dead multishot and PARK until a
                // buffer returns to the ring, then re-arm. Busy re-arming would
                // re-post ENOBUFS at once and spin the reactor, starving the
                // slot tasks whose write completions are what return buffers.
                Some(Err(e)) if e.raw_os_error() == Some(libc::ENOBUFS) => {
                    self.op = None;
                    // Park only if no buffer has been re-provided since we armed
                    // this recv; otherwise the buffers are already back and the
                    // loop re-arms immediately.
                    self.ring.wait_for_provide(self.armed_gen).await;
                }
                Some(Err(e)) => return Err(e),
                None => return Ok((&[], 0)), // EOF
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::os::fd::AsRawFd;
    use std::rc::Rc;

    use ioutgt_uring::bufring::BufRing;
    use ioutgt_uring::{QueueRuntime, RingConfig};

    use super::StreamReader;

    fn socketpair() -> (std::os::fd::OwnedFd, std::os::fd::OwnedFd) {
        let mut fds = [0i32; 2];
        // SAFETY: writes two fds into `fds` on success.
        let r = unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
        assert_eq!(r, 0, "socketpair failed");
        // SAFETY: fresh fds, exclusively owned.
        unsafe {
            (
                std::os::fd::FromRawFd::from_raw_fd(fds[0]),
                std::os::fd::FromRawFd::from_raw_fd(fds[1]),
            )
        }
    }

    fn send_all(fd: &std::os::fd::OwnedFd, data: &[u8]) {
        // SAFETY: plain blocking write of `data`.
        let n = unsafe { libc::write(fd.as_raw_fd(), data.as_ptr().cast(), data.len()) };
        assert_eq!(n, data.len() as isize, "short write");
    }

    /// When the ring reader *fully consumes* a buffer (the incremental ring
    /// reports `buf_more` clear — the kernel advanced past it) and calls `fill`
    /// to fetch the next chunk, it must hand the exhausted buffer back via
    /// `recv_done`, not `provide`. `recv_done` defers the kernel re-provide
    /// while any write still borrows the buffer; `provide` bypasses that and
    /// immediately returns it — which would let the ring recycle a buffer still
    /// in use by a zero-copy write.
    ///
    /// Proof: fill buffer 0 to capacity (so the next CQE switches buffers and
    /// the drain path runs), borrow it before that switch, then verify the
    /// kernel re-provide is held back until `release`.
    ///
    /// Under incremental consumption a buffer is only retired once it is full,
    /// so the test must push a whole buffer's worth of bytes through it — a
    /// handful of small messages would keep `buf_more` set and never trigger
    /// the drain path.
    #[test]
    fn ring_drain_calls_recv_done_not_provide() {
        let (a, b) = socketpair();
        let rt = QueueRuntime::new(RingConfig::default()).unwrap();
        rt.block_on(async move {
            // 256 KiB ring → two 128 KiB buffers.
            let ring: Rc<BufRing> = BufRing::new(9, 256 * 1024).unwrap();

            let mut reader = StreamReader::new_ring(a.as_raw_fd(), ring.clone());

            // First fill so we learn which buffer the kernel handed out, and
            // borrow it (a zero-copy write holding it open).
            send_all(&b, b"hello");
            let (bid0, first_len) = {
                let (win, bid) = reader.fill_with_bid().await.unwrap();
                assert!(!win.is_empty());
                (bid.unwrap(), win.len())
            };
            ring.borrow(bid0);

            // Drive bytes through buffer 0 until it is fully consumed (the
            // reader switches to the other buffer). Each fill hands back a
            // window; consume it and feed more. We track how much of buffer 0
            // has been consumed via the bid changing.
            reader.consume(first_len);
            let sender = std::thread::spawn(move || {
                // Push well over one buffer so the kernel fills bid0 to the
                // brim and moves on. A background thread avoids socket-buffer
                // backpressure deadlocking the single-threaded reactor.
                let chunk = vec![0xABu8; 64 * 1024];
                for _ in 0..6 {
                    send_all(&b, &chunk);
                }
                b
            });

            // Read until the current buffer differs from bid0: that fill is the
            // one that fully consumed bid0 and called recv_done(bid0).
            loop {
                let (cur, win_len) = {
                    let (win, bid) = reader.fill_with_bid().await.unwrap();
                    assert!(!win.is_empty(), "unexpected EOF");
                    (bid.unwrap(), win.len())
                };
                reader.consume(win_len);
                if cur != bid0 {
                    break;
                }
            }

            // bid0 was fully consumed and recv_done(bid0) ran, but a borrow is
            // held: with `recv_done` (correct) the provide is deferred and
            // provided_count is still 0. With `provide` (wrong) it would be 1.
            assert_eq!(
                ring.kernel_provided_count(bid0),
                0,
                "bid0 must not reach the kernel while a write borrow is held"
            );

            // Releasing the borrow must now flush the deferred provide.
            ring.release(bid0);
            assert_eq!(
                ring.kernel_provided_count(bid0),
                1,
                "bid0 must be provided to the kernel once the last borrow is released"
            );

            let _ = sender.join();
        });
    }

    /// Ring mode must deliver the exact same byte stream as classic mode for a
    /// multi-chunk send. Drive both readers over their own socketpairs with the
    /// same payload, draining each through `fill`/`consume`, and compare the
    /// reassembled streams byte for byte.
    #[test]
    fn ring_matches_classic_byte_stream() {
        const N: usize = 300 * 1024;
        const CHUNK: usize = 17 * 1024;

        #[allow(clippy::cast_possible_truncation)]
        let payload: Vec<u8> = (0..N).map(|i| ((i * 31 + 7) & 0xFF) as u8).collect();

        // Classic stream.
        let rt = QueueRuntime::new(RingConfig::default()).unwrap();
        let classic = {
            let (a, b) = socketpair();
            let payload = payload.clone();
            let sender = std::thread::spawn(move || {
                let mut sent = 0;
                while sent < N {
                    let end = (sent + CHUNK).min(N);
                    send_all(&b, &payload[sent..end]);
                    sent = end;
                }
                b // hold open until reader has all bytes
            });
            let out = rt.block_on(async move {
                let mut reader = StreamReader::new(a.as_raw_fd(), 64 * 1024);
                let mut got = Vec::with_capacity(N);
                while got.len() < N {
                    let win = reader.fill().await.unwrap();
                    if win.is_empty() {
                        break;
                    }
                    let len = win.len();
                    got.extend_from_slice(win);
                    reader.consume(len);
                }
                got
            });
            let _ = sender.join();
            out
        };

        // Ring stream.
        let ring_out = {
            let (a, b) = socketpair();
            let payload = payload.clone();
            let sender = std::thread::spawn(move || {
                let mut sent = 0;
                while sent < N {
                    let end = (sent + CHUNK).min(N);
                    send_all(&b, &payload[sent..end]);
                    sent = end;
                }
                b
            });
            let out = rt.block_on(async move {
                let ring: Rc<BufRing> = BufRing::new(11, 256 * 1024).unwrap();
                let mut reader = StreamReader::new_ring(a.as_raw_fd(), ring);
                let mut got = Vec::with_capacity(N);
                while got.len() < N {
                    let win = reader.fill().await.unwrap();
                    if win.is_empty() {
                        break;
                    }
                    let len = win.len();
                    got.extend_from_slice(win);
                    reader.consume(len);
                }
                got
            });
            let _ = sender.join();
            out
        };

        assert_eq!(classic, payload, "classic stream mismatch");
        assert_eq!(ring_out, payload, "ring stream mismatch");
        assert_eq!(classic, ring_out, "ring and classic streams differ");
    }
}
