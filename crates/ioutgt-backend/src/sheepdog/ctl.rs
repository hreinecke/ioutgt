//! The control plane's connection to a `sheep` gateway: one request at a
//! time, on a ring, with the calling thread blocked across it.
//!
//! The data path multiplexes every namespace's object IO over one connection
//! per queue thread ([`super::mux`]). The control plane wants none of that:
//! its callers are synchronous (`list_acls`, `SheepdogBackend::open`, the
//! path/ANA refresh thread), they run a handful of round trips and hang up,
//! and they must work on a thread that has no scheduler at all. So a [`Conn`]
//! is a socket plus a [`BlockingRing`]: each request is submitted to the ring
//! and awaited by parking the thread inside `io_uring_enter`, which is the
//! same ops, the same reactor, and the same completion path as the IO — just
//! without a runtime overhead that would buy nothing here.
//!
//! Every buffer these ops touch is **owned** by the reactor for the op's
//! lifetime ([`ops::send`]/[`ops::recv`], not the raw-pointer variants), and
//! payloads are copied out of it afterwards. The copies are irrelevant at
//! control-plane rates, and in exchange a request that is abandoned (the
//! release timeout below) leaves nothing the kernel could still write into.
//!
//! One connection is one request at a time: unlike the data path, nothing
//! here pipelines, so a response belongs to the request that just went out
//! and the `id` field is left zero.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::pin::pin;
use std::task::Poll;
use std::time::Duration;

use ioutgt_uring::{BlockingRing, RingConfig, ops};

use super::{SD_HDR_SIZE, new_stream_socket, resp_data_length, resp_result};

/// Ring geometry for a control connection: one op in flight at a time, so
/// the smallest ring the kernel will build is already generous.
const CTL_RING: RingConfig = RingConfig {
    sq_entries: 8,
    cq_entries: 16,
};

/// Bytes per `recv` while a payload is being read off the wire. Big enough
/// that a megabyte-scale reply (the inode object map, the cluster's VDI state
/// table) is a handful of round trips, small enough to reuse one buffer.
const RECV_CHUNK: usize = 64 * 1024;

/// One completed round trip.
pub(super) struct Resp {
    /// The 48-byte response header verbatim. Beyond the result and the
    /// payload length, some replies answer in it — `GET_VDI_INFO` puts the
    /// vid it resolved at byte 24 — so the whole header goes back to the
    /// caller rather than the fields this module happens to know about.
    pub hdr: [u8; SD_HDR_SIZE],
    /// Payload bytes the server sent (never more than the caller asked for;
    /// see [`Conn::request`]).
    pub len: usize,
}

impl Resp {
    /// The response's `SD_RES_*`.
    pub fn result(&self) -> u32 {
        resp_result(&self.hdr)
    }
}

/// A control-plane connection to one `sheep` gateway.
pub(super) struct Conn {
    ring: BlockingRing,
    fd: OwnedFd,
    /// Bound on one round trip, or `None` to wait for as long as the cluster
    /// takes (the startup default: an enumeration has nowhere better to be).
    timeout: Option<Duration>,
}

impl Conn {
    /// Dial `addr`.
    pub fn connect(addr: SocketAddr) -> io::Result<Conn> {
        let ring = BlockingRing::new(CTL_RING)?;
        let fd = new_stream_socket(&addr)?;
        ring.block_on(async { ops::connect(fd.as_raw_fd(), &addr)?.await })?;
        Ok(Conn {
            ring,
            fd,
            timeout: None,
        })
    }

    /// Take up an already-connected socket again — the one a backend parked
    /// at open time to hand its VDI lock back over — with `timeout` on every
    /// round trip made through it.
    pub fn adopt(fd: OwnedFd, timeout: Duration) -> io::Result<Conn> {
        Ok(Conn {
            ring: BlockingRing::new(CTL_RING)?,
            fd,
            timeout: Some(timeout),
        })
    }

    /// Give the socket up, keeping it open for a later [`Conn::adopt`]. The
    /// ring goes; a bare fd is `Send`, and a connection this outlives may be
    /// picked up again from another thread.
    pub fn into_fd(self) -> OwnedFd {
        self.fd
    }

    /// Send `hdr` (followed by `write`, for the ops that carry a payload) and
    /// read the response into `dst`, zero-filling whatever the server left
    /// short of it.
    ///
    /// A payload longer than `dst` is a protocol violation — the server was
    /// told how much room there was — and fails the request rather than being
    /// half-read: the connection's framing is unknowable from there on, and
    /// the caller drops it.
    pub fn request(
        &self,
        hdr: &[u8; SD_HDR_SIZE],
        write: &[u8],
        dst: &mut [u8],
    ) -> io::Result<Resp> {
        let fd = self.fd.as_raw_fd();
        self.ring.block_on(deadlined(self.timeout, async {
            send_all(fd, request_bytes(hdr, write)).await?;
            let resp = recv_header(fd).await?;
            let len = resp_data_length(&resp) as usize;
            if len > dst.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("response payload {len} exceeds the {} requested", dst.len()),
                ));
            }
            recv_into(fd, &mut dst[..len]).await?;
            dst[len..].fill(0);
            Ok(Resp { hdr: resp, len })
        }))
    }

    /// [`Conn::request`] for the ops whose answer is in the header: any
    /// payload is read off the wire and dropped.
    pub fn request_discard(&self, hdr: &[u8; SD_HDR_SIZE], write: &[u8]) -> io::Result<Resp> {
        let fd = self.fd.as_raw_fd();
        self.ring.block_on(deadlined(self.timeout, async {
            send_all(fd, request_bytes(hdr, write)).await?;
            let resp = recv_header(fd).await?;
            let len = resp_data_length(&resp) as usize;
            drain(fd, len).await?;
            Ok(Resp { hdr: resp, len })
        }))
    }
}

/// A request as one buffer: the header and its payload reach the wire in one
/// send, which is also what keeps them contiguous on it.
fn request_bytes(hdr: &[u8; SD_HDR_SIZE], write: &[u8]) -> Box<[u8]> {
    let mut out = Vec::with_capacity(SD_HDR_SIZE + write.len());
    out.extend_from_slice(hdr);
    out.extend_from_slice(write);
    out.into_boxed_slice()
}

/// Fail `fut` if it has not finished within `limit`; `None` waits.
///
/// The op in flight when the timer fires is dropped, which costs nothing
/// here: control-plane ops own their buffers, so the reactor holds them until
/// the kernel is done whether or not anyone is still waiting.
async fn deadlined<T>(
    limit: Option<Duration>,
    fut: impl Future<Output = io::Result<T>>,
) -> io::Result<T> {
    let Some(limit) = limit else {
        return fut.await;
    };
    let mut fut = pin!(fut);
    let mut timer = pin!(ops::sleep(limit)?);
    std::future::poll_fn(|cx| {
        if let Poll::Ready(out) = fut.as_mut().poll(cx) {
            return Poll::Ready(out);
        }
        match timer.as_mut().poll(cx) {
            Poll::Ready(_) => Poll::Ready(Err(io::Error::from(io::ErrorKind::TimedOut))),
            Poll::Pending => Poll::Pending,
        }
    })
    .await
}

/// Send all of `buf`, resuming across short sends.
async fn send_all(fd: RawFd, buf: Box<[u8]>) -> io::Result<()> {
    let mut buf = buf;
    loop {
        let len = buf.len();
        let (result, back) = ops::send(fd, buf)?.await;
        let sent = result? as usize;
        if sent >= len {
            return Ok(());
        }
        if sent == 0 {
            return Err(io::Error::from(io::ErrorKind::WriteZero));
        }
        buf = back[sent..].into();
    }
}

/// Read one 48-byte response header.
async fn recv_header(fd: RawFd) -> io::Result<[u8; SD_HDR_SIZE]> {
    let mut hdr = [0u8; SD_HDR_SIZE];
    recv_into(fd, &mut hdr).await?;
    Ok(hdr)
}

/// Fill `dst` from the socket, resuming across short receives. EOF before it
/// is full ends the connection: the response was cut in half.
async fn recv_into(fd: RawFd, dst: &mut [u8]) -> io::Result<()> {
    let mut chunk: Box<[u8]> = Box::default();
    let mut done = 0usize;
    while done < dst.len() {
        // One buffer, reused: it is resized only for a tail shorter than a
        // whole chunk. The recv owns it while the op is in flight, so the
        // bytes are copied out rather than landing in `dst` directly.
        let want = (dst.len() - done).min(RECV_CHUNK);
        if chunk.len() != want {
            chunk = vec![0u8; want].into_boxed_slice();
        }
        let (result, back) = ops::recv(fd, chunk)?.await;
        chunk = back;
        let got = result? as usize;
        if got == 0 {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }
        dst[done..done + got].copy_from_slice(&chunk[..got]);
        done += got;
    }
    Ok(())
}

/// Swallow `len` bytes of payload nobody wants, so the connection is left
/// where the next request expects it.
async fn drain(fd: RawFd, len: usize) -> io::Result<()> {
    let mut left = len;
    let mut chunk: Box<[u8]> = Box::default();
    while left > 0 {
        let want = left.min(RECV_CHUNK);
        if chunk.len() != want {
            chunk = vec![0u8; want].into_boxed_slice();
        }
        let (result, back) = ops::recv(fd, chunk)?.await;
        chunk = back;
        let got = result? as usize;
        if got == 0 {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }
        left -= got;
    }
    Ok(())
}
