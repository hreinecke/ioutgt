//! The fabric-transport seam. The harness (queue-thread pool + control loop,
//! in [`crate`]) is generic over a [`Transport`]: the transport supplies the
//! connection source (bind / accept / handshake) and the per-queue driver
//! (`run_queue`); everything else — pool, control API, stats, pinning, idle
//! teardown — is transport-neutral. [`TcpTransport`] is the NVMe/TCP
//! implementation; an NVMe/RDMA one slots in beside it without touching the
//! harness.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::os::fd::OwnedFd;
use std::rc::Rc;
use std::sync::Arc;

use ioutgt_backend::AnyBackend;
use ioutgt_core::controller::Registry;
use ioutgt_core::dispatch::ConnCtx;
use ioutgt_core::subsystem::{PortConfig, TransportType};
use ioutgt_nvme_tcp::connection::{ConnPermit, QueueConn, run_queue as tcp_run_queue};
use ioutgt_nvme_tcp::handshake::{accept_handshake, read_connect};

use crate::{CONNECT_DISABLE_SQFLOW, TargetConfig, sqsize_cap};

/// Install callback, run once a connection's dispatch context exists: the admin
/// thread registers the live controller for AER nudges, and every thread
/// records the queue's stats handle. Boxed so the generic pool can hand it to a
/// transport's `run_queue` without the pool being generic over the closure.
pub type OnCtx = Box<dyn FnOnce(&Rc<ConnCtx<AnyBackend>>)>;

/// A fabric transport. All methods are associated (the implementing type is a
/// ZST marker); the harness threads `Self::Conn` through the queue-thread pool
/// and mailbox. Connection-source methods run on the control thread's
/// `LocalSet` (non-`Send` futures are fine); `run_queue` runs on a queue thread.
pub trait Transport: 'static {
    /// Everything a queue thread needs to run one connection. Sent across the
    /// mailbox to the queue thread, so it must be `Send`.
    type Conn: Send + 'static;
    /// A freshly accepted, pre-handshake connection. Lives only on the control
    /// thread, between [`Transport::accept`] and [`Transport::handshake`].
    type Raw;
    /// The bound listening endpoint.
    type Listener;

    /// Transport type recorded in the served port model (discovery log entries,
    /// `LIST_CONTROLLER`).
    fn trtype() -> TransportType;

    /// Bind the listening endpoint; returns the listener and the actual bound
    /// address (an ephemeral port resolves to the real one).
    fn bind(cfg: &TargetConfig)
    -> impl Future<Output = io::Result<(Self::Listener, SocketAddr)>>;

    /// Accept one raw connection. Used inside a `select!`, so it must be cancel-safe.
    fn accept(listener: &Self::Listener) -> impl Future<Output = io::Result<Self::Raw>>;

    /// Complete the fabric handshake, yielding the queue id (for routing to a
    /// queue thread) and the queue `Conn`. Spawned per connection so a slow or
    /// hostile handshake never blocks [`Transport::accept`].
    fn handshake(
        raw: Self::Raw,
        cfg: Arc<TargetConfig>,
        port: Arc<PortConfig<AnyBackend>>,
        registry: Arc<Registry>,
        permit: ConnPermit,
    ) -> impl Future<Output = io::Result<(u16, Self::Conn)>>;

    /// Drive one queue connection to completion on the queue thread. `on_ctx`
    /// runs once the dispatch context exists.
    fn run_queue(conn: Self::Conn, on_ctx: OnCtx) -> impl Future<Output = ()>;
}

/// NVMe/TCP transport: a `TcpListener`, the ICReq/ICResp + first-Connect
/// handshake, and the TCP `run_queue`.
pub struct TcpTransport;

impl Transport for TcpTransport {
    type Conn = QueueConn<AnyBackend>;
    type Raw = (tokio::net::TcpStream, SocketAddr);
    type Listener = tokio::net::TcpListener;

    fn trtype() -> TransportType {
        TransportType::Tcp
    }

    async fn bind(cfg: &TargetConfig) -> io::Result<(Self::Listener, SocketAddr)> {
        let listener = tokio::net::TcpListener::bind(cfg.listen).await?;
        let local = listener.local_addr()?;
        Ok((listener, local))
    }

    async fn accept(listener: &Self::Listener) -> io::Result<Self::Raw> {
        listener.accept().await
    }

    async fn handshake(
        raw: Self::Raw,
        cfg: Arc<TargetConfig>,
        port: Arc<PortConfig<AnyBackend>>,
        registry: Arc<Registry>,
        permit: ConnPermit,
    ) -> io::Result<(u16, Self::Conn)> {
        let (mut stream, _peer) = raw;
        stream.set_nodelay(true)?;
        let negotiated = accept_handshake(
            &mut stream,
            cfg.allow_hdgst,
            cfg.allow_ddgst,
            ioutgt_nvme_tcp::MAX_H2C_DATA,
        )
        .await?;
        let first = read_connect(&mut stream, negotiated).await?;
        let connect = first.connect();
        let qid = connect.qid.get();
        let entries = connect.sqsize.get() as u32 + 1;
        // Enforce the advertised queue-size limit: each slot preallocates a
        // data buffer, so an oversized queue is a memory-amplification vector a
        // hostile host could exploit by ignoring the advertised ceiling.
        let cap = sqsize_cap(qid, port.io_queue_size);
        if !(2..=u32::from(cap)).contains(&entries) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "sqsize out of range",
            ));
        }
        let sqhd_disabled = connect.cattr & CONNECT_DISABLE_SQFLOW != 0;
        let std_stream = stream.into_std()?;
        let conn = QueueConn {
            fd: OwnedFd::from(std_stream),
            hdr_digest: negotiated.hdr_digest,
            data_digest: negotiated.data_digest,
            qid,
            #[allow(clippy::cast_possible_truncation)]
            sqsize: entries as u16,
            sqhd_disabled,
            send_zc: cfg.send_zc,
            connect_sqe: first.sqe,
            connect_data: first.data,
            port,
            registry,
            permit,
        };
        Ok((qid, conn))
    }

    fn run_queue(conn: Self::Conn, on_ctx: OnCtx) -> impl Future<Output = ()> {
        // `Box<dyn FnOnce(&Rc<ConnCtx<AnyBackend>>)>` satisfies the
        // `impl FnOnce(&Rc<ConnCtx<B>>)` bound with `B = AnyBackend`.
        tcp_run_queue(conn, on_ctx)
    }
}
