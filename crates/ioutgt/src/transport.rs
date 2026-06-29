//! NVMe/TCP implementation of the harness [`Transport`] seam: a `TcpListener`,
//! the ICReq/ICResp + first-Connect handshake, and the TCP `run_queue`. An
//! NVMe/RDMA transport implements the same trait in its own crate.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::os::fd::OwnedFd;
use std::sync::Arc;

use ioutgt_backend::AnyBackend;
use ioutgt_core::controller::Registry;
use ioutgt_core::subsystem::{PortConfig, TransportType};
use ioutgt_harness::{OnCtx, TargetConfig, Transport};
use ioutgt_nvme_tcp::connection::{ConnPermit, QueueConn, run_queue as tcp_run_queue};
use ioutgt_nvme_tcp::handshake::{accept_handshake, read_connect};

use crate::{CONNECT_DISABLE_SQFLOW, sqsize_cap};

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

    fn peer(raw: &Self::Raw) -> String {
        raw.1.to_string()
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
