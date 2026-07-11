//! The harness [`Transport`] implementation for NVMe/RDMA.
//!
//! The harness control loop is a plain-tokio thread (it drives a `UnixListener`
//! control socket and, for TCP, a `TcpListener`). Our CM event channel instead
//! parks on its fd via io_uring `POLL_ADD`, which needs an io_uring reactor — so
//! the CM listener runs on its **own** reactor thread ([`cm_thread_main`]) and
//! bridges each accepted connection to the control thread over a tokio channel.
//! `Transport::accept` simply drains that channel.
//!
//! Everything reactor-bound (QP build, `rdma_accept`, the queue's completion
//! reaping) stays in [`run_conn`], invoked from `run_queue` on a queue thread.
//! The cm_id crosses CM-thread → control-thread → queue-thread as `Send` data
//! ([`RdmaRaw`]/[`RdmaConn`]); `cm::Identifier` is `Send + Sync` (librdmacm
//! cm_id operations are thread-safe).

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use ioutgt_backend::AnyBackend;
use ioutgt_core::permit::ConnPermit;
use ioutgt_core::subsystem::{PortConfig, TransportType};
use ioutgt_harness::{OnCtx, TargetConfig, Transport};
use ioutgt_nvme::controller::Registry;
use ioutgt_uring::{QueueRuntime, RingConfig};
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::listener::{RdmaListener, RdmaRaw};
use crate::target::{RdmaConn, run_conn};

/// Accepted-but-not-yet-handshaked connections buffered between the CM thread
/// and the control thread (bounded so a stalled control thread applies
/// backpressure to the CM accept loop rather than growing unboundedly).
const ACCEPT_BACKLOG: usize = 256;

/// The NVMe/RDMA transport marker plugged into the harness pool.
pub struct RdmaTransport;

/// The bound listener as the harness holds it: the receiving end of the bridge
/// from the dedicated CM reactor thread. `recv` needs `&mut`, so the receiver
/// lives behind an async `Mutex`; `accept` is called sequentially on the control
/// thread, so the lock never actually contends.
pub struct RdmaListenerHandle {
    rx: Mutex<mpsc::Receiver<RdmaRaw>>,
    /// The CM thread runs for the process lifetime; its handle is detached.
    _cm_thread: std::thread::JoinHandle<()>,
}

impl Transport for RdmaTransport {
    type Conn = RdmaConn;
    type Raw = RdmaRaw;
    type Listener = RdmaListenerHandle;

    fn trtype() -> TransportType {
        TransportType::Rdma
    }

    fn peer(raw: &RdmaRaw) -> String {
        format!("rdma:qid{}", raw.qid)
    }

    async fn bind(cfg: &TargetConfig) -> io::Result<(RdmaListenerHandle, SocketAddr)> {
        let listen = cfg.listen;
        let (accept_tx, accept_rx) = mpsc::channel::<RdmaRaw>(ACCEPT_BACKLOG);
        let (ready_tx, ready_rx) = oneshot::channel::<io::Result<()>>();
        let cm_thread = std::thread::Builder::new()
            .name("ioutgt-rdma-cm".into())
            .spawn(move || cm_thread_main(listen, accept_tx, ready_tx))
            .map_err(|e| io::Error::other(format!("spawn CM thread: {e}")))?;
        // Block bind until the CM thread reports its bind result (rxe GID retry
        // lives in RdmaListener::bind), so the harness sees a ready listener.
        ready_rx
            .await
            .map_err(|_| io::Error::other("RDMA CM thread exited during bind"))??;
        Ok((
            RdmaListenerHandle {
                rx: Mutex::new(accept_rx),
                _cm_thread: cm_thread,
            },
            listen,
        ))
    }

    async fn accept(listener: &RdmaListenerHandle) -> io::Result<RdmaRaw> {
        // Cancel-safe: dropping this future (select!) drops the lock guard + the
        // recv future; mpsc recv loses no buffered RdmaRaw. The guard is a
        // statement temporary, so the lock is released before the match below.
        let received = listener.rx.lock().await.recv().await;
        match received {
            Some(raw) => Ok(raw),
            // The CM thread exited *after* a successful bind (a bind-time failure
            // already surfaced via the oneshot). Returning Err here would hot-spin
            // the harness control loop — its accept arm has no backoff and would
            // re-call accept into an instant Err forever. Park instead: the
            // listener is dead (no new connections), but existing connections and
            // the idle-teardown timer keep working and no core is burned.
            None => {
                tracing::error!(
                    "nvme-rdma CM listener thread exited; accepting no more connections"
                );
                std::future::pending().await
            }
        }
    }

    async fn handshake(
        raw: RdmaRaw,
        _cfg: Arc<TargetConfig>,
        port: Arc<PortConfig<AnyBackend>>,
        registry: Arc<Registry>,
        permit: ConnPermit,
    ) -> io::Result<(u16, RdmaConn)> {
        // The fabrics Connect itself is consumed later, in run_conn → bootstrap
        // (it arrives over the QP after rdma_accept), so the handshake here is
        // just packaging the routing token. qid routes to a queue thread.
        let qid = raw.qid;
        Ok((
            qid,
            RdmaConn {
                id: raw.id,
                qid: raw.qid,
                hsqsize: raw.hsqsize,
                port,
                registry,
                permit,
                stop: raw.stop,
            },
        ))
    }

    async fn run_queue(conn: RdmaConn, on_ctx: OnCtx) {
        if let Err(e) = run_conn(conn, on_ctx).await {
            tracing::warn!("nvme-rdma queue ended: {e}");
        }
    }
}

/// The dedicated CM reactor thread: build an io_uring runtime, bind the CM
/// listener (reporting the bind result on `ready_tx`), then forward each accepted
/// connection to the control thread until the channel closes or the listener
/// errors.
fn cm_thread_main(
    listen: SocketAddr,
    accept_tx: mpsc::Sender<RdmaRaw>,
    ready_tx: oneshot::Sender<io::Result<()>>,
) {
    let rt = match QueueRuntime::new(RingConfig::default()) {
        Ok(rt) => rt,
        Err(e) => {
            let _ = ready_tx.send(Err(e));
            return;
        }
    };
    rt.block_on(async move {
        let mut listener = match RdmaListener::bind(listen).await {
            Ok(l) => {
                let _ = ready_tx.send(Ok(()));
                l
            }
            Err(e) => {
                let _ = ready_tx.send(Err(e));
                return;
            }
        };
        loop {
            match listener.accept().await {
                Ok(raw) => {
                    if accept_tx.send(raw).await.is_err() {
                        break; // control thread / pool gone
                    }
                }
                Err(e) => {
                    tracing::warn!("nvme-rdma CM listener ended: {e}");
                    break;
                }
            }
        }
    });
}
