//! Target assembly: spawns the control thread, the admin queue thread,
//! and the IO queue threads, and wires connection handoff between them.
//!
//! Exposed as a library so integration tests can start a full target
//! in-process on an ephemeral port.

use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::os::fd::OwnedFd;
use std::sync::{Arc, mpsc};

use ioutgt_backend::{AnyBackend, FileBackend, MemoryBackend, NullBackend};
use ioutgt_core::controller::Registry;
use ioutgt_core::subsystem::{Namespace, PortConfig, Subsystem};
use ioutgt_tcp::connection::{QueueConn, run_queue};
use ioutgt_tcp::handshake::{accept_handshake, read_connect};
use ioutgt_uring::mailbox::{Mailbox, MailboxSender, mailbox};
use ioutgt_uring::{QueueRuntime, RingConfig};
use tracing::{info, warn};

/// Target configuration (grows a JSON schema in the control-plane
/// milestone).
#[derive(Debug, Clone)]
#[allow(missing_docs)]
pub struct TargetConfig {
    pub listen: SocketAddr,
    /// Number of IO queue threads (in addition to the admin thread).
    pub io_threads: usize,
    pub allow_hdgst: bool,
    pub allow_ddgst: bool,
    /// Pin queue threads to cores (sequential map; disable in tests).
    pub pin_threads: bool,
    /// NVM subsystem NQN served on this port.
    pub subsys_nqn: String,
    /// Memory/null namespace size in MiB.
    pub mem_size_mb: u64,
    /// Namespace backend.
    pub backend: BackendSpec,
}

/// Which backend serves namespace 1 (the JSON config schema replaces
/// this in the control-plane milestone).
#[derive(Debug, Clone, Default)]
pub enum BackendSpec {
    /// RAM-backed (tests, bring-up).
    #[default]
    Memory,
    /// Discard writes, zero reads (protocol-overhead measurement).
    Null,
    /// O_DIRECT file or block device at this path.
    File(std::path::PathBuf),
}

impl Default for TargetConfig {
    fn default() -> Self {
        TargetConfig {
            listen: "0.0.0.0:4420".parse().expect("static addr"),
            io_threads: 2,
            allow_hdgst: true,
            allow_ddgst: true,
            pin_threads: false,
            subsys_nqn: "nqn.2026-06.io.ioutgt:test".into(),
            mem_size_mb: 64,
            backend: BackendSpec::Memory,
        }
    }
}

/// Connect CATTR bit 2: host requests SQ flow control disabled.
const CONNECT_DISABLE_SQFLOW: u8 = 1 << 2;

type Conn = QueueConn<AnyBackend>;

fn spawn_queue_thread(name: String, core_id: Option<usize>) -> io::Result<MailboxSender<Conn>> {
    let (tx, mut rx): (MailboxSender<Conn>, Mailbox<Conn>) = mailbox()?;
    std::thread::Builder::new()
        .name(name.clone())
        .spawn(move || {
            if let Some(core) = core_id {
                let pinned = core_affinity::get_core_ids()
                    .and_then(|ids| ids.into_iter().find(|c| c.id == core))
                    .map(core_affinity::set_for_current)
                    .unwrap_or(false);
                if !pinned {
                    warn!(thread = %name, core, "could not pin thread");
                }
            }
            let rt = match QueueRuntime::new(RingConfig::default()) {
                Ok(rt) => rt,
                Err(err) => {
                    warn!(thread = %name, "queue runtime failed: {err}");
                    return;
                }
            };
            rt.block_on(async move {
                loop {
                    match rx.recv().await {
                        Ok(conn) => {
                            tokio::task::spawn_local(run_queue(conn));
                        }
                        Err(err) => {
                            warn!(thread = %name, "mailbox failed: {err}");
                            return;
                        }
                    }
                }
            });
        })?;
    Ok(tx)
}

/// Build the port snapshot: one NVM subsystem, one namespace.
fn build_port(config: &TargetConfig) -> io::Result<Arc<PortConfig<AnyBackend>>> {
    // 512B blocks: the most interop-tested format.
    let block_shift = 9;
    let backend = Arc::new(match &config.backend {
        BackendSpec::Memory => {
            AnyBackend::Memory(MemoryBackend::new(config.mem_size_mb << 20, block_shift))
        }
        BackendSpec::Null => {
            AnyBackend::Null(NullBackend::new(config.mem_size_mb << 20, block_shift))
        }
        BackendSpec::File(path) => {
            let file = FileBackend::open(path, block_shift)?;
            if !file.is_direct() {
                warn!(?path, "O_DIRECT unavailable; using buffered IO");
            }
            AnyBackend::File(file)
        }
    });
    let mut uuid = [0u8; 16];
    uuid[..8].copy_from_slice(&0x696F_7574_6774_0001u64.to_be_bytes()); // "ioutgt"
    uuid[8] = 0x80;
    let namespace = Arc::new(Namespace {
        nsid: 1,
        backend,
        uuid,
    });
    let subsystem = Arc::new(Subsystem {
        nqn: config.subsys_nqn.clone(),
        serial: "IOUTGT0001".into(),
        model: "ioutgt".into(),
        namespaces: BTreeMap::from([(1u32, namespace)]),
        max_qid: u16::try_from(config.io_threads.max(1)).unwrap_or(1),
        allow_any_host: true,
    });
    Ok(Arc::new(PortConfig {
        traddr: config.listen.ip().to_string(),
        trsvcid: config.listen.port().to_string(),
        subsystems: BTreeMap::from([(config.subsys_nqn.clone(), subsystem)]),
    }))
}

/// Start every thread of a target; returns the bound address (for
/// ephemeral-port tests). Runs until the process exits.
pub fn spawn_target(config: TargetConfig) -> io::Result<SocketAddr> {
    let admin_tx = spawn_queue_thread("ioutgt-admin".into(), None)?;
    let io_txs: Vec<MailboxSender<Conn>> = (0..config.io_threads)
        .map(|i| spawn_queue_thread(format!("ioutgt-io{i}"), config.pin_threads.then_some(i + 1)))
        .collect::<io::Result<_>>()?;

    // The control thread reports the bound address back synchronously.
    let (addr_tx, addr_rx) = mpsc::channel::<io::Result<SocketAddr>>();
    std::thread::Builder::new()
        .name("ioutgt-control".into())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(err) => {
                    let _ = addr_tx.send(Err(err));
                    return;
                }
            };
            let local = tokio::task::LocalSet::new();
            rt.block_on(local.run_until(control_loop(config, admin_tx, io_txs, addr_tx)));
        })?;
    addr_rx
        .recv()
        .map_err(|_| io::Error::other("control thread died during bind"))?
}

async fn control_loop(
    config: TargetConfig,
    admin_tx: MailboxSender<Conn>,
    io_txs: Vec<MailboxSender<Conn>>,
    addr_tx: mpsc::Sender<io::Result<SocketAddr>>,
) {
    let registry = Registry::new();
    let port = match build_port(&config) {
        Ok(port) => port,
        Err(err) => {
            let _ = addr_tx.send(Err(err));
            return;
        }
    };
    let listener = match tokio::net::TcpListener::bind(config.listen).await {
        Ok(listener) => listener,
        Err(err) => {
            let _ = addr_tx.send(Err(err));
            return;
        }
    };
    let local = listener
        .local_addr()
        .expect("bound listener has an address");
    let _ = addr_tx.send(Ok(local));
    info!(%local, "ioutgt listening");

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                warn!("accept failed: {err}");
                continue;
            }
        };
        let admin_tx = admin_tx.clone();
        let io_txs = io_txs.clone();
        let allow_hdgst = config.allow_hdgst;
        let allow_ddgst = config.allow_ddgst;
        let registry = Arc::clone(&registry);
        let port = Arc::clone(&port);
        tokio::task::spawn_local(async move {
            if let Err(err) = setup_connection(
                stream,
                allow_hdgst,
                allow_ddgst,
                &admin_tx,
                &io_txs,
                port,
                registry,
            )
            .await
            {
                warn!(%peer, "connection setup failed: {err}");
            }
        });
    }
}

/// ICReq/ICResp + first Connect capsule, then hand the socket to the
/// queue thread selected by qid.
#[allow(clippy::too_many_arguments)]
async fn setup_connection(
    mut stream: tokio::net::TcpStream,
    allow_hdgst: bool,
    allow_ddgst: bool,
    admin_tx: &MailboxSender<Conn>,
    io_txs: &[MailboxSender<Conn>],
    port: Arc<PortConfig<AnyBackend>>,
    registry: Arc<Registry>,
) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let negotiated = accept_handshake(
        &mut stream,
        allow_hdgst,
        allow_ddgst,
        ioutgt_tcp::MAX_H2C_DATA,
    )
    .await?;
    let first = read_connect(&mut stream, negotiated).await?;
    let connect = first.connect();
    let qid = connect.qid.get();
    let entries = connect.sqsize.get() as u32 + 1;
    if !(2..=1024).contains(&entries) {
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
        connect_sqe: first.sqe,
        connect_data: first.data,
        port,
        registry,
    };
    if qid == 0 {
        admin_tx.send(conn);
    } else if io_txs.is_empty() {
        return Err(io::Error::other("no IO threads"));
    } else {
        io_txs[(usize::from(qid) - 1) % io_txs.len()].send(conn);
    }
    Ok(())
}
