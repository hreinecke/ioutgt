//! Target assembly: spawns the control thread, the admin queue thread,
//! and the IO queue threads, and wires connection handoff between them.
//!
//! Exposed as a library so integration tests can start a full target
//! in-process on an ephemeral port.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::os::fd::OwnedFd;
use std::rc::{Rc, Weak};
use std::sync::{Arc, mpsc};

use ioutgt_backend::AnyBackend;
use ioutgt_control::config::{BackendConfig, FileConfig, NamespaceConfig, SubsystemConfig};
use ioutgt_control::server::{CtlState, build_backend};
use ioutgt_core::controller::Registry;
use ioutgt_core::dispatch::ConnCtx;
use ioutgt_core::subsystem::{Namespace, PortConfig, Subsystem};
use ioutgt_tcp::connection::{ConnPermit, QueueConn, run_queue};
use ioutgt_tcp::handshake::{accept_handshake, read_connect};
use ioutgt_uring::mailbox::{Mailbox, MailboxSender, mailbox};
use ioutgt_uring::{QueueRuntime, RingConfig};
use tracing::{info, warn};

/// Target configuration. Built from CLI flags, a JSON file
/// ([`TargetConfig::from_file`]), or [`TargetConfig::single_memory`] in
/// tests.
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
    /// Unix socket path for the runtime control API.
    pub control_socket: Option<std::path::PathBuf>,
    /// Subsystems served on this port.
    pub subsystems: Vec<SubsystemConfig>,
}

impl TargetConfig {
    /// One subsystem, one memory namespace — the test/bring-up shape.
    pub fn single_memory(nqn: &str, size_mb: u64) -> TargetConfig {
        TargetConfig {
            listen: "0.0.0.0:4420".parse().expect("static addr"),
            io_threads: 2,
            allow_hdgst: true,
            allow_ddgst: true,
            pin_threads: false,
            control_socket: None,
            subsystems: vec![SubsystemConfig {
                nqn: nqn.into(),
                serial: "IOUTGT0001".into(),
                model: "ioutgt".into(),
                allow_any_host: true,
                namespaces: vec![NamespaceConfig {
                    nsid: 1,
                    backend: BackendConfig::Memory { size_mb },
                }],
            }],
        }
    }

    /// Load and validate a JSON config file.
    pub fn from_file(path: &std::path::Path) -> io::Result<TargetConfig> {
        let file = FileConfig::load(path).map_err(io::Error::other)?;
        Ok(TargetConfig {
            listen: file.listen.parse().expect("validated"),
            io_threads: file.io_threads,
            allow_hdgst: file.header_digest,
            allow_ddgst: file.data_digest,
            pin_threads: file.pin_threads,
            control_socket: file.control_socket,
            subsystems: file.subsystems,
        })
    }
}

/// Connect CATTR bit 2: host requests SQ flow control disabled.
const CONNECT_DISABLE_SQFLOW: u8 = 1 << 2;

/// Maximum concurrent connections accepted. Bounds total preallocated
/// queue memory; a host that exceeds it is rejected at accept. (Deeper
/// mitigation — lazy slot-buffer allocation — is in the roadmap.)
const MAX_CONNECTIONS: usize = 256;

type Conn = QueueConn<AnyBackend>;

/// Messages to the admin queue thread.
enum AdminMsg {
    Conn(Conn),
    /// A namespace changed: nudge every live controller's AERs.
    NsChanged,
}

/// IO queue threads receive connections only.
fn spawn_io_thread(name: String, core_id: Option<usize>) -> io::Result<MailboxSender<Conn>> {
    let (tx, mut rx): (MailboxSender<Conn>, Mailbox<Conn>) = mailbox()?;
    spawn_pinned(name.clone(), core_id, move || {
        run_queue_thread(name, move |spawner| async move {
            loop {
                match rx.recv().await {
                    Ok(conn) => {
                        spawner(conn);
                    }
                    Err(err) => {
                        warn!("io mailbox failed: {err}");
                        return;
                    }
                }
            }
        })
    })?;
    Ok(tx)
}

/// The admin thread additionally tracks live controllers for AER nudges.
fn spawn_admin_thread(name: String) -> io::Result<MailboxSender<AdminMsg>> {
    let (tx, mut rx): (MailboxSender<AdminMsg>, Mailbox<AdminMsg>) = mailbox()?;
    spawn_pinned(name.clone(), None, move || {
        let rt = match QueueRuntime::new(RingConfig::default()) {
            Ok(rt) => rt,
            Err(err) => {
                warn!(thread = %name, "queue runtime failed: {err}");
                return;
            }
        };
        rt.block_on(async move {
            let live: Rc<RefCell<Vec<Weak<ConnCtx<AnyBackend>>>>> =
                Rc::new(RefCell::new(Vec::new()));
            loop {
                match rx.recv().await {
                    Ok(AdminMsg::Conn(conn)) => {
                        live.borrow_mut().retain(|weak| weak.strong_count() > 0);
                        let live = Rc::clone(&live);
                        tokio::task::spawn_local(async move {
                            run_queue(conn, |ctx| {
                                live.borrow_mut().push(Rc::downgrade(ctx));
                            })
                            .await;
                        });
                    }
                    Ok(AdminMsg::NsChanged) => {
                        live.borrow_mut().retain(|weak| {
                            weak.upgrade().is_some_and(|ctx| {
                                ctx.fire_ns_changed();
                                true
                            })
                        });
                    }
                    Err(err) => {
                        warn!("admin mailbox failed: {err}");
                        return;
                    }
                }
            }
        });
    })?;
    Ok(tx)
}

fn spawn_pinned(
    name: String,
    core_id: Option<usize>,
    body: impl FnOnce() + Send + 'static,
) -> io::Result<()> {
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
            body();
        })?;
    Ok(())
}

/// Run a queue-thread runtime whose main future receives a spawner for
/// connections.
fn run_queue_thread<F, Fut>(name: String, main: F)
where
    F: FnOnce(fn(Conn)) -> Fut,
    Fut: Future<Output = ()>,
{
    fn spawner(conn: Conn) {
        tokio::task::spawn_local(run_queue(conn, |_| {}));
    }
    let rt = match QueueRuntime::new(RingConfig::default()) {
        Ok(rt) => rt,
        Err(err) => {
            warn!(thread = %name, "queue runtime failed: {err}");
            return;
        }
    };
    rt.block_on(main(spawner));
}

/// Build the port snapshot from the configured subsystems.
fn build_port(config: &TargetConfig) -> io::Result<Arc<PortConfig<AnyBackend>>> {
    let mut subsystems = BTreeMap::new();
    for spec in &config.subsystems {
        let mut namespaces = BTreeMap::new();
        for ns in &spec.namespaces {
            let backend = build_backend(&ns.backend).map_err(io::Error::other)?;
            let mut uuid = [0u8; 16];
            uuid[..4].copy_from_slice(&ns.nsid.to_be_bytes());
            uuid[8] = 0x80;
            namespaces.insert(
                ns.nsid,
                Arc::new(Namespace {
                    nsid: ns.nsid,
                    backend: Arc::new(backend),
                    uuid,
                }),
            );
        }
        let subsystem = Arc::new(Subsystem::new(
            spec.nqn.clone(),
            spec.serial.clone(),
            spec.model.clone(),
            u16::try_from(config.io_threads.max(1)).unwrap_or(1),
            spec.allow_any_host,
            namespaces,
        ));
        subsystems.insert(spec.nqn.clone(), subsystem);
    }
    Ok(Arc::new(PortConfig {
        traddr: config.listen.ip().to_string(),
        trsvcid: config.listen.port().to_string(),
        subsystems,
    }))
}

/// Start every thread of a target; returns the bound address (for
/// ephemeral-port tests). Runs until the process exits.
pub fn spawn_target(config: TargetConfig) -> io::Result<SocketAddr> {
    let admin_tx = spawn_admin_thread("ioutgt-admin".into())?;
    let io_txs: Vec<MailboxSender<Conn>> = (0..config.io_threads)
        .map(|i| spawn_io_thread(format!("ioutgt-io{i}"), config.pin_threads.then_some(i + 1)))
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
    admin_tx: MailboxSender<AdminMsg>,
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

    // Runtime control API.
    if let Some(path) = &config.control_socket {
        let _ = std::fs::remove_file(path);
        match tokio::net::UnixListener::bind(path) {
            Ok(listener) => {
                // The API mutates served storage (ADD/REMOVE_NAMESPACE):
                // owner-only. Prefer a private dir (the CLI defaults to
                // $XDG_RUNTIME_DIR) over world-writable /tmp, where a
                // pre-bound squatter could still intercept first.
                use std::os::unix::fs::PermissionsExt;
                if let Err(err) =
                    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
                {
                    let _ = addr_tx.send(Err(err));
                    return;
                }
                let nudge_tx = admin_tx.clone();
                let state = Arc::new(CtlState {
                    port: Arc::clone(&port),
                    registry: Arc::clone(&registry),
                    notify_ns_changed: Box::new(move || nudge_tx.send(AdminMsg::NsChanged)),
                });
                info!(path = %path.display(), "control socket listening");
                tokio::task::spawn_local(ioutgt_control::server::serve(listener, state));
            }
            Err(err) => {
                let _ = addr_tx.send(Err(err));
                return;
            }
        }
    }

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

    // Bounds total preallocated queue memory across all queue threads.
    let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                warn!("accept failed: {err}");
                continue;
            }
        };
        let count = active.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        if count > MAX_CONNECTIONS {
            active.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            warn!(%peer, "connection limit {MAX_CONNECTIONS} reached; rejecting");
            continue; // stream drops here, closing the connection
        }
        let permit = ConnPermit::new(Arc::clone(&active));
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
                permit,
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
    admin_tx: &MailboxSender<AdminMsg>,
    io_txs: &[MailboxSender<Conn>],
    port: Arc<PortConfig<AnyBackend>>,
    registry: Arc<Registry>,
    permit: ConnPermit,
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
    // Enforce the advertised queue-size limit (CAP.MQES + 1): each slot
    // preallocates a data buffer, so an oversized queue is a memory
    // amplification vector a hostile host could exploit by ignoring MQES.
    if !(2..=u32::from(ioutgt_core::MAX_QUEUE_ENTRIES)).contains(&entries) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "sqsize out of range",
        ));
    }
    let sqhd_disabled = connect.cattr & CONNECT_DISABLE_SQFLOW != 0;

    let std_stream = stream.into_std()?;
    let conn = Conn {
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
        permit,
    };
    if qid == 0 {
        admin_tx.send(AdminMsg::Conn(conn));
    } else if io_txs.is_empty() {
        return Err(io::Error::other("no IO threads"));
    } else {
        io_txs[(usize::from(qid) - 1) % io_txs.len()].send(conn);
    }
    Ok(())
}
