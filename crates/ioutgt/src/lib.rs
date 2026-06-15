//! Target assembly: spawns the control thread and wires connection
//! handoff into the queue-thread pool (admin thread + N IO threads),
//! which is itself spawned lazily on the first accepted connection.
//!
//! Exposed as a library so integration tests can start a full target
//! in-process on an ephemeral port.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::os::fd::OwnedFd;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};

use ioutgt_backend::AnyBackend;
use ioutgt_control::config::{BackendConfig, FileConfig, NamespaceConfig, SubsystemConfig};
use ioutgt_control::server::{CtlState, build_backend};
use ioutgt_core::controller::Registry;
use ioutgt_core::dispatch::ConnCtx;
use ioutgt_core::queue::{QueueStats, QueueStatsSnapshot};
use ioutgt_core::subsystem::{Namespace, PortConfig, Subsystem, TransportType};
use ioutgt_cpus::{CpuTopology, group_cpus_evenly};
use ioutgt_nvme_tcp::connection::{ConnPermit, QueueConn, run_queue};
use ioutgt_nvme_tcp::handshake::{accept_handshake, read_connect};
use ioutgt_uring::mailbox::{Mailbox, MailboxSender, mailbox};
use ioutgt_uring::{QueueRuntime, RingConfig};
use tracing::{error, info, warn};

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
    /// Pin each IO queue thread to one CPU of its `group_cpus_evenly`
    /// group (disable in tests).
    pub pin_threads: bool,
    /// Zero-copy sends (SENDMSG_ZC) with notification-gated buffer
    /// reuse.
    pub send_zc: bool,
    /// Advertised IO MAXCMD ceiling (entries): the maximum IO queue
    /// depth the host may use. The admin queue is unaffected.
    pub io_queue_size: u16,
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
            send_zc: false,
            io_queue_size: 128,
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
            send_zc: file.send_zc,
            io_queue_size: file.io_queue_size,
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

/// A queue thread whose mailbox (and sender) already exist but whose OS
/// thread, io_uring ring, and runtime are not yet created. Calling it
/// spawns the thread; the pool is deferred until the first client
/// connects (see [`control_loop`]).
type PendingThread = Box<dyn FnOnce() -> io::Result<()> + Send>;

/// Reply channel for a stats request: the queue thread builds its JSON
/// on-thread (control-plane rate) and sends it back.
type StatsRequest = tokio::sync::oneshot::Sender<serde_json::Value>;

/// Messages to an IO queue thread.
enum IoMsg {
    Conn(Conn),
    Stats { reply: StatsRequest, clear: bool },
}

/// Messages to the admin queue thread.
enum AdminMsg {
    Conn(Conn),
    /// A namespace changed: nudge every live controller's AERs.
    NsChanged,
    Stats {
        reply: StatsRequest,
        clear: bool,
    },
}

/// Zero everything a queue thread counts: every live queue's counters,
/// the retired accumulator, and the thread's ring counters. Runs on the
/// owning thread (the only place the `Cell`s may be written).
fn clear_thread_stats(queues: &[Rc<QueueStats>], retired: &mut QueueStatsSnapshot) {
    for stats in queues {
        stats.reset();
    }
    *retired = QueueStatsSnapshot::default();
    let _ = ioutgt_uring::reset_reactor_stats();
}

/// Fold queues whose connection is gone (this list holds the only
/// remaining ref) into the retired accumulator, so lifetime totals stay
/// monotonic across reconnects. Called on every connection handoff and
/// stats request — each list entry was added by a handoff that pruned
/// first, which bounds the list under churn even if stats are never
/// queried.
fn prune_dead_queues(queues: &RefCell<Vec<Rc<QueueStats>>>, retired: &mut QueueStatsSnapshot) {
    queues.borrow_mut().retain(|stats| {
        if Rc::strong_count(stats) > 1 {
            return true;
        }
        retired.absorb(&stats.snapshot());
        false
    });
}

/// One queue thread's stats reply, built on the owning thread (the only
/// place its `Cell` counters may be read).
fn thread_stats_json(
    name: &str,
    queues: &[Rc<QueueStats>],
    retired: &QueueStatsSnapshot,
) -> serde_json::Value {
    fn counters_json(s: &QueueStatsSnapshot) -> serde_json::Value {
        serde_json::json!({
            "read_cmds": s.read_cmds, "write_cmds": s.write_cmds,
            "flush_cmds": s.flush_cmds, "other_cmds": s.other_cmds,
            "read_bytes": s.read_bytes, "write_bytes": s.write_bytes,
            "errors": s.errors,
        })
    }
    let ring = ioutgt_uring::reactor_stats().unwrap_or_default();
    let queues: Vec<_> = queues
        .iter()
        .map(|stats| {
            let snap = stats.snapshot();
            let mut value = counters_json(&snap);
            value["qid"] = snap.qid.into();
            value["cntlid"] = snap.cntlid.into();
            value
        })
        .collect();
    serde_json::json!({
        "name": name,
        "tid": ioutgt_core::controller::current_tid(),
        "ring": { "enters": ring.enters, "parks": ring.parks,
                  "sqes": ring.sqes, "cqes": ring.cqes },
        "queues": queues,
        "retired": counters_json(retired),
    })
}

/// Create an IO queue thread's mailbox and return its sender plus a
/// deferred spawn closure (the ring/runtime/OS thread are built only when
/// the closure runs). IO queue threads receive connections and stats
/// requests.
fn make_io_thread(
    name: String,
    core_id: Option<usize>,
) -> io::Result<(MailboxSender<IoMsg>, PendingThread)> {
    let (tx, mut rx): (MailboxSender<IoMsg>, Mailbox<IoMsg>) = mailbox()?;
    let spawn: PendingThread = Box::new(move || {
        spawn_pinned(name.clone(), core_id, move || {
            let rt = match QueueRuntime::new(RingConfig::default()) {
                Ok(rt) => rt,
                Err(err) => {
                    warn!(thread = %name, "queue runtime failed: {err}");
                    return;
                }
            };
            rt.block_on(async move {
                let queues: Rc<RefCell<Vec<Rc<QueueStats>>>> = Rc::new(RefCell::new(Vec::new()));
                let mut retired = QueueStatsSnapshot::default();
                loop {
                    match rx.recv().await {
                        Ok(IoMsg::Conn(conn)) => {
                            prune_dead_queues(&queues, &mut retired);
                            let queues = Rc::clone(&queues);
                            tokio::task::spawn_local(async move {
                                run_queue(conn, |ctx| {
                                    queues.borrow_mut().push(Rc::clone(&ctx.queue.stats));
                                })
                                .await;
                            });
                        }
                        Ok(IoMsg::Stats { reply, clear }) => {
                            prune_dead_queues(&queues, &mut retired);
                            let queues = queues.borrow();
                            let _ = reply.send(thread_stats_json(&name, &queues, &retired));
                            if clear {
                                clear_thread_stats(&queues, &mut retired);
                            }
                        }
                        Err(err) => {
                            warn!("io mailbox failed: {err}");
                            return;
                        }
                    }
                }
            });
        })
    });
    Ok((tx, spawn))
}

/// Create the admin queue thread's mailbox and return its sender plus a
/// deferred spawn closure. The admin thread additionally tracks live
/// controllers for AER nudges.
fn make_admin_thread(name: String) -> io::Result<(MailboxSender<AdminMsg>, PendingThread)> {
    let (tx, mut rx): (MailboxSender<AdminMsg>, Mailbox<AdminMsg>) = mailbox()?;
    let spawn: PendingThread = Box::new(move || {
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
                let queues: Rc<RefCell<Vec<Rc<QueueStats>>>> = Rc::new(RefCell::new(Vec::new()));
                let mut retired = QueueStatsSnapshot::default();
                loop {
                    match rx.recv().await {
                        Ok(AdminMsg::Conn(conn)) => {
                            live.borrow_mut().retain(|weak| weak.strong_count() > 0);
                            prune_dead_queues(&queues, &mut retired);
                            let live = Rc::clone(&live);
                            let queues = Rc::clone(&queues);
                            tokio::task::spawn_local(async move {
                                run_queue(conn, |ctx| {
                                    live.borrow_mut().push(Rc::downgrade(ctx));
                                    queues.borrow_mut().push(Rc::clone(&ctx.queue.stats));
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
                        Ok(AdminMsg::Stats { reply, clear }) => {
                            prune_dead_queues(&queues, &mut retired);
                            let queues = queues.borrow();
                            let _ = reply.send(thread_stats_json(&name, &queues, &retired));
                            if clear {
                                clear_thread_stats(&queues, &mut retired);
                            }
                        }
                        Err(err) => {
                            warn!("admin mailbox failed: {err}");
                            return;
                        }
                    }
                }
            });
        })
    });
    Ok((tx, spawn))
}

/// Pick the CPU each IO queue thread is pinned to: group all CPUs
/// evenly per NUMA/cluster/SMT locality (the kernel `group_cpus_evenly`
/// spread, i.e. what nvme-tcp queues see on the host side), one group
/// per IO thread, then select the group's first online CPU.
fn io_thread_cpus(io_threads: usize) -> Vec<Option<usize>> {
    let topo = match CpuTopology::from_sysfs() {
        Ok(topo) => topo,
        Err(err) => {
            warn!("cpu topology unavailable, io threads not pinned: {err}");
            return vec![None; io_threads];
        }
    };
    let groups = group_cpus_evenly(io_threads, &topo);
    (0..io_threads)
        .map(|i| {
            // groups can run out when io_threads > possible CPUs; a
            // group of only-offline CPUs yields no pinnable CPU.
            let group = groups.get(i);
            let cpu = group.and_then(|g| g.and(&topo.online).first());
            match (cpu, group) {
                (Some(cpu), Some(group)) => {
                    info!(thread = i, cpus = %group, cpu, "io queue affinity");
                }
                (None, Some(group)) => {
                    warn!(thread = i, cpus = %group, "no online cpu in group, thread not pinned");
                }
                (_, None) => {
                    warn!(
                        thread = i,
                        "more io threads than possible cpus, thread not pinned"
                    );
                }
            }
            cpu
        })
        .collect()
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

/// Build the port snapshot from the configured subsystems.
/// `bound` is the listener's actual local address, so ephemeral ports
/// (`--listen …:0`) report the real port in discovery log entries and
/// LIST_CONTROLLER, not the configured 0.
fn build_port(config: &TargetConfig, bound: SocketAddr) -> io::Result<Arc<PortConfig<AnyBackend>>> {
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
        traddr: bound.ip().to_string(),
        trsvcid: bound.port().to_string(),
        trtype: TransportType::Tcp,
        io_queue_size: config.io_queue_size,
        subsystems,
    }))
}

/// Start every thread of a target; returns the bound address (for
/// ephemeral-port tests). Runs until the process exits.
pub fn spawn_target(config: TargetConfig) -> io::Result<SocketAddr> {
    if config.send_zc {
        // Opt-in experiment: refuse to start rather than silently
        // fall back to the copying path.
        let features = ioutgt_uring::probe()?;
        if !features.sendmsg_zc {
            return Err(io::Error::other(
                "--send-zc requested but the kernel lacks IORING_OP_SENDMSG_ZC (need >= 6.1)",
            ));
        }
    }
    // Build each queue thread's mailbox (and sender) now, but defer the
    // OS thread / io_uring ring / runtime until the first client connects
    // (spawned by `control_loop`). The senders are live immediately, so
    // control-socket wiring and qid routing are unchanged.
    let (admin_tx, admin_pending) = make_admin_thread("ioutgt-admin".into())?;
    let io_cpus = if config.pin_threads {
        io_thread_cpus(config.io_threads)
    } else {
        vec![None; config.io_threads]
    };
    let mut io_txs: Vec<MailboxSender<IoMsg>> = Vec::with_capacity(config.io_threads);
    let mut pending: Vec<PendingThread> = Vec::with_capacity(config.io_threads + 1);
    pending.push(admin_pending);
    for (i, core_id) in io_cpus.into_iter().enumerate() {
        let (tx, io_pending) = make_io_thread(format!("ioutgt-io{i}"), core_id)?;
        io_txs.push(tx);
        pending.push(io_pending);
    }

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
            rt.block_on(local.run_until(control_loop(config, admin_tx, io_txs, pending, addr_tx)));
        })?;
    addr_rx
        .recv()
        .map_err(|_| io::Error::other("control thread died during bind"))?
}

async fn control_loop(
    config: TargetConfig,
    admin_tx: MailboxSender<AdminMsg>,
    io_txs: Vec<MailboxSender<IoMsg>>,
    pending: Vec<PendingThread>,
    addr_tx: mpsc::Sender<io::Result<SocketAddr>>,
) {
    let registry = Registry::new();

    // The queue-thread pool is spawned lazily on the first accepted
    // connection (`pending`, taken below). `pool_started` lets the
    // control-socket stats sources answer a query that arrives before
    // any client has connected without waiting on a thread that does not
    // exist yet — they reply with a zeroed snapshot instead.
    let mut pending = Some(pending);
    let pool_started = Arc::new(AtomicBool::new(false));

    // Bind before building the port so the model carries the actual
    // bound address (ephemeral ports resolve to the real one).
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

    let port = match build_port(&config, local) {
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
                let mut stats_sources: Vec<ioutgt_control::server::StatsSource> =
                    Vec::with_capacity(1 + io_txs.len());
                let stats_admin = admin_tx.clone();
                let admin_started = Arc::clone(&pool_started);
                stats_sources.push(Box::new(move |clear, reply| {
                    if !admin_started.load(Ordering::Relaxed) {
                        let _ = reply.send(thread_stats_json(
                            "ioutgt-admin",
                            &[],
                            &QueueStatsSnapshot::default(),
                        ));
                        return;
                    }
                    stats_admin.send(AdminMsg::Stats { reply, clear });
                }));
                for (i, io_tx) in io_txs.iter().enumerate() {
                    let io_tx = io_tx.clone();
                    let io_started = Arc::clone(&pool_started);
                    let name = format!("ioutgt-io{i}");
                    stats_sources.push(Box::new(move |clear, reply| {
                        if !io_started.load(Ordering::Relaxed) {
                            let _ = reply.send(thread_stats_json(
                                &name,
                                &[],
                                &QueueStatsSnapshot::default(),
                            ));
                            return;
                        }
                        io_tx.send(IoMsg::Stats { reply, clear });
                    }));
                }
                let state = Arc::new(CtlState {
                    port: Arc::clone(&port),
                    registry: Arc::clone(&registry),
                    notify_ns_changed: Box::new(move || nudge_tx.send(AdminMsg::NsChanged)),
                    stats_sources,
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
        // First client to reach the port spawns the whole queue-thread
        // pool (admin + every IO thread). Done before the per-connection
        // task is spawned, so the drainer exists by the time this
        // connection's `Conn` is routed; subsequent accepts skip this.
        if let Some(threads) = pending.take() {
            for spawn in threads {
                if let Err(err) = spawn() {
                    error!("queue thread spawn failed: {err}");
                }
            }
            pool_started.store(true, Ordering::Relaxed);
            info!("queue-thread pool started on first connection");
        }
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
        let send_zc = config.send_zc;
        let registry = Arc::clone(&registry);
        let port = Arc::clone(&port);
        tokio::task::spawn_local(async move {
            if let Err(err) = setup_connection(
                stream,
                allow_hdgst,
                allow_ddgst,
                send_zc,
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

/// Hard ceiling (entries) on a connecting queue's depth. IO queues
/// (qid > 0) are bounded by the configured MAXCMD (`io_queue_size`); the
/// admin queue (qid 0), host-fixed at `NVME_AQ_DEPTH`, keeps the CAP.MQES
/// guard so it is never rejected when `io_queue_size` is set small.
fn sqsize_cap(qid: u16, io_queue_size: u16) -> u16 {
    if qid == 0 {
        ioutgt_core::MAX_QUEUE_ENTRIES
    } else {
        io_queue_size
    }
}

/// ICReq/ICResp + first Connect capsule, then hand the socket to the
/// queue thread selected by qid.
#[allow(clippy::too_many_arguments)]
async fn setup_connection(
    mut stream: tokio::net::TcpStream,
    allow_hdgst: bool,
    allow_ddgst: bool,
    send_zc: bool,
    admin_tx: &MailboxSender<AdminMsg>,
    io_txs: &[MailboxSender<IoMsg>],
    port: Arc<PortConfig<AnyBackend>>,
    registry: Arc<Registry>,
    permit: ConnPermit,
) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let negotiated = accept_handshake(
        &mut stream,
        allow_hdgst,
        allow_ddgst,
        ioutgt_nvme_tcp::MAX_H2C_DATA,
    )
    .await?;
    let first = read_connect(&mut stream, negotiated).await?;
    let connect = first.connect();
    let qid = connect.qid.get();
    let entries = connect.sqsize.get() as u32 + 1;
    // Enforce the advertised queue-size limit: each slot preallocates a
    // data buffer, so an oversized queue is a memory-amplification vector
    // a hostile host could exploit by ignoring the advertised ceiling.
    let cap = sqsize_cap(qid, port.io_queue_size);
    if !(2..=u32::from(cap)).contains(&entries) {
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
        send_zc,
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
        io_txs[(usize::from(qid) - 1) % io_txs.len()].send(IoMsg::Conn(conn));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::sqsize_cap;

    #[test]
    fn sqsize_cap_bounds_io_at_config_admin_at_mqes() {
        // IO queues (qid > 0) are capped at the configured ceiling…
        assert_eq!(sqsize_cap(1, 64), 64);
        assert_eq!(sqsize_cap(3, 200), 200);
        // …while the admin queue keeps the hard CAP.MQES guard, so it is
        // never rejected even when io_queue_size is set below the admin
        // depth (NVME_AQ_DEPTH = 32).
        assert_eq!(sqsize_cap(0, 8), ioutgt_core::MAX_QUEUE_ENTRIES);
        assert_eq!(sqsize_cap(0, 256), ioutgt_core::MAX_QUEUE_ENTRIES);
    }
}
