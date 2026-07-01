//! Transport-neutral NVMe-oF target harness.
//!
//! Spawns the control thread and wires connection handoff into the
//! queue-thread pool (admin thread + N IO threads), which is itself spawned
//! lazily on the first accepted connection. The pool, control API, stats, CPU
//! pinning, and idle teardown are all transport-neutral: they are generic over
//! a [`Transport`], which supplies the connection source (bind / accept /
//! handshake) and the per-queue driver (`run_queue`). A frontend instantiates
//! [`spawn`] with its transport (e.g. NVMe/TCP or NVMe/RDMA).

pub mod client;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use ioutgt_backend::AnyBackend;
use ioutgt_control::config::{BackendConfig, FileConfig, NamespaceConfig, SubsystemConfig};
use ioutgt_control::server::{CtlState, build_backend};
use ioutgt_core::controller::Registry;
use ioutgt_core::dispatch::ConnCtx;
use ioutgt_core::permit::ConnPermit;
use ioutgt_core::queue::{QueueStats, QueueStatsSnapshot};
use ioutgt_core::subsystem::{Namespace, PortConfig, Subsystem, TransportType};
use ioutgt_cpus::{CpuTopology, group_cpus_evenly};
use ioutgt_uring::mailbox::{Mailbox, MailboxSender, mailbox};
use ioutgt_uring::{QueueRuntime, RingConfig};
use tracing::{error, info, warn};

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

    /// A short, human-readable description of the connection's peer (the TCP
    /// peer address, the RDMA source address), for accept-path diagnostics —
    /// computed before the handshake consumes the raw connection.
    fn peer(raw: &Self::Raw) -> String;

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
    /// Per-IO-queue data-buffer pool size in bytes (slots lease on demand).
    pub queue_buf_bytes: usize,
    /// Per-CONNECTION receive-ring size in bytes (`0` = ring off; the classic
    /// per-recv scratch buffer is used). When non-zero and supported, each IO
    /// connection owns a provided-buffer ring of this size and recv draws from
    /// it (zero-copy receive); memory scales as (connections × this).
    pub recv_buf_bytes: usize,
    /// Unix socket path for the runtime control API.
    pub control_socket: Option<std::path::PathBuf>,
    /// Tear the queue-thread pool down after this long with zero active
    /// connections, respawning it on the next connect; `None` keeps the
    /// pool alive for the process lifetime once spawned.
    pub idle_teardown: Option<Duration>,
    /// Subsystems served on this port.
    pub subsystems: Vec<SubsystemConfig>,
    /// Test-only: artificial per-write delay (microseconds) injected into
    /// memory-backed namespaces, emulating a slow real disk so recv-side data
    /// buffers stay referenced across the write. `0` keeps writes synchronous.
    pub mem_write_delay_us: u64,
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
            queue_buf_bytes: ioutgt_core::pool::DEFAULT_POOL_MB * 1024 * 1024,
            recv_buf_bytes: 0,
            control_socket: None,
            idle_teardown: Some(Duration::from_secs(30)),
            mem_write_delay_us: 0,
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
            queue_buf_bytes: file.queue_buf_mb.saturating_mul(1024 * 1024),
            recv_buf_bytes: file.recv_buf_mb.saturating_mul(1024 * 1024),
            control_socket: file.control_socket,
            idle_teardown: (file.idle_teardown_secs != 0)
                .then(|| Duration::from_secs(file.idle_teardown_secs)),
            mem_write_delay_us: 0,
            subsystems: file.subsystems,
        })
    }
}

/// Maximum concurrent connections accepted. Bounds total preallocated
/// queue memory; a host that exceeds it is rejected at accept. (Deeper
/// mitigation — lazy slot-buffer allocation — is in the roadmap.)
const MAX_CONNECTIONS: usize = 256;

/// A queue thread whose mailbox (and sender) already exist but whose OS
/// thread, io_uring ring, and runtime are not yet created. Calling it
/// spawns the thread; the pool is deferred until the first client
/// connects (see [`control_loop`]).
type PendingThread = Box<dyn FnOnce() -> io::Result<()> + Send>;

/// Reply channel for a stats request: the queue thread builds its JSON
/// on-thread (control-plane rate) and sends it back.
type StatsRequest = tokio::sync::oneshot::Sender<serde_json::Value>;

/// A queue thread's mailbox endpoints (sender kept by the control thread,
/// receiver moved onto the queue thread), parameterized by the transport's
/// connection type `C`.
type IoMailbox<C> = (MailboxSender<IoMsg<C>>, Mailbox<IoMsg<C>>);
type AdminMailbox<C> = (MailboxSender<AdminMsg<C>>, Mailbox<AdminMsg<C>>);

/// Messages to an IO queue thread. Generic over the transport's connection
/// type `C`; only `Conn` carries it.
enum IoMsg<C> {
    Conn(C),
    Stats {
        reply: StatsRequest,
        clear: bool,
    },
    /// Exit the mailbox loop so the thread (and its io_uring ring) is torn
    /// down. Sent only when the pool is idle (zero active connections).
    Shutdown,
}

/// Messages to the admin queue thread. Generic over the transport's connection
/// type `C`.
enum AdminMsg<C> {
    Conn(C),
    /// A namespace changed: nudge every live controller's AERs.
    NsChanged,
    Stats {
        reply: StatsRequest,
        clear: bool,
    },
    /// Exit the mailbox loop so the thread (and its io_uring ring) is torn
    /// down. Sent only when the pool is idle (zero active connections).
    Shutdown,
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
            // Transport-specific per-queue counters (RDMA WR classes), if any.
            if let Some(wr) = stats.transport_snapshot() {
                value["wr"] = wr
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), serde_json::Value::from(v)))
                    .collect();
            }
            value
        })
        .collect();
    serde_json::json!({
        "name": name,
        "tid": ioutgt_core::controller::current_tid(),
        "ring": { "parks": ring.parks, "sqes": ring.sqes,
                  "send_sqes": ring.send_sqes, "recv_sqes": ring.recv_sqes,
                  "read_sqes": ring.read_sqes, "write_sqes": ring.write_sqes,
                  "cqes": ring.cqes },
        "queues": queues,
        "retired": counters_json(retired),
    })
}

/// Create an IO queue thread's mailbox and return its sender plus a
/// deferred spawn closure (the ring/runtime/OS thread are built only when
/// the closure runs). IO queue threads receive connections and stats
/// requests; `T` is the fabric transport whose `run_queue` drives them.
fn make_io_thread<T: Transport>(
    name: String,
    core_id: Option<usize>,
) -> io::Result<(MailboxSender<IoMsg<T::Conn>>, PendingThread)> {
    let (tx, mut rx): IoMailbox<T::Conn> = mailbox()?;
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
                            let on_ctx: OnCtx = Box::new(move |ctx| {
                                queues.borrow_mut().push(Rc::clone(&ctx.queue.stats));
                            });
                            tokio::task::spawn_local(T::run_queue(conn, on_ctx));
                        }
                        Ok(IoMsg::Stats { reply, clear }) => {
                            prune_dead_queues(&queues, &mut retired);
                            let queues = queues.borrow();
                            let _ = reply.send(thread_stats_json(&name, &queues, &retired));
                            if clear {
                                clear_thread_stats(&queues, &mut retired);
                            }
                        }
                        Ok(IoMsg::Shutdown) => return,
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
fn make_admin_thread<T: Transport>(
    name: String,
) -> io::Result<(MailboxSender<AdminMsg<T::Conn>>, PendingThread)> {
    let (tx, mut rx): AdminMailbox<T::Conn> = mailbox()?;
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
                            let on_ctx: OnCtx = Box::new(move |ctx| {
                                live.borrow_mut().push(Rc::downgrade(ctx));
                                queues.borrow_mut().push(Rc::clone(&ctx.queue.stats));
                            });
                            tokio::task::spawn_local(T::run_queue(conn, on_ctx));
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
                        Ok(AdminMsg::Shutdown) => return,
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

/// For each IO queue thread, the CPU it is pinned to and the full online CPU
/// group it belongs to. CPUs are grouped evenly per NUMA/cluster/SMT locality
/// (the kernel `group_cpus_evenly` spread, i.e. what nvme-tcp queues see on the
/// host side), one group per IO thread; the thread is pinned to (and reported
/// as "active" on) the group's first online CPU, while the whole group is
/// surfaced (as a kernel cpulist, e.g. `"0-1,32-33"`) so the harness can steer
/// NIC IRQ affinity across it. Returns `(active_cpu, group_cpulist)` per thread
/// — `group` is `"*"` when the topology is unavailable or the group is empty.
fn io_thread_cpus(io_threads: usize) -> (Vec<Option<usize>>, Vec<String>) {
    let topo = match CpuTopology::from_sysfs() {
        Ok(topo) => topo,
        Err(err) => {
            warn!("cpu topology unavailable, io threads not pinned: {err}");
            return (vec![None; io_threads], vec!["*".to_owned(); io_threads]);
        }
    };
    let groups = group_cpus_evenly(io_threads, &topo);
    let mut cpus = Vec::with_capacity(io_threads);
    let mut group_lists = Vec::with_capacity(io_threads);
    for i in 0..io_threads {
        // groups can run out when io_threads > possible CPUs; a group of
        // only-offline CPUs yields no pinnable CPU.
        let group = groups.get(i);
        let online = group.map(|g| g.and(&topo.online));
        let cpu = online.as_ref().and_then(|g| g.first());
        let list = match &online {
            Some(g) if g.first().is_some() => g.to_string(),
            _ => "*".to_owned(),
        };
        match (cpu, group) {
            (Some(cpu), Some(group)) => info!(thread = i, cpus = %group, cpu, "io queue affinity"),
            (None, Some(group)) => {
                warn!(thread = i, cpus = %group, "no online cpu in group, thread not pinned");
            }
            (_, None) => warn!(
                thread = i,
                "more io threads than possible cpus, thread not pinned"
            ),
        }
        cpus.push(cpu);
        group_lists.push(list);
    }
    (cpus, group_lists)
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
/// LIST_CONTROLLER, not the configured 0. `trtype` is the serving fabric.
fn build_port(
    config: &TargetConfig,
    bound: SocketAddr,
    trtype: TransportType,
) -> io::Result<Arc<PortConfig<AnyBackend>>> {
    let mut subsystems = BTreeMap::new();
    for spec in &config.subsystems {
        let mut namespaces = BTreeMap::new();
        for ns in &spec.namespaces {
            let backend =
                build_backend(&ns.backend, config.recv_buf_bytes > 0).map_err(io::Error::other)?;
            // Test-only slow-disk emulation for memory namespaces.
            if config.mem_write_delay_us > 0
                && let AnyBackend::Memory(m) = &backend
            {
                m.set_write_delay_us(config.mem_write_delay_us);
            }
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
        trtype,
        io_queue_size: config.io_queue_size,
        queue_buf_bytes: config.queue_buf_bytes,
        recv_buf_bytes: config.recv_buf_bytes,
        subsystems,
    }))
}

/// The live mailbox senders for a spawned queue-thread pool: the admin
/// thread plus one per IO thread. Held behind `Mutex<Option<_>>` in
/// [`control_loop`] — `None` means the pool is currently down (before the
/// first connection, or after an idle teardown). Generic over the transport's
/// connection type `C`.
struct PoolSenders<C> {
    admin: MailboxSender<AdminMsg<C>>,
    io: Vec<MailboxSender<IoMsg<C>>>,
}

/// Build the pool's mailboxes: returns the senders plus the deferred
/// spawn closures (admin first, then one per IO thread). The OS threads /
/// io_uring rings are created only when the closures run.
fn build_pool<T: Transport>(
    io_cpus: &[Option<usize>],
) -> io::Result<(PoolSenders<T::Conn>, Vec<PendingThread>)> {
    let (admin, admin_pending) = make_admin_thread::<T>("ioutgt-admin".into())?;
    let mut io = Vec::with_capacity(io_cpus.len());
    let mut pending: Vec<PendingThread> = Vec::with_capacity(io_cpus.len() + 1);
    pending.push(admin_pending);
    for (i, core_id) in io_cpus.iter().enumerate() {
        let (tx, io_pending) = make_io_thread::<T>(format!("ioutgt-io{i}"), *core_id)?;
        io.push(tx);
        pending.push(io_pending);
    }
    Ok((PoolSenders { admin, io }, pending))
}

/// Spawn the queue-thread pool if it is currently down — the first
/// connection ever, or the first after an idle teardown. Idempotent;
/// runs the deferred spawn closures and publishes the senders.
fn ensure_pool_up<T: Transport>(
    senders: &Mutex<Option<PoolSenders<T::Conn>>>,
    io_cpus: &[Option<usize>],
) {
    let mut guard = senders.lock().expect("pool senders mutex");
    if guard.is_some() {
        return;
    }
    match build_pool::<T>(io_cpus) {
        Ok((pool, pending)) => {
            for spawn in pending {
                if let Err(err) = spawn() {
                    error!("queue thread spawn failed: {err}");
                }
            }
            *guard = Some(pool);
            info!("queue-thread pool spawned");
        }
        Err(err) => error!("queue-thread pool build failed: {err}"),
    }
}

/// Tear the idle pool down: signal every thread to exit its mailbox loop,
/// then drop the senders. Each thread returns from `block_on`, dropping
/// its `QueueRuntime` (io_uring ring); the mailbox eventfds close once the
/// last sender clone is gone. Only called with zero active connections, so
/// no thread is mid-`run_queue` and no op-slab drain is needed.
///
/// Exit is fire-and-forget: this returns before the threads have actually
/// died, and a respawn ([`ensure_pool_up`]) does not wait for them — a
/// teardown immediately followed by a reconnect can briefly run the old
/// and new pools side by side. That is harmless (independent threads,
/// rings, and fresh mailboxes), just transiently more threads.
fn teardown_pool<C: Send>(senders: &Mutex<Option<PoolSenders<C>>>) {
    let Some(pool) = senders.lock().expect("pool senders mutex").take() else {
        return;
    };
    for io_tx in &pool.io {
        io_tx.send(IoMsg::Shutdown);
    }
    pool.admin.send(AdminMsg::Shutdown);
    info!("queue-thread pool torn down after idle");
    // `pool` (the last sender clones) drops here.
}

/// A zeroed per-thread stats snapshot, the reply for a stats query while
/// the pool is down (no thread to ask).
fn zeroed_stats(name: &str) -> serde_json::Value {
    thread_stats_json(name, &[], &QueueStatsSnapshot::default())
}

/// One stats source per queue thread (admin + each IO). Each reads the
/// live sender through `senders`, so it tracks teardown/respawn; while the
/// pool is down it answers with a zeroed snapshot instead of blocking.
fn build_stats_sources<C: Send + 'static>(
    senders: &Arc<Mutex<Option<PoolSenders<C>>>>,
    io_threads: usize,
) -> Vec<ioutgt_control::server::StatsSource> {
    let mut sources: Vec<ioutgt_control::server::StatsSource> = Vec::with_capacity(1 + io_threads);
    let admin = Arc::clone(senders);
    sources.push(Box::new(move |clear, reply| {
        match admin.lock().expect("pool senders mutex").as_ref() {
            Some(pool) => pool.admin.send(AdminMsg::Stats { reply, clear }),
            None => {
                let _ = reply.send(zeroed_stats("ioutgt-admin"));
            }
        }
    }));
    for i in 0..io_threads {
        let io = Arc::clone(senders);
        let name = format!("ioutgt-io{i}");
        sources.push(Box::new(move |clear, reply| {
            match io
                .lock()
                .expect("pool senders mutex")
                .as_ref()
                .and_then(|pool| pool.io.get(i))
            {
                Some(io_tx) => io_tx.send(IoMsg::Stats { reply, clear }),
                None => {
                    let _ = reply.send(zeroed_stats(&name));
                }
            }
        }));
    }
    sources
}

/// Bind and serve the runtime control API on `path`, wiring its stats and
/// namespace-change hooks to the (possibly-down) pool through `senders`.
/// Must run on the control thread's `LocalSet` (uses `spawn_local`).
fn spawn_control_api<C: Send + 'static>(
    path: &std::path::Path,
    port: &Arc<PortConfig<AnyBackend>>,
    registry: &Arc<Registry>,
    senders: &Arc<Mutex<Option<PoolSenders<C>>>>,
    io_groups: &[String],
    io_threads: usize,
) -> io::Result<()> {
    // The API mutates served storage (ADD/REMOVE_NAMESPACE): owner-only.
    // Prefer a private dir (the CLI defaults to $XDG_RUNTIME_DIR) over
    // world-writable /tmp, where a pre-bound squatter could intercept first.
    let _ = std::fs::remove_file(path);
    let listener = tokio::net::UnixListener::bind(path)?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;

    let nudge = Arc::clone(senders);
    let state = Arc::new(CtlState {
        port: Arc::clone(port),
        registry: Arc::clone(registry),
        notify_ns_changed: Box::new(move || {
            // Pool down → no live controllers to AER; the namespace edit
            // still lands in the port model and shows up on the next connect.
            if let Some(pool) = nudge.lock().expect("pool senders mutex").as_ref() {
                pool.admin.send(AdminMsg::NsChanged);
            }
        }),
        stats_sources: build_stats_sources(senders, io_threads),
        io_thread_groups: io_groups.to_vec(),
    });
    info!(path = %path.display(), "control socket listening");
    tokio::task::spawn_local(ioutgt_control::server::serve(listener, state));
    Ok(())
}

/// Drives idle-teardown of the queue-thread pool: a coarse poll timer plus
/// the timestamp of when the pool last went fully idle.
struct IdleTeardown {
    /// Tear down after this long fully idle; `None` disables teardown.
    grace: Option<Duration>,
    tick: tokio::time::Interval,
    idle_since: Option<Instant>,
}

impl IdleTeardown {
    fn new(grace: Option<Duration>) -> Self {
        // Poll often enough to fire within roughly the grace period; coarse
        // by design (no cross-thread "reached zero" signal). When disabled,
        // an effectively-never tick keeps the `select!` arm well-formed.
        let period = grace
            .map(|g| (g / 4).clamp(Duration::from_millis(100), Duration::from_secs(5)))
            .unwrap_or_else(|| Duration::from_secs(3600));
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        IdleTeardown {
            grace,
            tick,
            idle_since: None,
        }
    }

    async fn tick(&mut self) {
        self.tick.tick().await;
    }

    /// Connection activity: restart the idle clock.
    fn reset(&mut self) {
        self.idle_since = None;
    }

    /// Tear the pool down if it has had zero active connections for the
    /// whole grace period; otherwise track/clear the idle timestamp.
    fn maybe_teardown<C: Send>(
        &mut self,
        senders: &Mutex<Option<PoolSenders<C>>>,
        active: &AtomicUsize,
    ) {
        let Some(grace) = self.grace else {
            return; // teardown disabled
        };
        let up = senders.lock().expect("pool senders mutex").is_some();
        if up && active.load(Ordering::Relaxed) == 0 {
            let since = *self.idle_since.get_or_insert_with(Instant::now);
            if since.elapsed() >= grace {
                teardown_pool(senders);
                self.idle_since = None;
            }
        } else {
            self.idle_since = None;
        }
    }
}

/// Handle one accepted connection: bring the pool up if down, account for the
/// connection, then spawn a per-connection task that finishes the transport's
/// handshake and routes the resulting `Conn` to a queue thread by qid. Runs on
/// the control thread's `LocalSet` (uses `spawn_local`); never blocks it.
#[allow(clippy::too_many_arguments)]
fn handle_accept<T: Transport>(
    accepted: io::Result<T::Raw>,
    config: &Arc<TargetConfig>,
    senders: &Arc<Mutex<Option<PoolSenders<T::Conn>>>>,
    io_cpus: &[Option<usize>],
    active: &Arc<AtomicUsize>,
    registry: &Arc<Registry>,
    port: &Arc<PortConfig<AnyBackend>>,
) {
    let raw = match accepted {
        Ok(raw) => raw,
        Err(err) => {
            warn!("accept failed: {err}");
            return;
        }
    };
    let peer = T::peer(&raw);
    // Bring the pool up if it is down (first connect or post-teardown).
    ensure_pool_up::<T>(senders, io_cpus);
    // Clone the live senders for routing, then drop the lock before the
    // async setup task (never hold the mutex across an await).
    let (admin_tx, io_txs) = match senders.lock().expect("pool senders mutex").as_ref() {
        Some(pool) => (pool.admin.clone(), pool.io.clone()),
        None => {
            warn!(%peer, "queue-thread pool unavailable; dropping connection");
            return;
        }
    };
    let count = active.fetch_add(1, Ordering::Relaxed) + 1;
    if count > MAX_CONNECTIONS {
        active.fetch_sub(1, Ordering::Relaxed);
        warn!(%peer, "connection limit {MAX_CONNECTIONS} reached; rejecting");
        return; // raw drops here, closing the connection
    }
    let permit = ConnPermit::new(Arc::clone(active));
    let config = Arc::clone(config);
    let registry = Arc::clone(registry);
    let port = Arc::clone(port);
    tokio::task::spawn_local(async move {
        match T::handshake(raw, config, port, registry, permit).await {
            Ok((qid, conn)) => {
                if qid == 0 {
                    admin_tx.send(AdminMsg::Conn(conn));
                } else if io_txs.is_empty() {
                    warn!(qid, %peer, "no IO threads; dropping connection");
                } else {
                    io_txs[(usize::from(qid) - 1) % io_txs.len()].send(IoMsg::Conn(conn));
                }
            }
            Err(err) => warn!(%peer, "connection setup failed: {err}"),
        }
    });
}

/// The control thread's main loop, generic over the fabric transport `T`:
/// bind, build the served port, serve the control API, then accept
/// connections (routing each to a queue thread) and run idle teardown.
async fn control_loop<T: Transport>(
    config: TargetConfig,
    addr_tx: mpsc::Sender<io::Result<SocketAddr>>,
) {
    let registry = Registry::new();

    // The queue-thread pool is spawned lazily on the first connection and
    // torn down after an idle grace period; `senders` is the single source
    // of truth for whether it is up. `None` = down (pre-first-connect or
    // post-teardown) → control-socket stats reply with a zeroed snapshot
    // and namespace-change nudges no-op (no live controllers). Control-
    // plane only — never locked on the IO path, never held across an await.
    let senders: Arc<Mutex<Option<PoolSenders<T::Conn>>>> = Arc::new(Mutex::new(None));
    // Per-IO-thread CPU assignment is fixed for the process (topology is
    // stable), so compute it once and reuse it for every (re)spawn. `io_cpus`
    // is the pinned (active) CPU per thread; `io_groups` is each thread's full
    // online CPU group, surfaced via `list` so the harness can steer NIC IRQs.
    let (io_cpus, io_groups) = if config.pin_threads {
        io_thread_cpus(config.io_threads)
    } else {
        (
            vec![None; config.io_threads],
            vec!["*".to_owned(); config.io_threads],
        )
    };

    // Bind before building the port so the model carries the actual bound
    // address (ephemeral ports resolve to the real one). On any setup
    // failure, report it back through `addr_tx` and stop.
    let (listener, local) = match T::bind(&config).await {
        Ok(bound) => bound,
        Err(err) => {
            let _ = addr_tx.send(Err(err));
            return;
        }
    };
    let port = match build_port(&config, local, T::trtype()) {
        Ok(port) => port,
        Err(err) => {
            let _ = addr_tx.send(Err(err));
            return;
        }
    };
    if let Some(path) = &config.control_socket {
        if let Err(err) = spawn_control_api(
            path,
            &port,
            &registry,
            &senders,
            &io_groups,
            config.io_threads,
        ) {
            let _ = addr_tx.send(Err(err));
            return;
        }
    }

    let _ = addr_tx.send(Ok(local));
    info!(%local, "ioutgt listening");

    // Config is shared into each per-connection handshake task.
    let config = Arc::new(config);
    // Bounds total preallocated queue memory across all queue threads.
    let active = Arc::new(AtomicUsize::new(0));
    let mut idle = IdleTeardown::new(config.idle_teardown);
    loop {
        tokio::select! {
            accepted = T::accept(&listener) => {
                // An accepted connection is activity — restart the idle clock.
                // An accept *error* is not (and must not defer teardown).
                if accepted.is_ok() {
                    idle.reset();
                }
                handle_accept::<T>(accepted, &config, &senders, &io_cpus, &active, &registry, &port);
            }
            _ = idle.tick() => idle.maybe_teardown(&senders, &active),
        }
    }
}

/// Start a target's control thread for transport `T`; returns the bound
/// address (for ephemeral-port tests). The queue-thread pool is spawned lazily
/// on the first connection and reclaimed after an idle grace period. Runs until
/// the process exits.
pub fn spawn<T: Transport>(config: TargetConfig) -> io::Result<SocketAddr> {
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
            rt.block_on(local.run_until(control_loop::<T>(config, addr_tx)));
        })?;
    addr_rx
        .recv()
        .map_err(|_| io::Error::other("control thread died during bind"))?
}
