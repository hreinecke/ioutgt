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
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use ioutgt_backend::{AnyBackend, VDI_MAX_HOLDERS};
use ioutgt_control::config::{BackendConfig, NamespaceConfig, SheepdogAcl, SubsystemConfig};
use ioutgt_control::nvmet::NvmetTarget;
use ioutgt_control::server::{CtlState, build_backend};
use ioutgt_core::permit::ConnPermit;
use ioutgt_core::queue::{QueueStats, QueueStatsSnapshot};
use ioutgt_core::registry::Registry;
pub use ioutgt_core::subsystem::TransportType;
use ioutgt_core::subsystem::{HostAcl, Namespace, PortConfig, Subsystem, SubsystemPort};
use ioutgt_cpus::{CpuTopology, spread_cpus};
use ioutgt_uring::mailbox::{Mailbox, MailboxSender, mailbox};
use ioutgt_uring::{QueueRuntime, RingConfig};
use tracing::{debug, error, info, warn};

/// What a connection reports to its queue thread once its dispatch state
/// exists. Built by the transport's `run_queue` from its per-connection
/// context; the harness never sees that context itself.
pub struct ConnHandles {
    /// The queue's lifetime stats, recorded for GET_STATS aggregation.
    pub stats: Rc<QueueStats>,
    /// Async-event nudges for this connection (a no-op on connections
    /// without async events, e.g. IO queues — those never reach the admin
    /// thread's nudge list anyway).
    pub changes: ChangeNudge,
    /// Ask this connection to wind down: unwedge whatever its `run_queue`
    /// is parked on (NVMe/TCP: `shutdown(2)` on the socket; NVMe/RDMA: the
    /// connection's stop `Notify`) so it runs its normal teardown — drain
    /// executing slots, join the send path, free the queue — and returns.
    ///
    /// The shutdown handshake ([`shutdown`]) fires this on every connection
    /// a queue thread is running, then waits for the tasks to finish. It
    /// must be safe to call at any time, including after the connection is
    /// already gone (then it does nothing).
    pub stop: Box<dyn Fn()>,
}

/// A connection's async-event nudges: a side-effect-free liveness probe (so
/// the pool can prune dead entries without firing events) plus one fire per
/// event the control plane raises.
pub struct ChangeNudge {
    /// `true` while the connection is alive; no side effects.
    pub alive: Box<dyn Fn() -> bool>,
    /// Fire the namespace-changed async event (a no-op once dead).
    pub ns_changed: Box<dyn Fn()>,
    /// Fire the ANA-changed async event (a no-op once dead, and on a
    /// connection whose subsystem does not report ANA).
    pub ana_changed: Box<dyn Fn()>,
    /// Fire the discovery-log-page-changed async event (a no-op once dead, and
    /// on anything but a discovery controller).
    pub disc_changed: Box<dyn Fn()>,
}

/// Install callback, run once a connection's dispatch context exists: the admin
/// thread keeps the connection's namespace-change nudge, and every thread
/// records the queue's stats handle. Boxed so the generic pool can hand it to a
/// transport's `run_queue` without the pool being generic over the closure.
pub type OnCtx = Box<dyn FnOnce(ConnHandles)>;

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
    fn bind(cfg: &TargetConfig) -> impl Future<Output = io::Result<(Self::Listener, SocketAddr)>>;

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

/// Target configuration. Built from CLI flags — optionally overlaid
/// with an nvmetcli-format file ([`TargetConfig::apply_file`]) — or
/// [`TargetConfig::single_memory`] in tests.
#[derive(Debug, Clone)]
#[allow(missing_docs)]
pub struct TargetConfig {
    pub listen: SocketAddr,
    /// Number of IO queue threads (in addition to the admin thread).
    pub io_threads: usize,
    pub allow_hdgst: bool,
    pub allow_ddgst: bool,
    /// Pin each IO queue thread to one CPU of its `spread_cpus`
    /// group (disable in tests).
    pub pin_threads: bool,
    /// Busy-poll the transport's completion sources on the IO queue threads
    /// instead of sleeping on events (`--poll`): trades one core per IO
    /// thread for per-IO latency. Transport-interpreted; TCP ignores it.
    pub poll: bool,
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
    /// Allocatable cntlid slice, inclusive. Multi-port configs give
    /// each port process a disjoint slice (see
    /// [`TargetConfig::apply_file`]); single-port targets own the full
    /// spec range. A target serving cluster storage subdivides whatever
    /// this leaves it once more, by holder slot, so that the *other*
    /// targets fronting the same volumes get a slice of their own
    /// ([`holder_cntlid_slice`]).
    pub cntlid_range: (u16, u16),
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
            poll: false,
            cntlid_range: (1, ioutgt_core::registry::CNTLID_MAX),
            subsystems: vec![SubsystemConfig {
                nqn: nqn.into(),
                serial: "IOUTGT0001".into(),
                model: "ioutgt".into(),
                allow_any_host: true,
                allowed_hosts: vec![],
                sheepdog_acl: None,
                mnan: None,
                namespaces: vec![NamespaceConfig {
                    nsid: 1,
                    backend: BackendConfig::Memory { size_mb },
                    uuid: None,
                }],
            }],
        }
    }

    /// Overlay an nvmetcli-format config file (kernel nvmet's
    /// save/restore schema): the file owns the target model — listen
    /// address and subsystems — while engine tuning keeps whatever the
    /// flags/defaults set.
    ///
    /// A file defining several ports for `trtype` is served one
    /// process per port: this call forks the extra port processes and
    /// returns in every one of them with its own port's model. The
    /// calling (foreground) process keeps the lowest portid and its
    /// configured control socket; each forked port derives
    /// `<socket>.port<id>` and dies with the parent (`PDEATHSIG`).
    ///
    /// Must be called before [`spawn`]; the multi-port path verifies
    /// the process is still single-threaded and errors otherwise.
    pub fn apply_file(&mut self, path: &std::path::Path, trtype: TransportType) -> io::Result<()> {
        let targets = ioutgt_control::nvmet::load(path, trtype).map_err(io::Error::other)?;
        let ports = u16::try_from(targets.len()).map_err(|_| io::Error::other("too many ports"))?;
        let (index, mine) = fork_extra_ports(targets)?;
        self.listen = mine.listen;
        self.subsystems = mine.subsystems;
        // Disjoint cntlid slice per port process: cntlids are unique
        // per subsystem on the wire, and a subsystem exported on
        // several ports is served by several processes — overlapping
        // slices would hand a multipath host duplicate cntlids for one
        // subsystem, which it rejects (nvme_validate_cntlid).
        let slice = ioutgt_core::registry::CNTLID_MAX / ports;
        let min = 1 + index * slice;
        let max = if index + 1 == ports {
            ioutgt_core::registry::CNTLID_MAX
        } else {
            min + slice - 1
        };
        self.cntlid_range = (min, max);
        if index > 0
            && let Some(sock) = &mut self.control_socket
        {
            sock.as_mut_os_string()
                .push(format!(".port{}", mine.portid));
        }
        Ok(())
    }
}

/// Fork one process per port beyond the first; every process returns
/// with the index and target it serves (index > 0 = a forked port
/// process, which dies with its parent via `PDEATHSIG`). The calling
/// process keeps the first target and stays in the foreground.
fn fork_extra_ports(mut targets: Vec<NvmetTarget>) -> io::Result<(u16, NvmetTarget)> {
    if targets.len() > 1 {
        // fork() duplicates only the calling thread: any other thread's
        // held lock (allocator, tracing) would deadlock the child. The
        // config path runs before spawn(), so enforce that it stayed
        // single-threaded rather than trusting the docs.
        let threads = std::fs::read_dir("/proc/self/task")?.count();
        if threads != 1 {
            return Err(io::Error::other(format!(
                "multi-port fork needs a single-threaded process ({threads} threads running)"
            )));
        }
    }
    for (extra, target) in targets.drain(1..).enumerate() {
        // SAFETY: the process is single-threaded (verified above), so
        // the fork duplicates no locks, threads, or event loops.
        match unsafe { libc::fork() } {
            -1 => return Err(io::Error::last_os_error()),
            0 => {
                // SAFETY: plain prctl on the calling process with
                // immediate integer arguments.
                unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) };
                let index = u16::try_from(extra + 1).expect("port count fits u16");
                return Ok((index, target));
            }
            _ => {}
        }
    }
    Ok((0, targets.remove(0)))
}

/// Announce the bound address on stderr as one write syscall: with a
/// multi-port config several processes share stderr, and per-fragment
/// writes (`eprintln!`) would interleave mid-line across them — the
/// multi-port test parses these lines.
pub fn announce_listening(name: &str, addr: SocketAddr) {
    let _ = std::io::Write::write_all(
        &mut std::io::stderr(),
        format!("{name} listening on {addr}\n").as_bytes(),
    );
}

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

/// Every port this process built ([`build_port`]), kept for the shutdown
/// walk. Nothing here is dropped on the way out — the queue threads own
/// `Arc`s to the same namespaces for the process's lifetime — so state a
/// backend holds *outside* the process (a Sheepdog VDI's cluster lock) has
/// to be handed back explicitly instead. Control plane only: appended at
/// startup, drained once at exit.
static LIVE_PORTS: Mutex<Vec<Arc<PortConfig<AnyBackend>>>> = Mutex::new(Vec::new());

/// One entry per control thread this process started ([`control_loop`]): send
/// it a reply channel to ask that target to stop serving IO, and it answers
/// once its queue threads have quiesced. The control thread returns right
/// after — a target that has been asked to stop stays stopped.
static QUIESCE_REQS: Mutex<Vec<QuiesceHandle>> = Mutex::new(Vec::new());

/// The control thread's end of the quiesce handshake. Two channel flavours
/// because the two ends live in different worlds: the request goes into the
/// control thread's `select!` (Tokio), the reply comes back to a plain thread
/// blocking in [`shutdown`] (std).
type QuiesceHandle = tokio::sync::mpsc::UnboundedSender<std::sync::mpsc::SyncSender<()>>;

/// Set for good the moment [`shutdown`] starts, before anything is torn down.
/// The control plane reads it to tell "the cluster is faulty" from "we are on
/// our way out": once this is set, a cluster that stops answering is expected
/// — often it is the same environment going down with us — and the state a
/// refresh would have updated is about to be released anyway.
static SHUTTING_DOWN: AtomicBool = AtomicBool::new(false);

/// Whether [`shutdown`] has begun.
fn shutting_down() -> bool {
    SHUTTING_DOWN.load(Ordering::Relaxed)
}

/// Read end of the self-pipe [`wait_for_shutdown`] sleeps on, `-1` before the
/// handler is installed.
static SHUTDOWN_READ: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(-1);

/// Write end of that pipe: the only state the signal handler touches, so it
/// is a plain atomic (nothing else would be async-signal-safe).
static SHUTDOWN_WRITE: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(-1);

/// SIGINT/SIGTERM handler: wake [`wait_for_shutdown`] with the signal number.
/// A relaxed atomic load and a one-byte `write` are async-signal-safe; the
/// real work happens back on the waiting thread.
extern "C" fn on_shutdown_signal(sig: libc::c_int) {
    // SAFETY: `__errno_location` yields this thread's errno slot, which the
    // `write` below may clobber — a handler must leave it as it found it.
    let errno = unsafe { libc::__errno_location() };
    // SAFETY: as above; the pointer is valid for the handler's lifetime.
    let saved = unsafe { *errno };
    let byte = u8::try_from(sig).unwrap_or(0);
    // SAFETY: the fd is our own pipe's write end (or -1, which `write` just
    // rejects with EBADF), and `byte` is a live one-byte local. A full pipe
    // (a second signal, nobody reading yet) fails with EAGAIN — the pending
    // byte already says what this one would.
    unsafe {
        libc::write(
            SHUTDOWN_WRITE.load(Ordering::Relaxed),
            std::ptr::from_ref(&byte).cast(),
            1,
        )
    };
    // SAFETY: as above.
    unsafe { *errno = saved };
}

/// Catch SIGINT (Ctrl-C) and SIGTERM instead of dying on them, so
/// [`wait_for_shutdown`] can release what the backends hold before exit.
///
/// Call it early in `main`: a target opening a Sheepdog cluster takes its VDI
/// locks inside [`spawn`], and a Ctrl-C arriving mid-open would otherwise
/// leave them behind. Idempotent, and [`wait_for_shutdown`] installs the
/// handler itself if the binary did not.
///
/// A *second* signal is not caught (`SA_RESETHAND`): it takes the default
/// action, so a shutdown wedged on an unresponsive cluster stays killable.
pub fn install_shutdown_handler() -> io::Result<()> {
    if SHUTDOWN_WRITE.load(Ordering::Relaxed) >= 0 {
        return Ok(());
    }
    let mut fds = [-1 as libc::c_int; 2];
    // SAFETY: `fds` is a live two-element array, exactly what pipe2 fills.
    if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
        return Err(io::Error::last_os_error());
    }
    SHUTDOWN_READ.store(fds[0], Ordering::Relaxed);
    // Published before the handler can run, so a signal never finds -1.
    SHUTDOWN_WRITE.store(fds[1], Ordering::Relaxed);
    for sig in [libc::SIGINT, libc::SIGTERM] {
        // SAFETY: an all-zero `sigaction` is its documented empty state; the
        // fields that matter are filled in below.
        let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
        action.sa_sigaction = on_shutdown_signal as *const () as libc::sighandler_t;
        action.sa_flags = libc::SA_RESETHAND | libc::SA_RESTART;
        // SAFETY: clears the mask of a live, owned `sigaction`.
        unsafe { libc::sigemptyset(&mut action.sa_mask) };
        // SAFETY: `action` is live for the call, which reads it and returns;
        // a null third argument means "old action not wanted".
        if unsafe { libc::sigaction(sig, &action, std::ptr::null_mut()) } != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Park the calling thread until this process is asked to stop — SIGINT
/// (Ctrl-C) or SIGTERM (what a forked port process gets when its parent
/// dies) — then [`shutdown`] the targets and return, for the caller to exit.
///
/// Every connection is dropped on the way out, so a host sees what any target
/// restart gives it; the difference is that IO stops *before* anything the
/// backends hold is handed back, rather than racing it.
pub fn wait_for_shutdown() -> io::Result<()> {
    install_shutdown_handler()?;
    let fd = SHUTDOWN_READ.load(Ordering::Relaxed);
    let mut sig = 0u8;
    loop {
        // SAFETY: one byte into a live local, from our own pipe's read end.
        let n = unsafe { libc::read(fd, std::ptr::from_mut(&mut sig).cast(), 1) };
        if n == 1 {
            break;
        }
        let err = io::Error::last_os_error();
        // The handler runs on whichever thread takes the signal, so this read
        // is itself interruptible; EOF is impossible while we hold the write
        // end.
        if n < 0 && err.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return Err(err);
    }
    let name = match libc::c_int::from(sig) {
        libc::SIGINT => "SIGINT",
        libc::SIGTERM => "SIGTERM",
        _ => "signal",
    };
    info!(signal = name, "shutting down");
    let released = shutdown();
    info!(namespaces = released, "backends released");
    Ok(())
}

/// Stop this process's targets and hand back the external state their
/// backends hold — Sheepdog VDI locks — so the next opener is not locked out.
/// Returns the number of namespaces walked.
///
/// Two phases, in this order:
///
/// 1. **Stop IO.** Every target's control thread stops accepting, winds its
///    connections down, and waits for its queue threads to report that no
///    command is executing and no backend op is in flight.
/// 2. **Release.** With nothing left to issue IO, stop refreshing the cluster
///    path lists, then walk the namespaces and let each backend give up what
///    it holds outside the process — its VDI registration, which is also what
///    advertised this target as a path to the volume.
///
/// [`SHUTTING_DOWN`] is set before either phase, so a cluster that stops
/// answering while this runs is reported as the expected teardown it is
/// rather than as a fault.
///
/// Doing it the other way round would let an in-flight write land on a VDI
/// this process no longer holds the lock for. Neither phase can hang: a
/// target that does not answer in [`TARGET_QUIESCE_BUDGET`] is reported and
/// the release goes ahead regardless — better a racy release than none.
///
/// Idempotent: both registries are taken, so a second call (or one from a
/// binary with no targets) finds nothing to do. A stopped target does not
/// come back. Belongs immediately before process exit; [`wait_for_shutdown`]
/// pairs the two.
pub fn shutdown() -> usize {
    SHUTTING_DOWN.store(true, Ordering::Relaxed);
    quiesce_targets();
    stop_cluster_refresh();
    let ports = std::mem::take(&mut *live_ports());
    let mut walked = 0;
    for port in ports {
        for subsys in port.subsystems.values() {
            for ns in subsys.snapshot().values() {
                ns.backend.shutdown();
                walked += 1;
            }
        }
    }
    walked
}

/// How long [`shutdown`] waits for all targets to stop serving IO. The
/// outermost of the three nested budgets: over what a control thread allows
/// its pool ([`POOL_QUIESCE_BUDGET`]), which is over what a queue thread
/// allows its connections ([`CONN_DRAIN_BUDGET_MS`]).
const TARGET_QUIESCE_BUDGET: Duration = Duration::from_secs(12);

/// Phase one of [`shutdown`]: ask every target to stop serving IO and wait
/// for them all to confirm. The requests go out first so the targets quiesce
/// in parallel, and the whole wait shares one deadline.
fn quiesce_targets() {
    let handles = std::mem::take(&mut *quiesce_reqs());
    let mut replies = Vec::with_capacity(handles.len());
    for handle in handles {
        // Rendezvous depth 1: the control thread must not block handing the
        // reply over if this side has already given up on it.
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        // An `Err` is a control thread that already returned (bind failure,
        // an earlier stop) — nothing left to quiesce.
        if handle.send(tx).is_ok() {
            replies.push(rx);
        }
    }
    let deadline = Instant::now() + TARGET_QUIESCE_BUDGET;
    let mut wedged = 0;
    for rx in replies {
        let left = deadline.saturating_duration_since(Instant::now());
        if rx.recv_timeout(left).is_err() {
            wedged += 1;
        }
    }
    if wedged > 0 {
        warn!(
            targets = wedged,
            "did not stop serving IO before the release"
        );
    }
}

/// Every port this process is currently serving, in the order they were built
/// — the same `Arc`s the queue threads hold, so the subsystem model reached
/// through them is the live one (a path list set here shows up in the next
/// discovery log a host reads).
///
/// Empty once [`shutdown`] has run.
pub fn ports() -> Vec<Arc<PortConfig<AnyBackend>>> {
    live_ports().clone()
}

/// [`LIVE_PORTS`], recovered from poisoning: a shutdown path that skipped the
/// release because some unrelated thread panicked would be the worse outcome.
fn live_ports() -> std::sync::MutexGuard<'static, Vec<Arc<PortConfig<AnyBackend>>>> {
    LIVE_PORTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// [`QUIESCE_REQS`], recovered from poisoning — for the same reason
/// [`live_ports`] is.
fn quiesce_reqs() -> std::sync::MutexGuard<'static, Vec<QuiesceHandle>> {
    QUIESCE_REQS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
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
    /// Stop this thread's connections, then exit the mailbox loop so the
    /// thread (and its io_uring ring) is torn down. See [`ShutdownAck`].
    Shutdown {
        ack: ShutdownAck,
    },
}

/// Messages to the admin queue thread. Generic over the transport's connection
/// type `C`.
enum AdminMsg<C> {
    Conn(C),
    /// A namespace changed: nudge every live controller's AERs.
    NsChanged,
    /// A namespace changed ANA group: same, with the ANA Change notice.
    AnaChanged,
    /// The discovery log changed: same, with the Discovery Log Page Change
    /// notice, which only the discovery controllers among them take.
    DiscChanged,
    Stats {
        reply: StatsRequest,
        clear: bool,
    },
    /// Stop this thread's connections, then exit the mailbox loop so the
    /// thread (and its io_uring ring) is torn down. See [`ShutdownAck`].
    Shutdown {
        ack: ShutdownAck,
    },
}

/// How a queue thread reports that it has stopped: `Some` on the shutdown
/// handshake, where the control thread waits for every thread to quiesce
/// before the process releases anything the backends hold; `None` on the
/// idle teardown, which sends `Shutdown` only when there is nothing left to
/// stop and nobody waiting.
type ShutdownAck = Option<tokio::sync::oneshot::Sender<()>>;

/// Where a connection's stop hook lands. The task is spawned first and
/// reports its [`ConnHandles`] a moment later (from inside `run_queue`, once
/// its dispatch context exists), so the queue thread hands the callback this
/// slot and finds the hook in it afterwards.
type StopSlot = Rc<RefCell<Option<Box<dyn Fn()>>>>;

/// The connections one queue thread is running: each `run_queue` task and
/// the hook that asks it to wind down. Thread-local by construction (`Rc`),
/// like everything else a queue thread owns.
#[derive(Default)]
struct ConnTracker {
    conns: Vec<ConnEntry>,
}

struct ConnEntry {
    task: tokio::task::JoinHandle<()>,
    stop: StopSlot,
}

/// How long a queue thread waits for its stopped connections to finish
/// before reporting the stragglers and exiting anyway. Comfortably over a
/// healthy teardown (drain executing slots, join the send path) but well
/// under the control thread's wait for the ack.
const CONN_DRAIN_BUDGET_MS: u32 = 5_000;

impl ConnTracker {
    /// Track a freshly spawned connection task and the slot its `on_ctx`
    /// fills with the stop hook, first dropping the entries of connections
    /// that have since ended (the same prune-on-handoff the stats and nudge
    /// lists do, which bounds the list under connect churn).
    fn track(&mut self, task: tokio::task::JoinHandle<()>, stop: StopSlot) {
        self.conns.retain(|conn| !conn.task.is_finished());
        self.conns.push(ConnEntry { task, stop });
    }

    /// The queue half of the shutdown handshake: ask every connection to
    /// wind down, then wait for their tasks to return — so no command is
    /// still executing, and no backend op still in flight, when the caller
    /// goes on to release what the backends hold. Returns the number of
    /// connections still running when the budget ran out (0 = quiesced).
    async fn quiesce(&mut self) -> usize {
        for conn in &self.conns {
            if let Some(stop) = conn.stop.borrow().as_ref() {
                stop();
            }
        }
        let running =
            |conns: &Vec<ConnEntry>| conns.iter().filter(|c| !c.task.is_finished()).count();
        let mut waited = 0;
        while running(&self.conns) > 0 && waited < CONN_DRAIN_BUDGET_MS {
            // Poll rather than await the handles: a connection whose backend
            // op is wedged leaks its tasks (they never finish), and shutdown
            // must not hang on one. Same 2 ms cadence as a connection's own
            // teardown quiesce.
            let Ok(sleep) = ioutgt_uring::ops::sleep(Duration::from_millis(2)) else {
                break;
            };
            if sleep.await.is_err() {
                break;
            }
            waited += 2;
        }
        running(&self.conns)
    }
}

/// A queue thread's last act before it leaves its mailbox loop: report any
/// connection that outlasted the drain budget, then ack the handshake. The
/// ack is what lets the shutdown path conclude that this thread's IO has
/// stopped, so it is sent last — and unconditionally, or the waiter would
/// sit out its whole timeout for nothing.
fn finish_shutdown(name: &str, straggling: usize, ack: ShutdownAck) {
    if straggling > 0 {
        warn!(
            thread = %name,
            connections = straggling,
            "shutdown drain timed out; connections still running"
        );
    }
    if let Some(ack) = ack {
        let _ = ack.send(());
    }
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
        "tid": ioutgt_cpus::thread::current_tid(),
        "ring": { "parks": ring.parks, "sqes": ring.sqes,
                  "send_sqes": ring.send_sqes, "recv_sqes": ring.recv_sqes,
                  "read_sqes": ring.read_sqes, "write_sqes": ring.write_sqes,
                  "cqes": ring.cqes,
                  "rw_sq_b1": ring.rw_submit_hist[0], "rw_sq_b2": ring.rw_submit_hist[1],
                  "rw_sq_b4": ring.rw_submit_hist[2], "rw_sq_b8": ring.rw_submit_hist[3],
                  "rw_sq_b16": ring.rw_submit_hist[4], "rw_sq_b32": ring.rw_submit_hist[5] },
        "queues": queues,
        "retired": counters_json(retired),
    })
}

/// Build a queue thread's io_uring runtime, logging and returning `None`
/// on failure (the thread then exits without running its mailbox loop).
fn queue_runtime(name: &str) -> Option<QueueRuntime> {
    match QueueRuntime::new(RingConfig::default()) {
        Ok(rt) => Some(rt),
        Err(err) => {
            warn!(thread = %name, "queue runtime failed: {err}");
            None
        }
    }
}

/// Answer a queue thread's stats request: prune dead queues, send the JSON
/// snapshot, then zero every counter if `clear` was set. Runs on the owning
/// thread (the only place its `Cell` counters may be touched).
fn reply_thread_stats(
    name: &str,
    queues: &RefCell<Vec<Rc<QueueStats>>>,
    retired: &mut QueueStatsSnapshot,
    reply: StatsRequest,
    clear: bool,
) {
    prune_dead_queues(queues, retired);
    let borrowed = queues.borrow();
    let _ = reply.send(thread_stats_json(name, &borrowed, retired));
    if clear {
        clear_thread_stats(&borrowed, retired);
    }
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
            let Some(rt) = queue_runtime(&name) else {
                return;
            };
            rt.block_on(async move {
                let queues: Rc<RefCell<Vec<Rc<QueueStats>>>> = Rc::new(RefCell::new(Vec::new()));
                let mut retired = QueueStatsSnapshot::default();
                let mut conns = ConnTracker::default();
                loop {
                    match rx.recv().await {
                        Ok(IoMsg::Conn(conn)) => {
                            prune_dead_queues(&queues, &mut retired);
                            let stop: StopSlot = Rc::new(RefCell::new(None));
                            let on_ctx: OnCtx = {
                                let queues = Rc::clone(&queues);
                                let stop = Rc::clone(&stop);
                                Box::new(move |handles| {
                                    queues.borrow_mut().push(Rc::clone(&handles.stats));
                                    *stop.borrow_mut() = Some(handles.stop);
                                })
                            };
                            conns.track(tokio::task::spawn_local(T::run_queue(conn, on_ctx)), stop);
                        }
                        Ok(IoMsg::Stats { reply, clear }) => {
                            reply_thread_stats(&name, &queues, &mut retired, reply, clear);
                        }
                        Ok(IoMsg::Shutdown { ack }) => {
                            finish_shutdown(&name, conns.quiesce().await, ack);
                            return;
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

/// Fire one nudge on every live connection, dropping the dead ones on the way
/// through. `pick` selects which event of the [`ChangeNudge`] to raise.
fn nudge_live(live: &Rc<RefCell<Vec<ChangeNudge>>>, pick: impl Fn(&ChangeNudge) -> &dyn Fn()) {
    live.borrow_mut().retain(|n| {
        let alive = (n.alive)();
        if alive {
            (pick(n))();
        }
        alive
    });
}

/// Create the admin queue thread's mailbox and return its sender plus a
/// deferred spawn closure. The admin thread additionally keeps the live
/// connections' async-event nudges.
fn make_admin_thread<T: Transport>(
    name: String,
) -> io::Result<(MailboxSender<AdminMsg<T::Conn>>, PendingThread)> {
    let (tx, mut rx): AdminMailbox<T::Conn> = mailbox()?;
    let spawn: PendingThread = Box::new(move || {
        spawn_pinned(name.clone(), None, move || {
            let Some(rt) = queue_runtime(&name) else {
                return;
            };
            rt.block_on(async move {
                // Async-event nudges for the live admin connections; pruned
                // alongside the stats list on every handoff so the list stays
                // bounded under connect churn even if nothing ever changes.
                let live: Rc<RefCell<Vec<ChangeNudge>>> = Rc::new(RefCell::new(Vec::new()));
                let queues: Rc<RefCell<Vec<Rc<QueueStats>>>> = Rc::new(RefCell::new(Vec::new()));
                let mut retired = QueueStatsSnapshot::default();
                let mut conns = ConnTracker::default();
                loop {
                    match rx.recv().await {
                        Ok(AdminMsg::Conn(conn)) => {
                            prune_dead_queues(&queues, &mut retired);
                            live.borrow_mut().retain(|n| (n.alive)());
                            let stop: StopSlot = Rc::new(RefCell::new(None));
                            let on_ctx: OnCtx = {
                                let live = Rc::clone(&live);
                                let queues = Rc::clone(&queues);
                                let stop = Rc::clone(&stop);
                                Box::new(move |handles| {
                                    queues.borrow_mut().push(Rc::clone(&handles.stats));
                                    live.borrow_mut().push(handles.changes);
                                    *stop.borrow_mut() = Some(handles.stop);
                                })
                            };
                            conns.track(tokio::task::spawn_local(T::run_queue(conn, on_ctx)), stop);
                        }
                        Ok(AdminMsg::NsChanged) => nudge_live(&live, |n| n.ns_changed.as_ref()),
                        Ok(AdminMsg::AnaChanged) => nudge_live(&live, |n| n.ana_changed.as_ref()),
                        Ok(AdminMsg::DiscChanged) => {
                            nudge_live(&live, |n| n.disc_changed.as_ref());
                        }
                        Ok(AdminMsg::Stats { reply, clear }) => {
                            reply_thread_stats(&name, &queues, &mut retired, reply, clear);
                        }
                        Ok(AdminMsg::Shutdown { ack }) => {
                            finish_shutdown(&name, conns.quiesce().await, ack);
                            return;
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

/// For each IO queue thread, the CPU it is pinned to and the full online CPU
/// group it belongs to. CPUs are grouped evenly per NUMA/cluster/SMT locality
/// (`spread_cpus` — same spirit as the managed-IRQ spread nvme-tcp host queues
/// get, though not bit-identical to any particular kernel's grouping), one
/// group per IO thread; the thread is pinned to (and reported
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
    let groups = spread_cpus(io_threads, &topo);
    let mut cpus = Vec::with_capacity(io_threads);
    let mut group_lists = Vec::with_capacity(io_threads);
    for i in 0..io_threads {
        // groups come back empty when io_threads > possible CPUs; a group of
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

/// What [`build_port`] hands back: the port itself, plus the things opening
/// cluster-backed namespaces reveals along the way.
struct BuiltPort {
    /// The served port: subsystems, namespaces, and the transport limits.
    port: Arc<PortConfig<AnyBackend>>,
    /// The cluster namespaces whose ANA state wants tracking, one entry per
    /// (subsystem, cluster).
    ana_specs: Vec<AnaSpec>,
    /// The subsystems that are a cluster ACL's export, whose host list,
    /// namespace table and discovery generation want tracking.
    acl_specs: Vec<ClusterAclSpec>,
    /// Where the cluster filed this target among the holders of its volumes,
    /// if any of them is cluster storage this target registered for.
    holder_slot: Option<HolderSlot>,
}

/// Build the port snapshot from the configured subsystems.
/// `bound` is the listener's actual local address, so ephemeral ports
/// (`--listen …:0`) report the real port in discovery log entries and
/// LIST_CONTROLLER, not the configured 0. `trtype` is the serving fabric.
///
/// Opening the namespaces is what registers this target on a Sheepdog cluster,
/// so this is also where the two things that registration tells us come from:
/// the ANA specs of the cluster namespaces, and the holder slot the cluster
/// gave us — the target's share of the subsystem's cntlid space.
fn build_port(
    config: &TargetConfig,
    bound: SocketAddr,
    trtype: TransportType,
) -> io::Result<BuiltPort> {
    let mut subsystems = BTreeMap::new();
    let mut ana_specs = Vec::new();
    let mut acl_specs = Vec::new();
    let mut holder_slot: Option<HolderSlot> = None;
    for spec in &config.subsystems {
        let mut namespaces = BTreeMap::new();
        // The Sheepdog namespaces that registered this target as a holder of
        // their volume: what the subsystem's path list is read back from.
        let mut registered: Vec<Arc<AnyBackend>> = Vec::new();
        // Every Sheepdog namespace, by cluster: what ANA reporting tracks.
        // Registration plays no part — a volume opened with locking off still
        // has a home node, and this target is still either on it or not.
        let mut cluster_ns: BTreeMap<SocketAddr, ClusterNamespaces> = BTreeMap::new();
        for ns in &spec.namespaces {
            let backend = build_backend(&ns.backend, config.recv_buf_bytes > 0, Some(bound))
                .map_err(io::Error::other)?;
            // Test-only slow-disk emulation for memory namespaces.
            if config.mem_write_delay_us > 0
                && let AnyBackend::Memory(m) = &backend
            {
                m.set_write_delay_us(config.mem_write_delay_us);
            }
            // Configured identity wins, then the storage's own (a Sheepdog
            // VDI's inode uuid); otherwise derive from (NQN, nsid).
            let uuid = ns
                .uuid
                .or_else(|| backend.uuid())
                .unwrap_or_else(|| ioutgt_core::subsystem::namespace_uuid(&spec.nqn, ns.nsid));
            let backend = Arc::new(backend);
            if backend.as_sheepdog().is_some_and(|sd| sd.owner().is_some()) {
                registered.push(Arc::clone(&backend));
            }
            let namespace = Arc::new(Namespace::new(ns.nsid, Arc::clone(&backend), uuid));
            if let Some(sd) = backend.as_sheepdog() {
                cluster_ns
                    .entry(sd.cluster())
                    .or_default()
                    .push((Arc::clone(&namespace), sd.vid()));
            }
            namespaces.insert(ns.nsid, namespace);
        }
        let subsystem = Arc::new(
            Subsystem::new(
            spec.nqn.clone(),
            spec.serial.clone(),
            spec.model.clone(),
            HostAcl {
                allow_any_host: spec.allow_any_host,
                hosts: spec.allowed_hosts.clone(),
            },
            namespaces,
        )
        // Where the storage carries its own namespace count (a Sheepdog ACL
        // object's `max_data_id_nr`), report it as MNAN.
        .with_mnan(spec.mnan)
        // Cluster storage: the paths to a volume are unequal, and which one
        // this is depends on where its objects live, so report ANA. A
        // subsystem that *is* a cluster ACL's export reports it even with
        // zero cluster namespaces open right now — one hot-added later
        // (`refresh_cluster_namespaces`) needs `Subsystem::ana()` already on,
        // since nothing here can flip it afterward.
        .with_ana(!cluster_ns.is_empty() || spec.sheepdog_acl.is_some()),
        );
        ana_specs.extend(cluster_ns.into_iter().map(|(cluster, namespaces)| AnaSpec {
            cluster,
            subsystem: Arc::clone(&subsystem),
            namespaces: Mutex::new(namespaces),
        }));
        // A subsystem that *is* a cluster ACL object gets its host list and
        // namespace table from that object, and keeps getting them: both may
        // change under a running target. Seeding the discovery generation
        // needs no queue-thread pool, so it happens now; the tracking entry
        // itself (and its `notify` closure) waits for `track_cluster_acls`,
        // called once `control_loop` has one.
        if let Some(acl) = spec.sheepdog_acl {
            subsystem.observe_disc_genctr(acl.epoch);
            acl_specs.push(ClusterAclSpec {
                acl,
                subsystem: Arc::clone(&subsystem),
                fabric: bound,
                ring_enabled: config.recv_buf_bytes > 0,
                trtype,
            });
        }
        // A subsystem holding cluster volumes advertises every target that
        // holds them too, itself included, as a path to it.
        let slot = track_cluster_paths(&subsystem, registered, trtype);
        // One registry serves the whole port, so its cntlid partition comes
        // from a single slot: the one in the lowest-vid volume, the same
        // volume every target holding it picks (see [`holder_cntlid_slice`]).
        // A port whose subsystems hold disjoint volumes has no such common
        // volume — then this is simply the first-registered one's slot, which
        // still collides with nobody serving *that* subsystem.
        if slot.is_some_and(|s| holder_slot.is_none_or(|held| s.vid < held.vid)) {
            holder_slot = slot;
        }
        subsystems.insert(spec.nqn.clone(), subsystem);
    }
    let port = Arc::new(PortConfig {
        traddr: bound.ip().to_string(),
        trsvcid: bound.port().to_string(),
        trtype,
        max_qid: u16::try_from(config.io_threads.max(1)).unwrap_or(1),
        io_queue_size: config.io_queue_size,
        queue_buf_bytes: config.queue_buf_bytes,
        recv_buf_bytes: config.recv_buf_bytes,
        poll: config.poll,
        subsystems,
    });
    // The backends are now open — and a Sheepdog one holds its VDI lock on
    // the cluster. Register the port so [`shutdown`] can hand that back.
    live_ports().push(Arc::clone(&port));
    Ok(BuiltPort {
        port,
        ana_specs,
        acl_specs,
        holder_slot,
    })
}

// ---------------------------------------------------------------------------
// Cluster paths: who else serves this subsystem's volumes
// ---------------------------------------------------------------------------

/// One subsystem's cluster-backed namespaces, and the path list they feed.
///
/// Opening a Sheepdog namespace registers this target's fabric address as a
/// holder of that volume (`SheepdogBackend::open`); the cluster's list of
/// holders is therefore the list of targets serving it, and the union across
/// the subsystem's volumes is the set of paths its discovery entries
/// advertise. Nothing here is a separate cluster object: the namespaces the
/// subsystem already opened *are* the registration.
struct ClusterPaths {
    /// The cluster the volumes live on.
    cluster: SocketAddr,
    /// The address this target registered as their holder, and the one its own
    /// discovery entry carries: what a host must connect to to reach it.
    owner: SocketAddr,
    /// The registered namespaces' backends, read for their vids and
    /// re-registered when the cluster stops listing us.
    namespaces: Vec<Arc<AnyBackend>>,
    /// Transport of the port that opened them.
    trtype: TransportType,
    /// The subsystem whose path list the holders feed.
    subsystem: Arc<Subsystem<AnyBackend>>,
}

/// Every subsystem with cluster-backed paths. Same lifetime rule as
/// [`LIVE_PORTS`]: appended at startup, drained once by [`shutdown`] — which
/// is what stops the refresh thread. The registrations themselves are handed
/// back by the backends (`AnyBackend::shutdown`), not from here.
static CLUSTER_PATHS: Mutex<Vec<Arc<ClusterPaths>>> = Mutex::new(Vec::new());

/// [`CLUSTER_PATHS`], recovered from poisoning (see [`live_ports`]).
fn cluster_paths() -> std::sync::MutexGuard<'static, Vec<Arc<ClusterPaths>>> {
    CLUSTER_PATHS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Raise a Discovery Log Page Change notice on this target's live discovery
/// controllers.
///
/// A single hook rather than one per tracked cluster object: the discovery log
/// is per *port*, not per subsystem — every subsystem on the port contributes
/// entries to the same page — so whatever moved, the notice is the same one and
/// goes to the same controllers. `None` before [`control_loop`] installs it
/// (the seeding reads in [`build_port`] run first, and have nobody to tell) and
/// again after [`shutdown`] clears it.
static DISC_NOTIFY: Mutex<Option<Box<dyn Fn() + Send + Sync>>> = Mutex::new(None);

/// [`DISC_NOTIFY`], recovered from poisoning (see [`live_ports`]).
fn disc_notify() -> std::sync::MutexGuard<'static, Option<Box<dyn Fn() + Send + Sync>>> {
    DISC_NOTIFY
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Wire the discovery-change notice to the target's queue-thread pool, which
/// [`control_loop`] owns and may currently have torn down — then the notice
/// no-ops, correctly: a pool that is down has no controllers to tell, and a
/// host that connects later reads the new log outright.
fn track_discovery_changes<C: Send + 'static>(senders: &Arc<Mutex<Option<PoolSenders<C>>>>) {
    let pool = Arc::clone(senders);
    *disc_notify() = Some(Box::new(move || {
        if let Some(pool) = pool.lock().unwrap_or_else(|p| p.into_inner()).as_ref() {
            pool.admin.send(AdminMsg::DiscChanged);
        }
    }));
}

/// Tell the hosts the discovery log moved, if there is a pool to tell them
/// through ([`track_discovery_changes`]).
fn notify_discovery_changed() {
    if let Some(notify) = disc_notify().as_ref() {
        notify();
    }
}

/// How often the refresh thread re-reads the holder lists, object locality
/// and ACL membership. All three are cold paths — a target joining or leaving
/// is rare, a volume's objects move only on a cluster rebalance, and a `dog
/// acl add member` is an administrator typing — so this only has to be well
/// inside the time a host takes to notice, seconds not milliseconds.
const CLUSTER_REFRESH_INTERVAL: Duration = Duration::from_secs(10);

/// Report a cluster that would not answer a control-plane query, at the level
/// the situation deserves: a warning while the target serves, because it means
/// the hosts' view of the paths or of ANA is going stale — but only a debug
/// line once [`shutdown`] has begun, when the cluster (or the connection to it)
/// going away is part of the teardown and the state is about to be dropped.
fn cluster_unreachable(subsystem: &str, cluster: SocketAddr, err: &io::Error, what: &str) {
    if shutting_down() {
        debug!(subsystem, %cluster, %err, "sheepdog: {what} unavailable during shutdown");
    } else {
        warn!(subsystem, %cluster, %err, "sheepdog: {what} unavailable");
    }
}

/// Seed `subsystem`'s path list from the holders of the cluster volumes it
/// just registered for, and arrange for it to be kept up.
///
/// `registered` are the namespace backends that took a registration — Sheepdog
/// ones with an owner. A subsystem with none of those (no cluster storage, or
/// locking turned off) keeps the default path list: this target alone.
///
/// Best-effort by design: a cluster that will not answer costs this target its
/// multi-path discovery entries, not its ability to serve the namespaces it
/// already opened.
///
/// Returns the holder slot the cluster gave this target, for the caller to
/// partition its cntlid space with ([`holder_cntlid_slice`]).
fn track_cluster_paths(
    subsystem: &Arc<Subsystem<AnyBackend>>,
    registered: Vec<Arc<AnyBackend>>,
    trtype: TransportType,
) -> Option<HolderSlot> {
    let (cluster, owner) = registered.first().and_then(|b| {
        let sd = b.as_sheepdog()?;
        Some((sd.cluster(), sd.owner()?))
    })?;
    // Volumes on a second cluster have their own holder lists, which say
    // nothing about who serves *this* one's; a subsystem spanning clusters
    // advertises the paths of the first and warns about the rest.
    let (namespaces, other): (Vec<_>, Vec<_>) = registered
        .into_iter()
        .partition(|b| b.as_sheepdog().is_some_and(|sd| sd.cluster() == cluster));
    if !other.is_empty() {
        warn!(subsystem = %subsystem.nqn, %cluster, ignored = other.len(),
              "sheepdog: namespaces on another cluster do not contribute paths");
    }
    let paths = Arc::new(ClusterPaths {
        cluster,
        owner,
        namespaces,
        trtype,
        subsystem: Arc::clone(subsystem),
    });
    // Seeding, not a change: the counter should still read as the generation
    // the cluster states, and there is no controller to notify yet anyway.
    let slot = refresh_cluster_paths(&paths, false);
    cluster_paths().push(paths);
    start_cluster_refresh();
    slot
}

/// Where the cluster filed this target among the holders of one volume.
///
/// Sheepdog's shared VDI lock is a fixed array of [`VDI_MAX_HOLDERS`]
/// participant slots, and a holder keeps its slot until it unregisters — the
/// cluster leaves a hole rather than renumbering the rest. That makes the slot
/// a small, stable, cluster-assigned identity for "which of the targets
/// serving this volume am I", which is exactly what is needed to partition an
/// identifier space no single target can allocate from alone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct HolderSlot {
    /// The volume whose participant list the slot is in.
    vid: u32,
    /// The slot itself, `0 ..` [`VDI_MAX_HOLDERS`].
    slot: u16,
}

/// Narrow this process's cntlid slice to the partition belonging to its
/// Sheepdog holder slot.
///
/// CNTLIDs must be unique per subsystem *across every target serving it*
/// (Linux rejects a duplicate in `nvme_validate_cntlid`), and the targets
/// fronting one cluster volume never talk to each other — they only share what
/// the cluster records. What the cluster records is the participant list, and a
/// slot in it is held by exactly one target: cut the cntlid range into
/// [`VDI_MAX_HOLDERS`] equal partitions, take the one matching our slot, and no
/// two paths to the same subsystem can mint the same cntlid, however many
/// controllers each of them has. The same reasoning as the per-port slice
/// [`TargetConfig::apply_file`] hands out, one level down: this subdivides
/// whatever that left us.
///
/// Falls back to the whole slice — the pre-cluster behaviour — when there is
/// no slot to key on (local storage, `?nolock`, or a cluster that would not
/// answer) or when the slice is too small to cut, which are also the cases
/// where nothing else is minting cntlids for the subsystem or where a wrong
/// answer is worse than an unpartitioned one.
fn holder_cntlid_slice(range: (u16, u16), holder: Option<HolderSlot>) -> (u16, u16) {
    let (base, top) = range;
    let Some(HolderSlot { vid, slot }) = holder else {
        return range;
    };
    let parts = u32::from(VDI_MAX_HOLDERS);
    let width = (u32::from(top - base) + 1) / parts;
    if width == 0 || u32::from(slot) >= parts {
        warn!(
            slot,
            vid = format_args!("{vid:x}"),
            cntlid_min = base,
            cntlid_max = top,
            "sheepdog: cntlid range not partitionable; paths may collide"
        );
        return range;
    }
    let min = u32::from(base) + u32::from(slot) * width;
    // The last partition keeps the remainder of an uneven cut, so the
    // boundaries are the same wherever they are computed.
    let max = if u32::from(slot) + 1 == parts {
        u32::from(top)
    } else {
        min + width - 1
    };
    let (min, max) = (
        u16::try_from(min).expect("min <= top"),
        u16::try_from(max).expect("max <= top"),
    );
    info!(
        slot,
        vid = format_args!("{vid:x}"),
        cntlid_min = min,
        cntlid_max = max,
        "sheepdog: cntlid partition from holder slot"
    );
    (min, max)
}

/// Re-read the holders of a subsystem's volumes into its path list,
/// re-registering any volume this target is no longer listed for (the cluster
/// dropped us while we were away — a restart, or an eviction and rejoin).
///
/// Returns where the cluster files this target among the volumes' holders,
/// which the first call — the one from [`track_cluster_paths`], before any
/// controller exists — turns into this target's cntlid partition
/// ([`holder_cntlid_slice`]). Later calls, from the refresh thread, have
/// nowhere to put it: the partition is fixed for the process.
///
/// A holder joining or leaving is a change to what the discovery log says, and
/// one no cluster counter records — a `vdi_epoch` moves with the ACL's volume
/// membership, not with who holds the volumes. So when the path list really
/// changes, `announce` bumps the subsystem's discovery generation and tells the
/// hosts. It is off for the seeding call above, where the list goes from empty
/// to whatever the cluster says and nobody has read it yet.
fn refresh_cluster_paths(paths: &ClusterPaths, announce: bool) -> Option<HolderSlot> {
    let vids: Vec<u32> = paths
        .namespaces
        .iter()
        .filter_map(|b| Some(b.as_sheepdog()?.vid()))
        .collect();
    let mut holders = match ioutgt_backend::vdi_holders(paths.cluster, &vids) {
        Ok(holders) => holders,
        Err(err) => {
            cluster_unreachable(&paths.subsystem.nqn, paths.cluster, &err, "holder list");
            return None;
        }
    };
    // Started before [`shutdown`] did, finishing after: a registration retaken
    // now is one the release walk may already have passed, so it would outlive
    // the process. The path list is equally moot at this point.
    if shutting_down() {
        return None;
    }
    let mut retaken = false;
    for (backend, holders) in paths.namespaces.iter().zip(&holders) {
        if holders.iter().any(|h| h.addr == paths.owner) {
            continue;
        }
        let Some(sd) = backend.as_sheepdog() else {
            continue;
        };
        match sd.reregister() {
            // A namespace whose registration cannot be retaken keeps serving
            // IO; it just stops contributing this target to its own holder
            // list until the cluster takes it again.
            Err(err) => warn!(subsystem = %paths.subsystem.nqn, vid = sd.vid(), %err,
                              "sheepdog: re-registration failed"),
            Ok(()) => retaken = true,
        }
    }
    if retaken {
        holders = ioutgt_backend::vdi_holders(paths.cluster, &vids).unwrap_or(holders);
    }
    // Our slot among the holders of the lowest-vid volume this subsystem
    // registered for. The lowest vid is a choice every target serving the
    // volume makes the same way, and within one volume the cluster hands out
    // each slot once — which is what makes the slot usable as a partition id.
    let slot = vids
        .iter()
        .zip(&holders)
        .min_by_key(|&(vid, _)| *vid)
        .and_then(|(&vid, holders)| {
            let index = holders.iter().find(|h| h.addr == paths.owner)?.index;
            Some(HolderSlot { vid, slot: index })
        });
    // The union across the subsystem's volumes: a target holding any of them
    // is a path to the subsystem. Sorted so every target computes the same
    // PORTID — the index in this list — for the same peer.
    let mut addrs: Vec<SocketAddr> = holders.into_iter().flatten().map(|h| h.addr).collect();
    addrs.sort_unstable();
    addrs.dedup();
    // Nobody holds anything: a cluster mid-restart, most likely. Leave the
    // path list as it was rather than flapping the hosts' view of it.
    if addrs.is_empty() {
        return slot;
    }
    // Every holder is another ioutgt target fronting the same cluster, so it
    // speaks the fabric this one does; the cluster records an address, not a
    // transport.
    let changed = paths.subsystem.set_ports(
        addrs
            .iter()
            .enumerate()
            .map(|(index, addr)| SubsystemPort {
                traddr: addr.ip().to_string(),
                trsvcid: addr.port().to_string(),
                trtype: paths.trtype,
                portid: u16::try_from(index).unwrap_or(u16::MAX),
            })
            .collect(),
    );
    if changed && announce {
        // The log's GENCTR must move before any host can be told to re-read it,
        // or the re-read looks unchanged and the notice is wasted.
        paths.subsystem.bump_disc_genctr();
        info!(subsystem = %paths.subsystem.nqn, paths = addrs.len(),
              genctr = paths.subsystem.disc_genctr(),
              "sheepdog: subsystem path list changed");
        notify_discovery_changed();
    }
    slot
}

// ---------------------------------------------------------------------------
// Cluster ANA: which of a subsystem's volumes live on the node we talk to
// ---------------------------------------------------------------------------

/// Cluster-backed namespaces of one subsystem, each with the vid behind it.
type ClusterNamespaces = Vec<(Arc<Namespace<AnyBackend>>, u32)>;

/// One subsystem's Sheepdog namespaces on one cluster, before the queue-thread
/// pool that will carry their ANA notices exists ([`build_port`] runs first).
///
/// The namespace list is mutable so a namespace hot-added later
/// ([`track_cluster_backend`]) can join it in place — this entry's `notify`
/// closure ([`ClusterAna::notify`]) captures the one queue-thread pool this
/// process actually has, and there is no way to reconstruct that closure from
/// the refresh thread, which is why hot-add mutates through it instead of
/// replacing the entry.
struct AnaSpec {
    /// The cluster gateway those namespaces are served through.
    cluster: SocketAddr,
    /// The subsystem they belong to; the one whose ANA state they are.
    subsystem: Arc<Subsystem<AnyBackend>>,
    /// Each namespace and the vid behind it.
    namespaces: Mutex<ClusterNamespaces>,
}

/// An [`AnaSpec`] wired to the target serving it.
struct ClusterAna {
    spec: AnaSpec,
    /// Raise an ANA Change notice on this target's live controllers. Per
    /// entry, not a shared global: a test binary (and in principle a future
    /// multi-port target) can have more than one queue-thread pool alive in
    /// one process, each with its own `senders`, and a single global closure
    /// would silently hand one subsystem's notices to another's pool.
    notify: Box<dyn Fn() + Send + Sync>,
}

/// Every subsystem reporting cluster-derived ANA. Same lifetime rule as
/// [`CLUSTER_PATHS`]: appended at startup, drained once by [`shutdown`].
static CLUSTER_ANA: Mutex<Vec<Arc<ClusterAna>>> = Mutex::new(Vec::new());

/// [`CLUSTER_ANA`], recovered from poisoning (see [`live_ports`]).
fn cluster_ana() -> std::sync::MutexGuard<'static, Vec<Arc<ClusterAna>>> {
    CLUSTER_ANA
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Seed the ANA state of every cluster-backed namespace `build_port` found,
/// and arrange for it to be kept up.
///
/// `senders` is the target's queue-thread pool, still being built: an ANA
/// change reaches the hosts as an async event on the admin thread's live
/// controllers, and a change that happens before the pool exists needs no
/// event at all (the state is already right when the host first reads it).
fn track_cluster_ana<C>(specs: Vec<AnaSpec>, senders: &Arc<Mutex<Option<PoolSenders<C>>>>)
where
    C: Send + 'static,
{
    if specs.is_empty() {
        return;
    }
    for spec in specs {
        let pool = Arc::clone(senders);
        let ana = Arc::new(ClusterAna {
            spec,
            notify: Box::new(move || {
                if let Some(pool) = pool.lock().unwrap_or_else(|p| p.into_inner()).as_ref() {
                    pool.admin.send(AdminMsg::AnaChanged);
                }
            }),
        });
        refresh_cluster_ana(&ana);
        cluster_ana().push(ana);
    }
    start_cluster_refresh();
}

/// Re-derive the ANA placement of a subsystem's cluster namespaces: each
/// volume's group is the zone of the cluster that owns its inode object's
/// placement on the hash ring — the same value however many targets ask, and
/// through whichever gateway — and this target's path to that group is
/// optimized exactly when the gateway it talks to is itself in that zone.
///
/// Best-effort, like the path refresh: a cluster that will not answer leaves
/// every namespace in the state it was last known to be in rather than
/// flapping the hosts' path choice on a transient failure. A namespace whose
/// group the ring could not resolve (`ClusterAnaState::grpids`, an empty-ring
/// cluster) is skipped the same way — there is nothing sound to report for it
/// this round, not a reason to distrust the rest.
fn refresh_cluster_ana(ana: &ClusterAna) {
    let spec = &ana.spec;
    // Cloned out from under the lock: the cluster round trip below is
    // blocking IO, and nothing else needs the lock held across it.
    let namespaces = spec
        .namespaces
        .lock()
        .expect("ana namespaces poisoned")
        .clone();
    let vids: Vec<u32> = namespaces
        .iter()
        .map(|&(_, vid)| vid)
        .collect();
    let state = match ioutgt_backend::cluster_ana_state(spec.cluster, &vids) {
        Ok(state) => state,
        Err(err) => {
            cluster_unreachable(&spec.subsystem.nqn, spec.cluster, &err, "ANA placement");
            return;
        }
    };
    let mut changed = spec.subsystem.merge_ana_zones(&state.zones);
    for ((ns, vid), grpid) in namespaces.iter().zip(state.grpids) {
        let Some(grpid) = grpid else { continue };
        let optimized = state.own_zone == Some(grpid);
        if spec.subsystem.set_ana_state(ns, grpid, optimized) {
            info!(subsystem = %spec.subsystem.nqn, nsid = ns.nsid,
                  vid = format_args!("{vid:x}"), grpid, optimized, "ANA state changed");
            changed = true;
        }
    }
    if changed {
        (ana.notify)();
    }
}

// ---------------------------------------------------------------------------
// Cluster ACLs: who the cluster says may connect, and which volumes it says
// the subsystem is made of
// ---------------------------------------------------------------------------

/// One subsystem that *is* the export of a cluster ACL object — whole-cluster
/// mode's own subsystems, never a hand-configured one (`%ACL`'s single-VDI
/// form leaves [`SubsystemConfig::sheepdog_acl`] `None`; see its doc).
///
/// Three things ride on the same ACL inode and so the same refresh: the host
/// list (the names `dog acl add member` writes are the hostnqns the
/// subsystem admits, [`ioutgt_backend::acl_state`]), the discovery-log
/// generation (`vdi_epoch`), and — since the ACL's member *VDIs* are exactly
/// this subsystem's namespaces — the namespace table itself: `dog acl add
/// vdi`/`remove vdi` on a running cluster adds or removes a namespace here
/// too, not only a discovery-log path entry.
struct ClusterAcl {
    /// The cluster gateway the ACL object is read from.
    cluster: SocketAddr,
    /// The ACL object's own vid.
    vid: u32,
    /// The subsystem the ACL became, whose host list and namespace table this
    /// keeps current.
    subsystem: Arc<Subsystem<AnyBackend>>,
    /// Whether a member VDI discovered here should take the cluster's shared
    /// lock, matching every namespace this ACL exported at startup
    /// ([`SheepdogAcl::lock`]).
    lock: bool,
    /// This target's own fabric address, for a hot-added namespace to
    /// register as the volume's holder — the same address `build_port`
    /// passed every namespace opened at startup.
    fabric: SocketAddr,
    /// Whether the port's queue threads use the io_uring provided-buffer
    /// ring (`--recv-buf-mb`), for a hot-added namespace's backend to be
    /// built identically to every other one (`build_backend`'s
    /// `ring_enabled`).
    ring_enabled: bool,
    /// Transport of the port that opened them, for a hot-added namespace's
    /// path-list entry ([`ClusterPaths::trtype`]).
    trtype: TransportType,
    /// Raise the changed-namespaces notice on this target's live controllers
    /// when the ACL's member VDIs move. Per entry, not a shared global, for
    /// the same reason [`ClusterAna::notify`] is: more than one queue-thread
    /// pool can be alive in one process (every target a test binary spawns
    /// has its own), and a single global closure would hand one subsystem's
    /// notice to another's pool.
    notify: Box<dyn Fn() + Send + Sync>,
}

/// A [`ClusterAcl`] before the queue-thread pool that will carry its
/// namespace-changed notices exists ([`build_port`] runs first) — the same
/// split [`AnaSpec`]/[`ClusterAna`] makes, for the same reason.
struct ClusterAclSpec {
    acl: SheepdogAcl,
    subsystem: Arc<Subsystem<AnyBackend>>,
    fabric: SocketAddr,
    ring_enabled: bool,
    trtype: TransportType,
}

/// Every subsystem whose host ACL, namespace table, or discovery generation
/// comes off a cluster ACL object. Same lifetime rule as [`CLUSTER_PATHS`]:
/// appended at startup, drained once by [`shutdown`].
static CLUSTER_ACLS: Mutex<Vec<Arc<ClusterAcl>>> = Mutex::new(Vec::new());

/// [`CLUSTER_ACLS`], recovered from poisoning (see [`live_ports`]).
fn cluster_acls() -> std::sync::MutexGuard<'static, Vec<Arc<ClusterAcl>>> {
    CLUSTER_ACLS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Keep every `spec.subsystem` up with the cluster ACL object it came from.
///
/// No read here, unlike [`track_cluster_paths`]: the config already carries
/// the members as they were when it enumerated the cluster, and the subsystem
/// was built with them. The first re-read is the refresh thread's.
///
/// `senders` is the target's queue-thread pool, still being built: a
/// namespace add/remove reaches the hosts as an async event on the admin
/// thread's live controllers, and one that happens before the pool exists
/// needs no event at all (the table is already right when the host first
/// reads it).
fn track_cluster_acls<C>(specs: Vec<ClusterAclSpec>, senders: &Arc<Mutex<Option<PoolSenders<C>>>>)
where
    C: Send + 'static,
{
    if specs.is_empty() {
        return;
    }
    for spec in specs {
        let pool = Arc::clone(senders);
        cluster_acls().push(Arc::new(ClusterAcl {
            cluster: spec.acl.cluster,
            vid: spec.acl.vid,
            subsystem: spec.subsystem,
            lock: spec.acl.lock,
            fabric: spec.fabric,
            ring_enabled: spec.ring_enabled,
            trtype: spec.trtype,
            notify: Box::new(move || {
                if let Some(pool) = pool.lock().unwrap_or_else(|p| p.into_inner()).as_ref() {
                    pool.admin.send(AdminMsg::NsChanged);
                }
            }),
        }));
    }
    start_cluster_refresh();
}

/// Re-read one ACL object's member names into its subsystem's host ACL, its
/// `vdi_epoch` into the subsystem's discovery-log generation, and its member
/// VDIs into the subsystem's namespace table ([`refresh_cluster_namespaces`]).
///
/// Best-effort, like the path and ANA refreshes: a cluster that will not
/// answer leaves the host list, generation and namespace table as they were —
/// the last state the cluster did state, which is a better answer than
/// either locking everyone out or letting everyone in because a gateway was
/// briefly down.
///
/// A host dropped from the ACL keeps the controllers it already has (see
/// [`Subsystem::set_host_acl`]); what changes is who may Connect next.
fn refresh_cluster_acl(acl: &ClusterAcl) {
    let state = match ioutgt_backend::acl_state(acl.cluster, acl.vid) {
        Ok(state) => state,
        Err(err) => {
            cluster_unreachable(&acl.subsystem.nqn, acl.cluster, &err, "ACL member list");
            return;
        }
    };
    // The cluster moved its own version of the ACL — `dog acl add vdi` or
    // `remove vdi`, the volumes the subsystem is made of. Nothing this target
    // serves changes under it (its namespaces were fixed when it opened them),
    // but the group a host discovers is not the one it discovered before, so
    // send the discovery hosts back to the log.
    if acl.subsystem.observe_disc_genctr(state.epoch) {
        info!(subsystem = %acl.subsystem.nqn, vid = format_args!("{:x}", acl.vid),
              genctr = state.epoch, "sheepdog: ACL epoch advanced");
        notify_discovery_changed();
    }
    if acl.subsystem.set_host_acl(state.host_acl()) {
        let host_acl = acl.subsystem.host_acl();
        if host_acl.allow_any_host {
            info!(subsystem = %acl.subsystem.nqn, vid = format_args!("{:x}", acl.vid),
                  "sheepdog: ACL has no members left; the subsystem admits any host again");
        } else {
            info!(subsystem = %acl.subsystem.nqn, vid = format_args!("{:x}", acl.vid),
                  hosts = ?host_acl.hosts, "sheepdog: ACL members changed");
        }
    }
    refresh_cluster_namespaces(acl);
}

/// Re-read the ACL's member VDI list and add or remove namespaces on
/// `acl.subsystem` to match: `dog acl add vdi`/`remove vdi` on a running
/// cluster changes what the subsystem exports, exactly as it does at
/// startup ([`ioutgt_control::cli`]'s `acl_subsystem`) — the refresh just
/// keeps doing what that did once.
///
/// Best-effort like the rest: a cluster that will not answer leaves the
/// namespace table as it was. A member vid this target cannot open (the
/// volume vanished between the ACL read and the open, or the cluster refuses
/// the lock) is skipped with a warning rather than failing the whole
/// refresh — the other members still need their turn. Only nsids this ACL
/// could plausibly have added are ever removed: a namespace added some other
/// way (`ADD_NAMESPACE`, or a different backend entirely) is not this
/// refresh's to take back.
fn refresh_cluster_namespaces(acl: &ClusterAcl) {
    let members = match ioutgt_backend::acl_members(acl.cluster, acl.vid) {
        Ok(members) => members,
        Err(err) => {
            cluster_unreachable(&acl.subsystem.nqn, acl.cluster, &err, "ACL member VDI list");
            return;
        }
    };
    // A snapshot's inode names the ACL too (its own vid stays acl_id), but a
    // frozen VDI is read-only and past `acl_subsystem` skips it the same way.
    let wanted: std::collections::BTreeMap<u32, &ioutgt_backend::VdiInfo> = members
        .iter()
        .filter(|vdi| !vdi.snapshot)
        .map(|vdi| (vdi.vid, vdi))
        .collect();
    let existing = acl.subsystem.snapshot();

    for (&nsid, ns) in existing.iter() {
        if wanted.contains_key(&nsid) {
            continue;
        }
        let Some(sd) = ns.backend.as_sheepdog() else {
            continue;
        };
        if sd.cluster() != acl.cluster {
            continue;
        }
        let Ok(removed) = acl.subsystem.remove_namespace(nsid) else {
            continue;
        };
        // Release the cluster lock now rather than trust the table's last
        // `Arc` to drop it: an IO queue that cached an older snapshot
        // (`NsCache`) keeps this namespace's backend alive for as long as it
        // goes without another command to notice the table moved on — on an
        // idle or failed-over-away-from path, that can be indefinitely, and
        // the VDI would stay locked long after the ACL, and `nvme list-ns`,
        // both agree it is gone.
        removed.backend.shutdown();
        info!(subsystem = %acl.subsystem.nqn, nsid, vid = format_args!("{:x}", sd.vid()),
              acl = format_args!("{:x}", acl.vid), "sheepdog VDI unexported");
        untrack_cluster_backend(&acl.subsystem, &removed.backend);
        (acl.notify)();
    }

    for (&nsid, vdi) in &wanted {
        if existing.contains_key(&nsid) {
            continue;
        }
        let backend = ioutgt_control::server::build_backend(
            &BackendConfig::Sheepdog {
                addr: acl.cluster.to_string(),
                vdi: vdi.name.clone(),
                tag: None,
                // Whole-cluster mode names the subsystem after the ACL
                // verbatim, so the subsystem's own NQN *is* the ACL's name —
                // no other record of it is needed here.
                acl: Some(acl.subsystem.nqn.clone()),
                lock: acl.lock,
            },
            acl.ring_enabled,
            Some(acl.fabric),
        );
        let backend = match backend {
            Ok(backend) => backend,
            Err(err) => {
                warn!(subsystem = %acl.subsystem.nqn, nsid, vdi = %vdi.name, %err,
                      "sheepdog: could not open a newly added VDI");
                continue;
            }
        };
        let uuid = vdi.uuid.unwrap_or_else(|| {
            ioutgt_core::subsystem::namespace_uuid(&format!("sheepdog:{}", vdi.name), vdi.vid)
        });
        let namespace = Namespace::new(nsid, Arc::new(backend), uuid);
        let ns = match acl.subsystem.add_namespace(namespace) {
            Ok(ns) => ns,
            Err(err) => {
                warn!(subsystem = %acl.subsystem.nqn, nsid, vdi = %vdi.name, %err,
                      "sheepdog: newly added VDI's nsid is already in use");
                continue;
            }
        };
        info!(nsid, vdi = %vdi.name, acl = %acl.subsystem.nqn, bytes = vdi.size,
              "sheepdog VDI exported");
        track_cluster_backend(&acl.subsystem, &ns, acl.trtype);
        (acl.notify)();
    }
}

/// Add a namespace [`refresh_cluster_namespaces`] just opened into this
/// subsystem's path and ANA tracking, alongside every namespace opened at
/// startup — otherwise a hot-added volume would light up in `nvme list-ns`
/// but never contribute a discovery-log path or ever get an ANA state past
/// its placeholder.
fn track_cluster_backend(
    subsystem: &Arc<Subsystem<AnyBackend>>,
    ns: &Arc<Namespace<AnyBackend>>,
    trtype: TransportType,
) {
    let Some(sd) = ns.backend.as_sheepdog() else {
        return;
    };
    if let Some(owner) = sd.owner() {
        let mut paths = cluster_paths();
        if let Some(pos) = paths
            .iter()
            .position(|p| Arc::ptr_eq(&p.subsystem, subsystem))
        {
            let old = Arc::clone(&paths[pos]);
            let mut namespaces = old.namespaces.clone();
            namespaces.push(Arc::clone(&ns.backend));
            paths[pos] = Arc::new(ClusterPaths {
                cluster: old.cluster,
                owner: old.owner,
                namespaces,
                trtype: old.trtype,
                subsystem: Arc::clone(subsystem),
            });
        } else {
            // The subsystem's first locked Sheepdog namespace: nothing to
            // append to yet, so seed a fresh entry the way
            // `track_cluster_paths` does at startup.
            drop(paths);
            let fresh = Arc::new(ClusterPaths {
                cluster: sd.cluster(),
                owner,
                namespaces: vec![Arc::clone(&ns.backend)],
                trtype,
                subsystem: Arc::clone(subsystem),
            });
            let _ = refresh_cluster_paths(&fresh, false);
            cluster_paths().push(fresh);
        }
    }

    let ana = cluster_ana();
    if let Some(entry) = ana
        .iter()
        .find(|a| Arc::ptr_eq(&a.spec.subsystem, subsystem))
    {
        entry
            .spec
            .namespaces
            .lock()
            .expect("ana namespaces poisoned")
            .push((Arc::clone(ns), sd.vid()));
    } else {
        // The subsystem's first Sheepdog namespace at all (ANA does not
        // depend on locking, unlike the path list above) — and nothing this
        // function can do about it: a fresh entry needs a `notify` closure
        // over the queue-thread pool, which only exists where `senders` does
        // (`track_cluster_ana`, called once from `control_loop` before this
        // refresh thread starts). A subsystem that reaches this had zero
        // cluster namespaces at startup, so restarting the target is what
        // picks the new one up for ANA; its namespace-table entry (already
        // added by the caller) and path-list entry (above) are unaffected.
        warn!(subsystem = %subsystem.nqn, nsid = ns.nsid,
              "sheepdog: first cluster namespace added after startup; ANA for it needs a restart");
    }
}

/// Drop `backend` from this subsystem's path and ANA tracking, so a namespace
/// [`Subsystem::remove_namespace`] just forgot does not linger — and keep
/// serving IO through it, in either refresh.
fn untrack_cluster_backend(subsystem: &Arc<Subsystem<AnyBackend>>, backend: &Arc<AnyBackend>) {
    let mut paths = cluster_paths();
    if let Some(pos) = paths
        .iter()
        .position(|p| Arc::ptr_eq(&p.subsystem, subsystem))
    {
        let old = Arc::clone(&paths[pos]);
        let namespaces: Vec<_> = old
            .namespaces
            .iter()
            .filter(|b| !Arc::ptr_eq(b, backend))
            .cloned()
            .collect();
        paths[pos] = Arc::new(ClusterPaths {
            cluster: old.cluster,
            owner: old.owner,
            namespaces,
            trtype: old.trtype,
            subsystem: Arc::clone(subsystem),
        });
    }
    drop(paths);

    let ana = cluster_ana();
    if let Some(entry) = ana
        .iter()
        .find(|a| Arc::ptr_eq(&a.spec.subsystem, subsystem))
    {
        entry
            .spec
            .namespaces
            .lock()
            .expect("ana namespaces poisoned")
            .retain(|(ns, ..)| !Arc::ptr_eq(&ns.backend, backend));
    }
}

/// Re-derive every tracked cluster's path list, ANA state and host ACL now
/// instead of waiting for the next [`CLUSTER_REFRESH_INTERVAL`] tick, and
/// return how many clusters were visited. Hosts see whatever changed exactly
/// as they would from the refresh thread: a new discovery log, an ANA Change
/// notice, a Connect the subsystem now takes (or no longer does).
///
/// Visits nothing, and so returns zero, once [`shutdown`] has begun: there is
/// no point re-reading state that is being released, and the refresh thread
/// takes the zero as its cue to end.
///
/// Serialized against itself ([`REFRESH_LOCK`]): production only ever has one
/// caller (the background thread's own loop, one tick after the last
/// finishes), but this is `pub` precisely so a caller that wants a change
/// visible *now* — a test, or a future "refresh now" control op — does not
/// have to wait out the full interval, and a second pass genuinely
/// overlapping the first is not merely redundant work. `refresh_cluster_namespaces`'s
/// add path reads the subsystem's current table, decides a vid is missing,
/// and only *then* opens and registers it — two overlapping passes can both
/// make that decision before either commits, open the same VDI twice, and
/// leave the loser's `Drop` releasing a cluster lock the winner still holds.
///
/// Blocking cluster IO — call it from a plain thread, never a queue thread.
pub fn refresh_clusters() -> usize {
    let _guard = REFRESH_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    if shutting_down() {
        return 0;
    }
    let paths = cluster_paths().clone();
    let ana = cluster_ana().clone();
    let acls = cluster_acls().clone();
    for paths in &paths {
        // The holder slot it reports is startup-only: the cntlid partition it
        // keys is fixed for the process (see [`holder_cntlid_slice`]).
        let _ = refresh_cluster_paths(paths, true);
    }
    for ana in &ana {
        refresh_cluster_ana(ana);
    }
    for acl in &acls {
        refresh_cluster_acl(acl);
    }
    paths.len() + ana.len() + acls.len()
}

/// Guards the whole body of [`refresh_clusters`] against a second pass
/// starting before the first finishes — see its doc comment for why that
/// matters beyond wasted work.
static REFRESH_LOCK: Mutex<()> = Mutex::new(());

/// Start the one thread that keeps every cluster path list, ANA state and
/// host ACL current, if it is not running already.
///
/// A plain OS thread rather than anything on a queue thread: the refresh is
/// blocking cluster IO, and a queue thread must never block. It ends when
/// [`shutdown`] drains the registry.
fn start_cluster_refresh() {
    static STARTED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    if STARTED.swap(true, Ordering::Relaxed) {
        return;
    }
    let spawned = std::thread::Builder::new()
        .name("ioutgt-acl".into())
        .spawn(|| {
            loop {
                std::thread::sleep(CLUSTER_REFRESH_INTERVAL);
                if refresh_clusters() == 0 {
                    // Shutdown drained the registries; nothing left to refresh.
                    return;
                }
            }
        });
    if let Err(err) = spawned {
        warn!(
            %err,
            "sheepdog: cluster path lists, ANA states and ACL membership will not be refreshed"
        );
    }
}

/// Stop tracking cluster paths, ANA and host ACLs, on the way into
/// [`shutdown`]'s release phase. Ends the refresh thread, so nothing
/// re-registers a volume behind the backend that is about to hand it back.
fn stop_cluster_refresh() {
    cluster_paths().clear();
    cluster_ana().clear();
    cluster_acls().clear();
    // The pool it posts to is going away with everything else; a notice raised
    // from here on would only be a message nobody reads.
    *disc_notify() = None;
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
        io_tx.send(IoMsg::Shutdown { ack: None });
    }
    pool.admin.send(AdminMsg::Shutdown { ack: None });
    info!("queue-thread pool torn down after idle");
    // `pool` (the last sender clones) drops here.
}

/// How long the control thread waits for the whole pool to report its IO
/// stopped. Over a queue thread's own drain budget ([`CONN_DRAIN_BUDGET_MS`],
/// which the threads run in parallel) plus slack, and under the shutdown
/// path's wait on this reply ([`TARGET_QUIESCE_BUDGET`]) — each layer gives
/// up before the one above it, so no layer can hang on a wedged one below.
const POOL_QUIESCE_BUDGET: Duration = Duration::from_secs(8);

/// Stop this target's IO: tell every queue thread to wind its connections
/// down and exit, then wait for all of them to report back. Unlike
/// [`teardown_pool`] (idle reclaim, fire-and-forget with no connections to
/// stop) this is the shutdown handshake — when it returns, no command is
/// executing and no backend op is in flight, so the caller may release what
/// the backends hold outside the process.
///
/// Returns `true` if the whole pool acked in time. On timeout it returns
/// anyway: a queue thread wedged on an unresponsive backend must not keep the
/// process from getting the rest of its shutdown done.
async fn quiesce_pool<C: Send>(senders: &Mutex<Option<PoolSenders<C>>>) -> bool {
    let Some(pool) = senders.lock().expect("pool senders mutex").take() else {
        return true; // pool down (never connected, or idle-torn-down)
    };
    // Fan the requests out first, then collect: the threads drain their
    // connections in parallel, so the wait is the slowest one, not the sum.
    let mut acks = Vec::with_capacity(pool.io.len() + 1);
    for io_tx in &pool.io {
        let (tx, rx) = tokio::sync::oneshot::channel();
        io_tx.send(IoMsg::Shutdown { ack: Some(tx) });
        acks.push(rx);
    }
    let (tx, rx) = tokio::sync::oneshot::channel();
    pool.admin.send(AdminMsg::Shutdown { ack: Some(tx) });
    acks.push(rx);
    let collect = async {
        for ack in acks {
            // An `Err` is a thread that died without acking — as quiesced as
            // it will ever be, and not worth waiting the budget out for.
            let _ = ack.await;
        }
    };
    tokio::time::timeout(POOL_QUIESCE_BUDGET, collect)
        .await
        .is_ok()
    // `pool` (the last sender clones) drops here — held until now so a
    // thread's mailbox stays open while it is still draining.
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

/// Refuse to take over a control-socket path that a running instance owns.
///
/// A stale file from a dead instance refuses the connect (or is not a
/// socket at all) and is safe to unlink and rebind; a successful connect
/// means another target is serving this path — hijacking it would orphan
/// that instance's listener on an unlinked inode and leave OUR file dead
/// once we exit, a doubly-confusing failure (the survivor's clients get
/// ECONNREFUSED while `ss` still shows it listening).
fn refuse_live_socket(path: &std::path::Path) -> io::Result<()> {
    if std::os::unix::net::UnixStream::connect(path).is_ok() {
        return Err(io::Error::new(
            io::ErrorKind::AddrInUse,
            format!(
                "control socket {} is owned by a running instance",
                path.display()
            ),
        ));
    }
    Ok(())
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
    refuse_live_socket(path)?;
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
    let BuiltPort {
        port,
        ana_specs,
        acl_specs,
        holder_slot,
    } = match build_port(&config, local, T::trtype()) {
        Ok(built) => built,
        Err(err) => {
            let _ = addr_tx.send(Err(err));
            return;
        }
    };
    // Only now is the cntlid range known: on a cluster it is this target's
    // partition of the configured one, keyed on the holder slot the volumes
    // were just registered into. Nothing above allocates a cntlid — the first
    // Connect is still several awaits away.
    let (cntlid_min, cntlid_max) = holder_cntlid_slice(config.cntlid_range, holder_slot);
    let registry = Registry::new(cntlid_min, cntlid_max);
    // Cluster-backed namespaces: seed their ANA state now — before the first
    // host can ask — and keep it current from the refresh thread.
    track_cluster_ana(ana_specs, &senders);
    // ...and the discovery log they and the path lists feed: from here on, a
    // change to it reaches the parked AERs of the live discovery controllers.
    track_discovery_changes(&senders);
    // ...and the ACLs whose host list, namespace table or discovery
    // generation the refresh thread keeps current: a namespace it adds or
    // removes now reaches the connected hosts' changed-namespaces AER too.
    track_cluster_acls(acl_specs, &senders);
    if let Some(path) = &config.control_socket
        && let Err(err) = spawn_control_api(
            path,
            &port,
            &registry,
            &senders,
            &io_groups,
            config.io_threads,
        )
    {
        let _ = addr_tx.send(Err(err));
        return;
    }

    // Register for the shutdown handshake before reporting the address:
    // `spawn` returns on that report, and a caller that shut down immediately
    // afterwards must not find this target unregistered. Everything that can
    // fail is above, so an entry here always belongs to a target that came up
    // — and once this loop returns, its closed channel is what tells
    // [`quiesce_targets`] there is nothing left here to stop.
    let (quiesce_tx, mut quiesce_rx) = tokio::sync::mpsc::unbounded_channel();
    quiesce_reqs().push(quiesce_tx);

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
            Some(reply) = quiesce_rx.recv() => {
                // Stop serving: nothing new is accepted from here on (this
                // loop is the only reader of `listener`), the pool winds its
                // connections down, and only then do we answer — the reply is
                // the caller's proof that this target's IO has stopped.
                if quiesce_pool(&senders).await {
                    info!(%local, "target stopped serving");
                } else {
                    warn!(%local, "target stopped accepting, but its queue threads did not report back");
                }
                let _ = reply.send(());
                return;
            }
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

#[cfg(test)]
mod tests {
    use super::{HolderSlot, VDI_MAX_HOLDERS, holder_cntlid_slice, refuse_live_socket};

    fn slot(slot: u16) -> Option<HolderSlot> {
        Some(HolderSlot { vid: 0x4712, slot })
    }

    #[test]
    fn holder_slots_get_disjoint_cntlid_partitions() {
        let full = (1, ioutgt_core::registry::CNTLID_MAX);
        // No cluster storage: the target owns everything it was given.
        assert_eq!(holder_cntlid_slice(full, None), full);

        // Every slot's partition, end to end: they tile the range without a
        // gap or an overlap, so no two targets can mint the same cntlid.
        let slices: Vec<(u16, u16)> = (0..VDI_MAX_HOLDERS)
            .map(|i| holder_cntlid_slice(full, slot(i)))
            .collect();
        assert_eq!(slices[0].0, full.0);
        assert_eq!(slices[usize::from(VDI_MAX_HOLDERS) - 1].1, full.1);
        for pair in slices.windows(2) {
            assert!(pair[0].0 <= pair[0].1, "partition {pair:?} is empty");
            assert_eq!(pair[1].0, pair[0].1 + 1, "partitions {pair:?} not adjacent");
        }

        // A slice already narrowed per port is subdivided the same way...
        let (min, max) = holder_cntlid_slice((1, 1000), slot(1));
        assert_eq!((min, max), (33, 64));
        // ...until there is nothing left to cut, when the whole slice is kept
        // rather than handing out an empty one.
        assert_eq!(holder_cntlid_slice((1, 20), slot(1)), (1, 20));

        // A slot the cluster cannot have handed out is not trusted either.
        assert_eq!(holder_cntlid_slice(full, slot(VDI_MAX_HOLDERS)), full);
    }

    #[test]
    fn socket_probe_distinguishes_live_stale_missing() {
        let dir = std::env::temp_dir().join(format!("ioutgt-probe-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("ctl.sock");

        // Missing: nothing to protect.
        assert!(refuse_live_socket(&path).is_ok());

        // Live: a bound listener owns the path — must refuse.
        let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let err = refuse_live_socket(&path).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::AddrInUse);

        // Stale: listener gone, file left behind — safe to take over.
        drop(listener);
        assert!(refuse_live_socket(&path).is_ok());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
