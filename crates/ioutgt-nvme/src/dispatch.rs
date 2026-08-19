//! Command dispatch: per-connection context and routing.
//!
//! Every command arrives in a slot; the slot task calls [`execute`]
//! which routes by queue role and opcode. Fabrics handlers live in
//! [`crate::fabrics_exec`], admin handlers in [`crate::admin`]; the IO
//! path proper lands with the IO milestone.

use std::cell::{Cell, OnceCell, RefCell};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use crate::fabrics::ConnectData;
use crate::spec::{Cqe, Sqe, admin_opcode};
use crate::status;

use crate::controller::RegisterState;
use ioutgt_core::backend::Backend;
use ioutgt_core::queue::QueueCore;
use ioutgt_core::registry::{Registry, TrafficFlag};
use ioutgt_core::subsystem::{NsCache, PortConfig, Subsystem};

/// Floor on [`keepalive_tick`]: however short a KATO the host asks for,
/// the keep-alive machinery wakes no more often than this.
pub const KEEPALIVE_TICK_MIN: Duration = Duration::from_millis(250);

/// Ceiling on [`keepalive_tick`], and the interval used while no KATO is
/// known yet (before Connect) or none was negotiated.
pub const KEEPALIVE_TICK_MAX: Duration = Duration::from_secs(5);

/// Poll interval of the keep-alive machinery for a controller with this
/// KATO: half the timeout, clamped to
/// [`KEEPALIVE_TICK_MIN`]..=[`KEEPALIVE_TICK_MAX`].
///
/// Both halves use it — the admin queue's watchdog, which decides the
/// controller has gone silent, and the IO queues' traffic beacons, which
/// publish liveness into [`TrafficFlag`]. Deriving it from KATO instead of
/// fixing it means a host that asked for a short timeout gets a
/// proportionally short one (and its controller reclaimed promptly), while
/// the common multi-second KATO still costs one wakeup every 5 s per queue
/// at most. KATO 0 disables keep-alive entirely, so there the tick only
/// paces the poll that notices a KATO arriving with Connect.
pub fn keepalive_tick(kato_ms: u32) -> Duration {
    if kato_ms == 0 {
        return KEEPALIVE_TICK_MAX;
    }
    Duration::from_millis(u64::from(kato_ms / 2)).clamp(KEEPALIVE_TICK_MIN, KEEPALIVE_TICK_MAX)
}

/// Per-connection dispatch context (single-threaded, shared by the
/// connection's tasks via `Rc`).
#[allow(missing_docs)]
pub struct ConnCtx<B> {
    pub queue: Rc<QueueCore<Sqe>>,
    pub port: Arc<PortConfig<B>>,
    pub registry: Arc<Registry>,
    /// Connect data of this queue's Connect command.
    pub connect_data: Box<ConnectData>,
    /// Peer "ip:port" of this queue's TCP connection (for `LIST_CONTROLLER`).
    pub peer: String,
    pub role: Role<B>,
}

/// Queue role, fixed at Connect routing time by qid.
#[allow(missing_docs)]
pub enum Role<B> {
    Admin(AdminState<B>),
    Io(IoState<B>),
}

/// Admin-queue controller state (the controller lives and dies with its
/// admin queue connection, as on fabrics).
#[allow(missing_docs)]
pub struct AdminState<B> {
    pub regs: RefCell<RegisterState>,
    /// Set once Connect executes.
    pub cntlid: Cell<u16>,
    pub subsys: RefCell<Option<Arc<Subsystem<B>>>>,
    /// This controller serves the well-known discovery subsystem.
    pub discovery: Cell<bool>,
    /// Negotiated keep-alive timeout in milliseconds (0 = disabled).
    pub kato_ms: Cell<u32>,
    /// Deadline bumped by every received command; the keep-alive watchdog
    /// closes the connection past it. Milliseconds of `Instant` elapsed.
    pub last_heard: Cell<std::time::Instant>,
    /// Traffic-based keep-alive (`CTRATT.TBKAS`): the controller's shared
    /// liveness flag, set once Connect has a cntlid. The watchdog takes it
    /// so traffic on any IO queue counts as being heard from. Empty on a
    /// transport that does not publish traffic, which simply falls back to
    /// Keep Alive commands.
    pub traffic: OnceCell<Arc<TrafficFlag>>,
    /// Pending async events (result dwords) and the wakers of parked
    /// AER futures.
    pub events: RefCell<std::collections::VecDeque<u32>>,
    pub aer_wakers: RefCell<Vec<std::task::Waker>>,
    /// Async event configuration (Set Features AEC).
    pub aec: Cell<u32>,
    /// Namespace inventory changed since the host last read the
    /// Changed-NS log page.
    pub ns_changed: Cell<bool>,
    /// Connection teardown in progress: parked AERs must resolve so
    /// their slots stop counting as executing.
    pub closing: Cell<bool>,
}

impl<B> AdminState<B> {
    /// Keep-alive verdict: `Some(silent_ms)` when the host has been quiet
    /// past KATO×2 + one [`keepalive_tick`] of grace for the poll
    /// granularity (mirrors nvmet's timer). `None` when KATO is disabled
    /// (0, e.g. a persistent discovery controller) or still alive.
    ///
    /// Called only from this controller's keep-alive watchdog, once per
    /// tick: it consumes the traffic flag, so two callers would race for
    /// it. `last_heard` is bumped by every dispatched admin command, and
    /// by traffic on any IO queue of the same controller.
    pub fn keepalive_expired(&self) -> Option<u64> {
        let kato = u64::from(self.kato_ms.get());
        if kato == 0 {
            return None;
        }
        // Traffic-based keep-alive: commands on an IO queue prove the host
        // is there just as well as a Keep Alive command does, and a busy
        // Linux host stops sending Keep Alives once we advertise TBKAS.
        if self.traffic.get().is_some_and(|flag| flag.take()) {
            self.last_heard.set(std::time::Instant::now());
            return None;
        }
        let silent = u64::try_from(self.last_heard.get().elapsed().as_millis()).unwrap_or(u64::MAX);
        let grace = u64::try_from(keepalive_tick(self.kato_ms.get()).as_millis()).expect("<= 5000");
        (silent > kato * 2 + grace).then_some(silent)
    }
}

/// IO-queue state.
#[allow(missing_docs)]
pub struct IoState<B> {
    /// Set once the IO-queue Connect validates against the registry.
    pub cntlid: Cell<u16>,
    /// Write-once at Connect: readable without a borrow guard, so slot
    /// tasks can hold `&Subsystem` across backend awaits safely.
    pub subsys: OnceCell<Arc<Subsystem<B>>>,
    /// Generation-validated namespace-table cache (hot path).
    pub ns_cache: NsCache<B>,
    /// Traffic-based keep-alive: the controller's shared liveness flag,
    /// set at Connect only when the controller runs a keep-alive timer
    /// (KATO ≠ 0). The transport's traffic beacon sets it when this queue
    /// has taken commands since the last tick — once per
    /// [`keepalive_tick`], never per command.
    pub traffic: OnceCell<Arc<TrafficFlag>>,
    /// KATO the controller negotiated on its admin queue (0 = no
    /// keep-alive), which sets this queue's beacon interval.
    pub kato_ms: Cell<u32>,
}

impl<B: Backend> ConnCtx<B> {
    /// Context for an admin queue: owns the controller registers.
    pub fn new_admin(
        queue: Rc<QueueCore<Sqe>>,
        port: Arc<PortConfig<B>>,
        registry: Arc<Registry>,
        connect_data: Box<ConnectData>,
        peer: String,
    ) -> Rc<Self> {
        Rc::new(ConnCtx {
            queue,
            port,
            registry,
            connect_data,
            peer,
            role: Role::Admin(AdminState {
                regs: RefCell::new(RegisterState::new(ioutgt_core::MAX_QUEUE_ENTRIES)),
                cntlid: Cell::new(0),
                subsys: RefCell::new(None),
                discovery: Cell::new(false),
                kato_ms: Cell::new(0),
                last_heard: Cell::new(std::time::Instant::now()),
                traffic: OnceCell::new(),
                events: RefCell::new(std::collections::VecDeque::new()),
                aer_wakers: RefCell::new(Vec::new()),
                // Optional notices enabled until the host programs AEC
                // (nvmet behaves the same).
                aec: Cell::new(crate::AEN_CFG_NS_ATTR),
                ns_changed: Cell::new(false),
                closing: Cell::new(false),
            }),
        })
    }

    /// Context for an IO queue.
    pub fn new_io(
        queue: Rc<QueueCore<Sqe>>,
        port: Arc<PortConfig<B>>,
        registry: Arc<Registry>,
        connect_data: Box<ConnectData>,
        peer: String,
    ) -> Rc<Self> {
        Rc::new(ConnCtx {
            queue,
            port,
            registry,
            connect_data,
            peer,
            role: Role::Io(IoState {
                cntlid: Cell::new(0),
                subsys: OnceCell::new(),
                ns_cache: NsCache::default(),
                traffic: OnceCell::new(),
                kato_ms: Cell::new(0),
            }),
        })
    }

    /// The admin state, when this is an admin-queue context.
    pub fn admin(&self) -> Option<&AdminState<B>> {
        match &self.role {
            Role::Admin(state) => Some(state),
            Role::Io(_) => None,
        }
    }

    /// Build a CQE for this queue.
    pub fn cqe(&self, result: u32, cid: u16, status_code: u16) -> Cqe {
        Cqe::new(
            result,
            self.queue.advance_sqhd(),
            self.queue.qid,
            cid,
            status_code,
        )
    }

    /// Begin teardown. On an admin queue: resolve parked AER futures
    /// (their completions go nowhere — the socket is gone — but the slots
    /// leave `Executing`, letting the drain finish instead of timing out
    /// and leaking the queue), the userspace analog of
    /// nvmet_async_events_failall(). On an IO queue: publish one last
    /// traffic tick, the analog of nvmet_sq_destroy()'s `reset_tbkas`.
    pub fn close(&self) {
        match &self.role {
            Role::Admin(admin) => {
                admin.closing.set(true);
                for waker in admin.aer_wakers.borrow_mut().drain(..) {
                    waker.wake();
                }
            }
            // An IO queue going away is itself a sign of life (the host is
            // reconnecting or tearing the controller down in order), and
            // its last commands may not have been published yet. Grant one
            // more keep-alive period, as nvmet_sq_destroy() does.
            Role::Io(io) => {
                if let Some(traffic) = io.traffic.get() {
                    traffic.set();
                }
            }
        }
    }

    /// Weak handles for the harness pool: a side-effect-free liveness
    /// probe and a [`fire_ns_changed`](Self::fire_ns_changed) trigger,
    /// both holding only a `Weak` on this context. Plain boxed closures
    /// (not a named type) so the pool needs no NVMe types.
    #[allow(clippy::type_complexity)]
    pub fn ns_nudge(self: &Rc<Self>) -> (Box<dyn Fn() -> bool>, Box<dyn Fn()>) {
        let alive = Rc::downgrade(self);
        let fire = Rc::downgrade(self);
        (
            Box::new(move || alive.strong_count() > 0),
            Box::new(move || {
                if let Some(ctx) = fire.upgrade() {
                    ctx.fire_ns_changed();
                }
            }),
        )
    }

    /// Namespace inventory changed: complete one parked AER with the
    /// NS_ATTR notice (if the host enabled it) so the host rescans.
    pub fn fire_ns_changed(&self) {
        let Role::Admin(admin) = &self.role else {
            return;
        };
        admin.ns_changed.set(true);
        if admin.aec.get() & crate::AEN_CFG_NS_ATTR == 0 {
            return;
        }
        // AER result DW0: type Notice (2) | info NS_ATTR_CHANGED (0) <<8
        // | Changed-NS log page (0x04) <<16. A single pending NS_ATTR
        // notice suffices (the host rescans everything on read), so
        // coalesce rather than letting the queue grow unbounded when no
        // AER is posted.
        const NS_ATTR_NOTICE: u32 = 0x0004_0002;
        let mut events = admin.events.borrow_mut();
        if !events.contains(&NS_ATTR_NOTICE) {
            events.push_back(NS_ATTR_NOTICE);
        }
        drop(events);
        for waker in admin.aer_wakers.borrow_mut().drain(..) {
            waker.wake();
        }
    }
}

/// Outcome of dispatching one command.
#[allow(missing_docs)]
pub struct Outcome {
    pub cqe: Cqe,
    /// C2H bytes the send path reads from the slot buffer.
    pub data_len: u32,
}

#[allow(missing_docs)]
impl Outcome {
    pub fn status(cqe: Cqe) -> Outcome {
        Outcome { cqe, data_len: 0 }
    }

    pub fn with_data(cqe: Cqe, data_len: u32) -> Outcome {
        Outcome { cqe, data_len }
    }
}

/// Route one command. `tag`'s slot holds any in-capsule payload.
pub async fn execute<B: Backend>(ctx: &Rc<ConnCtx<B>>, tag: u16, sqe: &Sqe) -> Outcome {
    if let Role::Admin(admin) = &ctx.role {
        admin.last_heard.set(std::time::Instant::now());
    }

    // Fabrics commands are legal on both queue types.
    if sqe.opcode == admin_opcode::FABRICS {
        ioutgt_core::queue::stat_add(&ctx.queue.stats.other_cmds, 1);
        return crate::fabrics_exec::execute(ctx, tag, sqe);
    }

    match &ctx.role {
        Role::Admin(admin) => {
            ioutgt_core::queue::stat_add(&ctx.queue.stats.other_cmds, 1);
            // Everything but Connect/Property requires an enabled
            // controller.
            if !admin.regs.borrow().ready() {
                return Outcome::status(ctx.cqe(
                    0,
                    sqe.cid.get(),
                    status::CONNECT_CTRL_BUSY | status::DNR,
                ));
            }
            crate::admin::execute(ctx, admin, tag, sqe).await
        }
        Role::Io(io) => crate::io::execute(ctx, io, tag, sqe).await,
    }
}
