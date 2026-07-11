//! Command dispatch: per-connection context and routing.
//!
//! Every command arrives in a slot; the slot task calls [`execute`]
//! which routes by queue role and opcode. Fabrics handlers live in
//! [`crate::fabrics_exec`], admin handlers in [`crate::admin`]; the IO
//! path proper lands with the IO milestone.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;

use crate::fabrics::ConnectData;
use crate::spec::{Cqe, Sqe, admin_opcode};
use crate::status;

use crate::controller::{RegisterState, Registry};
use ioutgt_core::backend::Backend;
use ioutgt_core::queue::QueueCore;
use ioutgt_core::subsystem::{NsCache, PortConfig, Subsystem};

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
    /// past KATO×2 + a 5 s grace (mirrors nvmet's timer). `None` when KATO is
    /// disabled (0, e.g. a persistent discovery controller) or still alive.
    /// `last_heard` is bumped by every dispatched command.
    pub fn keepalive_expired(&self) -> Option<u64> {
        let kato = u64::from(self.kato_ms.get());
        if kato == 0 {
            return None;
        }
        let silent = u64::try_from(self.last_heard.get().elapsed().as_millis()).unwrap_or(u64::MAX);
        (silent > kato * 2 + 5_000).then_some(silent)
    }
}

/// IO-queue state.
#[allow(missing_docs)]
pub struct IoState<B> {
    /// Set once the IO-queue Connect validates against the registry.
    pub cntlid: Cell<u16>,
    /// Write-once at Connect: readable without a borrow guard, so slot
    /// tasks can hold `&Subsystem` across backend awaits safely.
    pub subsys: std::cell::OnceCell<Arc<Subsystem<B>>>,
    /// Generation-validated namespace-table cache (hot path).
    pub ns_cache: NsCache<B>,
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
                subsys: std::cell::OnceCell::new(),
                ns_cache: NsCache::default(),
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

    /// Begin teardown: resolve parked AER futures (their completions go
    /// nowhere — the socket is gone — but the slots leave `Executing`,
    /// letting the drain finish instead of timing out and leaking the
    /// queue). The userspace analog of nvmet_async_events_failall().
    pub fn close(&self) {
        if let Role::Admin(admin) = &self.role {
            admin.closing.set(true);
            for waker in admin.aer_wakers.borrow_mut().drain(..) {
                waker.wake();
            }
        }
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
