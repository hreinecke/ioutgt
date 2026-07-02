//! NVMe/RDMA connection management: listen for connect requests, read the
//! host's [`crate::cmproto::CmReq`], build the queue pair on the connection's
//! device, and `accept` (with a [`crate::cmproto::CmRep`]) or `reject`.
//!
//! Self-contained over `rdma-mummy-sys` (librdmacm FFI): [`CmChannel`] owns the
//! event channel (its fd parks on the reactor), [`Identifier`] owns a `rdma_cm_id`,
//! and [`Event`] owns one event until acked. sideway is used for the verbs
//! side only — the single bridge is [`Identifier::get_device_context`], which turns
//! the cm_id's `ibv_context` into a sideway `DeviceContext` (see the layout
//! assertion there). All NVMe-specific knowledge stays in this crate.
//!
//! Types and methods deliberately mirror `sideway::rdmacm` names
//! (`Identifier`, `Event`, `EventType` variants, `get_qp_attr`,
//! `get_device_context`, `setup_timeout`, `max_read_atomic`, …) so that a
//! future switch back to upstream sideway — once it grows CM private-data /
//! reject / SEND_WITH_INV APIs — is a mechanical import swap. The deltas to
//! map then: `CmChannel::adopt(&event)` ⇢ `event.cm_id()`,
//! `attr.apply(&qp)` ⇢ `qp.modify(&attr)`, `Identifier` ⇢ `Arc<Identifier>`.
//!
//! Identity rule: a raw `rdma_cm_id` pointer from an event is only ever (a)
//! adopted into a [`Identifier`] on `ConnectRequest` (librdmacm hands us ownership
//! there) or (b) compared by value against live [`Identifier`]s. It is never
//! dereferenced for other event types — after we destroy an id, a late event
//! (e.g. `TimewaitExit`) may still carry the stale pointer.

use std::collections::HashMap;
use std::ffi::c_void;
use std::io;
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::ptr::NonNull;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use os_socketaddr::OsSocketAddr;
use rdma_mummy_sys::{
    ibv_context, ibv_modify_qp, ibv_qp_attr, rdma_accept, rdma_ack_cm_event, rdma_bind_addr,
    rdma_cm_event, rdma_cm_event_type, rdma_cm_id, rdma_conn_param, rdma_connect,
    rdma_create_event_channel, rdma_create_id, rdma_destroy_event_channel, rdma_destroy_id,
    rdma_disconnect, rdma_establish, rdma_event_channel, rdma_get_cm_event, rdma_init_qp_attr,
    rdma_listen, rdma_port_space, rdma_reject, rdma_resolve_addr, rdma_resolve_route,
};
use sideway::ibverbs::device_context::DeviceContext;
use sideway::ibverbs::queue_pair::{QueuePair, QueuePairState};

use crate::cq::{err_hup, pollin};

/// Owns the raw event channel; destroyed after every [`Identifier`] created on it
/// (each holds an `Arc` of this).
struct ChannelInner {
    channel: NonNull<rdma_event_channel>,
}

// SAFETY: librdmacm event channels are plain fds + heap state; the operations
// used here (get event, create id, destroy) are thread-safe in librdmacm, and
// destruction is sequenced after all ids via the Arc.
unsafe impl Send for ChannelInner {}
// SAFETY: as above; &self methods delegate to thread-safe librdmacm calls.
unsafe impl Sync for ChannelInner {}

impl Drop for ChannelInner {
    fn drop(&mut self) {
        // SAFETY: the pointer came from rdma_create_event_channel and every
        // cm_id created on this channel holds an Arc of self, so none remain.
        unsafe { rdma_destroy_event_channel(self.channel.as_ptr()) };
    }
}

/// A non-blocking RDMA-CM event channel whose fd the reactor parks on (io_uring
/// `POLL_ADD`), so CM events are awaited without busy-polling. One per listener
/// (and one per active connect on the client side).
pub struct CmChannel {
    inner: Arc<ChannelInner>,
}

impl CmChannel {
    /// Create a non-blocking CM event channel.
    pub fn new() -> io::Result<CmChannel> {
        // SAFETY: plain constructor FFI; null means failure (errno set).
        let raw = unsafe { rdma_create_event_channel() };
        let channel = NonNull::new(raw).ok_or_else(io::Error::last_os_error)?;
        // SAFETY: the channel's fd is valid; O_NONBLOCK makes rdma_get_cm_event
        // return EAGAIN instead of blocking the reactor thread.
        let rc = unsafe {
            let fd = channel.as_ref().fd;
            let flags = libc::fcntl(fd, libc::F_GETFL);
            if flags < 0 {
                -1
            } else {
                libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK)
            }
        };
        let inner = Arc::new(ChannelInner { channel });
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(CmChannel { inner })
    }

    fn fd(&self) -> RawFd {
        // SAFETY: the channel is valid for self's lifetime; fd is a plain field.
        unsafe { self.inner.channel.as_ref().fd }
    }

    /// Create an RC (`RDMA_PS_TCP`) cm_id on this channel.
    pub fn create_id(&self) -> io::Result<Identifier> {
        let mut raw: *mut rdma_cm_id = std::ptr::null_mut();
        // SAFETY: valid channel; librdmacm fills `raw` on success. No user
        // context — identity is the pointer itself.
        let rc = unsafe {
            rdma_create_id(
                self.inner.channel.as_ptr(),
                &mut raw,
                std::ptr::null_mut(),
                rdma_port_space::RDMA_PS_TCP,
            )
        };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        let id = NonNull::new(raw).ok_or_else(|| io::Error::other("rdma_create_id: null id"))?;
        Ok(Identifier {
            inner: Arc::new(IdentifierInner {
                id,
                _channel: Arc::clone(&self.inner),
            }),
        })
    }

    /// Adopt the child cm_id delivered by a `ConnectRequest` event (librdmacm
    /// hands its ownership to the event's consumer). Must be called at most
    /// once per connect request; the returned [`Identifier`] destroys it on drop.
    pub fn adopt(&self, event: &Event) -> io::Result<Identifier> {
        debug_assert!(matches!(event.event_type(), EventType::ConnectRequest));
        let raw = event.raw_id();
        let id = NonNull::new(raw).ok_or_else(|| io::Error::other("connect request without cm_id"))?;
        Ok(Identifier {
            inner: Arc::new(IdentifierInner {
                id,
                _channel: Arc::clone(&self.inner),
            }),
        })
    }

    /// Await the next CM event, parking the reactor on the channel fd whenever
    /// none is queued (level-triggered `POLL_ADD`, re-issued each empty wakeup).
    pub async fn next_event(&self) -> io::Result<Event> {
        loop {
            let mut raw: *mut rdma_cm_event = std::ptr::null_mut();
            // SAFETY: valid non-blocking channel; on success `raw` is the event
            // we own until rdma_ack_cm_event.
            let rc = unsafe { rdma_get_cm_event(self.inner.channel.as_ptr(), &mut raw) };
            if rc == 0 {
                let event =
                    NonNull::new(raw).ok_or_else(|| io::Error::other("null CM event"))?;
                return Ok(Event { event: Some(event) });
            }
            let err = io::Error::last_os_error();
            if err.kind() != io::ErrorKind::WouldBlock {
                return Err(err);
            }
            let revents = ioutgt_uring::ops::poll_add(self.fd(), pollin())?.await?;
            // POLL_ADD always reports POLLERR/POLLHUP; if the channel fd is
            // in error/hangup with no readable event, fail rather than
            // respin (librdmacm normally surfaces problems as events, but
            // guard the livelock anyway).
            if revents & pollin() == 0 && revents & err_hup() != 0 {
                return Err(io::Error::other("CM event channel error or hangup"));
            }
        }
    }
}

/// Owns one `rdma_cm_id`; destroyed on the last clone's drop.
struct IdentifierInner {
    id: NonNull<rdma_cm_id>,
    /// Keeps the event channel alive at least as long as the id (librdmacm
    /// requires ids destroyed before their channel).
    _channel: Arc<ChannelInner>,
}

// SAFETY: librdmacm cm_id operations are thread-safe, and a cm_id is moved
// (not shared mutably) across threads here: the CM reactor thread accepts it,
// a queue thread drives it. Same guarantee sideway declares for `Identifier`.
unsafe impl Send for IdentifierInner {}
// SAFETY: as above — all &self operations delegate to thread-safe librdmacm.
unsafe impl Sync for IdentifierInner {}

impl Drop for IdentifierInner {
    fn drop(&mut self) {
        // SAFETY: we own the id (created or adopted exactly once); librdmacm
        // requires any queued events to be acked first, which [`Event`]'s
        // ack-on-drop guarantees for events already retrieved.
        unsafe { rdma_destroy_id(self.id.as_ptr()) };
    }
}

/// A clonable owner of an `rdma_cm_id` (conceptually a socket). Cheap to clone
/// (`Arc`); the id is destroyed when the last clone drops.
#[derive(Clone)]
pub struct Identifier {
    inner: Arc<IdentifierInner>,
}

/// The RC-tuned `rdma_conn_param` shared by accept and connect.
fn conn_param(
    qp_num: u32,
    private_data: &[u8],
    responder_resources: u8,
    initiator_depth: u8,
) -> io::Result<rdma_conn_param> {
    // SAFETY: a zeroed rdma_conn_param is valid; we fill the fields we use.
    let mut cp: rdma_conn_param = unsafe { std::mem::zeroed() };
    cp.qp_num = qp_num;
    cp.responder_resources = responder_resources;
    cp.initiator_depth = initiator_depth;
    cp.flow_control = 1;
    cp.retry_count = 7;
    cp.rnr_retry_count = 7;
    if !private_data.is_empty() {
        cp.private_data = private_data.as_ptr() as *const c_void;
        cp.private_data_len =
            u8::try_from(private_data.len()).map_err(|_| io::Error::other("CM data > 255 bytes"))?;
    }
    Ok(cp)
}

/// The per-`ibv_context` `DeviceContext` cache: contexts are owned by
/// librdmacm (never closed here) and every caller for the same raw context
/// must get the same `Arc` (mirrors sideway's own `get_device_context`).
static DEVICE_CONTEXTS: LazyLock<Mutex<HashMap<usize, Arc<DeviceContext>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

impl Identifier {
    /// Whether `raw` (an event's id pointer) is this id. Pointer comparison
    /// only — see the module identity rule.
    pub fn is_raw(&self, raw: *mut rdma_cm_id) -> bool {
        self.inner.id.as_ptr() == raw
    }

    /// Bind to a local address (listener side).
    pub fn bind_addr(&self, addr: SocketAddr) -> io::Result<()> {
        // SAFETY: valid id; librdmacm copies the sockaddr synchronously.
        let rc =
            unsafe { rdma_bind_addr(self.inner.id.as_ptr(), OsSocketAddr::from(addr).as_mut_ptr()) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Start listening (after [`bind_addr`](Self::bind_addr)).
    pub fn listen(&self, backlog: i32) -> io::Result<()> {
        // SAFETY: valid bound id.
        let rc = unsafe { rdma_listen(self.inner.id.as_ptr(), backlog) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Resolve `dst` to an RDMA device (client side); completes with an
    /// `AddressResolved` (or `AddressError`) event.
    pub fn resolve_addr(
        &self,
        src: Option<SocketAddr>,
        dst: SocketAddr,
        timeout: Duration,
    ) -> io::Result<()> {
        let timeout_ms =
            i32::try_from(timeout.as_millis()).map_err(|_| io::Error::other("timeout too large"))?;
        let mut srcaddr = src.map(OsSocketAddr::from);
        // SAFETY: valid id; sockaddrs are copied synchronously.
        let rc = unsafe {
            rdma_resolve_addr(
                self.inner.id.as_ptr(),
                srcaddr.as_mut().map_or(std::ptr::null_mut(), |s| s.as_mut_ptr()),
                OsSocketAddr::from(dst).as_mut_ptr(),
                timeout_ms,
            )
        };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Resolve the route to the resolved address; completes with a
    /// `RouteResolved` (or `RouteError`) event.
    pub fn resolve_route(&self, timeout: Duration) -> io::Result<()> {
        let timeout_ms =
            i32::try_from(timeout.as_millis()).map_err(|_| io::Error::other("timeout too large"))?;
        // SAFETY: valid id whose address is resolved.
        let rc = unsafe { rdma_resolve_route(self.inner.id.as_ptr(), timeout_ms) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Complete an active-side connection whose QP is externally managed
    /// (after moving it to RTS on `ConnectResponse`).
    pub fn establish(&self) -> io::Result<()> {
        // SAFETY: valid id in the connect-response state.
        let rc = unsafe { rdma_establish(self.inner.id.as_ptr()) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Disconnect (sends the DREQ, or the DREP when answering one).
    pub fn disconnect(&self) -> io::Result<()> {
        // SAFETY: valid connected id.
        let rc = unsafe { rdma_disconnect(self.inner.id.as_ptr()) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Accept a connect request, binding the (already RTS) queue pair `qp_num`
    /// and returning `reply` (an encoded [`crate::cmproto::CmRep`]).
    pub fn accept(
        &self,
        qp_num: u32,
        reply: &[u8],
        responder_resources: u8,
        initiator_depth: u8,
    ) -> io::Result<()> {
        let mut cp = conn_param(qp_num, reply, responder_resources, initiator_depth)?;
        // SAFETY: valid id with a pending connect request; rdma_accept copies
        // the private data synchronously, so `reply` need only outlive this call.
        let rc = unsafe { rdma_accept(self.inner.id.as_ptr(), &mut cp) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Initiate a connect (client side), binding the INIT queue pair `qp_num`
    /// and sending `request` (an encoded [`crate::cmproto::CmReq`]).
    pub fn connect(
        &self,
        qp_num: u32,
        request: &[u8],
        responder_resources: u8,
        initiator_depth: u8,
    ) -> io::Result<()> {
        let mut cp = conn_param(qp_num, request, responder_resources, initiator_depth)?;
        // SAFETY: valid route-resolved id; rdma_connect copies the private data
        // synchronously.
        let rc = unsafe { rdma_connect(self.inner.id.as_ptr(), &mut cp) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Reject a connect request, returning `reason` (an encoded
    /// [`crate::cmproto::CmRej`]) to the host.
    pub fn reject(&self, reason: &[u8]) -> io::Result<()> {
        let (ptr, len) = if reason.is_empty() {
            (std::ptr::null(), 0u8)
        } else {
            (
                reason.as_ptr() as *const c_void,
                u8::try_from(reason.len()).map_err(|_| io::Error::other("CM reject > 255 bytes"))?,
            )
        };
        // SAFETY: valid id with a pending connect request; rdma_reject copies
        // the private data synchronously.
        let rc = unsafe { rdma_reject(self.inner.id.as_ptr(), ptr, len) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// The CM-derived queue-pair attributes for transitioning to `state`
    /// (librdmacm's `rdma_init_qp_attr`), applied with [`QueuePairAttribute::apply`].
    pub fn get_qp_attr(&self, state: QueuePairState) -> io::Result<QueuePairAttribute> {
        // SAFETY: a zeroed ibv_qp_attr is a valid input; librdmacm fills the
        // fields named by the returned mask.
        let mut attr: ibv_qp_attr = unsafe { std::mem::zeroed() };
        attr.qp_state = state as u32;
        let mut mask = 0i32;
        // SAFETY: valid id in a CM state that defines attributes for `state`.
        let rc = unsafe { rdma_init_qp_attr(self.inner.id.as_ptr(), &mut attr, &mut mask) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(QueuePairAttribute { attr, mask })
    }

    /// The sideway `DeviceContext` for the device this connection landed on
    /// (`cm_id->verbs`), or `None` before address resolution binds a device.
    ///
    /// This is the one bridge from the raw CM layer into sideway's verbs
    /// layer, pending an upstream API. sideway 0.4.3's `DeviceContext` is a
    /// single-field struct over `NonNull<ibv_context>` (asserted below; a
    /// one-field struct cannot be field-reordered), and `sideway` is pinned
    /// `=0.4.3` in Cargo.toml so an upgrade revisits this. The context is
    /// owned by librdmacm — cached per pointer and never closed, exactly like
    /// sideway's own `get_device_context`.
    pub fn get_device_context(&self) -> Option<Arc<DeviceContext>> {
        const _: () = assert!(
            std::mem::size_of::<DeviceContext>() == std::mem::size_of::<NonNull<ibv_context>>()
        );
        const _: () = assert!(
            std::mem::align_of::<DeviceContext>() == std::mem::align_of::<NonNull<ibv_context>>()
        );
        // SAFETY: reading the `verbs` field of a live cm_id.
        let verbs = unsafe { self.inner.id.as_ref().verbs };
        let ctx = NonNull::new(verbs)?;
        let mut cache = DEVICE_CONTEXTS.lock().expect("device-context cache poisoned");
        Some(Arc::clone(cache.entry(verbs as usize).or_insert_with(|| {
            // SAFETY: layout asserted above; the context outlives every user
            // (librdmacm keeps it open for the process; the cache leaks the Arc
            // by design, mirroring sideway).
            Arc::new(unsafe {
                std::mem::transmute::<NonNull<ibv_context>, DeviceContext>(ctx)
            })
        })))
    }
}

/// CM-derived QP attributes + mask from [`Identifier::get_qp_attr`].
pub struct QueuePairAttribute {
    attr: ibv_qp_attr,
    mask: i32,
}

impl QueuePairAttribute {
    /// The negotiated `max_rd_atomic` (outstanding RDMA READs this QP may
    /// initiate) — what the CM accept reply must advertise as initiator_depth.
    pub fn max_read_atomic(&self) -> u8 {
        self.attr.max_rd_atomic
    }

    /// Override the RC ACK timeout (4.096us × 2^t) before applying.
    pub fn setup_timeout(&mut self, t: u8) {
        self.attr.timeout = t;
    }

    /// Apply to `qp` (raw `ibv_modify_qp` with the CM-provided mask).
    pub fn apply(&self, qp: &impl QueuePair) -> io::Result<()> {
        // SAFETY: qp's raw handle is valid for its lifetime; attr/mask came
        // from rdma_init_qp_attr (ibv_modify_qp reads attr, never stores it).
        let rc = unsafe {
            ibv_modify_qp(
                qp.qp().as_ptr(),
                &self.attr as *const ibv_qp_attr as *mut ibv_qp_attr,
                self.mask,
            )
        };
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
        Ok(())
    }
}

/// The CM event types this target handles; everything else lands in `Other`
/// (acked and ignored/logged by the caller).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(missing_docs)] // names mirror librdmacm's RDMA_CM_EVENT_*
pub enum EventType {
    AddressResolved,
    AddressError,
    RouteResolved,
    ConnectRequest,
    ConnectResponse,
    Rejected,
    Established,
    Disconnected,
    DeviceRemoval,
    Other(u32),
}

impl From<u32> for EventType {
    fn from(v: u32) -> EventType {
        use rdma_cm_event_type as t;
        match v {
            t::RDMA_CM_EVENT_ADDR_RESOLVED => EventType::AddressResolved,
            t::RDMA_CM_EVENT_ADDR_ERROR => EventType::AddressError,
            t::RDMA_CM_EVENT_ROUTE_RESOLVED => EventType::RouteResolved,
            t::RDMA_CM_EVENT_CONNECT_REQUEST => EventType::ConnectRequest,
            t::RDMA_CM_EVENT_CONNECT_RESPONSE => EventType::ConnectResponse,
            t::RDMA_CM_EVENT_REJECTED => EventType::Rejected,
            t::RDMA_CM_EVENT_ESTABLISHED => EventType::Established,
            t::RDMA_CM_EVENT_DISCONNECTED => EventType::Disconnected,
            t::RDMA_CM_EVENT_DEVICE_REMOVAL => EventType::DeviceRemoval,
            other => EventType::Other(other),
        }
    }
}

/// One retrieved CM event, owned until acked (dropping acks as a backstop;
/// librdmacm frees the event and anything it references at ack).
pub struct Event {
    event: Option<NonNull<rdma_cm_event>>,
}

impl Event {
    fn raw(&self) -> NonNull<rdma_cm_event> {
        self.event.expect("event valid until ack/drop")
    }

    /// The event type.
    pub fn event_type(&self) -> EventType {
        // SAFETY: the event is valid until acked.
        EventType::from(unsafe { self.raw().as_ref().event })
    }

    /// The event status (e.g. the reject reason code on `Rejected`).
    pub fn status(&self) -> i32 {
        // SAFETY: the event is valid until acked.
        unsafe { self.raw().as_ref().status }
    }

    /// The raw cm_id this event refers to. Identity/adoption only — see the
    /// module identity rule.
    pub fn raw_id(&self) -> *mut rdma_cm_id {
        // SAFETY: the event is valid until acked; `id` is a plain field.
        unsafe { self.raw().as_ref().id }
    }

    /// Copy out the inbound CM private data (the connecting host's
    /// `nvme_rdma_cm_req` on a connect request). Copied, not borrowed, so the
    /// caller may `ack` afterwards (which frees the underlying buffer).
    pub fn private_data(&self) -> Vec<u8> {
        // Only connection-management events have the `conn` arm of the param
        // union active; reading it for any other event type would be union UB.
        if !matches!(
            self.event_type(),
            EventType::ConnectRequest | EventType::ConnectResponse
        ) {
            return Vec::new();
        }
        // SAFETY: the event is valid until acked; the type check above
        // guarantees the `conn` arm is active, and its private_data/_len
        // describe the inbound buffer.
        unsafe {
            let conn = &self.raw().as_ref().param.conn;
            if conn.private_data.is_null() || conn.private_data_len == 0 {
                Vec::new()
            } else {
                std::slice::from_raw_parts(
                    conn.private_data as *const u8,
                    conn.private_data_len as usize,
                )
                .to_vec()
            }
        }
    }

    /// Acknowledge and free the event (one-to-one with retrieval; drop is the
    /// backstop for early-return paths). Infallible, but keeps sideway's
    /// `Result` signature so call sites survive a switch-back unchanged.
    pub fn ack(mut self) -> io::Result<()> {
        self.ack_inner();
        Ok(())
    }

    fn ack_inner(&mut self) {
        if let Some(event) = self.event.take() {
            // SAFETY: the event was retrieved by rdma_get_cm_event and not yet
            // acked (the Option guards double-ack).
            unsafe { rdma_ack_cm_event(event.as_ptr()) };
        }
    }
}

impl Drop for Event {
    fn drop(&mut self) {
        self.ack_inner();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmproto::{CM_FMT_1_0, CmRep, CmReq};
    use crate::rdma_devices;
    use ioutgt_uring::{QueueRuntime, RingConfig, ops};
    use sideway::ibverbs::AccessFlags;
    use sideway::ibverbs::completion::{GenericCompletionQueue, WorkCompletionStatus};
    use sideway::ibverbs::memory_region::MemoryRegion;
    use sideway::ibverbs::protection_domain::ProtectionDomain;
    use sideway::ibverbs::queue_pair::{
        GenericQueuePair, PostSendGuard, SendOperationFlags, SetScatterGatherEntry,
        WorkRequestFlags,
    };
    use std::net::SocketAddr;

    use crate::oerr;

    type Held = (Arc<ProtectionDomain>, GenericCompletionQueue, GenericQueuePair);

    /// A 64-byte sample command capsule (a stand-in SQE); each byte = its index.
    fn sample_capsule() -> [u8; 64] {
        let mut b = [0u8; 64];
        for (i, x) in b.iter_mut().enumerate() {
            *x = u8::try_from(i).expect("index < 64");
        }
        b
    }

    /// Async busy-poll for a single WR on a private CQ — throwaway test
    /// scaffolding. The real run_queue must instead drive a shared CQ via the
    /// completion channel (req_notify + reactor `poll_add`, see `cq::wait`),
    /// dispatch EVERY drained CQE to its slot by `wr_id`, and keep RECVs armed.
    /// Yields (ring timer) between polls so the peer task runs; bounded so a
    /// lost completion fails instead of spinning forever.
    async fn busy_wc(cq: &GenericCompletionQueue, wr_id: u64) -> io::Result<()> {
        for _ in 0..5000 {
            if let Ok(poller) = cq.start_poll() {
                for wc in poller {
                    if wc.status() != WorkCompletionStatus::Success as u32 {
                        return Err(io::Error::other(format!(
                            "wr {} status {}",
                            wc.wr_id(),
                            wc.status()
                        )));
                    }
                    if wc.wr_id() == wr_id {
                        return Ok(());
                    }
                }
            }
            ops::sleep(Duration::from_millis(1)).unwrap().await.unwrap();
        }
        Err(io::Error::other(format!("wr {wr_id} never completed")))
    }

    /// Build an RC QP + CQ on the connection's device context (the one the cm_id
    /// landed on). Returns all three so the caller keeps them alive.
    fn build_qp(ctx: &Arc<DeviceContext>) -> io::Result<Held> {
        let mut cqb = ctx.create_cq_builder();
        cqb.setup_cqe(16);
        let cq = GenericCompletionQueue::from(cqb.build_ex().map_err(oerr)?);
        let pd = ctx.alloc_pd().map_err(oerr)?;
        let mut b = pd.create_qp_builder();
        b.setup_max_send_wr(16)
            .setup_max_recv_wr(16)
            .setup_max_send_sge(1)
            .setup_max_recv_sge(1)
            .setup_send_cq(cq.clone())
            .setup_recv_cq(cq.clone())
            .setup_send_ops_flags(
                SendOperationFlags::Send | SendOperationFlags::Write | SendOperationFlags::Read,
            );
        let qp: GenericQueuePair = b.build_ex().map_err(oerr)?.into();
        Ok((pd, cq, qp))
    }

    /// Listener side: accept one connection, returning the qid parsed from the
    /// host's CmReq once the connection reaches ESTABLISHED.
    async fn run_server(port: u16) -> io::Result<u16> {
        let ch = CmChannel::new()?;
        let listen_id = ch.create_id()?;
        listen_id.bind_addr(format!("0.0.0.0:{port}").parse().unwrap())?;
        listen_id.listen(1)?;
        eprintln!("[cm-server] bound + listening on {port}");

        let mut held: Option<Held> = None;
        let mut child: Option<Identifier> = None;
        let mut seen_qid = None;
        // Backing for the pre-posted command-capsule RECV (must outlive its MR).
        let mut recv_buf = vec![0u8; 64];
        let mut recv_mr: Option<Arc<MemoryRegion>> = None;
        loop {
            let event = ch.next_event().await?;
            let et = event.event_type();
            eprintln!("[cm-server] event {et:?}");
            match et {
                EventType::ConnectRequest => {
                    let req = CmReq::parse(&event.private_data())?;
                    seen_qid = Some(req.qid);
                    let id = ch.adopt(&event)?;
                    let ctx = id
                        .get_device_context()
                        .ok_or_else(|| io::Error::other("no device context"))?;
                    let (pd, cq, qp) = build_qp(&ctx)?;
                    id.get_qp_attr(QueuePairState::Init)?.apply(&qp)?;
                    id.get_qp_attr(QueuePairState::ReadyToReceive)?.apply(&qp)?;
                    id.get_qp_attr(QueuePairState::ReadyToSend)?.apply(&qp)?;
                    // Pre-post a RECV for the host's first command capsule, so it
                    // is ready before the connection establishes.
                    // SAFETY: recv_buf is a live, stable, owned buffer that
                    // outlives this MR (kept in recv_mr for the test); the NIC
                    // writes it via LocalWrite.
                    let mr = unsafe {
                        pd.reg_mr(
                            recv_buf.as_mut_ptr() as usize,
                            recv_buf.len(),
                            AccessFlags::LocalWrite,
                        )
                    }
                    .map_err(oerr)?;
                    let mut qp = qp;
                    {
                        let mut g = qp.start_post_recv();
                        let h = g.construct_wr(1);
                        // SAFETY: recv_buf is registered and lives for the test.
                        unsafe { h.setup_sge(mr.lkey(), recv_buf.as_ptr() as u64, 64) };
                        g.post().map_err(oerr)?;
                    }
                    recv_mr = Some(mr);
                    let rep = CmRep {
                        recfmt: CM_FMT_1_0,
                        crqsize: req.hsqsize,
                    }
                    .to_bytes();
                    id.accept(qp.qp_number(), &rep, 1, 1)?;
                    held = Some((pd, cq, qp));
                    child = Some(id);
                    event.ack()?;
                }
                EventType::Established => {
                    // Reap the host's command capsule (the client sends it now)
                    // and verify the capsule transport round-tripped it. Hold the
                    // QP/cm_id alive until the client disconnects.
                    let cq = &held.as_ref().expect("qp built before established").1;
                    busy_wc(cq, 1).await?;
                    assert_eq!(recv_buf, sample_capsule(), "command capsule round-trip");
                    // recv_mr is held in the outer scope (kept alive structurally).
                    let _keep = &recv_mr;
                    event.ack()?;
                }
                EventType::Disconnected => {
                    // Client finished and tore the connection down — done.
                    event.ack()?;
                    drop(held);
                    drop(child);
                    return Ok(seen_qid.expect("qid set before disconnect"));
                }
                EventType::DeviceRemoval => {
                    event.ack()?;
                    return Err(io::Error::other("server CM event DeviceRemoval"));
                }
                _ => {
                    event.ack()?;
                }
            }
        }
    }

    /// Client side: connect to `dst`, sending a CmReq with `qid`, drive to ESTABLISHED.
    async fn run_client(dst: SocketAddr, qid: u16) -> io::Result<()> {
        let ch = CmChannel::new()?;
        let id = ch.create_id()?;
        // Bind the source to the rxe netdev's IP so resolution picks that device
        // (resolving the self-IP with no source can route via `lo`, which has no
        // RDMA device → AddressError).
        let src = SocketAddr::new(dst.ip(), 0);
        // The rxe RoCEv2 GID for a fresh netdev IP populates asynchronously; a
        // too-early resolve fails SYNCHRONOUSLY (EADDRNOTAVAIL/ENODEV) rather
        // than with an AddressError event, so retry both forms.
        let mut sync_retries = 0u32;
        loop {
            match id.resolve_addr(Some(src), dst, Duration::from_secs(5)) {
                Ok(()) => break,
                Err(e) if sync_retries < 20 => {
                    sync_retries += 1;
                    eprintln!("[cm-client] resolve_addr not ready ({e}); retry {sync_retries}");
                    ops::sleep(Duration::from_millis(250)).unwrap().await.unwrap();
                }
                Err(e) => return Err(e),
            }
        }
        eprintln!("[cm-client] resolve_addr({src} -> {dst}) issued");
        let mut held: Option<Held> = None;
        let mut addr_retries = 0u32;
        loop {
            let event = ch.next_event().await?;
            let et = event.event_type();
            eprintln!("[cm-client] event {et:?}");
            match et {
                EventType::AddressResolved => {
                    id.resolve_route(Duration::from_secs(5))?;
                    event.ack()?;
                }
                EventType::AddressError => {
                    event.ack()?;
                    addr_retries += 1;
                    if addr_retries > 5 {
                        return Err(io::Error::other("resolve_addr failed (AddressError x5)"));
                    }
                    ops::sleep(Duration::from_millis(200)).unwrap().await.unwrap();
                    id.resolve_addr(Some(src), dst, Duration::from_secs(5))?;
                }
                EventType::RouteResolved => {
                    let ctx = id
                        .get_device_context()
                        .ok_or_else(|| io::Error::other("no device context"))?;
                    let (pd, cq, qp) = build_qp(&ctx)?;
                    id.get_qp_attr(QueuePairState::Init)?.apply(&qp)?;
                    let req = CmReq {
                        recfmt: CM_FMT_1_0,
                        qid,
                        hrqsize: 128,
                        hsqsize: 127,
                        cntlid: 0xffff,
                    };
                    id.connect(qp.qp_number(), &req.to_bytes(), 1, 1)?;
                    held = Some((pd, cq, qp));
                    event.ack()?;
                }
                EventType::ConnectResponse => {
                    // Validate the target's accept reply (the reverse private-data
                    // direction): the server set crqsize = our hsqsize.
                    let rep = CmRep::parse(&event.private_data())?;
                    assert_eq!(rep.crqsize, 127, "server crqsize round-trip");
                    // Active side with an externally-managed QP: move it to RTS
                    // and `establish()` — that completes the handshake; there is
                    // NO separate Established event on the active side.
                    {
                        let qp = &held.as_ref().expect("qp built before response").2;
                        id.get_qp_attr(QueuePairState::ReadyToReceive)?.apply(qp)?;
                        id.get_qp_attr(QueuePairState::ReadyToSend)?.apply(qp)?;
                    }
                    id.establish()?;
                    event.ack()?;
                    // Send a command capsule over the established QP (capsule
                    // transport check); wait for the send to complete before
                    // tearing the connection down.
                    let send_buf = sample_capsule().to_vec();
                    let send_mr = {
                        let pd = &held.as_ref().expect("qp built").0;
                        // SAFETY: send_buf is a live, stable, owned buffer that
                        // outlives this MR (dropped after the send completes). A
                        // SEND source is only locally read by the HCA, so it needs
                        // no access flag.
                        unsafe {
                            pd.reg_mr(send_buf.as_ptr() as usize, send_buf.len(), AccessFlags::none())
                        }
                        .map_err(oerr)?
                    };
                    {
                        let qp = &mut held.as_mut().expect("qp built").2;
                        let mut g = qp.start_post_send();
                        let h = g.construct_wr(2, WorkRequestFlags::Signaled).setup_send();
                        // SAFETY: send_buf is registered and lives across the wait.
                        unsafe { h.setup_sge(send_mr.lkey(), send_buf.as_ptr() as u64, 64) };
                        g.post().map_err(oerr)?;
                    }
                    busy_wc(&held.as_ref().expect("qp built").1, 2).await?;
                    drop(send_mr);
                    drop(send_buf);
                    // Connected. Tearing down here disconnects, which the server
                    // sees as Disconnected (its cue that we are done).
                    id.disconnect()?;
                    drop(held);
                    return Ok(());
                }
                EventType::Established => {
                    // Not expected for the active side, but harmless: done.
                    event.ack()?;
                    id.disconnect()?;
                    drop(held);
                    return Ok(());
                }
                EventType::Rejected => {
                    event.ack()?;
                    return Err(io::Error::other("client connect rejected"));
                }
                other => {
                    event.ack()?;
                    return Err(io::Error::other(format!("client CM event {other:?}")));
                }
            }
        }
    }

    /// In-process RDMA-CM loopback over soft-RoCE (rxe): a listener and a client
    /// in one reactor establish an RC connection through `accept`/`connect` with
    /// nvme-rdma private data, and the server validates the qid it round-tripped.
    /// Skips without an RDMA device or `IOUTGT_RXE_IP` (the rxe netdev's IP).
    #[test]
    fn rxe_cm_loopback() -> io::Result<()> {
        if rdma_devices().is_empty() {
            eprintln!("skip rxe_cm_loopback: no RDMA device");
            return Ok(());
        }
        let Ok(ip) = std::env::var("IOUTGT_RXE_IP") else {
            eprintln!("skip rxe_cm_loopback: IOUTGT_RXE_IP unset");
            return Ok(());
        };
        let rt = QueueRuntime::new(RingConfig::default())?;
        rt.block_on(async move {
            const PORT: u16 = 18515;
            const QID: u16 = 7;
            let server = tokio::task::spawn_local(run_server(PORT));
            // Let the server bind+listen before the client resolves to it.
            ops::sleep(Duration::from_millis(100)).unwrap().await.unwrap();
            let dst: SocketAddr = format!("{ip}:{PORT}").parse().unwrap();
            let client = tokio::task::spawn_local(run_client(dst, QID));

            // Whichever side finishes first is checked first, so an early
            // error surfaces immediately instead of being masked behind the
            // other side's (now hopeless) wait.
            let combined = async {
                let mut server = std::pin::pin!(server);
                let mut client = std::pin::pin!(client);
                tokio::select! {
                    sr = &mut server => {
                        let sq = sr.unwrap()?;
                        client.await.unwrap()?;
                        Ok::<u16, io::Error>(sq)
                    }
                    cr = &mut client => {
                        cr.unwrap()?;
                        server.await.unwrap()
                    }
                }
            };
            // Bound the whole handshake so a CM stall fails loudly (with the
            // per-event log above) instead of hanging the suite.
            tokio::select! {
                r = combined => {
                    assert_eq!(r?, QID, "server saw the qid the client sent");
                }
                _ = ops::sleep(Duration::from_secs(25)).unwrap() => {
                    panic!("rxe_cm_loopback timed out waiting for ESTABLISHED");
                }
            }
            Ok::<(), io::Error>(())
        })
    }
}
