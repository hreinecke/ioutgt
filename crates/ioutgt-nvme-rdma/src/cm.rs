//! NVMe/RDMA connection management: listen for connect requests, read the
//! host's [`crate::cmproto::CmReq`], build the queue pair on the connection's
//! device, and `accept` (with a [`crate::cmproto::CmRep`]) or `reject`.
//!
//! sideway drives the CM (its `EventChannel`/`Identifier` give the cm_id's
//! `DeviceContext` via `get_device_context()` and the CM-derived QP attrs via
//! `get_qp_attr()`), but does not wrap the operations that carry private data.
//! Those are done here over `rdma-mummy-sys` through sideway's two raw
//! escape-hatch accessors (`Event::event()`, `Identifier::id()` — the ioutgt
//! vendor patch). All NVMe-specific knowledge stays in this crate.

use std::ffi::c_void;
use std::io;
use std::os::fd::AsRawFd;
use std::sync::Arc;

use rdma_mummy_sys::{rdma_accept, rdma_conn_param, rdma_connect, rdma_reject};
use sideway::rdmacm::communication_manager::{
    Event, EventChannel, GetEventErrorKind, Identifier, PortSpace,
};

fn oerr<E: std::error::Error>(e: E) -> io::Error {
    io::Error::other(format!("{e:?}"))
}

fn pollin() -> u32 {
    u32::try_from(libc::POLLIN).expect("POLLIN fits u32")
}

/// A non-blocking RDMA-CM event channel whose fd the reactor parks on (io_uring
/// `POLL_ADD`), so CM events are awaited without busy-polling. One per listener
/// (and one per active connect on the client side).
pub struct CmChannel {
    channel: Arc<EventChannel>,
}

impl CmChannel {
    /// Create a non-blocking CM event channel.
    pub fn new() -> io::Result<CmChannel> {
        let channel = EventChannel::new().map_err(oerr)?;
        channel.set_nonblocking(true)?;
        Ok(CmChannel { channel })
    }

    /// Create an RC (`PortSpace::Tcp`) cm_id on this channel.
    pub fn create_id(&self) -> io::Result<Arc<Identifier>> {
        self.channel.create_id(PortSpace::Tcp).map_err(oerr)
    }

    /// Await the next CM event, parking the reactor on the channel fd whenever
    /// none is queued (level-triggered `POLL_ADD`, re-issued each empty wakeup).
    pub async fn next_event(&self) -> io::Result<Event> {
        loop {
            match self.channel.get_cm_event() {
                Ok(event) => return Ok(event),
                Err(e) if matches!(e.0, GetEventErrorKind::NoEvent) => {
                    ioutgt_uring::ops::poll_add(self.channel.as_raw_fd(), pollin())?.await?;
                }
                Err(e) => return Err(oerr(e)),
            }
        }
    }
}

/// Copy out the inbound CM private data carried by `event` (the connecting
/// host's `nvme_rdma_cm_req` on a connect request). Copied, not borrowed, so the
/// caller may `ack` the event afterwards (which frees the underlying buffer).
pub fn private_data(event: &Event) -> Vec<u8> {
    // SAFETY: `event()` is valid until the Event is acked/dropped; for a
    // connection-management event the `conn` arm of the param union is active
    // and its private_data/_len describe the inbound buffer.
    unsafe {
        let ev = event.event().as_ptr();
        let conn = &(*ev).param.conn;
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

/// Fill the common RC `rdma_conn_param` fields shared by accept and connect.
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

/// Accept a connect request on `id`, binding the (already RTS) queue pair
/// `qp_num` and returning `reply` (an encoded [`crate::cmproto::CmRep`]).
pub fn accept(
    id: &Identifier,
    qp_num: u32,
    reply: &[u8],
    responder_resources: u8,
    initiator_depth: u8,
) -> io::Result<()> {
    let mut cp = conn_param(qp_num, reply, responder_resources, initiator_depth)?;
    // SAFETY: `id()` is valid for the Identifier's lifetime; rdma_accept copies
    // the private data synchronously, so `reply` need only outlive this call.
    let rc = unsafe { rdma_accept(id.id().as_ptr(), &mut cp) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Initiate a connect on `id` (client side), binding the INIT queue pair
/// `qp_num` and sending `request` (an encoded [`crate::cmproto::CmReq`]).
pub fn connect(
    id: &Identifier,
    qp_num: u32,
    request: &[u8],
    responder_resources: u8,
    initiator_depth: u8,
) -> io::Result<()> {
    let mut cp = conn_param(qp_num, request, responder_resources, initiator_depth)?;
    // SAFETY: `id()` is valid; rdma_connect copies the private data synchronously.
    let rc = unsafe { rdma_connect(id.id().as_ptr(), &mut cp) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Reject a connect request on `id`, returning `reason` (an encoded
/// [`crate::cmproto::CmRej`]) to the host.
pub fn reject(id: &Identifier, reason: &[u8]) -> io::Result<()> {
    let (ptr, len) = if reason.is_empty() {
        (std::ptr::null(), 0u8)
    } else {
        (
            reason.as_ptr() as *const c_void,
            u8::try_from(reason.len()).unwrap_or(u8::MAX),
        )
    };
    // SAFETY: `id()` is valid; rdma_reject copies the private data synchronously.
    let rc = unsafe { rdma_reject(id.id().as_ptr(), ptr, len) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmproto::{CM_FMT_1_0, CmRep, CmReq};
    use crate::rdma_devices;
    use ioutgt_uring::{QueueRuntime, RingConfig, ops};
    use sideway::ibverbs::completion::GenericCompletionQueue;
    use sideway::ibverbs::device_context::DeviceContext;
    use sideway::ibverbs::protection_domain::ProtectionDomain;
    use sideway::ibverbs::queue_pair::{GenericQueuePair, QueuePair, QueuePairState, SendOperationFlags};
    use std::net::SocketAddr;
    use std::time::Duration;

    type Held = (Arc<ProtectionDomain>, GenericCompletionQueue, GenericQueuePair);

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
        listen_id
            .bind_addr(format!("0.0.0.0:{port}").parse().unwrap())
            .map_err(oerr)?;
        listen_id.listen(1).map_err(oerr)?;

        let mut held: Option<Held> = None;
        let mut child: Option<Arc<Identifier>> = None;
        let mut seen_qid = None;
        loop {
            let event = ch.next_event().await?;
            let ty = format!("{:?}", event.event_type());
            eprintln!("[cm-server] event {ty}");
            match ty.as_str() {
                "ConnectRequest" => {
                    let req = CmReq::parse(&private_data(&event))?;
                    seen_qid = Some(req.qid);
                    let id = event.cm_id().ok_or_else(|| io::Error::other("no child cm_id"))?;
                    let ctx = id
                        .get_device_context()
                        .ok_or_else(|| io::Error::other("no device context"))?;
                    let (pd, cq, mut qp) = build_qp(&ctx)?;
                    qp.modify(&id.get_qp_attr(QueuePairState::Init).map_err(oerr)?)
                        .map_err(oerr)?;
                    qp.modify(&id.get_qp_attr(QueuePairState::ReadyToReceive).map_err(oerr)?)
                        .map_err(oerr)?;
                    qp.modify(&id.get_qp_attr(QueuePairState::ReadyToSend).map_err(oerr)?)
                        .map_err(oerr)?;
                    let rep = CmRep {
                        recfmt: CM_FMT_1_0,
                        crqsize: req.hsqsize,
                    }
                    .to_bytes();
                    accept(&id, qp.qp_number(), &rep, 1, 1)?;
                    held = Some((pd, cq, qp));
                    child = Some(id);
                    event.ack().map_err(oerr)?;
                }
                "Established" => {
                    // Hold the QP/cm_id alive so the client reaches its OWN
                    // Established; tearing down here would disconnect it first.
                    event.ack().map_err(oerr)?;
                }
                "Disconnected" => {
                    // Client finished and tore the connection down — done.
                    event.ack().map_err(oerr)?;
                    drop(held);
                    drop(child);
                    return Ok(seen_qid.expect("qid set before disconnect"));
                }
                other => {
                    event.ack().map_err(oerr)?;
                    if other == "DeviceRemoval" {
                        return Err(io::Error::other(format!("server CM event {other}")));
                    }
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
        id.resolve_addr(Some(src), dst, Duration::from_secs(5)).map_err(oerr)?;
        let mut held: Option<Held> = None;
        let mut addr_retries = 0u32;
        loop {
            let event = ch.next_event().await?;
            let ty = format!("{:?}", event.event_type());
            eprintln!("[cm-client] event {ty}");
            match ty.as_str() {
                "AddressResolved" => {
                    id.resolve_route(Duration::from_secs(5)).map_err(oerr)?;
                    event.ack().map_err(oerr)?;
                }
                "AddressError" => {
                    event.ack().map_err(oerr)?;
                    addr_retries += 1;
                    if addr_retries > 5 {
                        return Err(io::Error::other("resolve_addr failed (AddressError x5)"));
                    }
                    ops::sleep(Duration::from_millis(200)).unwrap().await.unwrap();
                    id.resolve_addr(Some(src), dst, Duration::from_secs(5)).map_err(oerr)?;
                }
                "RouteResolved" => {
                    let ctx = id
                        .get_device_context()
                        .ok_or_else(|| io::Error::other("no device context"))?;
                    let (pd, cq, mut qp) = build_qp(&ctx)?;
                    qp.modify(&id.get_qp_attr(QueuePairState::Init).map_err(oerr)?)
                        .map_err(oerr)?;
                    let req = CmReq {
                        recfmt: CM_FMT_1_0,
                        qid,
                        hrqsize: 128,
                        hsqsize: 127,
                        cntlid: 0xffff,
                    };
                    // Encode the 32-byte request (reuse CmRep's layout would be wrong;
                    // build the bytes explicitly).
                    let mut req_bytes = [0u8; 32];
                    req_bytes[0..2].copy_from_slice(&req.recfmt.to_le_bytes());
                    req_bytes[2..4].copy_from_slice(&req.qid.to_le_bytes());
                    req_bytes[4..6].copy_from_slice(&req.hrqsize.to_le_bytes());
                    req_bytes[6..8].copy_from_slice(&req.hsqsize.to_le_bytes());
                    req_bytes[8..10].copy_from_slice(&req.cntlid.to_le_bytes());
                    connect(&id, qp.qp_number(), &req_bytes, 1, 1)?;
                    held = Some((pd, cq, qp));
                    event.ack().map_err(oerr)?;
                }
                "ConnectResponse" => {
                    // Active side with an externally-managed QP: move it to RTS
                    // and `establish()` — that completes the handshake; there is
                    // NO separate Established event on the active side.
                    let qp = &mut held.as_mut().expect("qp built before response").2;
                    qp.modify(&id.get_qp_attr(QueuePairState::ReadyToReceive).map_err(oerr)?)
                        .map_err(oerr)?;
                    qp.modify(&id.get_qp_attr(QueuePairState::ReadyToSend).map_err(oerr)?)
                        .map_err(oerr)?;
                    id.establish().map_err(oerr)?;
                    event.ack().map_err(oerr)?;
                    // Connected. Tearing down here disconnects, which the server
                    // sees as Disconnected (its cue that we are done).
                    id.disconnect().map_err(oerr)?;
                    drop(held);
                    return Ok(());
                }
                "Established" => {
                    // Not expected for the active side, but harmless: done.
                    event.ack().map_err(oerr)?;
                    id.disconnect().map_err(oerr)?;
                    drop(held);
                    return Ok(());
                }
                "Rejected" => {
                    event.ack().map_err(oerr)?;
                    return Err(io::Error::other("client connect rejected"));
                }
                other => {
                    event.ack().map_err(oerr)?;
                    return Err(io::Error::other(format!("client CM event {other}")));
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

            let combined = async {
                let sq = server.await.unwrap()?;
                client.await.unwrap()?;
                Ok::<u16, io::Error>(sq)
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
