//! The NVMe/RDMA CM listener: bind a CM event channel, accept connect
//! requests, and pump (ack) the lifecycle events of already-accepted
//! connections in between. Runs on its own OS thread (`ioutgt-rdma-cm`, see
//! [`crate::transport`]) with zero coupling to the `RdmaQueue` reactor in
//! [`crate::target`].

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::Notify;

use crate::cm::{CmChannel, EventType, Identifier};
use crate::cmproto::{CM_FMT_1_0, CmRej, CmReq, reject_status};
use crate::oerr;

/// A freshly accepted connection request, pre-QP-build: the cm_id plus the
/// host's [`CmReq`] routing fields. The CM-thread half of accepting (what the
/// harness `Transport::Raw` will be); [`RdmaListener::accept`] produces it and a
/// caller turns it into an [`RdmaConn`] (adding port/registry) for [`run_conn`].
pub struct RdmaRaw {
    /// The accepted CM identifier (see [`RdmaConn::id`]).
    pub(crate) id: Identifier,
    /// NVMe-oF queue id (0 = admin).
    pub(crate) qid: u16,
    /// Host SQ size, 0-based.
    pub(crate) hsqsize: u16,
    /// Fired by the listener when this connection's `Disconnected` arrives; the
    /// queue's reap loop ends on it (see [`RdmaConn::stop`]).
    pub(crate) stop: Arc<Notify>,
}

/// A live accepted connection the listener tracks: its cm_id (kept alive +
/// matched against later CM events) and the stop signal to end its queue.
struct ConnSlot {
    id: Identifier,
    stop: Arc<Notify>,
}

/// The NVMe/RDMA listener: a bound CM event channel that yields one accepted
/// connection per [`accept`](Self::accept), pumping (acking) the lifecycle events
/// of already-accepted connections in between. This is the connection-source seam
/// (the harness `Transport::bind`/`accept`); it owns the CM channel and holds
/// accepted cm_ids alive (best-effort teardown — see `docs/nvme-rdma.md`).
pub(crate) struct RdmaListener {
    ch: CmChannel,
    /// The listening cm_id — kept alive for the channel's lifetime.
    _listen_id: Identifier,
    /// Live accepted connections; entries are pruned on `Disconnected` (which
    /// also fires the connection's stop signal), bounding this across reconnects.
    conns: Vec<ConnSlot>,
}

impl RdmaListener {
    /// Bind a CM event channel + listen cm_id to `listen` and start listening.
    pub(crate) async fn bind(listen: SocketAddr) -> io::Result<RdmaListener> {
        let ch = CmChannel::new()?;
        let listen_id = ch.create_id()?;
        // The RDMA device's GID/IP association is populated asynchronously after a
        // soft-RoCE (rxe) netdev is added, so `rdma_bind_addr` on the concrete IP
        // can transiently fail with ENODEV even once the port is ACTIVE. Retry.
        // (Binding the unspecified address would skip GID resolution but does not
        // receive connects on rxe, so we bind the concrete IP.)
        let mut attempt = 0;
        loop {
            match listen_id.bind_addr(listen) {
                Ok(()) => break,
                Err(e) if attempt < 120 => {
                    attempt += 1;
                    if attempt % 8 == 0 {
                        tracing::info!(
                            "nvme-rdma bind {listen} not ready (attempt {attempt}): {e:?}"
                        );
                    }
                    ioutgt_uring::ops::sleep(std::time::Duration::from_millis(250))?.await?;
                }
                Err(e) => return Err(e),
            }
        }
        listen_id.listen(128)?;
        tracing::info!("nvme-rdma listening on {listen}");
        Ok(RdmaListener {
            ch,
            _listen_id: listen_id,
            conns: Vec::new(),
        })
    }

    /// Await the next accepted connection. The CM channel multiplexes all cm_ids,
    /// so this also acks the lifecycle events of already-accepted connections
    /// (Established, Disconnected, …) and rejects malformed connect requests,
    /// returning only on a valid CONNECT_REQUEST.
    pub(crate) async fn accept(&mut self) -> io::Result<RdmaRaw> {
        loop {
            let event = self.ch.next_event().await?;
            match event.event_type() {
                EventType::ConnectRequest => {
                    // Reject with the proper `nvme_rdma_cm_rej` status so the
                    // host logs the reason instead of a bare reject (nvmet
                    // parity). Adopt even when rejecting: ownership of the
                    // child cm_id is ours either way; dropping it destroys it.
                    let sts = match CmReq::parse(&event.private_data()) {
                        Ok(req) if req.recfmt != CM_FMT_1_0 => Err(reject_status::INVALID_RECFMT),
                        Ok(req) => Ok(req),
                        Err(_) => Err(reject_status::INVALID_LEN),
                    };
                    match (sts, self.ch.adopt(&event)) {
                        (Ok(req), Ok(id)) => {
                            let stop = Arc::new(Notify::new());
                            self.conns.push(ConnSlot {
                                id: id.clone(),
                                stop: Arc::clone(&stop),
                            });
                            event.ack().map_err(oerr)?;
                            return Ok(RdmaRaw {
                                id,
                                qid: req.qid,
                                hsqsize: req.hsqsize,
                                stop,
                            });
                        }
                        (Err(sts), id) => {
                            tracing::warn!(sts, "nvme-rdma rejecting connect request");
                            if let Ok(id) = id {
                                let _ = id.reject(&CmRej::new(sts).to_bytes());
                            }
                            event.ack().map_err(oerr)?;
                        }
                        (Ok(_), Err(e)) => {
                            tracing::warn!("nvme-rdma connect request without cm_id: {e}");
                            event.ack().map_err(oerr)?;
                        }
                    }
                }
                EventType::Established => event.ack().map_err(oerr)?,
                // The host tore the connection down: drop our keep-alive cm_id
                // clone so it isn't retained for the process lifetime (bounds
                // `conns` across reconnect churn — a reconnect-soak leak fix). The
                // queue's own clone (in its RdmaConn) drops when its reap loop ends
                // on the flushed completions, so the cm_id is destroyed then.
                EventType::Disconnected => {
                    // Match by raw pointer (never dereferenced): only a cm_id we
                    // still hold alive can compare equal. Send the DREP, fire the
                    // connection's stop signal so its reap loop ends (our
                    // manually-built QP isn't cm_id-associated, so
                    // rdma_disconnect doesn't flush it), and drop the slot —
                    // bounding `conns` across reconnects.
                    let raw = event.raw_id();
                    if let Some(pos) = self.conns.iter().position(|c| c.id.is_raw(raw)) {
                        let slot = self.conns.swap_remove(pos);
                        let _ = slot.id.disconnect();
                        slot.stop.notify_one();
                    }
                    event.ack().map_err(oerr)?;
                }
                other => {
                    tracing::debug!("nvme-rdma CM event {other:?}");
                    event.ack().map_err(oerr)?;
                }
            }
        }
    }
}
