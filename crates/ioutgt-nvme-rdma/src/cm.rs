//! NVMe/RDMA connection management: listen for connect requests, read the
//! host's [`cmproto::CmReq`], build the queue pair on the connection's device,
//! and `accept` (with a [`cmproto::CmRep`]) or `reject`.
//!
//! sideway drives the CM (its `EventChannel`/`Identifier` give the cm_id's
//! `DeviceContext` via `get_device_context()` and the CM-derived QP attrs via
//! `get_qp_attr()`), but does not wrap the operations that carry private data.
//! Those are done here over `rdma-mummy-sys` through sideway's two raw
//! escape-hatch accessors (`Event::event()`, `Identifier::id()` — the ioutgt
//! vendor patch). All NVMe-specific knowledge stays in this crate.

use std::ffi::c_void;
use std::io;

use rdma_mummy_sys::{rdma_accept, rdma_conn_param, rdma_reject};
use sideway::rdmacm::communication_manager::{Event, Identifier};

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

/// Accept a connect request on `id`, binding the (already RTS) queue pair
/// `qp_num` and returning `reply` (an encoded [`cmproto::CmRep`]) to the host.
/// `responder_resources`/`initiator_depth` bound RDMA READ/atomic concurrency.
pub fn accept(
    id: &Identifier,
    qp_num: u32,
    reply: &[u8],
    responder_resources: u8,
    initiator_depth: u8,
) -> io::Result<()> {
    // SAFETY: a zeroed rdma_conn_param is valid; we fill the fields we use.
    let mut cp: rdma_conn_param = unsafe { std::mem::zeroed() };
    cp.qp_num = qp_num;
    cp.responder_resources = responder_resources;
    cp.initiator_depth = initiator_depth;
    cp.flow_control = 1;
    cp.rnr_retry_count = 7;
    if !reply.is_empty() {
        cp.private_data = reply.as_ptr() as *const c_void;
        cp.private_data_len =
            u8::try_from(reply.len()).map_err(|_| io::Error::other("CM reply > 255 bytes"))?;
    }
    // SAFETY: `id()` is valid for the Identifier's lifetime; rdma_accept copies
    // the private data synchronously, so `reply` need only outlive this call.
    let rc = unsafe { rdma_accept(id.id().as_ptr(), &mut cp) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Reject a connect request on `id`, returning `reason` (an encoded
/// [`cmproto::CmRej`]) to the host.
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
