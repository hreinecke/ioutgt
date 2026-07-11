//! Queue-thread connection lifecycle: spin up the queue and its tasks,
//! drive the connection (Connect, then the recv loop), and tear it down.
//! All IO goes through the thread's io_uring reactor.
//!
//! The recv state machine (capsule commands with in-capsule or
//! R2T-solicited data, DDGST verification) lives in [`crate::recv`]; the
//! ordered send path (C2HData, R2Ts, response capsules) lives in
//! [`crate::send`]. This module only spawns and joins them.

use std::os::fd::{AsRawFd, OwnedFd};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use crate::queue::NvmeTcpQueue;
use crate::recv::recv_loop;
use crate::send::{self, ARENA_PER_ITEM};
use ioutgt_core::backend::Backend;
use ioutgt_nvme::controller::Registry;
use ioutgt_nvme::dispatch::{self, ConnCtx, Role};
use ioutgt_nvme::fabrics::ConnectData;
use ioutgt_nvme::spec::Sqe;
use ioutgt_nvme::subsystem::PortConfig;
use ioutgt_uring::ops;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

pub use ioutgt_core::permit::ConnPermit;

/// Everything a queue thread receives to run one connection.
#[allow(missing_docs)]
pub struct QueueConn<B> {
    pub fd: OwnedFd,
    pub hdr_digest: bool,
    pub data_digest: bool,
    pub qid: u16,
    /// Queue depth in entries (Connect sqsize + 1).
    pub sqsize: u16,
    /// SQ flow control disabled (Connect CATTR bit 2).
    pub sqhd_disabled: bool,
    /// Ship payload-carrying batches as SENDMSG_ZC, gating slot reuse
    /// on the zero-copy notification (--send-zc).
    pub send_zc: bool,
    /// The already-consumed Connect command, to be executed as this
    /// queue's first command.
    pub connect_sqe: Sqe,
    /// The Connect command's 1024-byte data payload.
    pub connect_data: Box<ConnectData>,
    /// Port configuration (subsystems reachable here).
    pub port: Arc<PortConfig<B>>,
    /// Cross-thread controller registry.
    pub registry: Arc<Registry>,
    /// Active-connection accounting; dropped when this connection ends.
    pub permit: ConnPermit,
}

/// Drive one queue connection to completion (EOF, error, or term).
///
/// `on_ctx` runs once the dispatch context exists — the binary's admin
/// thread uses it to register live controllers for AER nudges.
pub async fn run_queue<B: Backend>(conn: QueueConn<B>, on_ctx: impl FnOnce(&Rc<ConnCtx<B>>)) {
    // Admin: a pool sized so its (synchronous) data leases never block.
    // IO: a shared pool deliberately smaller than depth × MDTS — slots
    // lease on demand and park / fall back under pressure.
    let pool_bytes = if conn.qid == 0 {
        usize::from(conn.sqsize).max(1) * ioutgt_nvme::ADMIN_DATA_MAX
    } else {
        conn.port.queue_buf_bytes
    };
    let queue = NvmeTcpQueue::new(conn.qid, conn.sqsize, pool_bytes, conn.sqhd_disabled);
    // Register the whole data-buffer pool arena as one io_uring fixed buffer,
    // so disk IO from pooled slots uses READV_FIXED/WRITEV_FIXED — the kernel
    // reuses the pre-pinned mapping instead of mapping the pages every IO.
    // Best-effort: a no-op on kernels without fixed-buffer support, and the
    // backend then stays on plain readv/writev. Released at teardown.
    {
        let pool = queue.nvme.slots.pool();
        let (ptr, len) = pool.arena();
        match ioutgt_uring::register_pool_buffer(ptr, len) {
            Some(idx) => {
                pool.set_buf_index(idx);
                // Reserve the send-path header arena (page-aligned) from the
                // same registered buffer now, before any slot allocates, so a
                // contiguous run is guaranteed and arena + payloads share this
                // buf_index for vectored fixed-buffer ZC sends.
                let arena_bytes = 2 * usize::from(conn.sqsize) * ARENA_PER_ITEM;
                let reserved = pool.reserve_arena(arena_bytes).is_some();
                debug!(
                    qid = conn.qid,
                    idx, reserved, "pool buffer registered: fixed disk IO + send arena"
                );
            }
            // Distinguish the two None causes: a full table is a capacity
            // event worth a warning (the connection loses the optimization);
            // no kernel support is expected and stays quiet.
            None if ioutgt_uring::fixed_buffers_supported() => warn!(
                qid = conn.qid,
                "fixed-buffer table full; disk IO on plain readv/writev"
            ),
            None => debug!(
                qid = conn.qid,
                "no kernel fixed-buffer support; plain readv/writev"
            ),
        }
    }
    let fd = conn.fd.as_raw_fd();
    let peer = peer_of(fd);
    let ctx = if conn.qid == 0 {
        ConnCtx::new_admin(
            Rc::clone(&queue.nvme),
            Arc::clone(&conn.port),
            Arc::clone(&conn.registry),
            conn.connect_data,
            peer,
        )
    } else {
        ConnCtx::new_io(
            Rc::clone(&queue.nvme),
            Arc::clone(&conn.port),
            Arc::clone(&conn.registry),
            conn.connect_data,
            peer,
        )
    };

    on_ctx(&ctx);

    let mut tasks = spawn_slot_tasks(&queue, &ctx);
    if let Role::Admin(_) = &ctx.role {
        tasks.push(spawn_keepalive_watchdog(Rc::clone(&ctx), fd));
    }
    let send_task = spawn_send_task(
        Rc::clone(&queue),
        fd,
        conn.hdr_digest,
        conn.data_digest,
        conn.send_zc,
    );

    // The Connect command was consumed on the control thread; run it
    // through the normal slot pipeline as this queue's first command.
    let tag = queue.claim_tag().expect("fresh queue has free tags");
    queue.submit(tag, conn.connect_sqe);

    // Receive path (this task). The admin queue (qid 0) never carries an H2C
    // write payload, so the zero-copy ring buys it nothing — skip it and keep
    // the classic recv path, sparing a ring's pinned memory + 2 fixed-buffer
    // slots + a bgid per controller on the (shared) admin thread.
    let recv_buf_bytes = if conn.qid == 0 {
        0
    } else {
        conn.port.recv_buf_bytes
    };
    if let Err(err) = recv_loop(
        &queue,
        fd,
        conn.hdr_digest,
        conn.data_digest,
        recv_buf_bytes,
    )
    .await
    {
        debug!(qid = conn.qid, "connection closed: {err}");
    }

    teardown(&queue, &ctx, fd, send_task, tasks).await;
    // conn.fd drops here, closing the socket; in-flight ops orphan and
    // drain through the reactor.
}

/// One persistent task per command slot: each waits for its tag's next
/// command, executes it, and posts the completion.
fn spawn_slot_tasks<B: Backend>(
    queue: &Rc<NvmeTcpQueue>,
    ctx: &Rc<ConnCtx<B>>,
) -> Vec<JoinHandle<()>> {
    (0..queue.sqsize)
        .map(|tag| {
            let queue = Rc::clone(queue);
            let ctx = Rc::clone(ctx);
            tokio::task::spawn_local(async move {
                loop {
                    let sqe = queue.await_command(tag).await;
                    let outcome = dispatch::execute(&ctx, tag, &sqe).await;
                    queue.complete(tag, outcome.cqe, outcome.data_len);
                }
            })
        })
        .collect()
}

/// Keep-alive watchdog (admin queues): close the socket when the host
/// goes silent past KATO + grace, which unwinds the whole connection.
fn spawn_keepalive_watchdog<B: Backend>(ctx: Rc<ConnCtx<B>>, fd: i32) -> JoinHandle<()> {
    tokio::task::spawn_local(async move {
        loop {
            let Ok(sleep) = ops::sleep(Duration::from_secs(5)) else {
                return;
            };
            if sleep.await.is_err() {
                return;
            }
            let Role::Admin(admin) = &ctx.role else {
                return;
            };
            if let Some(silent) = admin.keepalive_expired() {
                info!(
                    cntlid = admin.cntlid.get(),
                    silent_ms = silent,
                    "keep-alive expired; closing connection"
                );
                // SAFETY: fd is valid for the connection's lifetime;
                // shutdown only signals, never frees.
                unsafe { libc::shutdown(fd, libc::SHUT_RDWR) };
                return;
            }
        }
    })
}

/// Send path. Held separately from the slot tasks: teardown must join
/// it (the gather send references slot buffers) before freeing the
/// queue.
fn spawn_send_task(
    queue: Rc<NvmeTcpQueue>,
    fd: i32,
    hdr_digest: bool,
    data_digest: bool,
    send_zc: bool,
) -> JoinHandle<()> {
    tokio::task::spawn_local(async move {
        if let Err(err) = send::send_loop(&queue, fd, hdr_digest, data_digest, send_zc).await {
            debug!(qid = queue.qid, "send loop ended: {err}");
            // A dead send path leaves the connection half-alive:
            // the recv loop keeps accepting commands whose
            // responses can never ship, and the host only notices
            // at its IO timeout (~30 s). Shut the socket down so
            // the recv loop sees EOF and teardown runs now.
            // SAFETY: fd is valid for the connection's lifetime;
            // shutdown only signals, never frees.
            unsafe { libc::shutdown(fd, libc::SHUT_RDWR) };
        }
    })
}

/// Poll `done` every 2 ms for up to 10 s; each call gets its own
/// budget. The teardown quiesce primitive.
async fn quiesce(mut done: impl FnMut() -> bool) {
    let mut waited = 0u32;
    while !done() && waited < 10_000 {
        match ops::sleep(Duration::from_millis(2)) {
            Ok(sleep) => {
                let _ = sleep.await;
            }
            Err(_) => break,
        }
        waited += 2;
    }
}

/// Post-recv teardown: quiesce executing slots and the send task, then
/// abort the per-tag tasks — or leak everything on timeout.
async fn teardown<B: Backend>(
    queue: &Rc<NvmeTcpQueue>,
    ctx: &Rc<ConnCtx<B>>,
    fd: i32,
    send_task: JoinHandle<()>,
    tasks: Vec<JoinHandle<()>>,
) {
    // Resolve parked AERs (their slots count as executing but reference
    // no kernel-visible memory) so the drain below terminates promptly.
    ctx.close();

    // Backend ops in flight reference slot memory: wait for executing
    // slots to finish before aborting tasks and freeing the queue.
    quiesce(|| queue.executing() == 0).await;
    // Stop the send task and wait for any in-flight send op before
    // anything it references is freed. shutdown() unwedges a send
    // parked on a full socket buffer; close_send() unparks an idle
    // send loop.
    queue.close_send();
    // SAFETY: fd is valid for the connection's lifetime; shutdown only
    // signals, never frees.
    unsafe { libc::shutdown(fd, libc::SHUT_RDWR) };
    // Own budget: the executing drain may have spent all of its wait,
    // and the send task needs at least one poll cycle to observe
    // close_send/shutdown.
    quiesce(|| send_task.is_finished()).await;
    if queue.executing() > 0 || !send_task.is_finished() {
        // A wedged backend op: leak the queue AND the slot tasks rather
        // than free memory the kernel may still write to. A suspended
        // backend future can own a private buffer (e.g. the write-zeroes
        // fallback chunk) referenced by an in-flight raw kernel op;
        // aborting the task would drop and free that buffer mid-DMA.
        // Leaking the tasks keeps every such future — and its buffer —
        // alive for the process's remaining lifetime. The same applies
        // to the send task: its in-flight gather op references slot
        // buffers and the batch arena.
        warn!(
            qid = queue.qid,
            executing = queue.executing(),
            "teardown timeout; leaking queue and tasks"
        );
        std::mem::forget(Rc::clone(queue));
        std::mem::forget(send_task);
        for task in tasks {
            std::mem::forget(task);
        }
    } else {
        send_task.abort();
        for task in &tasks {
            task.abort();
        }
        // All ops have drained: release the pool's fixed-buffer slot so the
        // index is reusable before the queue (and its arena) is freed on
        // return. The leak branch above intentionally keeps it pinned.
        if let Some(idx) = queue.nvme.slots.pool().take_buf_index() {
            ioutgt_uring::unregister_pool_buffer(idx);
        }
    }
    // Tear down the controller when its admin queue dies.
    if let Role::Admin(admin) = &ctx.role {
        let cntlid = admin.cntlid.get();
        if cntlid != 0 {
            ctx.registry.remove(cntlid);
            info!(cntlid, "controller removed");
        }
    }
}

/// Peer (remote) address of socket `fd` as `"ip:port"`, `"?"` on failure.
/// Used by `LIST_CONTROLLER` so the harness can map a connection's source
/// port to its qid for hardware NIC flow steering.
fn peer_of(fd: std::os::fd::RawFd) -> String {
    // SAFETY: a zeroed sockaddr_storage is a valid buffer for getpeername to
    // overwrite; `len` matches its size.
    let mut ss: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut len = libc::socklen_t::try_from(std::mem::size_of::<libc::sockaddr_storage>())
        .expect("sockaddr_storage fits socklen_t");
    // SAFETY: `fd` is a socket; ss/len describe a valid writable buffer of the
    // stated size.
    let rc = unsafe { libc::getpeername(fd, std::ptr::addr_of_mut!(ss).cast(), &mut len) };
    if rc != 0 {
        return "?".to_owned();
    }
    match i32::from(ss.ss_family) {
        libc::AF_INET => {
            // SAFETY: family is AF_INET, so `ss` is a sockaddr_in.
            let a = unsafe { &*std::ptr::addr_of!(ss).cast::<libc::sockaddr_in>() };
            let ip = std::net::Ipv4Addr::from(u32::from_be(a.sin_addr.s_addr));
            format!("{ip}:{}", u16::from_be(a.sin_port))
        }
        libc::AF_INET6 => {
            // SAFETY: family is AF_INET6, so `ss` is a sockaddr_in6.
            let a = unsafe { &*std::ptr::addr_of!(ss).cast::<libc::sockaddr_in6>() };
            let ip = std::net::Ipv6Addr::from(a.sin6_addr.s6_addr);
            format!("[{ip}]:{}", u16::from_be(a.sin6_port))
        }
        _ => "?".to_owned(),
    }
}
