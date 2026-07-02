//! Test-only RC-loopback scaffolding for the rxe gates (`#[cfg(test)]` — the
//! production path builds its resources in `target`/`cm` directly on sideway):
//! device open, PD/MR/CQ/QP creation, and a manual RESET→INIT→RTR→RTS loopback
//! connect. The connect parameters here (MTU, rd_atomic, timeouts) are
//! hardcoded for a single-host rxe loopback and are NOT what a CM-established
//! box connection negotiates — do not read them as production behavior.

use std::io;
use std::sync::Arc;

use sideway::ibverbs::AccessFlags;
use sideway::ibverbs::address::{AddressHandleAttribute, GidEntry, GidType};
use sideway::ibverbs::completion::{CompletionChannel, GenericCompletionQueue};
use sideway::ibverbs::device::{DeviceInfo, DeviceList};
use sideway::ibverbs::device_context::DeviceContext;
use sideway::ibverbs::memory_region::MemoryRegion;
use sideway::ibverbs::protection_domain::ProtectionDomain;
use sideway::ibverbs::queue_pair::{
    GenericQueuePair, QueuePair, QueuePairAttribute, QueuePairState, SendOperationFlags,
};

use crate::oerr;

/// The RDMA devices the host exposes, by name. Empty when no provider is
/// present — no HCA and no soft-RoCE (`rxe`) configured — which is the expected
/// state on a plain dev box, so callers must treat an empty list as "RDMA
/// unavailable", not an error.
pub fn rdma_devices() -> Vec<String> {
    match DeviceList::new() {
        Ok(list) => list.iter().map(|dev| dev.name()).collect(),
        Err(_) => Vec::new(),
    }
}

/// Where a remote RC peer lives, for wiring the INIT→RTR transition. In the
/// real transport these come from the RDMA-CM exchange; in the loopback test the
/// QP's own number and the local GID are used (a QP connected to itself).
pub struct RcDest {
    /// The peer QP number (`dest_qp_num`).
    pub qp_num: u32,
    /// The peer's GID (RoCE global routing; required on rxe).
    pub gid: sideway::ibverbs::address::Gid,
    /// The local source GID index used to reach the peer (verbs allows a wider
    /// index than `u8`; narrowed at the `connect_rc` call with an error rather
    /// than a silent substitution).
    pub src_gid_index: u32,
}

/// An opened RDMA device plus its protection domain — the root from which the
/// transport registers memory and builds queue pairs. One per reactor thread.
pub struct Rdma {
    /// The device context (`ibv_context`).
    pub ctx: Arc<DeviceContext>,
    /// The protection domain all this thread's MRs and QPs live in.
    pub pd: Arc<ProtectionDomain>,
    /// IB port number (RoCE devices expose a single port, 1).
    pub port: u8,
}

impl Rdma {
    /// Open the first RDMA device and allocate a protection domain, or `None`
    /// when no RDMA device is present (RDMA unavailable — not an error).
    pub fn open_first() -> io::Result<Option<Rdma>> {
        let list = DeviceList::new().map_err(oerr)?;
        let Some(dev) = list.iter().next() else {
            return Ok(None);
        };
        let ctx = dev.open().map_err(oerr)?;
        let pd = ctx.alloc_pd().map_err(oerr)?;
        Ok(Some(Rdma { ctx, pd, port: 1 }))
    }

    /// Register `[ptr, ptr+len)` as a memory region with the given access,
    /// returning the MR (which carries the lkey for local use and rkey for
    /// remote RDMA READ/WRITE).
    ///
    /// # Safety
    ///
    /// `ptr..ptr+len` must be a valid, owned region that outlives the returned
    /// MR; the kernel pins these pages for the MR's lifetime.
    pub unsafe fn register(
        &self,
        ptr: *mut u8,
        len: usize,
        access: AccessFlags,
    ) -> io::Result<Arc<MemoryRegion>> {
        // SAFETY: forwarded to the caller's contract on `ptr`/`len`.
        unsafe { self.pd.reg_mr(ptr as usize, len, access) }.map_err(oerr)
    }

    /// Create an extended completion queue with room for `cqe` entries.
    pub fn create_cq(&self, cqe: u32) -> io::Result<GenericCompletionQueue> {
        let mut builder = self.ctx.create_cq_builder();
        builder.setup_cqe(cqe);
        Ok(GenericCompletionQueue::from(builder.build_ex().map_err(oerr)?))
    }

    /// Create a completion channel — the fd the reactor parks on (via
    /// `IORING_OP_POLL_ADD`) to wake on completions instead of busy-polling.
    /// The fd is set non-blocking so the reactor-driven drain
    /// ([`crate::cq::drain_events`]) never blocks the queue thread on a spurious
    /// wakeup (it parks via `poll_add` instead).
    pub fn create_comp_channel(&self) -> io::Result<Arc<CompletionChannel>> {
        let channel = self.ctx.create_comp_channel().map_err(oerr)?;
        channel.set_nonblocking(true)?;
        Ok(channel)
    }

    /// Create an extended completion queue bound to `channel`, so a completion
    /// signals the channel fd once the CQ is armed (see [`crate::cq`]).
    pub fn create_cq_on_channel(
        &self,
        channel: &Arc<CompletionChannel>,
        cqe: u32,
    ) -> io::Result<GenericCompletionQueue> {
        let mut builder = self.ctx.create_cq_builder();
        builder.setup_cqe(cqe).setup_comp_channel(channel, 0);
        Ok(GenericCompletionQueue::from(builder.build_ex().map_err(oerr)?))
    }

    /// Build an RC queue pair bound to the given send/recv completion queues,
    /// with the send-op flags NVMe/RDMA needs (SEND, RDMA WRITE, RDMA READ).
    pub fn create_rc_qp(
        &self,
        send_cq: &GenericCompletionQueue,
        recv_cq: &GenericCompletionQueue,
        max_wr: u32,
        max_sge: u32,
    ) -> io::Result<GenericQueuePair> {
        let mut builder = self.pd.create_qp_builder();
        builder
            .setup_max_send_wr(max_wr)
            .setup_max_recv_wr(max_wr)
            .setup_max_send_sge(max_sge)
            .setup_max_recv_sge(max_sge)
            .setup_send_cq(send_cq.clone())
            .setup_recv_cq(recv_cq.clone())
            .setup_send_ops_flags(
                SendOperationFlags::Send | SendOperationFlags::Write | SendOperationFlags::Read,
            );
        Ok(builder.build_ex().map_err(oerr)?.into())
    }

    /// A usable local GID entry for RoCE global routing. Prefers a routable
    /// RoCEv2 GID (the IPv4-mapped, non-link-local one) — routing through a
    /// link-local `fe80::` GID fails the RTR transition with "network
    /// unreachable" on rxe — falling back through RoCEv2-link-local to any
    /// non-zero GID.
    pub fn local_gid(&self) -> io::Result<GidEntry> {
        let table = self.ctx.query_gid_table().map_err(oerr)?;
        let rank = |g: &GidEntry| -> i32 {
            if g.gid().is_zero() {
                return -1;
            }
            match (g.gid_type(), g.gid().is_unicast_link_local()) {
                (GidType::RoceV2, false) => 3,
                (GidType::RoceV2, true) => 2,
                (_, false) => 1,
                _ => 0,
            }
        };
        let best = table
            .iter()
            .enumerate()
            .filter(|(_, g)| rank(g) >= 0)
            .max_by_key(|(_, g)| rank(g))
            .map(|(i, _)| i)
            .ok_or_else(|| io::Error::other("no usable GID on port"))?;
        Ok(table.into_iter().nth(best).expect("index in range"))
    }

    /// Drive an RC QP RESET→INIT→RTR→RTS, wiring it to `dest`. The same
    /// sequence the transport runs once the RDMA-CM exchange yields the peer's
    /// QP number and GID; the loopback test passes the QP's own number.
    pub fn connect_rc(&self, qp: &mut GenericQueuePair, dest: &RcDest) -> io::Result<()> {
        // RESET → INIT
        let mut init = QueuePairAttribute::new();
        init.setup_state(QueuePairState::Init)
            .setup_pkey_index(0)
            .setup_port(self.port)
            .setup_access_flags(
                AccessFlags::LocalWrite | AccessFlags::RemoteWrite | AccessFlags::RemoteRead,
            );
        qp.modify(&init).map_err(oerr)?;

        // INIT → RTR
        let src_gid_index = u8::try_from(dest.src_gid_index)
            .map_err(|_| io::Error::other("src_gid_index exceeds u8"))?;
        let mut ah = AddressHandleAttribute::new();
        ah.setup_port(self.port)
            .setup_service_level(0)
            .setup_grh_dest_gid(&dest.gid)
            .setup_grh_src_gid_index(src_gid_index)
            .setup_grh_hop_limit(64);
        // Match the peer's path MTU by querying the port's active MTU rather than
        // hardcoding. Our QP is not cm_id-associated, so a path MTU below the host's
        // CM-negotiated value (1024 vs an mlx5 link's 4096) is an RC MTU mismatch
        // that stalls/corrupts large RDMA transfers under sustained load.
        let active_mtu = self
            .ctx
            .query_port(self.port)
            .map_err(|e| io::Error::other(format!("query_port active_mtu: {e:?}")))?
            .active_mtu();
        let mut rtr = QueuePairAttribute::new();
        rtr.setup_state(QueuePairState::ReadyToReceive)
            .setup_path_mtu(active_mtu)
            .setup_dest_qp_num(dest.qp_num)
            .setup_rq_psn(0)
            .setup_max_dest_read_atomic(1)
            .setup_min_rnr_timer(12)
            .setup_address_vector(&ah);
        qp.modify(&rtr).map_err(oerr)?;

        // RTR → RTS
        let mut rts = QueuePairAttribute::new();
        rts.setup_state(QueuePairState::ReadyToSend)
            .setup_sq_psn(0)
            .setup_timeout(14)
            .setup_retry_cnt(7)
            .setup_rnr_retry(7)
            .setup_max_read_atomic(1);
        qp.modify(&rts).map_err(oerr)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sideway::ibverbs::completion::WorkCompletionStatus;
    use sideway::ibverbs::queue_pair::{
        PostSendGuard, SetScatterGatherEntry, WorkRequestFlags,
    };

    #[test]
    fn enumerate_devices_links_and_runs() {
        // Exercises the sideway -> system libibverbs path end to end. The count
        // is environment dependent (0 without an HCA or a configured rxe
        // device), so we assert only that the call links, runs, and frees.
        println!("rdma devices: {:?}", rdma_devices());
    }

    /// Busy-poll `cq` until a completion with `wr_id` arrives (bounded so a lost
    /// completion fails the test instead of hanging). Asserts success status.
    /// Single-outstanding-WR only: it drops any completion whose `wr_id` does
    /// not match, which is correct for this strictly-sequential test but must
    /// not be reused once the real loops keep multiple WRs in flight.
    fn await_wc(cq: &GenericCompletionQueue, wr_id: u64) -> io::Result<()> {
        for _ in 0..50_000_000u64 {
            let Ok(poller) = cq.start_poll() else {
                continue; // CQ empty this round
            };
            for wc in poller {
                if wc.status() != WorkCompletionStatus::Success as u32 {
                    return Err(io::Error::other(format!(
                        "wr {} failed: status {}",
                        wc.wr_id(),
                        wc.status()
                    )));
                }
                if wc.wr_id() == wr_id {
                    return Ok(());
                }
            }
        }
        Err(io::Error::other(format!("wr {wr_id} never completed")))
    }

    /// Self-connected RC QP over a real device (soft-RoCE `rxe` in the test
    /// environment): SEND/RECV moves a capsule-sized message, RDMA WRITE pushes
    /// data into a remote-keyed region, RDMA READ pulls it back. Skips when no
    /// RDMA device is present so the suite still passes on a plain dev box.
    #[test]
    fn rxe_loopback_send_write_read() -> io::Result<()> {
        let Some(rdma) = Rdma::open_first()? else {
            eprintln!("skip rxe_loopback: no RDMA device (configure rdma_rxe to run)");
            return Ok(());
        };

        const LEN: u32 = 4096;
        const N: usize = LEN as usize;
        // Stable, page-able backing buffers (never reallocated for the test's life).
        let mut send_buf = vec![0u8; N];
        let mut recv_buf = vec![0u8; N];
        let mut remote_buf = vec![0u8; N];
        let mut read_back = vec![0u8; N];
        for (i, b) in send_buf.iter_mut().enumerate() {
            *b = u8::try_from(i % 251).unwrap_or(0);
        }

        let all = AccessFlags::LocalWrite | AccessFlags::RemoteWrite | AccessFlags::RemoteRead;
        // Each buffer outlives its MR (dropped at end of test) and is not moved
        // or resized while registered.
        // SAFETY: send_buf is live and stable for the test.
        let send_mr = unsafe { rdma.register(send_buf.as_mut_ptr(), N, all)? };
        // SAFETY: recv_buf is live and stable for the test.
        let recv_mr = unsafe { rdma.register(recv_buf.as_mut_ptr(), N, all)? };
        // SAFETY: remote_buf is live and stable for the test.
        let remote_mr = unsafe { rdma.register(remote_buf.as_mut_ptr(), N, all)? };
        // SAFETY: read_back is live and stable for the test.
        let read_mr = unsafe { rdma.register(read_back.as_mut_ptr(), N, all)? };

        let send_cq = rdma.create_cq(16)?;
        let recv_cq = rdma.create_cq(16)?;
        let mut qp = rdma.create_rc_qp(&send_cq, &recv_cq, 16, 1)?;

        let gid = rdma.local_gid()?;
        let dest = RcDest {
            qp_num: qp.qp_number(),
            gid: gid.gid(),
            src_gid_index: gid.gid_index(),
        };
        rdma.connect_rc(&mut qp, &dest)?;

        // --- SEND / RECV (NVMe-RDMA command & response capsules) ---
        {
            let mut g = qp.start_post_recv();
            let h = g.construct_wr(1);
            // SAFETY: recv region is registered and lives past the completion.
            unsafe { h.setup_sge(recv_mr.lkey(), recv_buf.as_ptr() as u64, LEN) };
            g.post().map_err(oerr)?;
        }
        {
            let mut g = qp.start_post_send();
            let h = g
                .construct_wr(2, WorkRequestFlags::Signaled)
                .setup_send();
            // SAFETY: send region is registered and lives past the completion.
            unsafe { h.setup_sge(send_mr.lkey(), send_buf.as_ptr() as u64, LEN) };
            g.post().map_err(oerr)?;
        }
        await_wc(&send_cq, 2)?;
        await_wc(&recv_cq, 1)?;
        assert_eq!(recv_buf, send_buf, "SEND/RECV payload mismatch");

        // --- RDMA WRITE (target pushes read-data into the host buffer) ---
        {
            let mut g = qp.start_post_send();
            let h = g
                .construct_wr(3, WorkRequestFlags::Signaled)
                .setup_write(remote_mr.rkey(), remote_buf.as_ptr() as u64);
            // SAFETY: local source region is registered and stable.
            unsafe { h.setup_sge(send_mr.lkey(), send_buf.as_ptr() as u64, LEN) };
            g.post().map_err(oerr)?;
        }
        await_wc(&send_cq, 3)?;
        assert_eq!(remote_buf, send_buf, "RDMA WRITE payload mismatch");

        // --- RDMA READ (target pulls write-data from the host buffer) ---
        {
            let mut g = qp.start_post_send();
            let h = g
                .construct_wr(4, WorkRequestFlags::Signaled)
                .setup_read(remote_mr.rkey(), remote_buf.as_ptr() as u64);
            // SAFETY: local destination region is registered and stable.
            unsafe { h.setup_sge(read_mr.lkey(), read_back.as_ptr() as u64, LEN) };
            g.post().map_err(oerr)?;
        }
        await_wc(&send_cq, 4)?;
        assert_eq!(read_back, remote_buf, "RDMA READ payload mismatch");

        Ok(())
    }
}
