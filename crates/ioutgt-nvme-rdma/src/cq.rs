//! Event-driven completion-queue reaping. Arm the CQ to post a notification on
//! its completion channel for the next completion, park the queue thread's
//! reactor on the channel fd via io_uring `POLL_ADD`, then consume + ack the
//! event(s). This replaces busy-polling `ibv_poll_cq`: the thread sleeps in its
//! normal `submit_and_wait` until a completion arrives.
//!
//! sideway wraps no CQ-event calls, so `ibv_req_notify_cq` / `ibv_get_cq_event`
//! / `ibv_ack_cq_events` are called through `rdma-mummy-sys` (sideway's own FFI
//! backend, so the `ibv_cq` / `ibv_comp_channel` types match its raw handles).
//!
//! These helpers assume one channel per CQ (`create_cq_on_channel`): every
//! event on `channel` belongs to `cq`, so the events are acked against `cq`.
//! A channel shared across CQs would need per-event cq dispatch.

use std::io;
use std::os::fd::AsRawFd;
use std::ptr;

use rdma_mummy_sys::{ibv_ack_cq_events, ibv_cq, ibv_get_cq_event, ibv_req_notify_cq};
use sideway::ibverbs::completion::{CompletionChannel, CompletionQueue, GenericCompletionQueue};

fn pollin() -> u32 {
    u32::try_from(libc::POLLIN).expect("POLLIN fits u32")
}

fn err_hup() -> u32 {
    u32::try_from(libc::POLLERR).unwrap_or(0) | u32::try_from(libc::POLLHUP).unwrap_or(0)
}

/// Arm `cq` so the next completion posts an event on its completion channel.
/// Call once before the first [`wait`]; `wait` re-arms after consuming, before
/// the caller drains, so a completion arriving mid-drain still re-signals the fd.
pub fn arm(cq: &GenericCompletionQueue) -> io::Result<()> {
    // SAFETY: `cq.cq()` is the live `ibv_cq` backing this queue (valid for the
    // CQ's lifetime); `ibv_req_notify_cq` only reads it to arm the channel.
    let rc = unsafe { ibv_req_notify_cq(cq.cq().as_ptr(), 0) };
    if rc != 0 {
        return Err(io::Error::from_raw_os_error(rc));
    }
    Ok(())
}

/// Consume and acknowledge every currently-queued event on `channel` (which
/// must be non-blocking — see [`crate::verbs::Rdma::create_comp_channel`]),
/// returning the count. One event may announce several completions, and several
/// may queue before a wakeup, so this drains until `EAGAIN`. Acks are batched.
///
/// Used both inside [`wait`] and at teardown: `ibv_destroy_cq` blocks until
/// every delivered event is acked, so the owner must call this before dropping
/// the CQ to clear any event that arrived after the last [`wait`].
pub fn drain_events(channel: &CompletionChannel, cq: &GenericCompletionQueue) -> io::Result<u32> {
    let mut count = 0u32;
    loop {
        let mut ev_cq: *mut ibv_cq = ptr::null_mut();
        let mut ev_ctx: *mut std::ffi::c_void = ptr::null_mut();
        // SAFETY: `channel.comp_channel()` is the live, non-blocking channel;
        // `ibv_get_cq_event` writes the event's cq/context through the out-params
        // and returns -1/EAGAIN when none are queued.
        let rc =
            unsafe { ibv_get_cq_event(channel.comp_channel().as_ptr(), &mut ev_cq, &mut ev_ctx) };
        if rc != 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EAGAIN) {
                break;
            }
            return Err(err);
        }
        count += 1;
    }
    if count > 0 {
        // SAFETY: with one channel per CQ, every drained event belongs to `cq`;
        // ack exactly the number consumed (batched).
        unsafe { ibv_ack_cq_events(cq.cq().as_ptr(), count) };
    }
    Ok(count)
}

/// Park the reactor on `channel`'s fd until `cq` notifies, then consume + ack
/// the queued event(s) and re-arm `cq`. The caller drains the CQ (sideway
/// `start_poll`) afterwards; because the re-arm happens *before* that drain, a
/// completion arriving mid-drain re-signals the channel, so no wakeup is lost.
/// The caller must [`arm`] once before the first call, and [`drain_events`] at
/// teardown to clear any trailing event.
pub async fn wait(channel: &CompletionChannel, cq: &GenericCompletionQueue) -> io::Result<()> {
    let revents = ioutgt_uring::ops::poll_add(channel.as_raw_fd(), pollin())?.await?;
    // POLL_ADD always reports POLLERR/POLLHUP; if the channel fd is in error or
    // hung up with no data (device removed/torn down), fail loudly rather than
    // letting the drain spin on a fd that will never carry an event again.
    if revents & pollin() == 0 && revents & err_hup() != 0 {
        return Err(io::Error::other("completion channel error or hangup"));
    }
    drain_events(channel, cq)?;
    arm(cq)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RcDest, Rdma};
    use ioutgt_uring::{QueueRuntime, RingConfig, ops};
    use sideway::ibverbs::AccessFlags;
    use sideway::ibverbs::completion::WorkCompletionStatus;
    use sideway::ibverbs::queue_pair::{
        PostSendGuard, QueuePair, SetScatterGatherEntry, WorkRequestFlags,
    };
    use std::time::Duration;

    /// Drain all currently-available completions as `(wr_id, status)`.
    fn drain(cq: &GenericCompletionQueue) -> Vec<(u64, u32)> {
        let mut out = Vec::new();
        if let Ok(poller) = cq.start_poll() {
            for wc in poller {
                out.push((wc.wr_id(), wc.status()));
            }
        }
        out
    }

    /// A self-connected RC QP whose completions are reaped **through the
    /// reactor** (arm → `POLL_ADD` park on the channel fd → get/ack event →
    /// drain), not by busy-polling: post a RECV + a SEND and confirm both
    /// completions arrive via the event path with the payload intact. Skips
    /// when no RDMA device is present; runs against soft-RoCE (rxe) in the VM.
    #[test]
    fn rxe_reactor_event_send_recv() -> io::Result<()> {
        let Some(rdma) = Rdma::open_first()? else {
            eprintln!("skip rxe_reactor: no RDMA device (configure rdma_rxe to run)");
            return Ok(());
        };
        let rt = QueueRuntime::new(RingConfig::default())?;
        rt.block_on(async move {
            const LEN: u32 = 4096;
            const N: usize = LEN as usize;
            let mut send_buf = vec![0u8; N];
            let mut recv_buf = vec![0u8; N];
            for (i, b) in send_buf.iter_mut().enumerate() {
                *b = u8::try_from(i % 251).unwrap_or(0);
            }
            let all =
                AccessFlags::LocalWrite | AccessFlags::RemoteWrite | AccessFlags::RemoteRead;
            // SAFETY: both buffers outlive their MRs and are not moved/resized.
            let send_mr = unsafe { rdma.register(send_buf.as_mut_ptr(), N, all)? };
            // SAFETY: see above.
            let recv_mr = unsafe { rdma.register(recv_buf.as_mut_ptr(), N, all)? };

            let channel = rdma.create_comp_channel()?;
            let cq = rdma.create_cq_on_channel(&channel, 16)?;
            let mut qp = rdma.create_rc_qp(&cq, &cq, 16, 1)?;
            let qp_num = qp.qp_number();
            let gid = rdma.local_gid()?;
            let dest = RcDest {
                qp_num,
                gid: gid.gid(),
                src_gid_index: gid.gid_index(),
            };
            rdma.connect_rc(&mut qp, &dest)?;

            // Arm before posting so the completion that follows notifies us.
            arm(&cq)?;
            {
                let mut g = qp.start_post_recv();
                let h = g.construct_wr(1);
                // SAFETY: recv region registered, lives past the completion.
                unsafe { h.setup_sge(recv_mr.lkey(), recv_buf.as_ptr() as u64, LEN) };
                g.post().map_err(|e| io::Error::other(format!("{e:?}")))?;
            }
            {
                let mut g = qp.start_post_send();
                let h = g.construct_wr(2, WorkRequestFlags::Signaled).setup_send();
                // SAFETY: send region registered, lives past the completion.
                unsafe { h.setup_sge(send_mr.lkey(), send_buf.as_ptr() as u64, LEN) };
                g.post().map_err(|e| io::Error::other(format!("{e:?}")))?;
            }

            // Reap both completions strictly through the reactor wakeup path,
            // bounding each park with a timer so a lost completion fails with a
            // diagnostic instead of hanging.
            let mut seen = Vec::new();
            for _ in 0..16 {
                tokio::select! {
                    r = wait(&channel, &cq) => r?,
                    _ = ops::sleep(Duration::from_secs(5)).unwrap() => {
                        panic!("timed out waiting for a CQ event, saw {seen:?}");
                    }
                }
                for (wr_id, status) in drain(&cq) {
                    assert_eq!(
                        status,
                        WorkCompletionStatus::Success as u32,
                        "wr {wr_id} failed: status {status}"
                    );
                    seen.push(wr_id);
                }
                if seen.contains(&1) && seen.contains(&2) {
                    break;
                }
            }
            // Clear any event that landed between the last re-arm and drain, so
            // the CQ/channel destroy at scope exit does not block.
            drain_events(&channel, &cq)?;

            assert!(
                seen.contains(&1) && seen.contains(&2),
                "missing completions, saw {seen:?}"
            );
            assert_eq!(recv_buf, send_buf, "SEND/RECV payload mismatch");
            Ok::<(), io::Error>(())
        })
    }
}
