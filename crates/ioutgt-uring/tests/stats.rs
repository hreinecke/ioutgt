//! Reactor counters: SQEs (with the send/recv split), CQEs, parks.

use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::time::Duration;

use ioutgt_uring::{QueueRuntime, RingConfig, ops, reactor_stats};

#[test]
fn reactor_counts_sqes_cqes_and_parks() {
    let rt = QueueRuntime::new(RingConfig::default()).expect("runtime");
    rt.block_on(async {
        let before = reactor_stats().expect("reactor live");
        // Two sequential timer ops: 2 SQEs, 2 CQEs, and the idle waits
        // must park (submit_and_wait) at least once.
        ops::sleep(Duration::from_millis(2))
            .expect("sleep op")
            .await
            .expect("sleep completes");
        ops::sleep(Duration::from_millis(2))
            .expect("sleep op")
            .await
            .expect("sleep completes");
        let after = reactor_stats().expect("reactor live");
        assert!(after.sqes >= before.sqes + 2, "sqes: {after:?}");
        assert!(after.cqes >= before.cqes + 2, "cqes: {after:?}");
        assert!(after.parks > before.parks, "parks: {after:?}");
        // Timer ops are neither send nor recv.
        assert_eq!(after.send_sqes, before.send_sqes, "send_sqes: {after:?}");
        assert_eq!(after.recv_sqes, before.recv_sqes, "recv_sqes: {after:?}");
    });
}

#[test]
fn send_and_recv_sqes_count_separately() {
    // A connected socket pair: one send op, one recv op, on the reactor.
    let (a, b) = UnixStream::pair().expect("socketpair");
    let (fa, fb) = (a.as_raw_fd(), b.as_raw_fd());
    let rt = QueueRuntime::new(RingConfig::default()).expect("runtime");
    rt.block_on(async {
        let before = reactor_stats().expect("reactor live");
        let (res, _) = ops::send(fa, vec![1u8, 2, 3].into_boxed_slice())
            .expect("send op")
            .await;
        assert_eq!(res.expect("send ok"), 3);
        let (res, buf) = ops::recv(fb, vec![0u8; 3].into_boxed_slice())
            .expect("recv op")
            .await;
        assert_eq!(res.expect("recv ok"), 3);
        assert_eq!(&buf[..3], &[1, 2, 3]);
        let after = reactor_stats().expect("reactor live");
        // Each is counted in its own bucket and in the total.
        assert_eq!(after.send_sqes, before.send_sqes + 1, "send: {after:?}");
        assert_eq!(after.recv_sqes, before.recv_sqes + 1, "recv: {after:?}");
        assert!(after.sqes >= before.sqes + 2, "sqes total: {after:?}");
    });
    drop((a, b));
}

#[test]
fn reactor_stats_errors_off_runtime() {
    assert!(reactor_stats().is_err());
}

#[test]
fn reactor_stats_reset_zeroes_counters() {
    let rt = QueueRuntime::new(RingConfig::default()).expect("runtime");
    rt.block_on(async {
        ops::sleep(Duration::from_millis(1))
            .expect("sleep op")
            .await
            .expect("sleep completes");
        assert!(reactor_stats().expect("live").sqes > 0);
        ioutgt_uring::reset_reactor_stats().expect("live");
        let after = reactor_stats().expect("live");
        assert_eq!(after, ioutgt_uring::ReactorStats::default(), "{after:?}");
        // Counting resumes from zero.
        ops::sleep(Duration::from_millis(1))
            .expect("sleep op")
            .await
            .expect("sleep completes");
        assert_eq!(reactor_stats().expect("live").sqes, 1);
    });
}
