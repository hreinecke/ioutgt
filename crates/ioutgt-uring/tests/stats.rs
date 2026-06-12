//! Reactor counters: SQEs, CQEs, parks, io_uring_enter calls.

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
        assert!(after.enters >= after.parks, "enters >= parks: {after:?}");
    });
}

#[test]
fn reactor_stats_errors_off_runtime() {
    assert!(reactor_stats().is_err());
}
