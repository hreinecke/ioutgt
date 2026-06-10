//! Ring timers (IORING_OP_TIMEOUT futures).

use std::time::{Duration, Instant};

use ioutgt_uring::{QueueRuntime, RingConfig, ops};

#[test]
fn sleep_fires_close_to_deadline() {
    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    rt.block_on(async {
        let start = Instant::now();
        ops::sleep(Duration::from_millis(50))
            .unwrap()
            .await
            .unwrap();
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(45),
            "fired early: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_millis(500),
            "fired far too late: {elapsed:?}"
        );
    });
}

#[test]
fn concurrent_sleeps_complete_in_order() {
    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    rt.block_on(async {
        let long = tokio::task::spawn_local(async {
            ops::sleep(Duration::from_millis(60))
                .unwrap()
                .await
                .unwrap();
            Instant::now()
        });
        let short = tokio::task::spawn_local(async {
            ops::sleep(Duration::from_millis(10))
                .unwrap()
                .await
                .unwrap();
            Instant::now()
        });
        let t_long = long.await.unwrap();
        let t_short = short.await.unwrap();
        assert!(t_short < t_long, "short sleep should fire first");
    });
}
