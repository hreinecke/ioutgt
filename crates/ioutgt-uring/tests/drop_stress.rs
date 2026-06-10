//! Cancellation-safety stress: futures dropped at arbitrary points must
//! never leak slab entries (the reactor reclaims them via terminal CQEs).

use std::future::Future;
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use ioutgt_uring::{QueueRuntime, RingConfig, ops};

const OPS: usize = 1000;

#[test]
fn dropped_ops_drain_completely() {
    // Sockets with no incoming data: every recv parks in the kernel until
    // cancelled.
    let (a, _b) = UnixStream::pair().unwrap();
    let rt = QueueRuntime::new(RingConfig {
        sq_entries: 256,
        cq_entries: 4096,
    })
    .unwrap();
    let reactor = rt.reactor().clone();

    rt.block_on(async move {
        let fd = a.as_raw_fd();
        let mut never_polled = Vec::new();
        let mut polled_once = Vec::new();
        for i in 0..OPS {
            let buf = vec![0u8; 16].into_boxed_slice();
            let op = ops::recv(fd, buf).unwrap();
            if i % 2 == 0 {
                never_polled.push(op);
            } else {
                polled_once.push(op);
            }
        }
        // Poll half of them exactly once (registers wakers), then drop all.
        for op in &mut polled_once {
            std::future::poll_fn(|cx| {
                assert!(Pin::new(&mut *op).poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        assert_eq!(reactor.pending_ops(), OPS);
        drop(never_polled);
        drop(polled_once);

        // All entries are orphaned now; cancels + terminal CQEs must
        // reclaim every one of them.
        for _ in 0..500 {
            if reactor.pending_ops() == 0 {
                break;
            }
            ops::sleep(Duration::from_millis(2)).unwrap().await.unwrap();
        }
        assert_eq!(
            reactor.pending_ops(),
            0,
            "leaked op entries after mass drop"
        );
    });
}

#[test]
fn drop_completed_but_unconsumed_op() {
    // An op whose CQE arrived but whose future was dropped before polling
    // the result must also be reclaimed.
    let (a, b) = UnixStream::pair().unwrap();
    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    let reactor = rt.reactor().clone();

    rt.block_on(async move {
        let buf = vec![0u8; 4].into_boxed_slice();
        let op = ops::recv(a.as_raw_fd(), buf).unwrap();

        // Complete it: write 4 bytes from the other end, then give the
        // reactor a chance to reap (the sleep forces a park cycle).
        let (res, _) = ops::send(b.as_raw_fd(), b"ping".to_vec().into_boxed_slice())
            .unwrap()
            .await;
        assert_eq!(res.unwrap(), 4);
        ops::sleep(Duration::from_millis(10))
            .unwrap()
            .await
            .unwrap();

        drop(op); // completed, never consumed
        assert_eq!(reactor.pending_ops(), 0, "completed-unconsumed op leaked");
    });
}
