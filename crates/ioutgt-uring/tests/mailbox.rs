//! Cross-thread doorbell: a sender must wake a queue thread parked inside
//! io_uring_enter.

use std::time::Duration;

use ioutgt_uring::{QueueRuntime, RingConfig, mailbox::mailbox};

#[test]
fn send_wakes_parked_runtime() {
    let (tx, mut rx) = mailbox::<u32>().unwrap();

    let queue_thread = std::thread::spawn(move || {
        let rt = QueueRuntime::new(RingConfig::default()).unwrap();
        rt.block_on(async move { rx.recv().await.unwrap() })
    });

    // Give the queue thread ample time to park in io_uring_enter.
    std::thread::sleep(Duration::from_millis(150));
    tx.send(0xC0FFEE);

    assert_eq!(queue_thread.join().unwrap(), 0xC0FFEE);
}

#[test]
fn messages_before_and_after_park_arrive_in_order() {
    let (tx, mut rx) = mailbox::<u32>().unwrap();
    tx.send(1); // queued before the runtime even exists

    let queue_thread = std::thread::spawn(move || {
        let rt = QueueRuntime::new(RingConfig::default()).unwrap();
        rt.block_on(async move {
            let mut out = Vec::new();
            for _ in 0..3 {
                out.push(rx.recv().await.unwrap());
            }
            out
        })
    });

    std::thread::sleep(Duration::from_millis(100));
    tx.send(2);
    std::thread::sleep(Duration::from_millis(50));
    tx.send(3);

    assert_eq!(queue_thread.join().unwrap(), vec![1, 2, 3]);
}
