//! Driving one future on the thread's io_uring with no scheduler under it.

use std::future::Future;
use std::io;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use crate::reactor::{Reactor, RingConfig};

/// A thread's io_uring, driven by blocking the thread that owns it.
///
/// [`QueueRuntime`](crate::QueueRuntime) is how a queue thread gets a
/// reactor: a Tokio current-thread scheduler whose park primitive is the
/// ring. This is how a thread with no scheduler — and no wish for one — gets
/// one: a control-plane caller with a synchronous signature and a handful of
/// round trips to make (the sheepdog backend's cluster lookups). It polls one
/// future and, between polls, parks the whole thread inside `io_uring_enter`.
///
/// A thread that already has a reactor (a queue thread, or a test that built
/// its `QueueRuntime` first) has that reactor **adopted**, not replaced:
/// `config` is then ignored and the ring outlives this handle. Adopting is
/// what makes the same synchronous call work whether or not a queue runtime
/// happens to be up on the thread, at the cost of the obvious: while
/// [`block_on`](Self::block_on) has the thread, that runtime's tasks do not
/// run (their wakers still fire — the park reaps every CQE, not just this
/// future's — so they resume as soon as the scheduler does).
///
/// The future may only await ops issued on this reactor. Left pending with
/// nothing in flight there would be nothing for the park to wait on, so
/// `block_on` panics rather than spinning on a future that can never finish.
pub struct BlockingRing {
    reactor: Rc<Reactor>,
    /// This handle built the ring, and so uninstalls it again on drop.
    owned: bool,
}

impl BlockingRing {
    /// Adopt the current thread's reactor, or build a private ring if it has
    /// none.
    pub fn new(config: RingConfig) -> io::Result<BlockingRing> {
        match Reactor::current() {
            Ok(reactor) => Ok(BlockingRing {
                reactor,
                owned: false,
            }),
            Err(_) => Ok(BlockingRing {
                reactor: Reactor::init(config)?,
                owned: true,
            }),
        }
    }

    /// Run `future` to completion, parking the calling thread in the ring
    /// while it waits.
    ///
    /// # Panics
    /// If the future is pending with no op in flight on this reactor and no
    /// waker woken: nothing could make it ready, and a park would spin.
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        let signal = Arc::new(Signal::default());
        let waker = Waker::from(Arc::clone(&signal));
        let mut cx = Context::from_waker(&waker);
        let mut future = pin!(future);
        loop {
            if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
                return output;
            }
            // Woken during the poll (an op that completed inline): re-poll
            // rather than wait for a CQE that has already been reaped.
            if signal.woken.swap(false, Ordering::Acquire) {
                continue;
            }
            assert!(
                self.reactor.pending_ops() > 0,
                "BlockingRing::block_on: the future is pending with nothing in \
                 flight on this reactor"
            );
            self.reactor.park();
        }
    }
}

impl Drop for BlockingRing {
    fn drop(&mut self) {
        // An adopted reactor belongs to the thread's QueueRuntime, which
        // uninstalls it itself. A private one goes now, so the next
        // BlockingRing (or a QueueRuntime) can have the thread.
        if self.owned {
            Reactor::clear_current();
        }
    }
}

/// The waker [`BlockingRing::block_on`] polls with: there is no run queue to
/// put a task on, so a wake is just a flag saying "poll again before you
/// park".
#[derive(Default)]
struct Signal {
    woken: AtomicBool,
}

impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::ops;

    #[test]
    fn drives_a_future_over_ring_ops() {
        let ring = BlockingRing::new(RingConfig::default()).unwrap();
        // Two sleeps in sequence: the thread parks in the ring for each, and
        // the second proves the reactor is still usable after a completion.
        ring.block_on(async {
            for _ in 0..2 {
                ops::sleep(Duration::from_millis(1)).unwrap().await.unwrap();
            }
        });
    }

    #[test]
    fn a_ready_future_never_parks() {
        let ring = BlockingRing::new(RingConfig::default()).unwrap();
        assert_eq!(ring.block_on(async { 42 }), 42);
    }

    #[test]
    fn the_private_ring_goes_back_at_drop() {
        // Two handles in sequence on one thread: the first must have
        // uninstalled its reactor for the second to be able to build one.
        for _ in 0..2 {
            let ring = BlockingRing::new(RingConfig::default()).unwrap();
            ring.block_on(async { ops::sleep(Duration::from_millis(1)).unwrap().await.unwrap() });
        }
    }

    #[test]
    fn a_live_reactor_is_adopted_not_replaced() {
        let rt = crate::QueueRuntime::new(RingConfig::default()).unwrap();
        {
            let ring = BlockingRing::new(RingConfig::default()).unwrap();
            ring.block_on(async { ops::sleep(Duration::from_millis(1)).unwrap().await.unwrap() });
        }
        // The queue runtime still owns a working reactor after the blocking
        // handle came and went.
        rt.block_on(async { ops::sleep(Duration::from_millis(1)).unwrap().await.unwrap() });
    }
}
