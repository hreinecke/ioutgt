//! Connection-count accounting shared by every transport's accept path.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// RAII guard for the active-connection counter: the count is
/// incremented by the acceptor before the permit is built, and
/// decremented here when the connection's `run_queue` returns. This is
/// how the control thread bounds concurrent connections (and thus total
/// preallocated queue memory) across queue threads.
pub struct ConnPermit(Arc<AtomicUsize>);

impl ConnPermit {
    /// Wrap an already-incremented counter; drop decrements it.
    pub fn new(counter: Arc<AtomicUsize>) -> Self {
        ConnPermit(counter)
    }
}

impl Drop for ConnPermit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}
