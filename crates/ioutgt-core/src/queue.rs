//! Per-queue context: the slot array plus SQ-head flow control and
//! per-queue lifetime stats. Generic over the per-slot command type
//! `C` (the SQE for NVMe, the request header for NBD) so the same
//! [`QueueCore`] serves every protocol; the send list is deliberately
//! absent — its work type belongs to the transport
//! ([`crate::slotq::SendList`] instantiated next to this in the
//! transport's composite).
//!
//! The send-work types (`SendWork`, `Completion`) and the methods that
//! push onto the list (`complete`, `solicit`, etc.) live in the
//! transport-side [`NvmeTcpQueue`][ioutgt_nvme_tcp::queue::NvmeTcpQueue]
//! (or its equivalent for other transports), not here.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use crate::slotq::SlotArray;
pub use crate::slotq::{Slot, SlotState};

/// Optional transport-specific per-queue counters (e.g. RDMA work-request
/// classes) reported alongside [`QueueStats`] under a `"wr"` object in
/// GET_STATS. Snapshotted on the owning queue thread, like the core counters.
pub trait TransportStats: std::fmt::Debug {
    /// `(label, value)` counter pairs to emit under `"wr"`.
    fn snapshot(&self) -> Vec<(&'static str, u64)>;
    /// Zero the counters (GET_STATS `clear`).
    fn reset(&self);
}

/// Per-queue lifetime IO counters. All writers run on the owning queue
/// thread (`Cell`, hence `!Sync` — a cross-thread read cannot compile);
/// GET_STATS snapshots them *on that thread* via the mailbox. Shared as
/// `Rc` so the thread's stats list can outlive the connection without
/// pinning slot memory.
#[derive(Debug)]
pub struct QueueStats {
    /// Queue id (immutable; reporting identity together with `cntlid`).
    pub qid: u16,
    /// Owning controller, set when Connect executes (0 until then).
    pub cntlid: Cell<u16>,
    /// NVM Read commands dispatched.
    pub read_cmds: Cell<u64>,
    /// NVM Write commands dispatched.
    pub write_cmds: Cell<u64>,
    /// NVM Flush commands dispatched.
    pub flush_cmds: Cell<u64>,
    /// Admin, fabrics, and non-Read/Write/Flush IO commands.
    pub other_cmds: Cell<u64>,
    /// Payload bytes of successful backend reads.
    pub read_bytes: Cell<u64>,
    /// Payload bytes of successful backend writes.
    pub write_bytes: Cell<u64>,
    /// IO-path commands completed with non-success status (validation
    /// and backend failures). Admin/fabrics failures are not counted,
    /// and a pre-dispatch rejection (unknown namespace, bad opcode)
    /// bumps this without a cmd-class counter — so the class counters
    /// do not necessarily sum to commands received.
    pub errors: Cell<u64>,
    /// Optional transport-specific counters (RDMA WR classes); `None` for
    /// transports without them (TCP). Set on the owning thread at install.
    transport: RefCell<Option<Rc<dyn TransportStats>>>,
}

/// Plain-`u64` copy of [`QueueStats`]; doubles as the fold accumulator
/// for torn-down queues ([`QueueStatsSnapshot::absorb`]).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[allow(missing_docs)]
pub struct QueueStatsSnapshot {
    pub qid: u16,
    pub cntlid: u16,
    pub read_cmds: u64,
    pub write_cmds: u64,
    pub flush_cmds: u64,
    pub other_cmds: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
    pub errors: u64,
}

/// Counter increment used on the IO path: a plain `Cell` add — no
/// atomics, no locks.
#[inline]
pub fn stat_add(cell: &Cell<u64>, n: u64) {
    cell.set(cell.get() + n);
}

impl QueueStats {
    /// Fresh zeroed counters for queue `qid`.
    pub fn new(qid: u16) -> QueueStats {
        QueueStats {
            qid,
            cntlid: Cell::new(0),
            read_cmds: Cell::new(0),
            write_cmds: Cell::new(0),
            flush_cmds: Cell::new(0),
            other_cmds: Cell::new(0),
            read_bytes: Cell::new(0),
            write_bytes: Cell::new(0),
            errors: Cell::new(0),
            transport: RefCell::new(None),
        }
    }

    /// Attach a transport-specific stats provider (owning thread; once at
    /// queue install). Reported under `"wr"` in GET_STATS.
    pub fn set_transport(&self, t: Rc<dyn TransportStats>) {
        *self.transport.borrow_mut() = Some(t);
    }

    /// Snapshot the transport-specific counters, if any (owning thread).
    pub fn transport_snapshot(&self) -> Option<Vec<(&'static str, u64)>> {
        self.transport.borrow().as_ref().map(|t| t.snapshot())
    }

    /// Copy out the current values (owning thread only).
    pub fn snapshot(&self) -> QueueStatsSnapshot {
        QueueStatsSnapshot {
            qid: self.qid,
            cntlid: self.cntlid.get(),
            read_cmds: self.read_cmds.get(),
            write_cmds: self.write_cmds.get(),
            flush_cmds: self.flush_cmds.get(),
            other_cmds: self.other_cmds.get(),
            read_bytes: self.read_bytes.get(),
            write_bytes: self.write_bytes.get(),
            errors: self.errors.get(),
        }
    }

    /// Zero the counters (owning thread only). Identity (`qid`,
    /// `cntlid`) is preserved — the queue is still the same queue.
    pub fn reset(&self) {
        self.read_cmds.set(0);
        self.write_cmds.set(0);
        self.flush_cmds.set(0);
        self.other_cmds.set(0);
        self.read_bytes.set(0);
        self.write_bytes.set(0);
        self.errors.set(0);
        if let Some(t) = self.transport.borrow().as_ref() {
            t.reset();
        }
    }
}

impl QueueStatsSnapshot {
    /// Accumulate `other`'s counters; identity fields stay untouched
    /// (the accumulator represents "all retired queues").
    pub fn absorb(&mut self, other: &QueueStatsSnapshot) {
        self.read_cmds += other.read_cmds;
        self.write_cmds += other.write_cmds;
        self.flush_cmds += other.flush_cmds;
        self.other_cmds += other.other_cmds;
        self.read_bytes += other.read_bytes;
        self.write_bytes += other.write_bytes;
        self.errors += other.errors;
    }
}

/// Transport-neutral per-queue context: the slot array plus SQ-head
/// flow control and stats, generic over the per-slot command type `C`
/// (`Sqe` for NVMe, the request header for NBD). The send list is
/// deliberately absent — its work type belongs to the transport
/// ([`crate::slotq::SendList`] instantiated next to this in the
/// transport's composite). `sqhd`/`sqhd_disabled` are NVMe SQ-head
/// flow control; protocols without it (NBD) construct with
/// `sqhd_disabled` and never advance.
pub struct QueueCore<C: Copy> {
    /// The command slots (also reachable through `Deref`).
    pub slots: SlotArray<C>,
    /// Queue depth in entries; slot count.
    pub sqsize: u16,
    /// Queue id (0 = admin).
    pub qid: u16,
    sqhd: Cell<u16>,
    /// Host requested SQ flow control disabled (Connect cattr bit;
    /// always true for protocols without SQ-head flow control).
    pub sqhd_disabled: bool,
    /// Lifetime IO counters, shared with the owning thread's stats
    /// list.
    pub stats: Rc<QueueStats>,
}

impl<C: Copy> std::ops::Deref for QueueCore<C> {
    type Target = SlotArray<C>;

    fn deref(&self) -> &Self::Target {
        &self.slots
    }
}

impl<C: Copy> QueueCore<C> {
    /// Allocate a queue: `sqsize` slots sharing a `pool_bytes` data-buffer
    /// pool, every slot's command stash initialized to `init`.
    pub fn new(
        qid: u16,
        sqsize: u16,
        pool_bytes: usize,
        sqhd_disabled: bool,
        init: C,
    ) -> Rc<QueueCore<C>> {
        Rc::new(QueueCore {
            slots: SlotArray::new(sqsize, pool_bytes, init),
            sqsize,
            qid,
            sqhd: Cell::new(0),
            sqhd_disabled,
            stats: Rc::new(QueueStats::new(qid)),
        })
    }

    /// Current sqhd, advancing it (call once per completion). 16-bit
    /// circular per the negotiated queue size, as in nvmet.
    pub fn advance_sqhd(&self) -> u16 {
        if self.sqhd_disabled {
            return 0;
        }
        let next = (self.sqhd.get() + 1) % self.sqsize;
        self.sqhd.set(next);
        next
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::Poll;

    #[test]
    fn tag_lifecycle_and_sqhd_wrap() {
        let q = QueueCore::new(1, 4, 4096, false, 0u64);
        // sqhd wraps modulo sqsize.
        assert_eq!(q.advance_sqhd(), 1);
        assert_eq!(q.advance_sqhd(), 2);
        assert_eq!(q.advance_sqhd(), 3);
        assert_eq!(q.advance_sqhd(), 0);
        assert_eq!(q.advance_sqhd(), 1);

        // Claim all four tags (deref to SlotArray); the fifth fails.
        let tags: Vec<u16> = (0..4).map(|_| q.claim_tag().unwrap()).collect();
        assert!(q.claim_tag().is_none());
        assert!(!q.idle());

        // Walk one slot through the full lifecycle via the deref'd
        // engine, including the await_command transition (Ready).
        let tag = tags[0];
        q.submit(tag, 0u64);
        assert_eq!(q.slot(tag).state(), SlotState::Ready);
        {
            let fut = q.await_command(tag);
            let mut fut = std::pin::pin!(fut);
            let waker = std::task::Waker::noop();
            let mut cx = std::task::Context::from_waker(waker);
            let Poll::Ready(_sqe) = fut.as_mut().poll(&mut cx) else {
                panic!("ready slot must dispatch immediately");
            };
        }
        assert_eq!(q.executing(), 1);
        q.begin_respond(tag);
        assert_eq!(q.executing(), 0);
        assert_eq!(q.slot(tag).state(), SlotState::Responding);
        q.release_tag(tag);
        assert_eq!(q.slot(tag).state(), SlotState::Free);
        assert_eq!(q.free_tags(), 1);
    }

    #[test]
    fn sqhd_disabled_reports_zero() {
        let q = QueueCore::new(1, 8, 64, true, 0u64);
        assert_eq!(q.advance_sqhd(), 0);
        assert_eq!(q.advance_sqhd(), 0);
    }

    #[test]
    fn queue_stats_snapshot_and_absorb() {
        let stats = QueueStats::new(3);
        stats.cntlid.set(7);
        stat_add(&stats.read_cmds, 2);
        stat_add(&stats.read_bytes, 8192);
        stat_add(&stats.errors, 1);
        let snap = stats.snapshot();
        assert_eq!((snap.qid, snap.cntlid), (3, 7));
        assert_eq!((snap.read_cmds, snap.read_bytes, snap.errors), (2, 8192, 1));

        let mut retired = QueueStatsSnapshot::default();
        retired.absorb(&snap);
        retired.absorb(&snap);
        assert_eq!(retired.read_cmds, 4);
        assert_eq!(retired.read_bytes, 16384);
        assert_eq!(retired.errors, 2);
        // Identity does not aggregate: the accumulator is "all retired
        // queues", not any one of them.
        assert_eq!((retired.qid, retired.cntlid), (0, 0));

        // Reset zeros the counters but keeps the identity.
        stats.reset();
        let snap = stats.snapshot();
        assert_eq!((snap.qid, snap.cntlid), (3, 7));
        assert_eq!(
            snap,
            QueueStatsSnapshot {
                qid: 3,
                cntlid: 7,
                ..QueueStatsSnapshot::default()
            }
        );
    }

    #[test]
    fn queue_core_owns_zeroed_stats() {
        let queue = QueueCore::new(1, 4, 4096, false, 0u64);
        assert_eq!(
            queue.stats.snapshot(),
            QueueStatsSnapshot {
                qid: 1,
                ..QueueStatsSnapshot::default()
            }
        );
    }
}
