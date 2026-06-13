//! NVMe queue context: the slot array plus SQ-head flow control and
//! per-queue lifetime stats. The send list is deliberately absent —
//! its work type belongs to the transport ([`crate::slotq::SendList`]
//! instantiated next to this in the transport's composite).
//!
//! The send-work types (`SendWork`, `Completion`) and the methods that
//! push onto the list (`complete`, `solicit`, etc.) live in the
//! transport-side [`TcpQueue`][ioutgt_nvme_tcp::queue::TcpQueue] (or its
//! equivalent for other transports), not here.

use std::cell::Cell;
use std::rc::Rc;

use ioutgt_nvme::spec::Sqe;

use crate::slotq::SlotArray;
pub use crate::slotq::{Slot, SlotState};

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
        }
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

/// NVMe command slot: the generic slot stashing an [`Sqe`].
pub type CmdSlot = Slot<Sqe>;

impl Slot<Sqe> {
    /// The received SQE (alias of [`Slot::cmd`], NVMe naming).
    pub fn sqe(&self) -> Sqe {
        self.cmd()
    }

    /// Park the SQE while its in-capsule payload is still arriving
    /// (state stays `Receiving`; [`SlotArray::submit`] delivers it).
    pub fn stash_sqe(&self, sqe: Sqe) {
        self.stash_cmd(sqe);
    }

    /// The SQE parked by [`Slot::stash_sqe`].
    pub fn stashed_sqe(&self) -> Sqe {
        self.cmd()
    }
}

/// Transport-neutral NVMe queue context: the slot array plus SQ-head
/// flow control and stats. The send list is deliberately absent —
/// its work type belongs to the transport ([`crate::slotq::SendList`]
/// instantiated next to this in the transport's composite).
pub struct NvmeQueue {
    /// The command slots (also reachable through `Deref`).
    pub slots: SlotArray<Sqe>,
    /// Queue depth in entries; slot count.
    pub sqsize: u16,
    /// Queue id (0 = admin).
    pub qid: u16,
    sqhd: Cell<u16>,
    /// Host requested SQ flow control disabled (Connect cattr bit).
    pub sqhd_disabled: bool,
    /// Lifetime IO counters, shared with the owning thread's stats
    /// list.
    pub stats: Rc<QueueStats>,
}

impl std::ops::Deref for NvmeQueue {
    type Target = SlotArray<Sqe>;

    fn deref(&self) -> &Self::Target {
        &self.slots
    }
}

impl NvmeQueue {
    /// Allocate a queue: `sqsize` slots each with a `slot_buf_size`
    /// data buffer.
    pub fn new(qid: u16, sqsize: u16, slot_buf_size: usize, sqhd_disabled: bool) -> Rc<NvmeQueue> {
        Rc::new(NvmeQueue {
            slots: SlotArray::new(sqsize, slot_buf_size, Sqe::zeroed()),
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
        let q = NvmeQueue::new(1, 4, 4096, false);
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
        q.submit(tag, Sqe::zeroed());
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
        let q = NvmeQueue::new(1, 8, 64, true);
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
    fn nvme_queue_owns_zeroed_stats() {
        let queue = NvmeQueue::new(1, 4, 4096, false);
        assert_eq!(
            queue.stats.snapshot(),
            QueueStatsSnapshot {
                qid: 1,
                ..QueueStatsSnapshot::default()
            }
        );
    }
}
