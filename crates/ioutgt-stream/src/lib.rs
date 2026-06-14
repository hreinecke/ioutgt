//! Protocol-neutral ZC-aware gather-send harness for stream transports.
//!
//! A stream transport (NVMe/TCP today, NBD next) drains an ordered
//! [`SendList<W>`] of completed work, packs each item's headers into a
//! per-batch arena and references its payload in place from the slot
//! buffers, and ships the whole batch as one vectored `sendmsg` — or
//! `SENDMSG_ZC` under zero-copy mode, gating slot reuse on the kernel's
//! zero-copy notification. All of that machinery — the double-buffered
//! batches, short-send resume, the ZC pin-budget fallback to copying,
//! the notification reaping, the anti-deadlock drain, and the
//! release-tag timing — is protocol-agnostic and lives here in
//! [`StreamSender`].
//!
//! The one transport-specific concern is *staging one work item*:
//! encoding its protocol headers into the [`GatherBatch`] and saying how
//! its slot tag is released ([`Staged`]). The transport passes that as a
//! closure; the harness never inspects `W`.
//!
//! Sits above `ioutgt-core` (slot engine) and `ioutgt-uring` (reactor)
//! in the DAG, keeping both leaves free of each other — see the crate
//! map in `docs/architecture.md` §4.

use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::{Context, Poll};

use ioutgt_core::slotq::{SendList, SlotArray};
use ioutgt_uring::ops;
use ioutgt_uring::sendbatch::GatherBatch;

/// How a staged work item's slot tag is released, decided by the
/// transport's staging closure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Staged {
    /// No tag to release (e.g. an NVMe R2T solicitation — the slot
    /// stays `Receiving`).
    NoRelease,
    /// Release at the send CQE: the op references no slot memory
    /// (header/capsule-only).
    AtCqe(u16),
    /// Release only after the batch's zero-copy notifications: the op
    /// references slot buffers in place.
    AtNotif(u16),
}

/// Zero-copy send counters, read by the transport after [`StreamSender::run`]
/// for logging.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ZcStats {
    /// Batches shipped with at least one outstanding ZC notification
    /// (i.e. a genuine zero-copy send, not a copy fallback).
    pub zc_batches: u64,
    /// ZC notifications that reported the kernel copied after all (no
    /// pages were pinned); a hint the ZC path isn't paying off.
    pub zc_copied: u64,
    /// ZC sends refused by the pinned-page budget (`RLIMIT_MEMLOCK`:
    /// ENOMEM/ENOBUFS) and shipped by copy instead.
    pub zc_fallbacks: u64,
}

/// One batch's gather state: a [`GatherBatch`] for the protocol-free
/// arena/iovec/msghdr mechanics, plus tag-accounting and ZC notification
/// vectors. Exactly one batch ships at a time; `reset()` recycles
/// everything. All memory is preallocated at construction.
struct GatherSendBatch {
    gather: GatherBatch,
    /// Worst-case header bytes / iovec entries per item, for the
    /// headroom check (mirrors the args the transport sized `new` with).
    arena_per_item: usize,
    iovs_per_item: usize,
    /// Tags safe to release at the send CQE: their slot memory the op
    /// never references.
    tags_at_cqe: Vec<u16>,
    /// Tags whose slot buffers ride in the iovecs: released only after
    /// every notification for this batch is reaped.
    tags_at_notif: Vec<u16>,
    /// One notification per ZC op issued for this batch; >1 only after
    /// short-send re-issues (the cold path).
    pending_notifs: Vec<ops::ZcNotif>,
}

impl GatherSendBatch {
    fn new(sqsize: u16, arena_per_item: usize, iovs_per_item: usize) -> GatherSendBatch {
        let n = usize::from(sqsize);
        GatherSendBatch {
            gather: GatherBatch::new(n * arena_per_item, n * iovs_per_item + iovs_per_item),
            arena_per_item,
            iovs_per_item,
            tags_at_cqe: Vec::with_capacity(n),
            tags_at_notif: Vec::with_capacity(n),
            pending_notifs: Vec::with_capacity(8),
        }
    }

    fn reset(&mut self) {
        self.gather.reset();
        self.tags_at_cqe.clear();
        self.tags_at_notif.clear();
        debug_assert!(
            self.pending_notifs.is_empty(),
            "batch reset with notifications outstanding"
        );
    }

    /// Headroom for one more worst-case item?
    #[inline]
    fn fits(&self) -> bool {
        self.gather.fits(self.arena_per_item, self.iovs_per_item)
    }

    /// Release the notif-gated tags and recycle the batch for staging.
    /// Callers must have reaped every pending notification first.
    fn recycle<C: Copy>(&mut self, slots: &SlotArray<C>) {
        for tag in self.tags_at_notif.drain(..) {
            slots.release_tag(tag);
        }
        self.reset();
    }
}

/// The send-path driver: a serial state machine that drains a
/// [`SendList<W>`] and puts each batch on the wire, one op at a time,
/// double-buffering so a batch's ZC notifications can overlap the next
/// batch's staging without ever pipelining two send ops (wire ordering).
pub struct StreamSender {
    batches: [GatherSendBatch; 2],
    /// Indices into `batches` with outstanding notifications, oldest
    /// first (len ≤ 2).
    inflight: VecDeque<usize>,
    zc_batches: u64,
    zc_copied: u64,
    zc_fallbacks: u64,
}

impl StreamSender {
    /// A sender for a queue of `sqsize` slots. `arena_per_item` /
    /// `iovs_per_item` are the transport's worst-case per-item sizings
    /// (they bound the preallocated arena and iovec list).
    pub fn new(sqsize: u16, arena_per_item: usize, iovs_per_item: usize) -> StreamSender {
        StreamSender {
            batches: [
                GatherSendBatch::new(sqsize, arena_per_item, iovs_per_item),
                GatherSendBatch::new(sqsize, arena_per_item, iovs_per_item),
            ],
            inflight: VecDeque::with_capacity(2),
            zc_batches: 0,
            zc_copied: 0,
            zc_fallbacks: 0,
        }
    }

    /// Run the send loop to completion. `stage` packs ONE work item's
    /// headers into the [`GatherBatch`] and returns its tag-release
    /// class; the harness pulls work from `work`, releases tags on
    /// `slots`, and ships on `fd`. Every exit path — orderly close
    /// (`SendList::close`), send error, or `WriteZero` — drains all
    /// outstanding ZC notifications before returning, so the caller can
    /// free queue memory immediately after joining the send task.
    pub async fn run<C, W, F>(
        &mut self,
        fd: i32,
        send_zc: bool,
        slots: &SlotArray<C>,
        work: &SendList<W>,
        mut stage: F,
    ) -> std::io::Result<()>
    where
        C: Copy,
        F: FnMut(&mut GatherBatch, &W) -> Staged,
    {
        let result = self
            .send_batches(fd, send_zc, slots, work, &mut stage)
            .await;
        // The kernel may still hold page references (arena + slot
        // buffers) that the caller frees right after joining this task.
        self.drain(slots).await;
        result
    }

    /// Snapshot the zero-copy counters (read after [`Self::run`]).
    pub fn stats(&self) -> ZcStats {
        ZcStats {
            zc_batches: self.zc_batches,
            zc_copied: self.zc_copied,
            zc_fallbacks: self.zc_fallbacks,
        }
    }

    /// The core loop: each turn gets a first work item, `acquire`s a
    /// free batch, `stage_batch`es to greedily gather more behind it,
    /// `ship_batch`es the single gather op (looping only to finish a
    /// short send), then `retire`s to release tags and classify.
    async fn send_batches<C, W, F>(
        &mut self,
        fd: i32,
        send_zc: bool,
        slots: &SlotArray<C>,
        work: &SendList<W>,
        stage: &mut F,
    ) -> std::io::Result<()>
    where
        C: Copy,
        F: FnMut(&mut GatherBatch, &W) -> Staged,
    {
        let mut carry: Option<W> = None;
        loop {
            let first = match carry.take() {
                Some(item) => item,
                None => match self.next_work_reaping(slots, work).await {
                    Some(item) => item,
                    None => return Ok(()), // close(): teardown
                },
            };
            let idx = self.acquire(slots).await;
            let batch = &mut self.batches[idx];
            carry = stage_batch(batch, first, work, stage);
            ship_batch(fd, batch, send_zc, &mut self.zc_fallbacks).await?;
            self.retire(slots, idx);
        }
    }

    /// Index of a batch free for staging, reaping the oldest in-flight
    /// batch first when both are awaiting notifications.
    async fn acquire<C: Copy>(&mut self, slots: &SlotArray<C>) -> usize {
        if self.inflight.len() == self.batches.len() {
            self.reap_oldest(slots).await;
        }
        (0..self.batches.len())
            .find(|i| !self.inflight.contains(i))
            .expect("two batches, at most one in flight here")
    }

    /// Await every notification of the oldest in-flight batch, then
    /// release its notif-gated tags and recycle it.
    async fn reap_oldest<C: Copy>(&mut self, slots: &SlotArray<C>) {
        if let Some(idx) = self.inflight.pop_front() {
            self.reap(slots, idx).await;
        }
    }

    /// Await every notification of one batch, then recycle it.
    async fn reap<C: Copy>(&mut self, slots: &SlotArray<C>, idx: usize) {
        while let Some(notif) = self.batches[idx].pending_notifs.pop() {
            if notif.await {
                self.zc_copied += 1;
            }
        }
        self.batches[idx].recycle(slots);
    }

    /// Drain every outstanding notification. [`Self::run`] runs this on
    /// every send-path exit: the kernel may still hold page references
    /// (arena + slot buffers) that the caller frees right after joining
    /// the send task. Covers the in-flight list AND the batch a send
    /// error abandoned mid-ship — that one is not on the list but may
    /// hold notifs from its earlier ZC ops.
    async fn drain<C: Copy>(&mut self, slots: &SlotArray<C>) {
        while let Some(idx) = self.inflight.pop_front() {
            self.reap(slots, idx).await;
        }
        for idx in 0..self.batches.len() {
            self.reap(slots, idx).await;
        }
    }

    /// Park on send work while reaping the oldest in-flight batch's
    /// notifications — the anti-deadlock invariant: tag release must
    /// never depend on new send work arriving (with all tags
    /// notif-gated and the host idle, work can only be *produced* once a
    /// notif frees a tag).
    async fn next_work_reaping<C, W>(
        &mut self,
        slots: &SlotArray<C>,
        work: &SendList<W>,
    ) -> Option<W>
    where
        C: Copy,
    {
        loop {
            let Some(&front) = self.inflight.front() else {
                return work.next().await;
            };
            enum Wait<W> {
                Work(Option<W>),
                Reaped,
            }
            let zc_copied = &mut self.zc_copied;
            let batch = &mut self.batches[front];
            let outcome = poll_fn(|cx| {
                if let Poll::Ready(item) = work.poll_next(cx) {
                    return Poll::Ready(Wait::Work(item));
                }
                if poll_notifs(&mut batch.pending_notifs, zc_copied, cx) {
                    return Poll::Ready(Wait::Reaped);
                }
                Poll::Pending
            })
            .await;
            match outcome {
                Wait::Work(item) => return item,
                Wait::Reaped => {
                    self.inflight.pop_front();
                    self.batches[front].recycle(slots);
                }
            }
        }
    }

    /// A batch just hit the socket: release what the send CQE allows and
    /// classify the batch. Notif-gated iff it actually holds
    /// notifications — a batch whose every ZC attempt fell back has none
    /// and is immediately reusable, like the plain path.
    fn retire<C: Copy>(&mut self, slots: &SlotArray<C>, idx: usize) {
        let batch = &mut self.batches[idx];
        for tag in batch.tags_at_cqe.drain(..) {
            slots.release_tag(tag);
        }
        if batch.pending_notifs.is_empty() {
            batch.recycle(slots);
        } else {
            self.zc_batches += 1;
            self.inflight.push_back(idx);
        }
    }
}

impl Drop for StreamSender {
    fn drop(&mut self) {
        // Tripwire: `run` always drains before returning, so a live
        // sender is never dropped mid-flight. A trip here means a code
        // path shipped ZC ops without draining — a use-after-free risk.
        debug_assert!(
            self.inflight.is_empty(),
            "StreamSender dropped with in-flight ZC batches"
        );
        debug_assert!(
            self.batches.iter().all(|b| b.pending_notifs.is_empty()),
            "StreamSender dropped with pending ZC notifications"
        );
    }
}

/// Stage `first` and every immediately-available work item into the
/// batch, stopping at its headroom. Returns the item that didn't fit (it
/// is staged first next round).
fn stage_batch<W, F>(
    batch: &mut GatherSendBatch,
    first: W,
    work: &SendList<W>,
    stage: &mut F,
) -> Option<W>
where
    F: FnMut(&mut GatherBatch, &W) -> Staged,
{
    let mut item = Some(first);
    while let Some(w) = item {
        if !batch.fits() {
            return Some(w); // flush first, stage next round
        }
        match stage(&mut batch.gather, &w) {
            Staged::NoRelease => {}
            Staged::AtCqe(tag) => batch.tags_at_cqe.push(tag),
            Staged::AtNotif(tag) => batch.tags_at_notif.push(tag),
        }
        item = work.try_next();
    }
    None
}

/// Ship one staged batch; on short send advance the iovecs and re-issue
/// so nothing else can interleave on the wire (ordering). The re-issue
/// stays ZC: the batch's pages are pinned regardless.
async fn ship_batch(
    fd: i32,
    batch: &mut GatherSendBatch,
    send_zc: bool,
    zc_fallbacks: &mut u64,
) -> std::io::Result<()> {
    let mut use_zc = send_zc;
    loop {
        let n = if use_zc {
            // SAFETY: msghdr, iovecs, arena, and referenced slot buffers
            // stay allocated until this batch's notifs are reaped
            // (reap_oldest/drain precede reset, and the caller joins this
            // task — or leaks the queue — before freeing anything). The
            // kernel snapshots the iovec array at issue, so advance()
            // after the send CQE never races it.
            let mut op = unsafe { ops::sendmsg_zc_raw(fd, batch.gather.msghdr()) }?;
            let res = op.sent().await;
            // Stash the notif BEFORE examining the result: a failed ZC
            // send can still have pinned pages (F_MORE), and only the
            // stashed handle makes the teardown drain wait for them.
            batch.pending_notifs.push(op.into_notif());
            match res {
                Ok(n) => n as usize,
                // ZC pins pages against the per-user RLIMIT_MEMLOCK
                // (ENOMEM past it; ENOBUFS for the optmem variant); two
                // full batches alone can exceed the default 8 MiB, and
                // the budget is shared across connections. An expected
                // operational condition, not a connection error: ship
                // the rest of this batch by copy.
                Err(err) if matches!(err.raw_os_error(), Some(libc::ENOMEM | libc::ENOBUFS)) => {
                    *zc_fallbacks += 1;
                    use_zc = false;
                    continue;
                }
                Err(err) => return Err(err),
            }
        } else {
            // SAFETY: the msghdr, iovec array, arena, and referenced slot
            // buffers all outlive the await — the batch is owned by this
            // task, slots release only after the batch completes, and the
            // caller joins this task (or leaks the queue) before freeing
            // anything.
            let op = unsafe { ops::sendmsg_raw(fd, batch.gather.msghdr()) }?;
            op.await? as usize
        };
        if n == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        if batch.gather.advance(n) {
            return Ok(());
        }
    }
}

/// Poll every pending notification once; completed ones are recorded in
/// `zc_copied` and removed. No future is ever dropped unfinished, so
/// completion is never silently lost. True when none remain.
fn poll_notifs(notifs: &mut Vec<ops::ZcNotif>, zc_copied: &mut u64, cx: &mut Context<'_>) -> bool {
    let mut i = 0;
    while i < notifs.len() {
        match Pin::new(&mut notifs[i]).poll(cx) {
            Poll::Ready(copied) => {
                notifs.swap_remove(i);
                if copied {
                    *zc_copied += 1;
                }
            }
            Poll::Pending => i += 1,
        }
    }
    notifs.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy)]
    enum TestWork {
        Payload(u16),
        Header(u16),
        Solicit,
    }

    /// A synthetic staging closure: writes a fixed 2-byte header so the
    /// batch has content, and classifies by work variant.
    fn stage(gather: &mut GatherBatch, work: &TestWork) -> Staged {
        gather.arena_tail()[..2].copy_from_slice(b"hi");
        gather.push_arena(2);
        match *work {
            TestWork::Payload(tag) => Staged::AtNotif(tag),
            TestWork::Header(tag) => Staged::AtCqe(tag),
            TestWork::Solicit => Staged::NoRelease,
        }
    }

    #[test]
    fn stage_batch_splits_tag_release_classes() {
        let work: SendList<TestWork> = SendList::new(4);
        work.push(TestWork::Header(2)); // greedily drained after `first`
        work.push(TestWork::Solicit);
        let mut batch = GatherSendBatch::new(4, 8, 4);

        let carry = stage_batch(&mut batch, TestWork::Payload(1), &work, &mut stage);

        assert!(carry.is_none());
        assert_eq!(batch.tags_at_notif, vec![1]);
        assert_eq!(batch.tags_at_cqe, vec![2]); // Solicit contributes no tag
    }

    #[test]
    fn recycle_releases_notif_tags_to_the_slot_array() {
        let slots: SlotArray<u8> = SlotArray::new(4, 64, 0);
        // Walk a tag to Responding so release_tag's debug_assert holds.
        let tag = slots.claim_tag().unwrap();
        slots.respond_receiving(tag);

        let mut batch = GatherSendBatch::new(4, 8, 4);
        batch.tags_at_notif.push(tag);
        batch.recycle(&slots);

        assert!(batch.tags_at_notif.is_empty());
        assert!(slots.idle(), "the notif-gated tag returned to the freelist");
    }
}
