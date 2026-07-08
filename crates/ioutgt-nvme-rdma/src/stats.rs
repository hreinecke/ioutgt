//! Per-queue RDMA work-request statistics: the WR-class counters (READ,
//! WRITE, SEND, RECV) and batch-size histograms `RdmaQueue` bumps on its hot
//! path, exposed to GET_STATS via [`TransportStats`].

use std::cell::Cell;

use ioutgt_core::queue::TransportStats;

/// Per-queue RDMA work-request counters, one class each for READ (host
/// write-data pulls), WRITE (read-data pushes), SEND (CQE capsules), and RECV
/// (command capsules). `posted`/`done` are cumulative (reset by GET_STATS
/// `clear`); `inflight` is a live gauge (posted−done), never reset, so it stays
/// accurate across a clear. Reported under `"wr"` in GET_STATS via
/// [`TransportStats`]. All access is on the owning queue thread (`Cell`).
#[derive(Debug, Default)]
pub(crate) struct WrClass {
    posted: Cell<u64>,
    done: Cell<u64>,
    inflight: Cell<i64>,
}

impl WrClass {
    /// Count `n` posted WRs: bump the cumulative `posted` and the live gauge.
    #[inline]
    pub(crate) fn post_n(&self, n: u64) {
        self.posted.set(self.posted.get() + n);
        self.inflight.set(self.inflight.get() + n as i64);
    }

    #[inline]
    pub(crate) fn post(&self) {
        self.post_n(1);
    }

    /// Count a completed WR: bump cumulative `done`, drop the live gauge.
    #[inline]
    pub(crate) fn complete(&self) {
        self.done.set(self.done.get() + 1);
        self.inflight.set(self.inflight.get() - 1);
    }
}

/// Log2-bucketed batch-size histogram — buckets for 1, 2, 3-4, 5-8, 9-16 and
/// 17+ items — cheap enough for the hot path (one branch + one Cell bump).
/// Exposed through GET_STATS so `stat` can show the *distribution* of
/// submission and completion batch sizes, not just their averages.
#[derive(Debug, Default)]
pub(crate) struct BatchHist([Cell<u64>; 6]);

impl BatchHist {
    #[inline]
    pub(crate) fn record(&self, n: usize) {
        let idx = match n {
            0 => return,
            1 => 0,
            2 => 1,
            3..=4 => 2,
            5..=8 => 3,
            9..=16 => 4,
            _ => 5,
        };
        self.0[idx].set(self.0[idx].get() + 1);
    }
}

/// GET_STATS key names for the three batch histograms (wire-format stable):
/// WRs per read-batch doorbell, WRs per response-batch doorbell, and CQEs per
/// non-empty poll.
const HIST_KEYS: [[&str; 6]; 4] = [
    [
        "read_db_b1",
        "read_db_b2",
        "read_db_b4",
        "read_db_b8",
        "read_db_b16",
        "read_db_b32",
    ],
    [
        "resp_db_b1",
        "resp_db_b2",
        "resp_db_b4",
        "resp_db_b8",
        "resp_db_b16",
        "resp_db_b32",
    ],
    [
        "recv_db_b1",
        "recv_db_b2",
        "recv_db_b4",
        "recv_db_b8",
        "recv_db_b16",
        "recv_db_b32",
    ],
    [
        "poll_b1", "poll_b2", "poll_b4", "poll_b8", "poll_b16", "poll_b32",
    ],
];

#[derive(Debug, Default)]
pub(crate) struct RdmaWrStats {
    /// Host write-data pulls (RDMA READ), read-data pushes (RDMA WRITE),
    /// CQE capsules (SEND), and command capsules (RECV).
    pub(crate) read: WrClass,
    pub(crate) write: WrClass,
    pub(crate) send: WrClass,
    pub(crate) recv: WrClass,
    /// Non-empty CQ polls (completion batches). `*_done / poll_batches` is the
    /// average number of each WR class reaped per batch.
    pub(crate) poll_batches: Cell<u64>,
    /// Send-queue doorbells rung (`ibv_post_send` calls for READ/WRITE/SEND).
    /// `(read+write+send)_posted / sq_doorbells` is the submission batch size —
    /// 1.0 with one WR per post, higher once WRs are chained per doorbell.
    pub(crate) sq_doorbells: Cell<u64>,
    /// WRs chained per read-batch doorbell (`post_reads_batch`).
    pub(crate) read_db: BatchHist,
    /// WRs chained per response-batch doorbell (`post_responses_batch`;
    /// a response is 1 SEND, or WRITE+SEND when it carries read data).
    pub(crate) resp_db: BatchHist,
    /// RECV WRs per repost doorbell (the recv queue's doorbell, not counted in
    /// `sq_doorbells`). Always singletons BY DESIGN — see the RNR note in
    /// `handle_recv`; this column is the canary that keeps it that way.
    pub(crate) recv_db: BatchHist,
    /// CQEs reaped per non-empty CQ poll.
    pub(crate) poll: BatchHist,
}

impl RdmaWrStats {
    /// Count a send-queue doorbell (`ibv_post_send`), whatever it batched.
    #[inline]
    pub(crate) fn doorbell(&self) {
        self.sq_doorbells.set(self.sq_doorbells.get() + 1);
    }

    /// Count a single-WR send-queue post plus the doorbell it rings.
    #[inline]
    pub(crate) fn sq_post(&self, class: &WrClass) {
        class.post();
        self.doorbell();
    }

    /// The classes with their GET_STATS key names (wire-format stable).
    fn classes(&self) -> [([&'static str; 3], &WrClass); 4] {
        [
            (["read_posted", "read_done", "read_inflight"], &self.read),
            (
                ["write_posted", "write_done", "write_inflight"],
                &self.write,
            ),
            (["send_posted", "send_done", "send_inflight"], &self.send),
            (["recv_posted", "recv_done", "recv_inflight"], &self.recv),
        ]
    }

    /// The scalar counters with their GET_STATS keys (wire-format stable).
    fn scalars(&self) -> [(&'static str, &Cell<u64>); 2] {
        [
            ("poll_batches", &self.poll_batches),
            ("sq_doorbells", &self.sq_doorbells),
        ]
    }

    /// The batch histograms paired with their GET_STATS key rows. Pairing keys
    /// to histograms here — rather than zipping `HIST_KEYS` positionally in both
    /// `snapshot` and `reset` — is what keeps the two from drifting out of sync.
    fn hists(&self) -> [(&'static [&'static str; 6], &BatchHist); 4] {
        [
            (&HIST_KEYS[0], &self.read_db),
            (&HIST_KEYS[1], &self.resp_db),
            (&HIST_KEYS[2], &self.recv_db),
            (&HIST_KEYS[3], &self.poll),
        ]
    }
}

impl TransportStats for RdmaWrStats {
    fn snapshot(&self) -> Vec<(&'static str, u64)> {
        let mut out = Vec::with_capacity(38);
        for (names, class) in self.classes() {
            let gauge = u64::try_from(class.inflight.get().max(0)).unwrap_or(0);
            out.extend([
                (names[0], class.posted.get()),
                (names[1], class.done.get()),
                (names[2], gauge),
            ]);
        }
        for (key, cell) in self.scalars() {
            out.push((key, cell.get()));
        }
        for (keys, hist) in self.hists() {
            for (key, cell) in keys.iter().zip(&hist.0) {
                out.push((key, cell.get()));
            }
        }
        out
    }

    fn reset(&self) {
        for (_, class) in self.classes() {
            class.posted.set(0);
            class.done.set(0);
        }
        for (_, cell) in self.scalars() {
            cell.set(0);
        }
        for (_, hist) in self.hists() {
            for cell in &hist.0 {
                cell.set(0);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The GET_STATS key list + order is a stable wire format the `stat` tooling
    /// reads; lock it so the table-driven `snapshot` can't silently reorder it.
    #[test]
    fn stats_snapshot_key_order_is_stable() {
        let keys: Vec<&str> = RdmaWrStats::default()
            .snapshot()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(
            keys,
            [
                "read_posted",
                "read_done",
                "read_inflight",
                "write_posted",
                "write_done",
                "write_inflight",
                "send_posted",
                "send_done",
                "send_inflight",
                "recv_posted",
                "recv_done",
                "recv_inflight",
                "poll_batches",
                "sq_doorbells",
                "read_db_b1",
                "read_db_b2",
                "read_db_b4",
                "read_db_b8",
                "read_db_b16",
                "read_db_b32",
                "resp_db_b1",
                "resp_db_b2",
                "resp_db_b4",
                "resp_db_b8",
                "resp_db_b16",
                "resp_db_b32",
                "recv_db_b1",
                "recv_db_b2",
                "recv_db_b4",
                "recv_db_b8",
                "recv_db_b16",
                "recv_db_b32",
                "poll_b1",
                "poll_b2",
                "poll_b4",
                "poll_b8",
                "poll_b16",
                "poll_b32",
            ]
        );
    }
}
