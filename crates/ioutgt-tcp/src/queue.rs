//! The NVMe/TCP per-connection rendezvous: the core's [`NvmeQueue`]
//! plus this transport's ordered send list. The recv loop, slot
//! tasks, and send loop share one `Rc<TcpQueue>` and never call each
//! other — exactly the shape `QueueCore` had, with the send-work
//! types now owned here (an NVMe/RDMA transport has no R2T; an NBD
//! transport has no CQE — the work type is transport property).

use std::rc::Rc;
use std::task::Poll;

use ioutgt_core::queue::NvmeQueue;
use ioutgt_core::slotq::{SendList, SlotState};
use ioutgt_nvme::spec::Cqe;

/// A completed command waiting for the send path.
#[derive(Debug, Clone, Copy)]
#[allow(missing_docs)]
pub struct Completion {
    pub tag: u16,
    pub cqe: Cqe,
    /// Bytes of read data in the slot buffer to send as C2HData
    /// before (or instead of, with the success flag) the response
    /// capsule.
    pub data_len: u32,
}

/// One unit of work for the send path. R2Ts originate from the
/// receive path (solicit write data) but must serialize with
/// responses on the wire, so they travel through the same list.
#[derive(Debug, Clone, Copy)]
#[allow(missing_docs)]
pub enum SendWork {
    Response(Completion),
    R2t {
        tag: u16,
        cid: u16,
        offset: u32,
        length: u32,
    },
}

/// The connection's shared queue state.
pub struct TcpQueue {
    /// Core-side NVMe context (slots, sqhd, stats).
    pub nvme: Rc<NvmeQueue>,
    /// This transport's send list.
    pub send: SendList<SendWork>,
}

impl std::ops::Deref for TcpQueue {
    type Target = NvmeQueue;

    fn deref(&self) -> &Self::Target {
        &self.nvme
    }
}

impl TcpQueue {
    /// Allocate the queue pair for one connection.
    pub fn new(qid: u16, sqsize: u16, slot_buf_size: usize, sqhd_disabled: bool) -> Rc<TcpQueue> {
        Rc::new(TcpQueue {
            nvme: NvmeQueue::new(qid, sqsize, slot_buf_size, sqhd_disabled),
            send: SendList::new(sqsize),
        })
    }

    /// Queue a completion for the send path (slot task side).
    pub fn complete(&self, tag: u16, cqe: Cqe, data_len: u32) {
        self.nvme.begin_respond(tag);
        self.send
            .push(SendWork::Response(Completion { tag, cqe, data_len }));
    }

    /// Fail a command still in the receive phase (payload/digest)
    /// without dispatching it — e.g. a data-digest mismatch, where
    /// executing the write would persist corrupt data.
    pub fn complete_receiving(&self, tag: u16, cqe: Cqe) {
        self.nvme.respond_receiving(tag);
        self.send.push(SendWork::Response(Completion {
            tag,
            cqe,
            data_len: 0,
        }));
    }

    /// Queue an R2T soliciting write data for `tag` (recv path side;
    /// the slot stays `Receiving`).
    pub fn solicit(&self, tag: u16, cid: u16, offset: u32, length: u32) {
        debug_assert_eq!(self.nvme.slot(tag).state(), SlotState::Receiving);
        self.send.push(SendWork::R2t {
            tag,
            cid,
            offset,
            length,
        });
    }

    /// Non-blocking pop of send work (batching: drain without
    /// parking).
    pub fn try_next_send_work(&self) -> Option<SendWork> {
        self.send.try_next()
    }

    /// Poll-shaped pop, for combining with ZC-notification polling in
    /// one hand-rolled future.
    pub fn poll_send_work(&self, cx: &mut std::task::Context<'_>) -> Poll<Option<SendWork>> {
        self.send.poll_next(cx)
    }

    /// Await the next send-path work item; `None` after
    /// [`Self::close_send`] (pending work is delivered first).
    pub async fn next_send_work(&self) -> Option<SendWork> {
        self.send.next().await
    }

    /// Wake the send loop into orderly exit.
    pub fn close_send(&self) {
        self.send.close();
    }
}
