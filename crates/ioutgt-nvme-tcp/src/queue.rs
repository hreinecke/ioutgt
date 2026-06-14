//! The NVMe/TCP per-connection rendezvous: the core's
//! [`QueueCore<Sqe>`][QueueCore] plus this transport's ordered send
//! list. The recv loop, slot tasks, and send loop share one
//! `Rc<NvmeTcpQueue>` and never call each other, with the send-work
//! types owned here (an NVMe/RDMA transport has no R2T; an NBD
//! transport has no CQE — the work type is transport property).

use std::rc::Rc;

use ioutgt_core::queue::QueueCore;
use ioutgt_core::slotq::{SendList, SlotState};
use ioutgt_nvme::spec::{Cqe, Sqe};

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
pub struct NvmeTcpQueue {
    /// Core-side NVMe queue context (slots, sqhd, stats).
    pub nvme: Rc<QueueCore<Sqe>>,
    /// This transport's send list.
    pub send: SendList<SendWork>,
}

impl std::ops::Deref for NvmeTcpQueue {
    type Target = QueueCore<Sqe>;

    fn deref(&self) -> &Self::Target {
        &self.nvme
    }
}

impl NvmeTcpQueue {
    /// Allocate the queue pair for one connection.
    pub fn new(
        qid: u16,
        sqsize: u16,
        slot_buf_size: usize,
        sqhd_disabled: bool,
    ) -> Rc<NvmeTcpQueue> {
        Rc::new(NvmeTcpQueue {
            nvme: QueueCore::new(qid, sqsize, slot_buf_size, sqhd_disabled, Sqe::zeroed()),
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

    /// Wake the send loop into orderly exit.
    pub fn close_send(&self) {
        self.send.close();
    }
}
