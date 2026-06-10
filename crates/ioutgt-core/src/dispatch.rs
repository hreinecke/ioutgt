//! Command dispatch: routes a received SQE to its handler.
//!
//! M3 scope: structure + stubs. Fabrics/admin handlers land in M4, the
//! IO path in M5; until then every command completes with
//! INVALID_OPCODE so the slot/response machinery can be exercised end
//! to end.

use std::rc::Rc;

use ioutgt_nvme::spec::{Cqe, Sqe};
use ioutgt_nvme::status;

use crate::queue::QueueCore;

/// Dispatch one command on an IO queue. Returns the CQE and the number
/// of C2H data bytes left in the slot buffer for the send path.
pub async fn execute_io(queue: &Rc<QueueCore>, tag: u16, sqe: &Sqe) -> (Cqe, u32) {
    let _ = tag;
    let status = status::INVALID_OPCODE | status::DNR;
    let cqe = Cqe::new(0, queue.advance_sqhd(), queue.qid, sqe.cid.get(), status);
    (cqe, 0)
}

/// Dispatch one command on the admin queue (stub, see module docs).
pub async fn execute_admin(queue: &Rc<QueueCore>, tag: u16, sqe: &Sqe) -> (Cqe, u32) {
    let _ = tag;
    let status = status::INVALID_OPCODE | status::DNR;
    let cqe = Cqe::new(0, queue.advance_sqhd(), queue.qid, sqe.cid.get(), status);
    (cqe, 0)
}
