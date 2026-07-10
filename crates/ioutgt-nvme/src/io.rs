//! IO command execution: Read, Write, Flush, Write Zeroes, DSM.
//!
//! By the time a command reaches a slot task, the transport has already
//! gathered all H2C payload into the slot buffer (in-capsule or R2T),
//! so handlers here only validate, call the backend, and report status.
//! This mirrors nvmet: `io-cmd-*.c` never see transport details.

use std::rc::Rc;

use crate::spec::{DsmRange, RwCommand, Sqe, io_opcode};
use crate::status;
use zerocopy::FromBytes;

use crate::dispatch::{ConnCtx, IoState, Outcome};
use crate::subsystem::Namespace;
use ioutgt_core::backend::{Backend, BackendError, LbaRange};
use ioutgt_core::queue::stat_add;

/// NVMe status code for a backend failure, per nvmet's
/// blk_to_nvme_status mapping. Lives here — not on the `Backend`
/// trait — because the trait is shared with non-NVMe transports.
pub fn nvme_status(err: BackendError) -> u16 {
    match err {
        BackendError::OutOfRange => status::LBA_RANGE | status::DNR,
        BackendError::NoSpace => status::CAP_EXCEEDED | status::DNR,
        BackendError::Unsupported => status::INVALID_OPCODE | status::DNR,
        BackendError::Io(_) => status::INTERNAL | status::DNR,
    }
}

fn err_outcome<B: Backend>(ctx: &Rc<ConnCtx<B>>, cid: u16, code: u16) -> Outcome {
    stat_add(&ctx.queue.stats.errors, 1);
    Outcome::status(ctx.cqe(0, cid, code))
}

fn map_backend<B: Backend>(
    ctx: &Rc<ConnCtx<B>>,
    cid: u16,
    result: Result<(), BackendError>,
    data_len: u32,
) -> Outcome {
    match result {
        Ok(()) => Outcome::with_data(ctx.cqe(0, cid, status::SUCCESS), data_len),
        Err(err) => err_outcome(ctx, cid, nvme_status(err)),
    }
}

/// Execute one IO-queue command (slot task context).
pub async fn execute<B: Backend>(
    ctx: &Rc<ConnCtx<B>>,
    io: &IoState<B>,
    tag: u16,
    sqe: &Sqe,
) -> Outcome {
    let cid = sqe.cid.get();
    // No commands before this queue's Connect has bound a subsystem.
    let Some(subsys) = io.subsys.get() else {
        return err_outcome(ctx, cid, status::CMD_SEQ_ERROR | status::DNR);
    };
    let table = io.ns_cache.get(subsys);
    let Some(ns) = table.get(&sqe.nsid.get()) else {
        return err_outcome(ctx, cid, status::INVALID_NS | status::DNR);
    };

    match sqe.opcode {
        io_opcode::FLUSH => {
            stat_add(&ctx.queue.stats.flush_cmds, 1);
            map_backend(ctx, cid, ns.backend.flush().await, 0)
        }
        io_opcode::READ => {
            stat_add(&ctx.queue.stats.read_cmds, 1);
            read(ctx, ns, tag, sqe).await
        }
        io_opcode::WRITE => {
            stat_add(&ctx.queue.stats.write_cmds, 1);
            write(ctx, ns, tag, sqe).await
        }
        io_opcode::WRITE_ZEROES => {
            stat_add(&ctx.queue.stats.other_cmds, 1);
            let rw = RwCommand::parse(sqe);
            let range = LbaRange {
                slba: rw.slba,
                nlb: u32::from(rw.nlb) + 1,
            };
            map_backend(ctx, cid, ns.backend.write_zeroes(range).await, 0)
        }
        io_opcode::DSM => {
            stat_add(&ctx.queue.stats.other_cmds, 1);
            dsm(ctx, ns, tag, sqe).await
        }
        _ => {
            stat_add(&ctx.queue.stats.other_cmds, 1);
            err_outcome(ctx, cid, status::INVALID_OPCODE | status::DNR)
        }
    }
}

/// Transfer length from the LBA count, validated against MDTS and the
/// SGL-declared length.
fn checked_len<B: Backend>(backend: &B, sqe: &Sqe) -> Result<(RwCommand, u32), u16> {
    let rw = RwCommand::parse(sqe);
    let nlb = u64::from(rw.nlb) + 1;
    let len = nlb << backend.block_shift();
    if len > u64::from(crate::MDTS_BYTES) {
        return Err(status::INVALID_FIELD | status::DNR);
    }
    // The SGL data-length field is 32-bit for an in-capsule data-block
    // descriptor (NVMe/TCP) but only 24-bit for a keyed SGL data-block
    // descriptor (NVMe/RDMA: bytes 8..11, with byte 11 starting the key). Every
    // valid transfer is <= MDTS (128 KiB) < 2^24, so the top byte is always zero
    // for a real length; mask it so the check reads both descriptor formats.
    if u64::from(sqe.dptr.length.get() & 0x00FF_FFFF) != len {
        return Err(status::DATA_SGL_LEN_INVALID | status::DNR);
    }
    backend.check_range(rw.slba, nlb).map_err(nvme_status)?;
    #[allow(clippy::cast_possible_truncation)]
    Ok((rw, len as u32))
}

// Slot-data borrows held across backend awaits are sound: the slot is
// `Executing`, so neither the recv path nor the send path touches it
// until `complete()`; everything is one thread.
#[allow(clippy::await_holding_refcell_ref)]
async fn read<B: Backend>(ctx: &Rc<ConnCtx<B>>, ns: &Namespace<B>, tag: u16, sqe: &Sqe) -> Outcome {
    let cid = sqe.cid.get();
    let (rw, len) = match checked_len(ns.backend.as_ref(), sqe) {
        Ok(v) => v,
        Err(code) => return err_outcome(ctx, cid, code),
    };
    // Lease the read buffer on demand (parks if the pool is momentarily
    // full — backpressure, never a deadlock).
    ctx.queue.lease_await(tag, len as usize).await;
    let slot = ctx.queue.slot(tag);
    let buf = slot.data();
    let result = ns
        .backend
        .read_segs(rw.slba, buf.segs(), len as usize, buf.buf_index())
        .await;
    drop(buf);
    if result.is_ok() {
        stat_add(&ctx.queue.stats.read_bytes, u64::from(len));
    }
    map_backend(ctx, cid, result, len)
}

// See `read` for why the borrow across the await is sound.
#[allow(clippy::await_holding_refcell_ref)]
async fn write<B: Backend>(
    ctx: &Rc<ConnCtx<B>>,
    ns: &Namespace<B>,
    tag: u16,
    sqe: &Sqe,
) -> Outcome {
    let cid = sqe.cid.get();
    let (rw, len) = match checked_len(ns.backend.as_ref(), sqe) {
        Ok(v) => v,
        Err(code) => return err_outcome(ctx, cid, code),
    };
    let slot = ctx.queue.slot(tag);
    // The transport must have delivered exactly the SGL-declared bytes
    // into the buffer it leased while receiving.
    if slot.data_len() != len {
        tracing::warn!(
            qid = ctx.queue.qid,
            tag,
            data_len = slot.data_len(),
            len,
            "write: transport-delivered length mismatch; failing DATA_XFER_ERROR"
        );
        return err_outcome(ctx, cid, status::DATA_XFER_ERROR | status::DNR);
    }
    let buf = slot.data();
    let result = ns
        .backend
        .write_segs(rw.slba, buf.segs(), len as usize, buf.buf_index())
        .await;
    drop(buf);
    // FUA on a memory/file backend: flush after write.
    let result = match (result, rw.fua) {
        (Ok(()), true) => ns.backend.flush().await,
        (other, _) => other,
    };
    if result.is_ok() {
        stat_add(&ctx.queue.stats.write_bytes, u64::from(len));
    }
    map_backend(ctx, cid, result, 0)
}

async fn dsm<B: Backend>(ctx: &Rc<ConnCtx<B>>, ns: &Namespace<B>, tag: u16, sqe: &Sqe) -> Outcome {
    let cid = sqe.cid.get();
    // Deallocate is the only attribute we act on; others are accepted
    // no-ops per spec.
    let deallocate = sqe.cdw11.get() & (1 << 2) != 0;
    let nr = ((sqe.cdw10.get() & 0xFF) as usize) + 1;
    let needed = nr * size_of::<DsmRange>();
    let slot = ctx.queue.slot(tag);
    if (slot.data_len() as usize) < needed {
        return err_outcome(ctx, cid, status::DATA_SGL_LEN_INVALID | status::DNR);
    }
    if !deallocate {
        return Outcome::status(ctx.cqe(0, cid, status::SUCCESS));
    }
    // The range list may span pool segments; gather it contiguous first
    // (DSM is cold, so the copy is cheap).
    let ranges: Vec<LbaRange> = {
        let mut bytes = vec![0u8; needed];
        let mut off = 0;
        slot.data().for_each_seg(0, needed, |chunk| {
            bytes[off..off + chunk.len()].copy_from_slice(chunk);
            off += chunk.len();
        });
        (0..nr)
            .map(|i| {
                let raw = DsmRange::read_from_bytes(&bytes[i * 16..i * 16 + 16])
                    .expect("16 aligned bytes");
                LbaRange {
                    slba: raw.slba.get(),
                    nlb: raw.nlb.get(),
                }
            })
            .collect()
    };
    map_backend(ctx, cid, ns.backend.discard(&ranges).await, 0)
}
