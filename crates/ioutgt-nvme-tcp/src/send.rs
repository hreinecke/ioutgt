//! Send path: encode staged work (responses, R2Ts) into a gather batch and
//! ship it. The recv loop, slot tasks, and send loop (this module) share one
//! `Rc<NvmeTcpQueue>` and never call each other — [`send_loop`] only drains
//! `queue.send` and drives [`StreamSender`], with zero references to the
//! recv-side phase machine in `connection.rs`.

use std::rc::Rc;

use crate::queue::{NvmeTcpQueue, SendWork};
use ioutgt_nvme::{digest, pdu};
use ioutgt_stream::{Staged, StreamSender};
use ioutgt_uring::sendbatch::GatherBatch;
use tracing::debug;

/// Worst-case arena bytes per staged item: C2HData header (24+4
/// HDGST) + DDGST trailer (4) + response capsule (24+4).
pub(crate) const ARENA_PER_ITEM: usize = 64;
/// Worst-case iovec entries per staged item: a C2HData header, the read
/// payload (up to `MAX_SEGS` segments when the slot lease is scattered),
/// the DDGST trailer, and the response capsule. Adjacent arena chunks
/// merge, so the header/digest/capsule collapse; the payload segments do
/// not. This bounds one item's iovec use so the batch `fits()` check
/// never lets staging overrun the iovec cap (which clamps to UIO_MAXIOV).
pub(crate) const IOVS_PER_ITEM: usize = ioutgt_core::pool::MAX_SEGS + 3;

/// Stage one work item: header pieces into the arena (sans-IO encoders
/// unchanged), payload referenced in place from the slot buffer, DDGST
/// computed over the slot and trailed in the arena.
pub(crate) fn stage_send_work(
    gather: &mut GatherBatch,
    queue: &Rc<NvmeTcpQueue>,
    work: &SendWork,
    hdr_digest: bool,
    data_digest: bool,
) {
    match *work {
        SendWork::R2t {
            tag,
            cid,
            offset,
            length,
        } => {
            let n = pdu::encode_r2t(gather.arena_tail(), cid, tag, offset, length, hdr_digest);
            gather.push_arena(n);
        }
        SendWork::Response(completion) => {
            let success_elide =
                completion.data_len > 0 && queue.sqhd_disabled && completion.cqe.status.get() == 0;
            if completion.data_len > 0 {
                let data_len = completion.data_len as usize;
                let n = pdu::encode_c2h_data(
                    gather.arena_tail(),
                    completion.cqe.cid.get(),
                    0,
                    completion.data_len,
                    true,
                    success_elide,
                    hdr_digest,
                    data_digest,
                );
                gather.push_arena(n);
                let slot_data = queue.slot(completion.tag).data();
                // The payload rides in place from the slot buffer's
                // segments (one when contiguous); the slot stays claimed
                // until release_tag after the batch send completes.
                let mut remaining = data_len;
                for seg in slot_data.segs() {
                    if remaining == 0 {
                        break;
                    }
                    let take = remaining.min(seg.len);
                    gather.push_raw(seg.ptr.cast_const(), take);
                    remaining -= take;
                }
                // Account this item's payload so the sender can pick copy vs
                // ZC for the whole batch by average per-item size.
                gather.note_payload(data_len);
                if data_digest {
                    let mut crc = digest::Crc32c::new();
                    slot_data.for_each_seg(0, data_len, |c| crc.update(c));
                    gather.arena_tail()[..4].copy_from_slice(&crc.finalize().to_le_bytes());
                    gather.push_arena(4);
                }
            }
            if !success_elide {
                let n = pdu::encode_capsule_resp(gather.arena_tail(), &completion.cqe, hdr_digest);
                gather.push_arena(n);
            }
        }
    }
}

/// Tag-release class for a send work item: payload-carrying responses
/// gate on the batch's ZC notification (the op references the slot
/// buffer), capsule-only responses release at the send CQE, R2Ts
/// release nothing.
pub(crate) fn release_class(work: &SendWork) -> Staged {
    match *work {
        SendWork::Response(c) if c.data_len > 0 => Staged::AtNotif(c.tag),
        SendWork::Response(c) => Staged::AtCqe(c.tag),
        SendWork::R2t { .. } => Staged::NoRelease,
    }
}

/// Send loop: drain ALL pending completions/R2Ts into one gather list
/// and ship it as a single SENDMSG — or SENDMSG_ZC under `--send-zc`.
/// All the batching, short-send resume, and zero-copy notification
/// machinery lives in the transport-neutral [`StreamSender`]; here we
/// only encode NVMe PDUs (`stage_send_work`) and classify each work
/// item's tag release.
pub(crate) async fn send_loop(
    queue: &Rc<NvmeTcpQueue>,
    fd: i32,
    hdr_digest: bool,
    data_digest: bool,
    send_zc: bool,
) -> std::io::Result<()> {
    // The send arenas were reserved from the registered data pool at queue
    // install (above), so headers and slot payloads share one buf_index —
    // enabling vectored fixed-buffer ZC sends (no per-send IOMMU map). `None`
    // (pool unregistered) keeps the heap arena + plain SENDMSG_ZC path.
    let pool = queue.nvme.slots.pool();
    let pool_arena = pool
        .send_arena()
        .and_then(|(ptr, _)| pool.buf_index().map(|idx| (ptr, idx)));
    let mut sender = StreamSender::new(queue.sqsize, ARENA_PER_ITEM, IOVS_PER_ITEM, pool_arena);
    let result = sender
        .run(
            fd,
            send_zc,
            &queue.nvme.slots,
            &queue.send,
            |gather, work: &SendWork| {
                stage_send_work(gather, queue, work, hdr_digest, data_digest);
                release_class(work)
            },
        )
        .await;
    if send_zc {
        let s = sender.stats();
        debug!(
            qid = queue.qid,
            zc_batches = s.zc_batches,
            zc_copied = s.zc_copied,
            zc_fallbacks = s.zc_fallbacks,
            "send loop ZC stats"
        );
    }
    result
}

#[cfg(test)]
mod gather_tests {
    use crate::queue::Completion;
    use ioutgt_nvme::spec::Cqe;

    use super::*;

    /// Sizing a test gather arena as the send path does for `sqsize`.
    fn gather_for(sqsize: u16) -> GatherBatch {
        let n = usize::from(sqsize);
        GatherBatch::new(n * ARENA_PER_ITEM, n * IOVS_PER_ITEM + IOVS_PER_ITEM)
    }

    /// Linearize the staged iovecs (what the kernel would put on the wire).
    fn gather(g: &GatherBatch) -> Vec<u8> {
        let mut out = Vec::new();
        for e in g.iovs() {
            // SAFETY: entries reference the gather arena and slot
            // buffers owned by the test, sized by construction.
            let s = unsafe { std::slice::from_raw_parts(e.iov_base.cast::<u8>(), e.iov_len) };
            out.extend_from_slice(s);
        }
        out
    }

    #[test]
    fn scattered_read_payload_linearizes() {
        use ioutgt_core::pool::PAGE;
        // Fragment a 4-page pool so a 2-page read lease must scatter.
        let queue = NvmeTcpQueue::new(1, 8, 4 * PAGE, false);
        let a = queue.pool().alloc(PAGE).unwrap();
        let _b = queue.pool().alloc(PAGE).unwrap();
        let c = queue.pool().alloc(PAGE).unwrap();
        let _d = queue.pool().alloc(PAGE).unwrap();
        drop(a);
        drop(c); // free pages 0 and 2 — two non-adjacent holes
        queue.lease_or_owned(2, 2 * PAGE);
        assert!(
            !queue.slot(2).data().is_contiguous(),
            "the 2-page lease should be scattered across the holes"
        );

        #[allow(clippy::cast_possible_truncation)]
        let payload: Vec<u8> = (0..(2 * PAGE) as u32).map(|i| (i % 251) as u8).collect();
        queue.slot(2).data().write_at(0, &payload);

        let mut g = gather_for(queue.sqsize);
        let cqe = Cqe::new(0, 1, 1, 7, 0);
        #[allow(clippy::cast_possible_truncation)]
        let item = SendWork::Response(Completion {
            tag: 2,
            cqe,
            data_len: (2 * PAGE) as u32,
        });
        assert!(g.fits(ARENA_PER_ITEM, IOVS_PER_ITEM));
        stage_send_work(&mut g, &queue, &item, true, true);

        // The scattered payload must linearize to the same wire bytes as a
        // contiguous one: C2HData hdr | payload | DDGST | response capsule.
        let mut expect = vec![0u8; 4 * PAGE];
        #[allow(clippy::cast_possible_truncation)]
        let len = (2 * PAGE) as u32;
        let mut off = pdu::encode_c2h_data(&mut expect, 7, 0, len, true, false, true, true);
        expect[off..off + 2 * PAGE].copy_from_slice(&payload);
        off += 2 * PAGE;
        let crc = digest::crc32c(&payload);
        expect[off..off + 4].copy_from_slice(&crc.to_le_bytes());
        off += 4;
        off += pdu::encode_capsule_resp(&mut expect[off..], &cqe, true);
        expect.truncate(off);

        assert_eq!(gather(&g), expect, "scattered payload sends correct bytes");
        // Payload contributes two (non-merged) iovec entries.
        assert!(g.iovs().len() >= 4, "header + 2 payload segs + trailer");
    }

    #[test]
    fn batch_matches_linear_encoding() {
        let queue = NvmeTcpQueue::new(1, 4, 128 * 1024, false);
        #[allow(clippy::cast_possible_truncation)]
        let payload: Vec<u8> = (0..1000u32).map(|i| i as u8).collect();
        queue.lease_or_owned(2, 1000);
        queue.slot(2).data().write_at(0, &payload);

        let mut g = gather_for(queue.sqsize);
        let cqe = Cqe::new(0, 1, 1, 7, 0);
        let items = [
            SendWork::R2t {
                tag: 3,
                cid: 9,
                offset: 0,
                length: 4096,
            },
            SendWork::Response(Completion {
                tag: 2,
                cqe,
                data_len: 1000,
            }),
        ];
        for item in &items {
            assert!(g.fits(ARENA_PER_ITEM, IOVS_PER_ITEM));
            stage_send_work(&mut g, &queue, item, true, true);
        }

        // Reference: the same PDUs encoded linearly (the old staging
        // layout): R2T | C2HData hdr | payload | DDGST | resp capsule.
        let mut expect = vec![0u8; 8192];
        let mut off = pdu::encode_r2t(&mut expect, 9, 3, 0, 4096, true);
        off += pdu::encode_c2h_data(&mut expect[off..], 7, 0, 1000, true, false, true, true);
        expect[off..off + 1000].copy_from_slice(&payload);
        off += 1000;
        let crc = digest::crc32c(&payload);
        expect[off..off + 4].copy_from_slice(&crc.to_le_bytes());
        off += 4;
        off += pdu::encode_capsule_resp(&mut expect[off..], &cqe, true);
        expect.truncate(off);

        assert_eq!(gather(&g), expect);
        // Arena-contiguous chunks merge: [R2T+C2H hdr][payload][DDGST+capsule].
        assert_eq!(g.iovs().len(), 3);
    }

    #[test]
    fn batch_elides_and_merges_without_digests() {
        // sqhd_disabled queue: a successful read elides the response
        // capsule; digests off exercises the bare-header layout.
        let queue = NvmeTcpQueue::new(1, 4, 128 * 1024, true);
        let payload = [0xa5u8; 512];
        queue.lease_or_owned(1, 512);
        queue.slot(1).data().write_at(0, &payload);

        let mut g = gather_for(queue.sqsize);
        let read_cqe = Cqe::new(0, 1, 1, 5, 0);
        let flush_cqe = Cqe::new(0, 2, 1, 6, 0);
        let items = [
            // Elided: C2HData header + payload, no capsule.
            SendWork::Response(Completion {
                tag: 1,
                cqe: read_cqe,
                data_len: 512,
            }),
            // Data-less response: capsule only.
            SendWork::Response(Completion {
                tag: 3,
                cqe: flush_cqe,
                data_len: 0,
            }),
        ];
        for item in &items {
            assert!(g.fits(ARENA_PER_ITEM, IOVS_PER_ITEM));
            stage_send_work(&mut g, &queue, item, false, false);
        }

        let mut expect = vec![0u8; 4096];
        let mut off = pdu::encode_c2h_data(&mut expect, 5, 0, 512, true, true, false, false);
        expect[off..off + 512].copy_from_slice(&payload);
        off += 512;
        off += pdu::encode_capsule_resp(&mut expect[off..], &flush_cqe, false);
        expect.truncate(off);

        assert_eq!(gather(&g), expect);
        // [C2H hdr][payload][capsule]: capsule can't merge across the
        // slot-payload entry.
        assert_eq!(g.iovs().len(), 3);
    }

    #[test]
    fn release_class_splits_tag_release() {
        let read_cqe = Cqe::new(0, 1, 1, 5, 0);
        let flush_cqe = Cqe::new(0, 2, 1, 6, 0);
        // Payload-carrying: slot referenced by the op → notif-gated.
        assert_eq!(
            release_class(&SendWork::Response(Completion {
                tag: 1,
                cqe: read_cqe,
                data_len: 4096,
            })),
            Staged::AtNotif(1),
        );
        // Capsule-only: arena bytes only → released at the send CQE.
        assert_eq!(
            release_class(&SendWork::Response(Completion {
                tag: 2,
                cqe: flush_cqe,
                data_len: 0,
            })),
            Staged::AtCqe(2),
        );
        // R2T: no tag to release at all.
        assert_eq!(
            release_class(&SendWork::R2t {
                tag: 3,
                cid: 9,
                offset: 0,
                length: 4096,
            }),
            Staged::NoRelease,
        );
    }
}
