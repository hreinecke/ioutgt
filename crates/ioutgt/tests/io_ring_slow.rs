#![allow(clippy::cast_possible_truncation)] // test indices bounded by constants

//! Zero-copy recv-ring torture against a *slow* backend.
//!
//! The instant memory backend completes a write before the next recv chunk
//! even arrives, so a retained recv-ring sub-buffer is borrowed for almost no
//! time. A real SSD makes the write take long enough that many in-flight
//! writes pin both sub-buffers at once, driving the recv loop into the
//! 2-buffer back-pressure (ENOBUFS park) and deferred-re-provide paths — the
//! load- and timing-dependent corner the box reproduces and the in-process
//! suites miss.
//!
//! This test reconstructs that pressure locally with a deliberately tiny ring
//! (two sub-buffers), an artificial per-write delay (`mem_write_delay_us`), and
//! a *sliding-window* pipeline that keeps many whole-transfer (single-PDU) R2T
//! writes outstanding at once — so the ring stays saturated, sub-buffers stay
//! borrowed across slow writes, and recv repeatedly parks on ENOBUFS and
//! resumes on deferred re-provide. Then it reads everything back and verifies
//! byte-for-byte. A corruption, assertion, or hang here is the recv-ring
//! lifecycle bug.

mod common;

use std::collections::HashMap;
use std::time::Duration;

use common::{Client, NQN, rw_sqe};
use ioutgt_nvme::pdu::PduKind;
use ioutgt_nvme::{spec, status};

const BLOCK: u64 = 512;
/// Whole-transfer sizes; the larger ones (> 16 KiB inline limit) take the R2T
/// path and, when they fit a sub-buffer, are retained zero-copy as one PDU.
const SIZES: &[u32] = &[65_536, 131_072, 65_536, 32_768, 131_072, 98_304];

fn fill_pattern(buf: &mut [u8], slba: u64, generation: u8) {
    for (block_index, chunk) in buf.chunks_mut(BLOCK as usize).enumerate() {
        let lba = slba + block_index as u64;
        let seed = lba
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(u64::from(generation));
        let bytes = seed.to_le_bytes();
        for (i, b) in chunk.iter_mut().enumerate() {
            *b = bytes[i % 8] ^ (i as u8).wrapping_mul(13);
        }
    }
}

#[derive(Clone)]
struct Region {
    slba: u64,
    len: u32,
    generation: u8,
}

/// Total whole-transfer writes pushed through the pipeline.
const TOTAL: usize = 800;
/// Target number of writes outstanding at once (near the 64-deep queue) so the
/// two-sub-buffer ring stays saturated and recv keeps parking on ENOBUFS.
const WINDOW: usize = 48;

fn submit_write(io: &mut Client, cid: u16, region: &Region) {
    let nblocks = u64::from(region.len) / BLOCK;
    let nlb0 = u16::try_from(nblocks - 1).unwrap();
    let mut sqe = rw_sqe(
        spec::io_opcode::WRITE,
        cid,
        region.slba,
        nlb0,
        region.len,
        true,
    );
    sqe.nsid.set(1);
    io.send_capsule(&sqe, &[]);
}

/// Sliding-window write pipeline over one connection, then read-back verify.
fn drive_connection(
    addr: std::net::SocketAddr,
    qid: u16,
    cntlid: u16,
    band_base: u64,
    hdgst: bool,
    ddgst: bool,
) {
    let mut io = Client::handshake(addr, hdgst, ddgst);
    io.connect(qid, 64, cntlid, 1);
    // A hang (buffer-leak deadlock) must fail the test, not block forever.
    io.stream()
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();

    let generation = (qid as u8).wrapping_add(1);

    // Precompute disjoint regions.
    let mut regions: Vec<Region> = Vec::with_capacity(TOTAL);
    let mut slba = band_base;
    for i in 0..TOTAL {
        let len = SIZES[i % SIZES.len()];
        regions.push(Region {
            slba,
            len,
            generation,
        });
        slba += u64::from(len) / BLOCK;
    }

    // cid → region index, for in-flight writes (awaiting R2T then response).
    let mut inflight: HashMap<u16, usize> = HashMap::new();
    let mut next_idx = 0usize;
    let mut completed = 0usize;
    let mut next_cid: u16 = 1;
    let mut alloc_cid = || {
        let c = next_cid;
        next_cid = next_cid.wrapping_add(1);
        if next_cid == 0 {
            next_cid = 1;
        }
        c
    };

    // Prime the window.
    for _ in 0..WINDOW.min(TOTAL) {
        let cid = alloc_cid();
        let idx = next_idx;
        next_idx += 1;
        inflight.insert(cid, idx);
        submit_write(&mut io, cid, &regions[idx]);
    }

    // Drain R2Ts (send the H2C payload) and responses (free a window slot and
    // submit the next write) until every write has completed.
    while completed < TOTAL {
        let (decoded, _) = io.recv_pdu();
        match decoded.kind {
            PduKind::R2T {
                cid, ttag, offset, ..
            } => {
                assert_eq!(offset, 0);
                let idx = *inflight.get(&cid).expect("R2T for unknown cid");
                let region = regions[idx].clone();
                let mut data = vec![0u8; region.len as usize];
                fill_pattern(&mut data, region.slba, region.generation);
                io.send_h2c_data_one_write(cid, ttag, 0, &data, true);
            }
            PduKind::CapsuleResp(cqe) => {
                assert_eq!(cqe.status.get() >> 1, status::SUCCESS, "write failed");
                let cid = cqe.cid.get();
                inflight.remove(&cid).expect("response for unknown cid");
                completed += 1;
                if next_idx < TOTAL {
                    let cid = alloc_cid();
                    let idx = next_idx;
                    next_idx += 1;
                    inflight.insert(cid, idx);
                    submit_write(&mut io, cid, &regions[idx]);
                }
            }
            other => panic!("unexpected PDU during writes: {other:?}"),
        }
    }

    // Read everything back and verify byte-for-byte.
    let mut cid: u16 = 1;
    for region in &regions {
        let nblocks = u64::from(region.len) / BLOCK;
        let nlb0 = u16::try_from(nblocks - 1).unwrap();
        let mut sqe = rw_sqe(
            spec::io_opcode::READ,
            cid,
            region.slba,
            nlb0,
            region.len,
            true,
        );
        sqe.nsid.set(1);
        io.send_capsule(&sqe, &[]);
        let (decoded, payload) = io.recv_pdu();
        assert!(
            matches!(decoded.kind, PduKind::C2HData { .. }),
            "expected C2HData, got {:?}",
            decoded.kind
        );
        let cqe = io.recv_response();
        assert_eq!(cqe.status.get() >> 1, status::SUCCESS, "read failed");
        let mut expect = vec![0u8; region.len as usize];
        fill_pattern(&mut expect, region.slba, region.generation);
        assert_eq!(
            payload, expect,
            "qid={qid} verify failed at slba={} len={}",
            region.slba, region.len
        );
        cid = cid.wrapping_add(1).max(1);
    }
}

fn run(recv_buf_bytes: usize, write_delay_us: u64, hdgst: bool, ddgst: bool) {
    let mut config = ioutgt::TargetConfig::single_memory(NQN, 1024);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    config.recv_buf_bytes = recv_buf_bytes;
    config.mem_write_delay_us = write_delay_us;
    let addr = ioutgt::spawn_target(config).expect("target start");

    let mut admin = Client::handshake(addr, hdgst, ddgst);
    let cntlid = admin.connect(0, 32, 0xFFFF, 1);

    drive_connection(addr, 1, cntlid, 0, hdgst, ddgst);
}

/// Saturated retain pipeline, 256 KiB ring (two 128 KiB sub-buffers), slow
/// writes — the headline reproducer.
#[test]
fn slow_backend_recv_ring_retain() {
    run(256 * 1024, 300, false, false);
}

/// Same with header+data digests: the DDGST is folded over retained ring
/// memory window by window, never over bytes the kernel has not delivered.
#[test]
fn slow_backend_recv_ring_retain_digests() {
    run(256 * 1024, 300, true, true);
}

/// Smaller ring (two 32 KiB sub-buffers): most transfers exceed a sub-buffer,
/// so retains fail the fit check and fall to the copy path while the few that
/// fit stay borrowed — exercises the retain/copy boundary under back-pressure.
#[test]
fn slow_backend_recv_ring_small_buffers() {
    run(64 * 1024, 200, false, false);
}

/// Larger 2 MiB-sub-buffer ring matching the box's `--recv-buf-mb 4`: a 64 KiB
/// or 128 KiB payload is retained far inside one sub-buffer and spans several
/// recv chunks before completing.
#[test]
fn slow_backend_recv_ring_box_geometry() {
    run(4 * 1024 * 1024, 250, false, false);
}
