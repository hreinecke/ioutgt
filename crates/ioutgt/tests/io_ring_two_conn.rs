#![allow(clippy::cast_possible_truncation)] // test indices bounded by constants

//! Two connections sharing ONE reactor thread, each with its OWN recv ring.
//!
//! Connections routed to the same io thread (qid `n` → io thread `(n-1) % N`)
//! share its single io_uring. With `io_threads = 1`, every IO queue lands on
//! thread 0. Two independent controllers (each its own admin + one qid-1 IO
//! connection) therefore run two multishot recvs on the same reactor, each
//! drawing from its own per-connection 2-sub-buffer ring (distinct `bgid`s
//! from the thread-local pool).
//!
//! REGRESSION GUARD for the shared-ring offset-desync corruption. Recv rings
//! used to be created once per reactor thread and SHARED by every connection on
//! it. A recv CQE carries only `(bid, len)`, not the buffer offset, and
//! `StreamReader`'s ring mode reads and advances `BufRing::recv_off(bid)` inside
//! each connection's own task; two connections' tasks do not run in
//! CQE-completion order, so the shared per-buffer offset desynced and one
//! connection read bytes the kernel delivered to the other — framing/data
//! corruption (observed as "response for unknown cid", a verify mismatch, or a
//! broken pipe). The fix gives each connection its OWN ring (one consumer per
//! ring ⇒ the offset cannot desync). This test pins two connections to one
//! io-thread with the ring on and verifies both complete with byte-correct
//! data, even under a slow backend that keeps both rings saturated and parked
//! on ENOBUFS together.

mod common;

use std::collections::HashMap;
use std::time::Duration;

use common::{Client, NQN, rw_sqe};
use ioutgt_nvme::pdu::PduKind;
use ioutgt_nvme::{spec, status};

const BLOCK: u64 = 512;
/// Whole-transfer sizes; the larger ones (> 16 KiB inline limit) take the R2T
/// path and, when they fit a sub-buffer, are retained zero-copy as one PDU.
const SIZES: &[u32] = &[65_536, 131_072, 98_304, 32_768, 131_072, 65_536];

/// Whole-transfer writes per connection.
const TOTAL: usize = 400;
/// Outstanding writes per connection, so the shared ring stays saturated and
/// both connections park on ENOBUFS together.
const WINDOW: usize = 32;

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
fn drive_connection(addr: std::net::SocketAddr, cntlid: u16, generation: u8, band_base: u64) {
    let mut io = Client::handshake(addr, false, false);
    io.connect(1, 64, cntlid, 1);
    // A hang (lost-wakeup deadlock) must fail the test, not block forever.
    io.stream()
        .set_read_timeout(Some(Duration::from_secs(20)))
        .unwrap();

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

    for _ in 0..WINDOW.min(TOTAL) {
        let cid = alloc_cid();
        let idx = next_idx;
        next_idx += 1;
        inflight.insert(cid, idx);
        submit_write(&mut io, cid, &regions[idx]);
    }

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
            "gen={generation} verify failed at slba={} len={}",
            region.slba, region.len
        );
        cid = cid.wrapping_add(1).max(1);
    }
}

/// Two IO connections on a single io thread, each with its own per-connection
/// recv ring, under a slow backend. Both should finish and read back
/// byte-for-byte. Regression guard for the shared-ring offset-desync
/// corruption documented at the top of this file.
#[test]
fn two_connections_per_connection_ring_no_corruption() {
    let mut config = ioutgt::TargetConfig::single_memory(NQN, 2048);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    config.recv_buf_bytes = 256 * 1024; // two 128 KiB sub-buffers
    config.mem_write_delay_us = 300;
    let addr = ioutgt::spawn_target(config).expect("target start");

    // Two independent controllers, each its own admin connection (kept alive
    // for controller liveness) plus one qid-1 IO connection. With io_threads=1
    // both IO connections route to io thread 0 ((1-1) % 1 == 0), sharing the
    // reactor but each with its OWN recv ring. Drive them concurrently so both
    // park on ENOBUFS.
    let mut admin_a = Client::handshake(addr, false, false);
    let cntlid_a = admin_a.connect(0, 32, 0xFFFF, 1);
    let mut admin_b = Client::handshake(addr, false, false);
    let cntlid_b = admin_b.connect(0, 32, 0xFFFF, 1);

    // Disjoint bands within the shared 2 GiB namespace (both controllers see
    // the same backing store): A near 0, B at 1 M blocks.
    let h1 = std::thread::spawn(move || drive_connection(addr, cntlid_a, 1, 0));
    let h2 = std::thread::spawn(move || drive_connection(addr, cntlid_b, 2, 1_000_000));
    let r1 = h1.join();
    let r2 = h2.join();
    drop(admin_a);
    drop(admin_b);
    r1.expect("controller A connection");
    r2.expect("controller B connection");
}
