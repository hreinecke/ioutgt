#![allow(clippy::cast_possible_truncation)] // test sizes bounded by constants

//! Mixed ZC-then-fallback within a single batch: with RLIMIT_MEMLOCK
//! capped *above* one op's worth of pages but *below* a full-depth
//! batch, the first SENDMSG_ZC of a batch pins up to the budget and
//! short-succeeds (a real, skb-lifetime notification), and the
//! re-issue of the remainder fails its first pin with ENOMEM and
//! falls back to the copying path (an immediately-resolving
//! notification). Both kinds then coexist in the batch's
//! `pending_notifs` — the subtlest accounting path in the send loop:
//! reaping, tag release, and teardown must treat them uniformly.
//!
//! Separate file from `io_send_zc_memlock.rs` deliberately: the
//! rlimit is process-wide, and that test caps below a single slot
//! buffer (all-fallback), while this one needs partial-pin headroom.

mod common;

use std::time::Duration;

use common::{Client, NQN, rw_sqe};
use ioutgt_nvme::pdu::PduKind;
use ioutgt_nvme::{spec, status};

const BLOCK: u64 = 512;
const BIG: u32 = 131_072;
const DEPTH: u16 = 8;

/// Two slot buffers' worth: more than one ZC op's first pins, far
/// less than a full-depth burst batch (DEPTH × 128K = 1 MiB).
fn cap_memlock() {
    let cap = libc::rlimit {
        rlim_cur: 256 * 1024,
        rlim_max: 256 * 1024,
    };
    // SAFETY: plain syscall on a local struct; lowering our own limit.
    let rc = unsafe { libc::setrlimit(libc::RLIMIT_MEMLOCK, &cap) };
    assert_eq!(rc, 0, "setrlimit(RLIMIT_MEMLOCK)");
}

#[test]
fn zc_mixed_batch_real_and_fallback_notifs() {
    let mut config = ioutgt::TargetConfig::single_memory(NQN, 64);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    config.send_zc = true;
    let addr = ioutgt::spawn_target(config).expect("target start");

    // Cap only after the rings exist: pinned-page accounting happens
    // per ZC send against the limit current at send time.
    cap_memlock();

    let mut admin = Client::handshake(addr, false, false);
    let cntlid = admin.connect(0, 32, 0xFFFF, 1);
    let mut io = Client::handshake(addr, false, false);
    io.connect(1, DEPTH, cntlid, 1);
    io.stream()
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    // Seed data.
    let mut data = vec![0u8; BIG as usize];
    for (i, b) in data.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(11).wrapping_add(3);
    }
    let nlb0 = u16::try_from(u64::from(BIG) / BLOCK - 1).unwrap();
    let mut sqe = rw_sqe(spec::io_opcode::WRITE, 1, 0, nlb0, BIG, true);
    sqe.nsid.set(1);
    io.send_capsule(&sqe, &[]);
    let (decoded, _) = io.recv_pdu();
    let PduKind::R2T { ttag, .. } = decoded.kind else {
        panic!("expected R2T, got {:?}", decoded.kind);
    };
    io.send_h2c_data(1, ttag, 0, &data, true);
    let cqe = io.recv_response();
    assert_eq!(cqe.status.get() >> 1, status::SUCCESS);

    // Full-depth bursts force ~1 MiB batches against the 256 KiB pin
    // budget: ZC short-success then ENOMEM fallback within the same
    // batch. The idle gap + probe per round additionally checks that
    // reaping a mixed batch releases every tag with no further work.
    for round in 0..20 {
        for cid in 0..DEPTH {
            let mut sqe = rw_sqe(spec::io_opcode::READ, 100 + cid, 0, nlb0, BIG, true);
            sqe.nsid.set(1);
            io.send_capsule(&sqe, &[]);
        }
        let mut responses = 0;
        while responses < usize::from(DEPTH) {
            let (decoded, payload) = io.recv_pdu();
            match decoded.kind {
                PduKind::C2HData { .. } => {
                    assert_eq!(payload, data, "corrupted read in round {round}");
                }
                PduKind::CapsuleResp(cqe) => {
                    assert_eq!(cqe.status.get() >> 1, status::SUCCESS);
                    responses += 1;
                }
                other => panic!("unexpected PDU {other:?} in round {round}"),
            }
        }

        // Idle: the mixed batch's notifs must be reaped without new
        // send work before the probe can claim a tag.
        std::thread::sleep(Duration::from_millis(10));
        let mut sqe = rw_sqe(spec::io_opcode::READ, 400, 0, nlb0, BIG, true);
        sqe.nsid.set(1);
        io.send_capsule(&sqe, &[]);
        let (decoded, _) = io.recv_pdu();
        assert!(matches!(decoded.kind, PduKind::C2HData { .. }));
        let cqe = io.recv_response();
        assert_eq!(cqe.status.get() >> 1, status::SUCCESS);
    }
}
