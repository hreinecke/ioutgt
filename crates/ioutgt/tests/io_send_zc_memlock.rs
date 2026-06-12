#![allow(clippy::cast_possible_truncation)] // test sizes bounded by constants

//! Regression test for the SENDMSG_ZC pinned-page budget: zero-copy
//! sends charge pinned pages against the per-user RLIMIT_MEMLOCK
//! (io_uring returns ENOMEM past it), and a full-depth 128K batch can
//! exceed the budget on its own. The send path must fall back to the
//! copying SENDMSG for the rest of the batch instead of killing the
//! send task — which left the connection half-alive until the host's
//! 30 s IO timeout (the field symptom: ~27 s of zero IOPS, then a
//! controller reset).
//!
//! This file holds exactly one test: the rlimit is process-wide, so
//! it must not share a binary with tests that need real ZC budget.

mod common;

use std::time::Duration;

use common::{Client, NQN, rw_sqe};
use ioutgt_nvme::pdu::PduKind;
use ioutgt_nvme::{spec, status};

const BLOCK: u64 = 512;
const BIG: u32 = 131_072;
const DEPTH: u16 = 8;

/// Cap RLIMIT_MEMLOCK below one slot buffer (process-wide), so every
/// ZC pin attempt fails with ENOMEM regardless of ambient per-user
/// pinned memory.
fn cap_memlock() {
    let cap = libc::rlimit {
        rlim_cur: 64 * 1024,
        rlim_max: 64 * 1024,
    };
    // SAFETY: plain syscall on a local struct; lowering our own limit.
    let rc = unsafe { libc::setrlimit(libc::RLIMIT_MEMLOCK, &cap) };
    assert_eq!(rc, 0, "setrlimit(RLIMIT_MEMLOCK)");
}

#[test]
fn zc_survives_memlock_exhaustion() {
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
    // Without the fallback the send task dies on the first ZC pin and
    // the reads below never complete; fail fast instead of hanging.
    io.stream()
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    // Seed data.
    let mut data = vec![0u8; BIG as usize];
    for (i, b) in data.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(7);
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

    // Full-depth 128K read bursts: every response batch wants the ZC
    // path (payload >= SEND_ZC_MIN) and every pin attempt ENOMEMs.
    for round in 0..10 {
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
    }
}
