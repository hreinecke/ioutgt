#![allow(clippy::cast_possible_truncation)] // test sizes bounded by constants

//! --send-zc end to end: data integrity over loopback (the kernel
//! copies, but the whole notification-gated lifecycle runs), and the
//! idle-after-burst liveness gate — notification reaping must never
//! depend on new send work arriving (spec: anti-deadlock invariant).

mod common;

use std::time::Duration;

use common::{Client, NQN, rw_sqe};
use ioutgt_nvme::pdu::PduKind;
use ioutgt_nvme::{spec, status};

const BLOCK: u64 = 512;
/// 128 KiB: well past SEND_ZC_MIN (16 KiB), forcing the ZC path.
const BIG: u32 = 131_072;

fn spawn_zc_target() -> std::net::SocketAddr {
    let mut config = ioutgt::TargetConfig::single_memory(NQN, 64);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    config.send_zc = true;
    ioutgt::spawn_target(config).expect("target start")
}

fn fill_pattern(buf: &mut [u8], seed: u8) {
    for (i, b) in buf.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(31).wrapping_add(seed);
    }
}

fn write_big(io: &mut Client, cid: u16, slba: u64, seed: u8) {
    let mut data = vec![0u8; BIG as usize];
    fill_pattern(&mut data, seed);
    let nlb0 = u16::try_from(u64::from(BIG) / BLOCK - 1).unwrap();
    let mut sqe = rw_sqe(spec::io_opcode::WRITE, cid, slba, nlb0, BIG, true);
    sqe.nsid.set(1);
    io.send_capsule(&sqe, &[]);
    let (decoded, _) = io.recv_pdu();
    let PduKind::R2T { ttag, length, .. } = decoded.kind else {
        panic!("expected R2T, got {:?}", decoded.kind);
    };
    assert_eq!(length, BIG);
    io.send_h2c_data(cid, ttag, 0, &data, true);
    let cqe = io.recv_response();
    assert_eq!(cqe.status.get() >> 1, status::SUCCESS);
}

fn read_big_sqe(cid: u16, slba: u64) -> spec::Sqe {
    let nlb0 = u16::try_from(u64::from(BIG) / BLOCK - 1).unwrap();
    let mut sqe = rw_sqe(spec::io_opcode::READ, cid, slba, nlb0, BIG, true);
    sqe.nsid.set(1);
    sqe
}

/// 128K reads ride the ZC path; their C2HData payloads must verify
/// against what was written.
#[test]
fn zc_read_write_verify() {
    let addr = spawn_zc_target();
    let mut admin = Client::handshake(addr, false, false);
    let cntlid = admin.connect(0, 32, 0xFFFF, 1);
    let mut io = Client::handshake(addr, false, false);
    io.connect(1, 32, cntlid, 1);
    io.stream()
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();

    for round in 0u8..8 {
        let slba = u64::from(round) * (u64::from(BIG) / BLOCK);
        write_big(&mut io, u16::from(round) + 10, slba, round);

        io.send_capsule(&read_big_sqe(u16::from(round) + 100, slba), &[]);
        let (decoded, payload) = io.recv_pdu();
        assert!(
            matches!(decoded.kind, PduKind::C2HData { .. }),
            "expected C2HData, got {:?}",
            decoded.kind
        );
        let cqe = io.recv_response();
        assert_eq!(cqe.status.get() >> 1, status::SUCCESS);
        let mut expect = vec![0u8; BIG as usize];
        fill_pattern(&mut expect, round);
        assert_eq!(payload, expect, "ZC read corrupted in round {round}");
    }
}

/// As above with both digests on: the DDGST trailer rides the ZC
/// batch from the arena.
#[test]
fn zc_read_write_verify_digests() {
    let addr = spawn_zc_target();
    let mut admin = Client::handshake(addr, true, true);
    let cntlid = admin.connect(0, 32, 0xFFFF, 1);
    let mut io = Client::handshake(addr, true, true);
    io.connect(1, 32, cntlid, 1);
    io.stream()
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();

    for round in 0u8..4 {
        let slba = u64::from(round) * (u64::from(BIG) / BLOCK);
        write_big(
            &mut io,
            u16::from(round) + 10,
            slba,
            round.wrapping_add(0x40),
        );

        io.send_capsule(&read_big_sqe(u16::from(round) + 100, slba), &[]);
        let (decoded, payload) = io.recv_pdu();
        assert!(matches!(decoded.kind, PduKind::C2HData { .. }));
        let cqe = io.recv_response();
        assert_eq!(cqe.status.get() >> 1, status::SUCCESS);
        let mut expect = vec![0u8; BIG as usize];
        fill_pattern(&mut expect, round.wrapping_add(0x40));
        assert_eq!(payload, expect, "ZC+DDGST read corrupted in round {round}");
    }
}

/// Burst the full queue depth in big reads, drain the responses, go
/// idle, then issue one more command. If notification reaping
/// depended on new send work, the burst would leave every tag
/// notif-gated and this final command would hang on await_tag.
#[test]
fn zc_idle_after_burst_keeps_tags_live() {
    const DEPTH: u16 = 8;
    let addr = spawn_zc_target();
    let mut admin = Client::handshake(addr, false, false);
    let cntlid = admin.connect(0, 32, 0xFFFF, 1);
    let mut io = Client::handshake(addr, false, false);
    io.connect(1, DEPTH, cntlid, 1);
    io.stream()
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();

    write_big(&mut io, 9, 0, 0xee);

    for round in 0..50 {
        // Full-depth burst of ZC-path reads of the same region.
        for cid in 0..DEPTH {
            io.send_capsule(&read_big_sqe(200 + cid, 0), &[]);
        }
        // Drain: one C2HData + one CapsuleResp per command, in
        // whatever completion order the target chose.
        let mut data_pdus = 0;
        let mut responses = 0;
        while responses < usize::from(DEPTH) {
            let (decoded, payload) = io.recv_pdu();
            match decoded.kind {
                PduKind::C2HData { .. } => {
                    assert_eq!(payload.len(), BIG as usize);
                    data_pdus += 1;
                }
                PduKind::CapsuleResp(cqe) => {
                    assert_eq!(cqe.status.get() >> 1, status::SUCCESS);
                    responses += 1;
                }
                other => panic!("unexpected PDU {other:?} in round {round}"),
            }
        }
        assert_eq!(data_pdus, usize::from(DEPTH));

        // Idle gap: notifs must be reaped with no send work pending.
        std::thread::sleep(Duration::from_millis(20));

        // Liveness probe: must complete within the read timeout.
        io.send_capsule(&read_big_sqe(400, 0), &[]);
        let (decoded, _) = io.recv_pdu();
        assert!(matches!(decoded.kind, PduKind::C2HData { .. }));
        let cqe = io.recv_response();
        assert_eq!(cqe.status.get() >> 1, status::SUCCESS);
    }
}
