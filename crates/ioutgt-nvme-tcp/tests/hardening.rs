//! M8 hardening: malformed input must terminate cleanly (C2HTermReq or
//! close) and never wedge the target — every abuse case ends by proving
//! a fresh connection still completes a full IO round-trip.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use common::{Client, NQN, connect_sqe, pattern, rw_sqe};
use ioutgt_nvme::pdu::{self, PduDecoder, PduKind};
use ioutgt_nvme::{digest, spec, status};
use zerocopy::IntoBytes;

fn start_target() -> std::net::SocketAddr {
    let mut config = ioutgt_nvme_tcp::TargetConfig::single_memory(NQN, 16);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    ioutgt_nvme_tcp::spawn_target(config).expect("target start")
}

/// Full health probe: a fresh admin+IO connection does a 4K write/read.
fn assert_target_alive(addr: std::net::SocketAddr) {
    let mut admin = Client::handshake(addr, false, false);
    let cntlid = admin.connect(0, 32, 0xFFFF, 1);
    let mut io = Client::handshake(addr, false, false);
    io.connect(1, 32, cntlid, 1);
    let data = pattern(4096, 0x5A);
    io.send_capsule(&rw_sqe(spec::io_opcode::WRITE, 2, 0, 7, 4096, false), &data);
    assert_eq!(io.recv_response().status.get() >> 1, status::SUCCESS);
    io.send_capsule(&rw_sqe(spec::io_opcode::READ, 3, 0, 7, 4096, true), &[]);
    let (_, payload) = io.recv_pdu();
    assert_eq!(payload, data);
    let _ = io.recv_response();
}

/// Expect a C2HTermReq with `fes` (or a bare close, which some paths
/// use), then EOF.
fn expect_term_then_close(stream: &mut TcpStream, expect_fes: Option<u16>) {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut buf = [0u8; 256];
    let n = stream.read(&mut buf).unwrap_or(0);
    if n == 0 {
        assert!(expect_fes.is_none(), "expected term PDU, got bare close");
        return;
    }
    let mut decoder = PduDecoder::new(false);
    decoder.feed(&buf[..n]).expect("term parses");
    assert!(decoder.is_complete(), "partial term PDU");
    let decoded = decoder.take().unwrap();
    let PduKind::H2CTerm { fes, .. } = decoded.kind else {
        panic!("expected term, got {:?}", decoded.kind);
    };
    if let Some(expect) = expect_fes {
        assert_eq!(fes, expect, "term FES");
    }
    // Then EOF.
    let n = stream.read(&mut buf).unwrap_or(0);
    assert_eq!(n, 0, "connection should close after term");
}

#[test]
fn unknown_pdu_type_terminates() {
    let addr = start_target();
    let mut admin = Client::handshake(addr, false, false);
    admin.connect(0, 32, 0xFFFF, 1);
    // Unknown type 0xEE with a plausible header.
    admin
        .stream()
        .write_all(&[0xEE, 0, 24, 0, 24, 0, 0, 0])
        .unwrap();
    expect_term_then_close(admin.stream(), Some(pdu::fes::INVALID_PDU_HDR));
    assert_target_alive(addr);
}

#[test]
fn oversized_plen_terminates() {
    let addr = start_target();
    let mut admin = Client::handshake(addr, false, false);
    admin.connect(0, 32, 0xFFFF, 1);
    // CapsuleCmd claiming 64 MiB PLEN (over the 32 MiB protocol cap).
    let mut hdr = [0u8; 8];
    hdr[0] = pdu::pdu_type::CAPSULE_CMD;
    hdr[2] = 72;
    hdr[4..8].copy_from_slice(&(64u32 << 20).to_le_bytes());
    admin.stream().write_all(&hdr).unwrap();
    expect_term_then_close(admin.stream(), Some(pdu::fes::INVALID_PDU_HDR));
    assert_target_alive(addr);
}

#[test]
fn bogus_ttag_h2cdata_terminates() {
    let addr = start_target();
    let mut admin = Client::handshake(addr, false, false);
    let cntlid = admin.connect(0, 32, 0xFFFF, 1);
    let mut io = Client::handshake(addr, false, false);
    io.connect(1, 32, cntlid, 1);
    // H2CData for a tag nobody solicited.
    io.send_h2c_data(7, 9999, 0, &[0u8; 64], true);
    expect_term_then_close(io.stream(), None);
    assert_target_alive(addr);
}

#[test]
fn wrong_offset_h2cdata_terminates() {
    let addr = start_target();
    let mut admin = Client::handshake(addr, false, false);
    let cntlid = admin.connect(0, 32, 0xFFFF, 1);
    let mut io = Client::handshake(addr, false, false);
    io.connect(1, 32, cntlid, 1);

    // Solicit a 128K transfer, then send data at a bogus offset.
    io.send_capsule(
        &rw_sqe(spec::io_opcode::WRITE, 4, 0, 255, 131_072, true),
        &[],
    );
    let (decoded, _) = io.recv_pdu();
    let PduKind::R2T { ttag, .. } = decoded.kind else {
        panic!("expected R2T");
    };
    io.send_h2c_data(4, ttag, 4096, &[0u8; 4096], false); // offset must be 0
    expect_term_then_close(io.stream(), Some(pdu::fes::DATA_OUT_OF_RANGE));
    assert_target_alive(addr);
}

#[test]
fn ddgst_mismatch_fails_command_keeps_connection() {
    let addr = start_target();
    let mut admin = Client::handshake(addr, true, true);
    let cntlid = admin.connect(0, 32, 0xFFFF, 1);
    let mut io = Client::handshake(addr, true, true);
    io.connect(1, 32, cntlid, 1);

    // In-capsule write with a corrupted data digest, built by hand.
    let sqe = rw_sqe(spec::io_opcode::WRITE, 5, 0, 7, 4096, false);
    let payload = pattern(4096, 1);
    let mut frame = Vec::new();
    let mut hdr = [0u8; 80];
    let n = pdu::encode_capsule_cmd(&mut hdr, &sqe, true, 4096, true);
    frame.extend_from_slice(&hdr[..n]);
    frame.extend_from_slice(&payload);
    let bad_crc = digest::crc32c(&payload) ^ 0xFFFF_FFFF;
    frame.extend_from_slice(&bad_crc.to_le_bytes());
    io.stream().write_all(&frame).unwrap();

    // The command fails with DATA_XFER_ERROR; the connection stays up,
    // as nvmet does (there is no NVMe/TCP data-digest-error FES).
    let cqe = io.recv_response();
    assert_eq!(cqe.cid.get(), 5);
    assert_eq!(
        cqe.status.get() >> 1,
        status::DATA_XFER_ERROR | status::DNR,
        "data digest error status"
    );
    // The same connection keeps serving: a clean write/read round-trips.
    let data = pattern(4096, 9);
    io.send_capsule(&rw_sqe(spec::io_opcode::WRITE, 6, 0, 7, 4096, false), &data);
    assert_eq!(io.recv_response().status.get() >> 1, status::SUCCESS);
    io.send_capsule(&rw_sqe(spec::io_opcode::READ, 7, 0, 7, 4096, true), &[]);
    let (_, got) = io.recv_pdu();
    assert_eq!(got, data, "connection still healthy after digest error");
    let _ = io.recv_response();
    assert_target_alive(addr);
}

#[test]
fn garbage_after_handshake_terminates() {
    let addr = start_target();
    let mut admin = Client::handshake(addr, false, false);
    admin.connect(0, 32, 0xFFFF, 1);
    // Deterministic pseudo-random garbage.
    let mut state = 0x1234_5678_u32;
    let garbage: Vec<u8> = (0..512)
        .map(|_| {
            state = state.wrapping_mul(1664525).wrapping_add(1013904223);
            (state >> 24) as u8
        })
        .collect();
    let _ = admin.stream().write_all(&garbage);
    expect_term_then_close(admin.stream(), None);
    assert_target_alive(addr);
}

#[test]
fn mid_r2t_disconnect_recovers() {
    let addr = start_target();
    let mut admin = Client::handshake(addr, false, false);
    let cntlid = admin.connect(0, 32, 0xFFFF, 1);
    {
        let mut io = Client::handshake(addr, false, false);
        io.connect(1, 32, cntlid, 1);
        // Solicit 128K, deliver only 64K, drop the socket.
        io.send_capsule(
            &rw_sqe(spec::io_opcode::WRITE, 6, 0, 255, 131_072, true),
            &[],
        );
        let (decoded, _) = io.recv_pdu();
        assert!(matches!(decoded.kind, PduKind::R2T { .. }));
        io.send_h2c_data(6, 0, 0, &pattern(65_536, 3), false);
        // io drops here: connection torn down with the slot mid-receive.
    }
    std::thread::sleep(Duration::from_millis(200));
    assert_target_alive(addr);
}

#[test]
fn second_connect_on_admin_queue_rejected() {
    // A second Connect on an already-bound admin queue must be rejected
    // (not mint and leak another cntlid). The connection stays usable.
    let addr = start_target();
    let mut admin = Client::handshake(addr, false, false);
    let cntlid = admin.connect(0, 32, 0xFFFF, 1);
    assert!(cntlid >= 1);

    // Re-send Connect through the normal command path; expect a
    // non-success status, connection alive.
    let (sqe, data) = connect_sqe(0, 32, 0xFFFF, 0x55);
    admin.send_capsule(&sqe, data.as_bytes());
    let cqe = admin.recv_response();
    assert_eq!(cqe.cid.get(), 0x55);
    assert_ne!(
        cqe.status.get() >> 1,
        status::SUCCESS,
        "duplicate connect must fail"
    );
    // The original controller still works: enable + identify succeed.
    admin.enable_controller(0x56);
    let id = admin.identify(spec::cns::CONTROLLER, 0, 0x57);
    assert_eq!(id.len(), 4096);
}

#[test]
fn cc_enable_before_connect_rejected() {
    // The only way to reach the dispatcher with cntlid 0 and the queue
    // still alive is a structurally-valid Connect that fails logically
    // (unknown subsystem): the connection stays up, no controller bound.
    // A Property Set CC then must be rejected (no enabling a cntlid-0
    // controller).
    let addr = start_target();
    let mut client = Client::handshake(addr, false, false);
    let (sqe, mut data) = connect_sqe(0, 32, 0xFFFF, 1);
    data.subsysnqn = [0u8; 256];
    let bogus = b"nqn.2026-06.io.ioutgt:does-not-exist";
    data.subsysnqn[..bogus.len()].copy_from_slice(bogus);
    client.send_capsule(&sqe, data.as_bytes());
    let cqe = client.recv_response();
    assert_ne!(
        cqe.status.get() >> 1,
        status::SUCCESS,
        "connect to unknown subsys must fail"
    );
    // Controller never bound; CC.EN must be rejected.
    let status_code = client.set_property_cc(0x10, 2);
    assert_ne!(
        status_code,
        status::SUCCESS,
        "CC.EN before connect must fail"
    );
}

#[test]
fn kato_expiry_closes_connection() {
    let addr = start_target();
    let mut admin = Client::handshake(addr, false, false);
    // Connect with KATO = 1s, then go silent: the watchdog polls every
    // kato/2 = 500ms and expires the controller at 2*kato + one tick =
    // 2.5s, so we must be closed in ~3s.
    let cntlid = admin.connect_with_kato(0, 32, 0xFFFF, 1, 1_000);
    assert!(cntlid >= 1);
    let start = Instant::now();
    admin
        .stream()
        .set_read_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    let mut buf = [0u8; 64];
    let n = admin.stream().read(&mut buf).unwrap_or(0);
    assert_eq!(n, 0, "server should close the silent connection");
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(18),
        "keep-alive expiry too slow: {elapsed:?}"
    );
    // Controller must be gone: a fresh admin connect gets a new cntlid
    // and the target is healthy.
    assert_target_alive(addr);
}
