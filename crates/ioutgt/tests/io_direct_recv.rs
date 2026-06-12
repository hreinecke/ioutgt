//! Direct-to-slot recv path: large H2C payload tails are received
//! straight into the slot buffer (no recv-buffer→slot copy). These
//! tests pin wire segmentation with the fragmentation-controlled
//! client sender so the gate variable — `remaining` at buffer-drain
//! time — is deterministic, and verify byte-exact behavior across the
//! direct path, the copy path, threshold edges, DDGST on/off/mismatch,
//! and mid-tail disconnects.

mod common;

use std::io::Write;
use std::time::Duration;

use common::{Client, NQN, pattern, rw_sqe};
use ioutgt_nvme::pdu::{self, PduKind};
use ioutgt_nvme::{spec, status};

/// Mirror of connection.rs's `H2C_DIRECT_MIN` (16 KiB).
const THRESHOLD: usize = 16 * 1024;
/// Long enough that the target's recv loop drains its buffer (and arms
/// the next recv) between our writes on loopback.
const DELAY: Duration = Duration::from_millis(30);

fn start_target() -> std::net::SocketAddr {
    let mut config = ioutgt::TargetConfig::single_memory(NQN, 16);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    ioutgt::spawn_target(config).expect("target start")
}

/// Admin + IO queue pair; the admin connection must outlive the IO one
/// (the controller dies with its admin queue).
fn connect_pair(addr: std::net::SocketAddr, hdgst: bool, ddgst: bool) -> (Client, Client) {
    let mut admin = Client::handshake(addr, hdgst, ddgst);
    let cntlid = admin.connect(0, 32, 0xFFFF, 1);
    let mut io = Client::handshake(addr, hdgst, ddgst);
    io.connect(1, 32, cntlid, 1);
    (admin, io)
}

/// Send a write capsule (host-resident data) and return the R2T's ttag.
fn solicit_write(io: &mut Client, cid: u16, slba: u64, len: u32) -> u16 {
    let nlb0 = u16::try_from(len / 512 - 1).unwrap();
    io.send_capsule(
        &rw_sqe(spec::io_opcode::WRITE, cid, slba, nlb0, len, true),
        &[],
    );
    let (decoded, _) = io.recv_pdu();
    let PduKind::R2T {
        cid: rcid,
        ttag,
        offset,
        length,
    } = decoded.kind
    else {
        panic!("expected R2T, got {:?}", decoded.kind);
    };
    assert_eq!((rcid, offset, length), (cid, 0, len), "R2T solicits all");
    ttag
}

/// Read `len` bytes back and assert success.
fn read_back(io: &mut Client, cid: u16, slba: u64, len: u32) -> Vec<u8> {
    let nlb0 = u16::try_from(len / 512 - 1).unwrap();
    io.send_capsule(
        &rw_sqe(spec::io_opcode::READ, cid, slba, nlb0, len, true),
        &[],
    );
    let (decoded, payload) = io.recv_pdu();
    assert!(
        matches!(decoded.kind, PduKind::C2HData { .. }),
        "expected C2HData, got {:?}",
        decoded.kind
    );
    let cqe = io.recv_response();
    assert_eq!(cqe.status.get() >> 1, status::SUCCESS, "read cid {cid}");
    payload
}

/// One large R2T write delivered at a pinned fragmentation (header
/// alone, then `chunks`), then a byte-exact readback.
fn write_fragmented_and_verify(
    io: &mut Client,
    cid: u16,
    slba: u64,
    data: &[u8],
    chunks: &[usize],
) {
    let len = u32::try_from(data.len()).unwrap();
    let ttag = solicit_write(io, cid, slba, len);
    io.send_h2c_data_fragmented(cid, ttag, 0, data, true, chunks, DELAY);
    let cqe = io.recv_response();
    assert_eq!(cqe.status.get() >> 1, status::SUCCESS, "write cid {cid}");
    assert_eq!(cqe.cid.get(), cid);
    assert_eq!(
        read_back(io, cid + 1, slba, len),
        data,
        "readback cid {cid}"
    );
}

/// Several fragmentations of a 128 KiB (8× threshold) R2T write: the
/// header arrives alone, so the whole payload is a direct tail.
fn run_large_tail_fragmentations(hdgst: bool, ddgst: bool) {
    let addr = start_target();
    let (_admin, mut io) = connect_pair(addr, hdgst, ddgst);
    let len = 128 * 1024;
    let cases: &[&[usize]] = &[
        &[len],            // whole tail in one segment
        &[4096],           // small first chunk, remainder after
        &[8192; 4],        // several short chunks, remainder after
        &[1, 4095, 12288], // byte-torn start of the tail
    ];
    for (i, chunks) in cases.iter().enumerate() {
        let seed = u8::try_from(0x20 + i).unwrap();
        let data = pattern(len, seed);
        let cid = u16::try_from(2 + 2 * i).unwrap();
        let slba = u64::try_from(i * 256).unwrap();
        write_fragmented_and_verify(&mut io, cid, slba, &data, chunks);
    }
}

#[test]
fn large_tail_fragmentations_no_digest() {
    run_large_tail_fragmentations(false, false);
}

#[test]
fn large_tail_fragmentations_full_digest() {
    run_large_tail_fragmentations(true, true);
}

/// Header + payload in ONE 128 KiB write: the target's 64 KiB buffer
/// captures the header plus a payload prefix (fused copy+CRC) and the
/// rest arrives as a direct tail — exercises the prefix-CRC + warm
/// tail-CRC combination.
fn run_prefix_plus_tail(hdgst: bool, ddgst: bool) {
    let addr = start_target();
    let (_admin, mut io) = connect_pair(addr, hdgst, ddgst);
    let len: u32 = 128 * 1024;
    let data = pattern(len as usize, 0x42);
    let ttag = solicit_write(&mut io, 2, 0, len);
    io.send_h2c_data_one_write(2, ttag, 0, &data, true);
    let cqe = io.recv_response();
    assert_eq!(cqe.status.get() >> 1, status::SUCCESS, "prefix+tail write");
    assert_eq!(read_back(&mut io, 3, 0, len), data, "prefix+tail readback");
}

#[test]
fn prefix_plus_tail_no_digest() {
    run_prefix_plus_tail(false, false);
}

#[test]
fn prefix_plus_tail_full_digest() {
    run_prefix_plus_tail(true, true);
}

/// Threshold edges with pinned segmentation: the transfer is split
/// into two H2CData PDUs; the first PDU's header is sent alone and
/// flushed, so when the buffer drains `remaining == its payload size`
/// exactly — threshold−1 (copy path), threshold (direct path),
/// threshold+1 (direct path). The second PDU completes the transfer.
fn run_threshold_edges(hdgst: bool, ddgst: bool) {
    let addr = start_target();
    let (_admin, mut io) = connect_pair(addr, hdgst, ddgst);
    let len: u32 = 32 * 1024;
    for (i, edge) in [THRESHOLD - 1, THRESHOLD, THRESHOLD + 1]
        .into_iter()
        .enumerate()
    {
        let seed = u8::try_from(0x60 + i).unwrap();
        let data = pattern(len as usize, seed);
        let cid = u16::try_from(2 + 2 * i).unwrap();
        let slba = u64::try_from(i * 64).unwrap();
        let ttag = solicit_write(&mut io, cid, slba, len);
        let split = u32::try_from(edge).unwrap();
        io.send_h2c_data_fragmented(cid, ttag, 0, &data[..edge], false, &[edge], DELAY);
        io.send_h2c_data_fragmented(cid, ttag, split, &data[edge..], true, &[data.len()], DELAY);
        let cqe = io.recv_response();
        assert_eq!(cqe.status.get() >> 1, status::SUCCESS, "edge {edge} write");
        assert_eq!(read_back(&mut io, cid + 1, slba, len), data, "edge {edge}");
    }
}

#[test]
fn threshold_edges_no_digest() {
    run_threshold_edges(false, false);
}

#[test]
fn threshold_edges_full_digest() {
    run_threshold_edges(true, true);
}

/// Tail of 0: the whole (≥ threshold) payload is written in ONE buffer
/// with its header, so it lands in the connection buffer as prefix and
/// the direct path must not fire — the plain copy path handles it.
#[test]
fn tail_zero_whole_payload_with_header() {
    let addr = start_target();
    let (_admin, mut io) = connect_pair(addr, false, false);
    let len: u32 = 32 * 1024;
    let data = pattern(len as usize, 0x33);
    let ttag = solicit_write(&mut io, 2, 0, len);
    io.send_h2c_data_one_write(2, ttag, 0, &data, true);
    let cqe = io.recv_response();
    assert_eq!(cqe.status.get() >> 1, status::SUCCESS, "one-write H2C");
    assert_eq!(read_back(&mut io, 3, 0, len), data, "one-write readback");
}

/// DDGST mismatch confined to the direct tail: payload bytes intact,
/// digest corrupted. The command fails with DATA_XFER_ERROR|DNR and
/// the connection stays up (as nvmet) — both for a single-segment tail
/// and for a tail delivered as multiple short chunks (the kernel
/// assembles them under MSG_WAITALL before the warm CRC pass).
#[test]
fn ddgst_mismatch_on_direct_tail() {
    let addr = start_target();
    let (_admin, mut io) = connect_pair(addr, true, true);
    let len: u32 = 64 * 1024;
    let cases: &[&[usize]] = &[
        &[len as usize], // tail in one segment
        &[8192; 4],      // tail as several short chunks + remainder
    ];
    for (i, chunks) in cases.iter().enumerate() {
        let seed = u8::try_from(0x70 + i).unwrap();
        let data = pattern(len as usize, seed);
        let cid = u16::try_from(2 + 2 * i).unwrap();
        let ttag = solicit_write(&mut io, cid, 0, len);
        io.send_h2c_data_fragmented_bad_ddgst(cid, ttag, 0, &data, true, chunks, DELAY);
        let cqe = io.recv_response();
        assert_eq!(cqe.cid.get(), cid);
        assert_eq!(
            cqe.status.get() >> 1,
            status::DATA_XFER_ERROR | status::DNR,
            "DDGST mismatch fails the command"
        );
        // Same connection keeps serving: clean in-capsule round-trip.
        let probe = pattern(4096, seed ^ 0xFF);
        let pcid = cid + 1;
        io.send_capsule(
            &rw_sqe(spec::io_opcode::WRITE, pcid, 512, 7, 4096, false),
            &probe,
        );
        assert_eq!(io.recv_response().status.get() >> 1, status::SUCCESS);
        assert_eq!(read_back(&mut io, pcid + 1, 512, 4096), probe);
    }
    // And a clean large direct write still round-trips afterwards.
    let data = pattern(len as usize, 0x7F);
    write_fragmented_and_verify(&mut io, 40, 1024, &data, &[len as usize]);
}

/// Health probe: a fresh admin+IO pair completes a large direct-tail
/// write/read round-trip.
fn assert_target_alive(addr: std::net::SocketAddr) {
    let (_admin, mut io) = connect_pair(addr, false, false);
    let data = pattern(128 * 1024, 0x5A);
    write_fragmented_and_verify(&mut io, 2, 0, &data, &[128 * 1024]);
}

/// Client vanishes while a direct tail is outstanding: (a) right after
/// the H2CData header (zero tail bytes delivered), (b) after a partial
/// tail (the WAITALL recv returns short, the re-armed recv sees EOF).
/// The target must tear the queue down cleanly and keep serving.
#[test]
fn mid_tail_disconnect_recovers() {
    let addr = start_target();

    for partial in [0usize, 32 * 1024] {
        let mut admin = Client::handshake(addr, false, false);
        let cntlid = admin.connect(0, 32, 0xFFFF, 1);
        {
            let mut io = Client::handshake(addr, false, false);
            io.connect(1, 32, cntlid, 1);
            let ttag = solicit_write(&mut io, 6, 0, 131_072);
            // H2CData header claiming 128 KiB, then only `partial`
            // payload bytes, then drop the socket mid-tail.
            let mut hdr = [0u8; 32];
            let n = pdu::encode_h2c_data(&mut hdr, 6, ttag, 0, 131_072, true, false, false);
            io.stream().write_all(&hdr[..n]).unwrap();
            std::thread::sleep(DELAY);
            if partial > 0 {
                io.stream().write_all(&pattern(partial, 0x11)).unwrap();
                std::thread::sleep(DELAY);
            }
            // io drops here: direct tail outstanding on the slot.
        }
        std::thread::sleep(Duration::from_millis(200));
        assert_target_alive(addr);
    }
}
