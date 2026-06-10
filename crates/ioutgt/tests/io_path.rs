//! M5 exit test (host-only half): a raw NVMe/TCP client connects admin +
//! IO queues, then drives the data path — in-capsule write, R2T write
//! split across H2CData PDUs, reads with verification, write-zeroes —
//! with and without digests.

mod common;

use common::{Client, NQN, pattern, rw_sqe};
use ioutgt_nvme::pdu::PduKind;
use ioutgt_nvme::{spec, status};

fn run_io_flow(hdgst: bool, ddgst: bool) {
    let mut config = ioutgt::TargetConfig::single_memory(NQN, 16);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    let addr = ioutgt::spawn_target(config).expect("target start");

    // Admin queue: stays open so the controller stays registered.
    let mut admin = Client::handshake(addr, hdgst, ddgst);
    let cntlid = admin.connect(0, 32, 0xFFFF, 1);
    assert!(cntlid >= 1);

    // IO queue.
    let mut io = Client::handshake(addr, hdgst, ddgst);
    io.connect(1, 64, cntlid, 1);

    // --- 4K in-capsule write at LBA 8 ---
    let data4k = pattern(4096, 0x11);
    let sqe = rw_sqe(spec::io_opcode::WRITE, 2, 8, 7, 4096, false);
    io.send_capsule(&sqe, &data4k);
    let cqe = io.recv_response();
    assert_eq!(cqe.status.get() >> 1, status::SUCCESS, "inline write");

    // --- 4K read back ---
    let sqe = rw_sqe(spec::io_opcode::READ, 3, 8, 7, 4096, true);
    io.send_capsule(&sqe, &[]);
    let (decoded, payload) = io.recv_pdu();
    let PduKind::C2HData {
        cid, length, last, ..
    } = decoded.kind
    else {
        panic!("expected C2HData, got {:?}", decoded.kind);
    };
    assert_eq!((cid, length, last), (3, 4096, true));
    assert_eq!(payload, data4k, "4K readback");
    let cqe = io.recv_response();
    assert_eq!(cqe.status.get() >> 1, status::SUCCESS);
    assert_eq!(cqe.cid.get(), 3);

    // --- 128K write via R2T, host splits into two H2CData PDUs ---
    let data128k = pattern(128 * 1024, 0x77);
    let sqe = rw_sqe(spec::io_opcode::WRITE, 4, 64, 255, 131_072, true);
    io.send_capsule(&sqe, &[]);
    let (decoded, _) = io.recv_pdu();
    let PduKind::R2T {
        cid,
        ttag,
        offset,
        length,
    } = decoded.kind
    else {
        panic!("expected R2T, got {:?}", decoded.kind);
    };
    assert_eq!((cid, offset, length), (4, 0, 131_072));
    io.send_h2c_data(4, ttag, 0, &data128k[..65_536], false);
    io.send_h2c_data(4, ttag, 65_536, &data128k[65_536..], true);
    let cqe = io.recv_response();
    assert_eq!(cqe.status.get() >> 1, status::SUCCESS, "R2T write");
    assert_eq!(cqe.cid.get(), 4);

    // --- 128K read back ---
    let sqe = rw_sqe(spec::io_opcode::READ, 5, 64, 255, 131_072, true);
    io.send_capsule(&sqe, &[]);
    let (decoded, payload) = io.recv_pdu();
    let PduKind::C2HData { length, .. } = decoded.kind else {
        panic!("expected C2HData, got {:?}", decoded.kind);
    };
    assert_eq!(length, 131_072);
    assert_eq!(payload, data128k, "128K readback");
    let cqe = io.recv_response();
    assert_eq!(cqe.status.get() >> 1, status::SUCCESS);

    // --- flush, write-zeroes over the 4K block, read back zeroes ---
    let mut sqe = spec::Sqe::zeroed();
    sqe.opcode = spec::io_opcode::FLUSH;
    sqe.flags = spec::CMD_FLAGS_SGL_METABUF;
    sqe.cid.set(6);
    sqe.nsid.set(1);
    io.send_capsule(&sqe, &[]);
    assert_eq!(
        io.recv_response().status.get() >> 1,
        status::SUCCESS,
        "flush"
    );

    let sqe = rw_sqe(spec::io_opcode::WRITE_ZEROES, 7, 8, 7, 0, true);
    io.send_capsule(&sqe, &[]);
    assert_eq!(
        io.recv_response().status.get() >> 1,
        status::SUCCESS,
        "write zeroes"
    );

    let sqe = rw_sqe(spec::io_opcode::READ, 8, 8, 7, 4096, true);
    io.send_capsule(&sqe, &[]);
    let (_, payload) = io.recv_pdu();
    assert!(payload.iter().all(|&b| b == 0), "zeroes after write-zeroes");
    let _ = io.recv_response();

    // --- out-of-range read draws LBA_RANGE|DNR ---
    let sqe = rw_sqe(spec::io_opcode::READ, 9, u64::MAX / 2, 7, 4096, true);
    io.send_capsule(&sqe, &[]);
    let cqe = io.recv_response();
    assert_eq!(
        cqe.status.get() >> 1,
        status::LBA_RANGE | status::DNR,
        "range check"
    );
}

#[test]
fn io_path_no_digests() {
    run_io_flow(false, false);
}

#[test]
fn io_path_full_digests() {
    run_io_flow(true, true);
}
