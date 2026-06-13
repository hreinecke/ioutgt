//! End-to-end M3 exit test: a raw NVMe/TCP client performs ICReq/ICResp
//! against an in-process target, sends a Connect capsule, and receives a
//! response capsule produced by the queue-thread slot pipeline.

use std::io::{Read, Write};
use std::net::TcpStream;

use ioutgt_nvme::fabrics::{ConnectCommand, ConnectData};
use ioutgt_nvme::pdu::{self, PduDecoder, PduKind};
use ioutgt_nvme::{digest, spec};
use zerocopy::{FromBytes, IntoBytes};

fn start_target() -> std::net::SocketAddr {
    let mut config = ioutgt::TargetConfig::single_memory("nqn.2026-06.io.ioutgt:test", 16);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    ioutgt::spawn_target(config).expect("target start")
}

/// Build the 64-byte Connect SQE + 1024-byte data for qid 0.
fn connect_capsule(cid: u16) -> (spec::Sqe, ConnectData) {
    let mut cmd: ConnectCommand = zerocopy::FromZeros::new_zeroed();
    cmd.opcode = spec::admin_opcode::FABRICS;
    cmd.fctype = ioutgt_nvme::fabrics::fctype::CONNECT;
    cmd.cid.set(cid);
    cmd.qid.set(0);
    cmd.sqsize.set(31); // 32 entries, 0-based
    cmd.kato.set(15_000);
    cmd.dptr.length.set(1024);
    cmd.dptr.sgl_type = spec::sgl::TYPE_DATA_BLOCK_OFFSET;

    let mut data = ConnectData::zeroed();
    data.cntlid.set(0xFFFF);
    let subnqn = b"nqn.2026-06.io.ioutgt:test";
    data.subsysnqn[..subnqn.len()].copy_from_slice(subnqn);
    let hostnqn = b"nqn.2014-08.org.nvmexpress:uuid:11111111-2222-3333-4444-555555555555";
    data.hostnqn[..hostnqn.len()].copy_from_slice(hostnqn);

    let sqe = spec::Sqe::read_from_bytes(cmd.as_bytes()).expect("64 bytes");
    (sqe, data)
}

fn handshake(stream: &mut TcpStream, want_hdgst: bool, want_ddgst: bool) -> (bool, bool) {
    let mut buf = [0u8; 256];
    let n = pdu::encode_icreq(&mut buf, want_hdgst, want_ddgst, 4);
    stream.write_all(&buf[..n]).unwrap();

    let mut resp = [0u8; 128];
    stream.read_exact(&mut resp).unwrap();
    let mut decoder = PduDecoder::new(false);
    decoder.feed(&resp).unwrap();
    assert!(decoder.is_complete());
    let decoded = decoder.take().unwrap();
    let PduKind::IcResp(icresp) = decoded.kind else {
        panic!("expected ICResp, got {:?}", decoded.kind);
    };
    assert_eq!(icresp.pfv.get(), pdu::PFV_1_0);
    assert_eq!(icresp.cpda, 0);
    assert_eq!(icresp.maxdata.get(), ioutgt_nvme_tcp::MAX_H2C_DATA);
    (
        icresp.digest & pdu::DIGEST_HDGST != 0,
        icresp.digest & pdu::DIGEST_DDGST != 0,
    )
}

fn send_connect_and_read_response(
    stream: &mut TcpStream,
    hdgst: bool,
    ddgst: bool,
    cid: u16,
) -> spec::Cqe {
    let (sqe, data) = connect_capsule(cid);
    let mut capsule = Vec::new();
    let mut hdr = [0u8; 80];
    let n = pdu::encode_capsule_cmd(&mut hdr, &sqe, hdgst, 1024, ddgst);
    capsule.extend_from_slice(&hdr[..n]);
    capsule.extend_from_slice(data.as_bytes());
    if ddgst {
        let crc = digest::crc32c(data.as_bytes());
        capsule.extend_from_slice(&crc.to_le_bytes());
    }
    stream.write_all(&capsule).unwrap();

    // Response capsule: 24 bytes + optional HDGST.
    let want = 24 + usize::from(hdgst) * 4;
    let mut resp = vec![0u8; want];
    stream.read_exact(&mut resp).unwrap();
    let mut decoder = PduDecoder::new(hdgst);
    decoder.feed(&resp).unwrap();
    assert!(decoder.is_complete());
    let decoded = decoder.take().unwrap();
    let PduKind::CapsuleResp(cqe) = decoded.kind else {
        panic!("expected response capsule, got {:?}", decoded.kind);
    };
    cqe
}

#[test]
fn icreq_icresp_and_connect_pipeline() {
    let addr = start_target();

    // Digest matrix: (request hdgst, request ddgst).
    for (want_hdgst, want_ddgst) in [(false, false), (true, false), (false, true), (true, true)] {
        let mut stream = TcpStream::connect(addr).unwrap();
        let (hdgst, ddgst) = handshake(&mut stream, want_hdgst, want_ddgst);
        assert_eq!(hdgst, want_hdgst, "hdgst negotiation");
        assert_eq!(ddgst, want_ddgst, "ddgst negotiation");

        let cqe = send_connect_and_read_response(&mut stream, hdgst, ddgst, 0x77);
        // Connect must succeed and return a non-zero dynamic cntlid in
        // DW0, through the queue-thread slot pipeline.
        assert_eq!(cqe.cid.get(), 0x77);
        assert_eq!(
            cqe.status.get() >> 1,
            ioutgt_nvme::status::SUCCESS,
            "connect status"
        );
        assert!(cqe.result.get() >= 1, "cntlid allocated");
    }
}

#[test]
fn malformed_icreq_rejected() {
    let addr = start_target();
    let mut stream = TcpStream::connect(addr).unwrap();
    // ICReq with bogus PFV.
    let mut buf = [0u8; 128];
    pdu::encode_icreq(&mut buf, false, false, 1);
    buf[8] = 0xFF; // pfv low byte
    stream.write_all(&buf).unwrap();
    // Server must close the connection (read returns 0).
    let mut resp = [0u8; 16];
    let n = stream.read(&mut resp).unwrap_or(0);
    assert_eq!(n, 0, "server should close on bad PFV");
}
