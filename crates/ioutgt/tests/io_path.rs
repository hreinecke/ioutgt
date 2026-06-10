//! M5 exit test (host-only half): a raw NVMe/TCP client connects admin +
//! IO queues, then drives the data path — in-capsule write, R2T write
//! split across H2CData PDUs, reads with verification, write-zeroes —
//! with and without digests.

use std::io::{Read, Write};
use std::net::TcpStream;

use ioutgt_nvme::fabrics::{ConnectCommand, ConnectData, fctype};
use ioutgt_nvme::pdu::{self, DecodedPdu, PduDecoder, PduKind};
use ioutgt_nvme::{digest, spec, status};
use zerocopy::{FromBytes, FromZeros, IntoBytes};

const NQN: &str = "nqn.2026-06.io.ioutgt:test";
const HOSTNQN: &str = "nqn.2014-08.org.nvmexpress:uuid:0a0a0a0a-1111-2222-3333-444444444444";

struct Client {
    stream: TcpStream,
    hdgst: bool,
    ddgst: bool,
}

impl Client {
    fn handshake(addr: std::net::SocketAddr, hdgst: bool, ddgst: bool) -> Client {
        let mut stream = TcpStream::connect(addr).unwrap();
        let mut buf = [0u8; 128];
        let n = pdu::encode_icreq(&mut buf, hdgst, ddgst, 4);
        stream.write_all(&buf[..n]).unwrap();
        let mut resp = [0u8; 128];
        stream.read_exact(&mut resp).unwrap();
        let mut decoder = PduDecoder::new(false);
        decoder.feed(&resp).unwrap();
        let decoded = decoder.take().unwrap();
        let PduKind::IcResp(icresp) = decoded.kind else {
            panic!("expected ICResp")
        };
        assert_eq!(icresp.digest & pdu::DIGEST_HDGST != 0, hdgst);
        assert_eq!(icresp.digest & pdu::DIGEST_DDGST != 0, ddgst);
        Client {
            stream,
            hdgst,
            ddgst,
        }
    }

    fn send_capsule(&mut self, sqe: &spec::Sqe, data: &[u8]) {
        let mut frame = Vec::with_capacity(80 + data.len() + 4);
        let mut hdr = [0u8; 80];
        let n = pdu::encode_capsule_cmd(
            &mut hdr,
            sqe,
            self.hdgst,
            u32::try_from(data.len()).unwrap(),
            self.ddgst,
        );
        frame.extend_from_slice(&hdr[..n]);
        frame.extend_from_slice(data);
        if self.ddgst && !data.is_empty() {
            frame.extend_from_slice(&digest::crc32c(data).to_le_bytes());
        }
        self.stream.write_all(&frame).unwrap();
    }

    fn send_h2c_data(&mut self, cid: u16, ttag: u16, offset: u32, data: &[u8], last: bool) {
        let mut hdr = [0u8; 32];
        let n = pdu::encode_h2c_data(
            &mut hdr,
            cid,
            ttag,
            offset,
            u32::try_from(data.len()).unwrap(),
            last,
            self.hdgst,
            self.ddgst,
        );
        self.stream.write_all(&hdr[..n]).unwrap();
        self.stream.write_all(data).unwrap();
        if self.ddgst {
            self.stream
                .write_all(&digest::crc32c(data).to_le_bytes())
                .unwrap();
        }
    }

    /// Read one PDU (header + payload), verifying digests.
    fn recv_pdu(&mut self) -> (DecodedPdu, Vec<u8>) {
        let mut decoder = PduDecoder::new(self.hdgst);
        let mut byte = [0u8; 1];
        loop {
            self.stream.read_exact(&mut byte).unwrap();
            decoder.feed(&byte).unwrap();
            if decoder.is_complete() {
                break;
            }
        }
        let decoded = decoder.take().unwrap();
        let mut payload = vec![0u8; decoded.data_len as usize];
        self.stream.read_exact(&mut payload).unwrap();
        if decoded.ddgst {
            let mut crc = [0u8; 4];
            self.stream.read_exact(&mut crc).unwrap();
            assert_eq!(
                u32::from_le_bytes(crc),
                digest::crc32c(&payload),
                "C2H DDGST"
            );
        }
        (decoded, payload)
    }

    fn recv_response(&mut self) -> spec::Cqe {
        let (decoded, _) = self.recv_pdu();
        let PduKind::CapsuleResp(cqe) = decoded.kind else {
            panic!("expected response capsule, got {:?}", decoded.kind);
        };
        cqe
    }

    fn connect(&mut self, qid: u16, sqsize: u16, cntlid: u16, cid: u16) -> u16 {
        let mut cmd: ConnectCommand = FromZeros::new_zeroed();
        cmd.opcode = spec::admin_opcode::FABRICS;
        cmd.fctype = fctype::CONNECT;
        cmd.cid.set(cid);
        cmd.qid.set(qid);
        cmd.sqsize.set(sqsize - 1);
        cmd.kato.set(if qid == 0 { 60_000 } else { 0 });
        cmd.dptr.length.set(1024);
        cmd.dptr.sgl_type = spec::sgl::TYPE_DATA_BLOCK_OFFSET;
        let mut data = ConnectData::zeroed();
        data.cntlid.set(cntlid);
        data.subsysnqn[..NQN.len()].copy_from_slice(NQN.as_bytes());
        data.hostnqn[..HOSTNQN.len()].copy_from_slice(HOSTNQN.as_bytes());
        let sqe = spec::Sqe::read_from_bytes(cmd.as_bytes()).unwrap();
        self.send_capsule(&sqe, data.as_bytes());
        let cqe = self.recv_response();
        assert_eq!(cqe.status.get() >> 1, status::SUCCESS, "connect qid {qid}");
        u16::try_from(cqe.result.get() & 0xFFFF).unwrap()
    }
}

fn rw_sqe(opcode: u8, cid: u16, slba: u64, nlb0: u16, len: u32, transport_sgl: bool) -> spec::Sqe {
    let mut sqe = spec::Sqe::zeroed();
    sqe.opcode = opcode;
    sqe.flags = spec::CMD_FLAGS_SGL_METABUF;
    sqe.cid.set(cid);
    sqe.nsid.set(1);
    #[allow(clippy::cast_possible_truncation)]
    sqe.cdw10.set(slba as u32);
    sqe.cdw11.set((slba >> 32) as u32);
    sqe.cdw12.set(u32::from(nlb0));
    sqe.dptr.length.set(len);
    sqe.dptr.sgl_type = if transport_sgl {
        spec::sgl::TYPE_TRANSPORT_DATA_BLOCK
    } else {
        spec::sgl::TYPE_DATA_BLOCK_OFFSET
    };
    sqe
}

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    #[allow(clippy::cast_possible_truncation)]
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

fn run_io_flow(hdgst: bool, ddgst: bool) {
    let addr = ioutgt::spawn_target(ioutgt::TargetConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        io_threads: 1,
        allow_hdgst: true,
        allow_ddgst: true,
        pin_threads: false,
        subsys_nqn: NQN.into(),
        mem_size_mb: 16,
        backend: ioutgt::BackendSpec::Memory,
    })
    .expect("target start");

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
