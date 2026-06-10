//! Shared raw NVMe/TCP test client (sans-io codec underneath).
#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::TcpStream;

use ioutgt_nvme::fabrics::{ConnectCommand, ConnectData, fctype};
use ioutgt_nvme::pdu::{self, DecodedPdu, PduDecoder, PduKind};
use ioutgt_nvme::{digest, spec, status};
use zerocopy::{FromBytes, FromZeros, IntoBytes};

pub const NQN: &str = "nqn.2026-06.io.ioutgt:test";
pub const HOSTNQN: &str = "nqn.2014-08.org.nvmexpress:uuid:0a0a0a0a-1111-2222-3333-444444444444";

pub struct Client {
    stream: TcpStream,
    hdgst: bool,
    ddgst: bool,
}

impl Client {
    pub fn handshake(addr: std::net::SocketAddr, hdgst: bool, ddgst: bool) -> Client {
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

    pub fn send_capsule(&mut self, sqe: &spec::Sqe, data: &[u8]) {
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

    pub fn send_h2c_data(&mut self, cid: u16, ttag: u16, offset: u32, data: &[u8], last: bool) {
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
    pub fn recv_pdu(&mut self) -> (DecodedPdu, Vec<u8>) {
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

    pub fn recv_response(&mut self) -> spec::Cqe {
        let (decoded, _) = self.recv_pdu();
        let PduKind::CapsuleResp(cqe) = decoded.kind else {
            panic!("expected response capsule, got {:?}", decoded.kind);
        };
        cqe
    }

    pub fn connect(&mut self, qid: u16, sqsize: u16, cntlid: u16, cid: u16) -> u16 {
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

pub fn rw_sqe(
    opcode: u8,
    cid: u16,
    slba: u64,
    nlb0: u16,
    len: u32,
    transport_sgl: bool,
) -> spec::Sqe {
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

/// Deterministic test payload.
pub fn pattern(len: usize, seed: u8) -> Vec<u8> {
    #[allow(clippy::cast_possible_truncation)]
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

impl Client {
    /// Identify: returns the 4096-byte payload after asserting success.
    pub fn identify(&mut self, cns: u8, nsid: u32, cid: u16) -> Vec<u8> {
        let mut sqe = spec::Sqe::zeroed();
        sqe.opcode = spec::admin_opcode::IDENTIFY;
        sqe.flags = spec::CMD_FLAGS_SGL_METABUF;
        sqe.cid.set(cid);
        sqe.nsid.set(nsid);
        sqe.cdw10.set(u32::from(cns));
        sqe.dptr.length.set(4096);
        sqe.dptr.sgl_type = spec::sgl::TYPE_TRANSPORT_DATA_BLOCK;
        self.send_capsule(&sqe, &[]);
        let (decoded, payload) = self.recv_pdu();
        assert!(
            matches!(decoded.kind, PduKind::C2HData { .. }),
            "identify expects data"
        );
        let cqe = self.recv_response();
        assert_eq!(cqe.status.get() >> 1, status::SUCCESS, "identify cns {cns}");
        payload
    }

    /// Enable the controller (Property Set CC.EN, as the host driver
    /// does before any admin command).
    pub fn enable_controller(&mut self, cid: u16) {
        use ioutgt_nvme::fabrics::{PropertyCommand, cc, prop};
        let mut cmd: PropertyCommand = FromZeros::new_zeroed();
        cmd.opcode = spec::admin_opcode::FABRICS;
        cmd.fctype = fctype::PROPERTY_SET;
        cmd.cid.set(cid);
        cmd.attrib = 0; // 4-byte property
        cmd.offset.set(prop::CC);
        cmd.value.set(u64::from(
            cc::EN | (6 << cc::IOSQES_SHIFT) | (4 << cc::IOCQES_SHIFT),
        ));
        let sqe = spec::Sqe::read_from_bytes(cmd.as_bytes()).unwrap();
        self.send_capsule(&sqe, &[]);
        let cqe = self.recv_response();
        assert_eq!(cqe.status.get() >> 1, status::SUCCESS, "enable controller");
    }

    /// Post an Async Event Request (no response until an event fires).
    pub fn post_aer(&mut self, cid: u16) {
        let mut sqe = spec::Sqe::zeroed();
        sqe.opcode = spec::admin_opcode::ASYNC_EVENT;
        sqe.cid.set(cid);
        self.send_capsule(&sqe, &[]);
    }
}
