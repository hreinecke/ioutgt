//! Shared raw NVMe/TCP test client (sans-io codec underneath).
#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

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
        // Segmentation-sensitive tests rely on each write hitting the
        // wire immediately (the target side sets nodelay already).
        stream.set_nodelay(true).unwrap();
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

    /// Send an H2CData PDU with controlled wire segmentation: the
    /// header is written (and pushed — nodelay) alone, then the payload
    /// in `chunks`-sized pieces (any remainder as one final chunk),
    /// sleeping `inter_chunk_delay` before each payload write so the
    /// target observes the segmentation. The DDGST trailer (when the
    /// connection negotiated data digests) follows the last chunk.
    #[allow(clippy::too_many_arguments)]
    pub fn send_h2c_data_fragmented(
        &mut self,
        cid: u16,
        ttag: u16,
        offset: u32,
        data: &[u8],
        last: bool,
        chunks: &[usize],
        inter_chunk_delay: Duration,
    ) {
        self.send_h2c_data_chunked(
            cid,
            ttag,
            offset,
            data,
            last,
            chunks,
            inter_chunk_delay,
            false,
        );
    }

    /// [`Client::send_h2c_data_fragmented`] with the DDGST trailer
    /// bit-flipped (digest connections only): the payload bytes are
    /// intact, the digest is wrong.
    #[allow(clippy::too_many_arguments)]
    pub fn send_h2c_data_fragmented_bad_ddgst(
        &mut self,
        cid: u16,
        ttag: u16,
        offset: u32,
        data: &[u8],
        last: bool,
        chunks: &[usize],
        inter_chunk_delay: Duration,
    ) {
        assert!(self.ddgst, "corrupting a DDGST needs a ddgst connection");
        self.send_h2c_data_chunked(
            cid,
            ttag,
            offset,
            data,
            last,
            chunks,
            inter_chunk_delay,
            true,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn send_h2c_data_chunked(
        &mut self,
        cid: u16,
        ttag: u16,
        offset: u32,
        data: &[u8],
        last: bool,
        chunks: &[usize],
        inter_chunk_delay: Duration,
        corrupt_ddgst: bool,
    ) {
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
        let mut sent = 0;
        for &chunk in chunks {
            if sent >= data.len() {
                break;
            }
            std::thread::sleep(inter_chunk_delay);
            let end = (sent + chunk).min(data.len());
            self.stream.write_all(&data[sent..end]).unwrap();
            sent = end;
        }
        if sent < data.len() {
            std::thread::sleep(inter_chunk_delay);
            self.stream.write_all(&data[sent..]).unwrap();
        }
        if self.ddgst {
            let mut crc = digest::crc32c(data);
            if corrupt_ddgst {
                crc ^= 0xFFFF_FFFF;
            }
            self.stream.write_all(&crc.to_le_bytes()).unwrap();
        }
    }

    /// Send an H2CData PDU as one buffer in a single write (header +
    /// payload + digest), so the whole payload arrives in the target's
    /// connection buffer together with the header.
    pub fn send_h2c_data_one_write(
        &mut self,
        cid: u16,
        ttag: u16,
        offset: u32,
        data: &[u8],
        last: bool,
    ) {
        let mut frame = Vec::with_capacity(32 + data.len() + 4);
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
        frame.extend_from_slice(&hdr[..n]);
        frame.extend_from_slice(data);
        if self.ddgst {
            frame.extend_from_slice(&digest::crc32c(data).to_le_bytes());
        }
        self.stream.write_all(&frame).unwrap();
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

    /// Raw stream access (abuse tests).
    pub fn stream(&mut self) -> &mut TcpStream {
        &mut self.stream
    }

    pub fn connect(&mut self, qid: u16, sqsize: u16, cntlid: u16, cid: u16) -> u16 {
        let kato = if qid == 0 { 60_000 } else { 0 };
        self.connect_with_kato(qid, sqsize, cntlid, cid, kato)
    }

    pub fn connect_with_kato(
        &mut self,
        qid: u16,
        sqsize: u16,
        cntlid: u16,
        cid: u16,
        kato: u32,
    ) -> u16 {
        let mut cmd: ConnectCommand = FromZeros::new_zeroed();
        cmd.opcode = spec::admin_opcode::FABRICS;
        cmd.fctype = fctype::CONNECT;
        cmd.cid.set(cid);
        cmd.qid.set(qid);
        cmd.sqsize.set(sqsize - 1);
        cmd.kato.set(kato);
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

    /// Property Set CC and return the (unshifted) status code.
    pub fn set_property_cc(&mut self, value: u32, cid: u16) -> u16 {
        use ioutgt_nvme::fabrics::{PropertyCommand, fctype as fct, prop};
        let mut cmd: PropertyCommand = FromZeros::new_zeroed();
        cmd.opcode = spec::admin_opcode::FABRICS;
        cmd.fctype = fct::PROPERTY_SET;
        cmd.cid.set(cid);
        cmd.offset.set(prop::CC);
        cmd.value.set(u64::from(value));
        let sqe = spec::Sqe::read_from_bytes(cmd.as_bytes()).unwrap();
        self.send_capsule(&sqe, &[]);
        self.recv_response().status.get() >> 1
    }
}

/// Admin-queue Connect to `nqn` as the fixed test HOSTNQN; returns the
/// full CQE (status, and the granted cntlid in the result dword on
/// success). The connection drops on return, tearing the controller
/// down.
pub fn connect_cqe(addr: std::net::SocketAddr, nqn: &str) -> spec::Cqe {
    let mut client = Client::handshake(addr, false, false);
    let (sqe, mut data) = connect_sqe(0, 32, 0xFFFF, 1);
    data.subsysnqn = [0; 256];
    data.subsysnqn[..nqn.len()].copy_from_slice(nqn.as_bytes());
    client.send_capsule(&sqe, data.as_bytes());
    client.recv_response()
}

/// Admin-queue Connect to `nqn`; the phase-stripped CQE status only.
pub fn connect_status(addr: std::net::SocketAddr, nqn: &str) -> u16 {
    connect_cqe(addr, nqn).status.get() >> 1
}

/// A 1 MiB backing file for file-backed namespace configs.
pub fn backing_file(dir: &std::path::Path, name: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    std::fs::File::create(&path)
        .unwrap()
        .set_len(1 << 20)
        .unwrap();
    path
}

/// Build a Connect SQE + data capsule (for hand-driven Connect tests).
pub fn connect_sqe(qid: u16, sqsize: u16, cntlid: u16, cid: u16) -> (spec::Sqe, ConnectData) {
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
    (sqe, data)
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
