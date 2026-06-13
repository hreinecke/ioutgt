//! Control-thread connection setup: ICReq/ICResp and the first Connect
//! capsule, over ordinary Tokio sockets (control-plane rate; the socket
//! moves to a queue thread immediately afterwards).

use std::io;

use ioutgt_nvme::fabrics::ConnectCommand;
use ioutgt_nvme::pdu::{self, PduDecoder, PduKind};
use ioutgt_nvme::{digest, fabrics};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use zerocopy::FromBytes;

/// Negotiated connection parameters from the IC exchange.
#[derive(Debug, Clone, Copy)]
#[allow(missing_docs)]
pub struct Negotiated {
    pub hdr_digest: bool,
    pub data_digest: bool,
    pub maxr2t: u32,
}

fn proto_err(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// Server side of the IC exchange: validate ICReq, reply ICResp.
/// Digest enablement is the intersection of host request and our policy.
pub async fn accept_handshake(
    stream: &mut TcpStream,
    allow_hdgst: bool,
    allow_ddgst: bool,
    maxdata: u32,
) -> io::Result<Negotiated> {
    let mut buf = [0u8; 128];
    stream.read_exact(&mut buf).await?;

    let mut decoder = PduDecoder::new(false);
    decoder
        .feed(&buf)
        .map_err(|e| proto_err(&format!("bad ICReq: {e}")))?;
    if !decoder.is_complete() {
        return Err(proto_err("short ICReq"));
    }
    let decoded = decoder
        .take()
        .map_err(|e| proto_err(&format!("bad ICReq: {e}")))?;
    let PduKind::IcReq(icreq) = decoded.kind else {
        return Err(proto_err("expected ICReq"));
    };
    if icreq.pfv.get() != pdu::PFV_1_0 {
        return Err(proto_err("unsupported PFV"));
    }
    if icreq.hpda != 0 {
        return Err(proto_err("HPDA != 0 unsupported"));
    }

    let negotiated = Negotiated {
        hdr_digest: allow_hdgst && icreq.digest & pdu::DIGEST_HDGST != 0,
        data_digest: allow_ddgst && icreq.digest & pdu::DIGEST_DDGST != 0,
        maxr2t: icreq.maxr2t.get().max(1),
    };

    let mut resp = [0u8; 128];
    let n = pdu::encode_icresp(
        &mut resp,
        negotiated.hdr_digest,
        negotiated.data_digest,
        maxdata,
    );
    stream.write_all(&resp[..n]).await?;
    Ok(negotiated)
}

/// The first command capsule of a queue, parsed enough to route it.
#[allow(missing_docs)]
pub struct FirstCapsule {
    pub sqe: ioutgt_nvme::spec::Sqe,
    /// The 1024-byte Connect data payload.
    pub data: Box<fabrics::ConnectData>,
}

impl FirstCapsule {
    /// View the SQE as a Connect command.
    pub fn connect(&self) -> &ConnectCommand {
        ConnectCommand::ref_from_bytes(zerocopy::IntoBytes::as_bytes(&self.sqe))
            .expect("SQE and ConnectCommand are both 64 bytes, alignment 1")
    }
}

/// Read the Connect capsule (CapsuleCmd + 1024B in-capsule data) after
/// the IC exchange.
pub async fn read_connect(
    stream: &mut TcpStream,
    negotiated: Negotiated,
) -> io::Result<FirstCapsule> {
    let mut decoder = PduDecoder::new(negotiated.hdr_digest);
    let mut buf = [0u8; 256];
    let mut pending: Vec<u8> = Vec::new();

    // Assemble the capsule header.
    let decoded = loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        let consumed = decoder
            .feed(&buf[..n])
            .map_err(|e| proto_err(&format!("bad capsule: {e}")))?;
        if decoder.is_complete() {
            pending.extend_from_slice(&buf[consumed..n]);
            break decoder
                .take()
                .map_err(|e| proto_err(&format!("bad capsule: {e}")))?;
        }
        debug_assert_eq!(consumed, n);
    };
    let PduKind::CapsuleCmd(sqe) = decoded.kind else {
        return Err(proto_err("expected command capsule"));
    };
    if sqe.opcode != ioutgt_nvme::spec::admin_opcode::FABRICS {
        return Err(proto_err("first command must be fabrics Connect"));
    }
    let expected = 1024 + if decoded.ddgst { 4 } else { 0 };
    if decoded.data_len != 1024 {
        return Err(proto_err("Connect data must be 1024 bytes"));
    }

    // Read payload (+ DDGST) — `pending` holds bytes already received.
    let mut payload = vec![0u8; expected];
    let already = pending.len().min(expected);
    payload[..already].copy_from_slice(&pending[..already]);
    if already < expected {
        stream.read_exact(&mut payload[already..]).await?;
    }
    if decoded.ddgst {
        let wire = u32::from_le_bytes(payload[1024..1028].try_into().expect("4 bytes"));
        if wire != digest::crc32c(&payload[..1024]) {
            return Err(proto_err("Connect data digest mismatch"));
        }
    }
    let data =
        fabrics::ConnectData::read_from_bytes(&payload[..1024]).expect("1024 bytes, alignment 1");
    Ok(FirstCapsule {
        sqe,
        data: Box::new(data),
    })
}
