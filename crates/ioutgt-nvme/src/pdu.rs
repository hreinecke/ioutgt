//! NVMe/TCP PDU definitions and the incremental sans-io codec.
//!
//! The decoder assembles and validates PDU *headers* (including HDGST)
//! from arbitrarily fragmented input; payload bytes never pass through it
//! — the transport copies them straight into command-slot buffers and
//! tracks DDGST with [`crate::digest::Crc32c`]. This keeps the codec
//! allocation-free and lets the data path stay zero-copy.

#![allow(missing_docs)] // wire-format mirrors: the NVMe spec is the documentation

use zerocopy::little_endian::{U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::digest;
use crate::spec::{Cqe, Sqe};

/// PDU types.
pub mod pdu_type {
    pub const ICREQ: u8 = 0x0;
    pub const ICRESP: u8 = 0x1;
    pub const H2C_TERM: u8 = 0x2;
    pub const C2H_TERM: u8 = 0x3;
    pub const CAPSULE_CMD: u8 = 0x4;
    pub const CAPSULE_RESP: u8 = 0x5;
    pub const H2C_DATA: u8 = 0x6;
    pub const C2H_DATA: u8 = 0x7;
    pub const R2T: u8 = 0x9;
}

/// PDU header flags.
pub mod pdu_flags {
    pub const HDGST: u8 = 1 << 0;
    pub const DDGST: u8 = 1 << 1;
    pub const DATA_LAST: u8 = 1 << 2;
    /// On the final C2HData: completion is implied-successful, no
    /// response capsule follows.
    pub const DATA_SUCCESS: u8 = 1 << 3;
}

/// Fatal Error Status codes for term PDUs.
pub mod fes {
    pub const INVALID_PDU_HDR: u16 = 0x1;
    pub const PDU_SEQ_ERR: u16 = 0x2;
    pub const HDR_DIGEST_ERR: u16 = 0x3;
    pub const DATA_OUT_OF_RANGE: u16 = 0x4;
    pub const DATA_LIMIT_EXCEEDED: u16 = 0x5;
    pub const UNSUPPORTED_PARAM: u16 = 0x6;
}

/// NVMe/TCP protocol format version 1.0.
pub const PFV_1_0: u16 = 0;

/// Digest negotiation bits in ICReq/ICResp `digest`.
pub const DIGEST_HDGST: u8 = 1 << 0;
pub const DIGEST_DDGST: u8 = 1 << 1;

/// Common 8-byte header.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub struct CommonHeader {
    pub pdu_type: u8,
    pub flags: u8,
    /// Header length (CH + PSH, excluding HDGST).
    pub hlen: u8,
    /// PDU data offset from PDU start (0 when no data).
    pub pdo: u8,
    /// Total PDU length including digests and data.
    pub plen: U32,
}

/// ICReq (128 bytes): connection initialization request.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub struct IcReq {
    pub hdr: CommonHeader,
    pub pfv: U16,
    /// Host PDU data alignment (we require 0).
    pub hpda: u8,
    pub digest: u8,
    pub maxr2t: U32,
    pub rsvd: [u8; 112],
}

/// ICResp (128 bytes).
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub struct IcResp {
    pub hdr: CommonHeader,
    pub pfv: U16,
    /// Controller PDU data alignment (we send 0).
    pub cpda: u8,
    pub digest: u8,
    /// Max H2CData PDU payload the controller accepts.
    pub maxdata: U32,
    pub rsvd: [u8; 112],
}

/// Termination request (24-byte header; may carry the offending PDU
/// header as data, which we neither send nor require).
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub struct TermPdu {
    pub hdr: CommonHeader,
    pub fes: U16,
    pub fei: U32,
    pub rsvd: [u8; 10],
}

/// Command capsule: CH + 64-byte SQE (+ optional HDGST, in-capsule data,
/// DDGST counted only in `plen`).
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub struct CmdCapsule {
    pub hdr: CommonHeader,
    pub sqe: Sqe,
}

/// Response capsule: CH + 16-byte CQE.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub struct RspCapsule {
    pub hdr: CommonHeader,
    pub cqe: Cqe,
}

/// H2CData / C2HData / R2T share this 24-byte layout (field names differ
/// in the spec but offsets coincide).
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub struct DataPdu {
    pub hdr: CommonHeader,
    pub cid: U16,
    /// Transfer tag (ioutgt: the command slot index).
    pub ttag: U16,
    pub data_offset: U32,
    pub data_length: U32,
    pub rsvd: [u8; 4],
}

const _: () = {
    assert!(size_of::<CommonHeader>() == 8);
    assert!(size_of::<IcReq>() == 128);
    assert!(size_of::<IcResp>() == 128);
    assert!(size_of::<TermPdu>() == 24);
    assert!(size_of::<CmdCapsule>() == 72);
    assert!(size_of::<RspCapsule>() == 24);
    assert!(size_of::<DataPdu>() == 24);
};

/// Largest header we ever buffer: ICReq/ICResp (128); other headers are
/// ≤ 72 (+4 HDGST).
const MAX_HDR: usize = 128;

/// Hard upper bound we accept for any PLEN (32 MiB) — protocol sanity,
/// not a negotiated limit; the transport enforces MAXH2CDATA/IOCCSZ on
/// top.
const MAX_PLEN: u32 = 32 * 1024 * 1024;

/// Decode error; carries the term-PDU FES the transport should send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PduError {
    pub fes: u16,
    /// Additional error info (offending field offset where applicable).
    pub fei: u32,
}

impl PduError {
    fn hdr_field(offset: u32) -> Self {
        PduError {
            fes: fes::INVALID_PDU_HDR,
            fei: offset,
        }
    }
}

impl std::fmt::Display for PduError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PDU error fes={:#x} fei={:#x}", self.fes, self.fei)
    }
}

impl std::error::Error for PduError {}

/// A decoded PDU header. `data_len` payload bytes (then a 4-byte DDGST if
/// `ddgst`) follow on the stream; the transport consumes them.
#[derive(Debug, Clone, Copy)]
pub struct DecodedPdu {
    pub kind: PduKind,
    /// Payload bytes following the header on the wire.
    pub data_len: u32,
    /// A data digest trails the payload.
    pub ddgst: bool,
}

/// Typed view of the header (small, copied out of the decoder).
#[derive(Debug, Clone, Copy)]
pub enum PduKind {
    IcReq(IcReq),
    IcResp(IcResp),
    H2CTerm {
        fes: u16,
        fei: u32,
    },
    CapsuleCmd(Sqe),
    CapsuleResp(Cqe),
    H2CData {
        cid: u16,
        ttag: u16,
        offset: u32,
        length: u32,
        last: bool,
    },
    C2HData {
        cid: u16,
        offset: u32,
        length: u32,
        last: bool,
        success: bool,
    },
    R2T {
        cid: u16,
        ttag: u16,
        offset: u32,
        length: u32,
    },
}

enum DecodeState {
    /// Collecting the fixed 8-byte common header.
    Common,
    /// Collecting the remainder (`need` total bytes known).
    Rest,
    /// Full header buffered, awaiting `take()`.
    Complete,
}

/// Incremental header decoder. Feed bytes; when [`PduDecoder::feed`]
/// reports completion, call [`PduDecoder::take`].
pub struct PduDecoder {
    /// ICReq/ICResp must arrive before digests are enabled; the transport
    /// constructs the decoder per connection after negotiation, except the
    /// handshake decoder which uses `hdr_digest = false`.
    hdr_digest: bool,
    buf: [u8; MAX_HDR],
    have: usize,
    need: usize,
    state: DecodeState,
}

impl PduDecoder {
    pub fn new(hdr_digest: bool) -> Self {
        PduDecoder {
            hdr_digest,
            buf: [0; MAX_HDR],
            have: 0,
            need: 8,
            state: DecodeState::Common,
        }
    }

    /// Whether a complete header is buffered ([`take`](Self::take) it).
    pub fn is_complete(&self) -> bool {
        matches!(self.state, DecodeState::Complete)
    }

    /// Consume bytes from `input` toward the current header; returns how
    /// many were consumed. Stops consuming once a header completes.
    pub fn feed(&mut self, input: &[u8]) -> Result<usize, PduError> {
        let mut consumed = 0;
        loop {
            match self.state {
                DecodeState::Complete => return Ok(consumed),
                DecodeState::Common | DecodeState::Rest => {
                    let want = self.need - self.have;
                    if want > 0 {
                        let take = want.min(input.len() - consumed);
                        if take == 0 {
                            return Ok(consumed);
                        }
                        self.buf[self.have..self.have + take]
                            .copy_from_slice(&input[consumed..consumed + take]);
                        self.have += take;
                        consumed += take;
                        if self.have < self.need {
                            return Ok(consumed);
                        }
                    }
                    if matches!(self.state, DecodeState::Common) {
                        self.need = self.full_header_len()?;
                        self.state = DecodeState::Rest;
                        // Loop: maybe the rest is already available.
                    } else {
                        self.state = DecodeState::Complete;
                    }
                }
            }
        }
    }

    /// Header length (incl. HDGST where applicable) derived from the
    /// validated common header.
    fn full_header_len(&self) -> Result<usize, PduError> {
        let hdr = CommonHeader::read_from_bytes(&self.buf[..8]).expect("8 bytes buffered");
        let (expected_hlen, digestable): (u32, bool) = match hdr.pdu_type {
            pdu_type::ICREQ | pdu_type::ICRESP => (128, false),
            pdu_type::H2C_TERM | pdu_type::C2H_TERM => (24, false),
            pdu_type::CAPSULE_CMD => (72, true),
            pdu_type::CAPSULE_RESP => (24, true),
            pdu_type::H2C_DATA | pdu_type::C2H_DATA | pdu_type::R2T => (24, true),
            _ => return Err(PduError::hdr_field(0)),
        };
        if u32::from(hdr.hlen) != expected_hlen {
            return Err(PduError::hdr_field(2));
        }
        let hdgst = digestable && self.hdr_digest;
        if hdr.flags & pdu_flags::HDGST != 0 && !hdgst {
            return Err(PduError::hdr_field(1));
        }
        let plen = hdr.plen.get();
        let header_wire = expected_hlen + if hdgst { 4 } else { 0 };
        if plen < header_wire || plen > MAX_PLEN {
            return Err(PduError::hdr_field(4));
        }
        // Fixed-size PDUs must have an exact PLEN.
        let exact = match hdr.pdu_type {
            pdu_type::ICREQ | pdu_type::ICRESP => Some(128),
            pdu_type::CAPSULE_RESP | pdu_type::R2T => Some(header_wire),
            _ => None,
        };
        if let Some(exact) = exact {
            if plen != exact {
                return Err(PduError::hdr_field(4));
            }
        }
        // A data digest with no data to digest is malformed.
        let ddgst = hdr.flags & pdu_flags::DDGST != 0;
        if ddgst && plen < header_wire + 4 + 1 {
            return Err(PduError::hdr_field(4));
        }
        Ok(expected_hlen as usize + if hdgst { 4 } else { 0 })
    }

    /// Validate digests/fields, parse the typed header, and reset for the
    /// next PDU.
    pub fn take(&mut self) -> Result<DecodedPdu, PduError> {
        assert!(self.is_complete(), "take() before header complete");
        let hdr = CommonHeader::read_from_bytes(&self.buf[..8]).expect("buffered");
        let hlen = hdr.hlen as usize;
        let digestable = !matches!(
            hdr.pdu_type,
            pdu_type::ICREQ | pdu_type::ICRESP | pdu_type::H2C_TERM | pdu_type::C2H_TERM
        );
        let hdgst = digestable && self.hdr_digest;
        if hdgst {
            let wire = u32::from_le_bytes(self.buf[hlen..hlen + 4].try_into().expect("4 bytes"));
            let computed = digest::crc32c(&self.buf[..hlen]);
            if wire != computed {
                return Err(PduError {
                    fes: fes::HDR_DIGEST_ERR,
                    fei: 0,
                });
            }
        }
        let ddgst = digestable && hdr.flags & pdu_flags::DDGST != 0;
        let header_wire = u32::from(hdr.hlen) + if hdgst { 4 } else { 0 };
        let plen = hdr.plen.get();
        let data_len = plen - header_wire - if ddgst { 4 } else { 0 };
        // We negotiate CPDA/HPDA = 0: any nonzero pad (PDO beyond the
        // header) is a protocol violation.
        if hdr.pdo != 0 && u32::from(hdr.pdo) != header_wire {
            return Err(PduError::hdr_field(3));
        }

        let kind = match hdr.pdu_type {
            pdu_type::ICREQ => {
                PduKind::IcReq(IcReq::read_from_bytes(&self.buf[..128]).expect("buffered"))
            }
            pdu_type::ICRESP => {
                PduKind::IcResp(IcResp::read_from_bytes(&self.buf[..128]).expect("buffered"))
            }
            pdu_type::H2C_TERM | pdu_type::C2H_TERM => {
                let term = TermPdu::read_from_bytes(&self.buf[..24]).expect("buffered");
                // Term PDU data (offending header copy) is bounded by 152.
                if plen > 152 {
                    return Err(PduError::hdr_field(4));
                }
                PduKind::H2CTerm {
                    fes: term.fes.get(),
                    fei: term.fei.get(),
                }
            }
            pdu_type::CAPSULE_CMD => {
                let cmd = CmdCapsule::read_from_bytes(&self.buf[..72]).expect("buffered");
                PduKind::CapsuleCmd(cmd.sqe)
            }
            pdu_type::CAPSULE_RESP => {
                let rsp = RspCapsule::read_from_bytes(&self.buf[..24]).expect("buffered");
                PduKind::CapsuleResp(rsp.cqe)
            }
            pdu_type::H2C_DATA => {
                let d = DataPdu::read_from_bytes(&self.buf[..24]).expect("buffered");
                if d.data_length.get() != data_len {
                    return Err(PduError::hdr_field(12));
                }
                PduKind::H2CData {
                    cid: d.cid.get(),
                    ttag: d.ttag.get(),
                    offset: d.data_offset.get(),
                    length: d.data_length.get(),
                    last: hdr.flags & pdu_flags::DATA_LAST != 0,
                }
            }
            pdu_type::C2H_DATA => {
                let d = DataPdu::read_from_bytes(&self.buf[..24]).expect("buffered");
                PduKind::C2HData {
                    cid: d.cid.get(),
                    offset: d.data_offset.get(),
                    length: d.data_length.get(),
                    last: hdr.flags & pdu_flags::DATA_LAST != 0,
                    success: hdr.flags & pdu_flags::DATA_SUCCESS != 0,
                }
            }
            pdu_type::R2T => {
                let d = DataPdu::read_from_bytes(&self.buf[..24]).expect("buffered");
                PduKind::R2T {
                    cid: d.cid.get(),
                    ttag: d.ttag.get(),
                    offset: d.data_offset.get(),
                    length: d.data_length.get(),
                }
            }
            _ => unreachable!("validated in full_header_len"),
        };

        self.have = 0;
        self.need = 8;
        self.state = DecodeState::Common;
        Ok(DecodedPdu {
            kind,
            data_len,
            ddgst,
        })
    }
}

// ---------------------------------------------------------------------
// Encoders: write headers into caller-provided buffers, return length.
// ---------------------------------------------------------------------

fn common(pdu_type: u8, flags: u8, hlen: u8, pdo: u8, plen: u32) -> CommonHeader {
    CommonHeader {
        pdu_type,
        flags,
        hlen,
        pdo,
        plen: U32::new(plen),
    }
}

fn put_hdgst(buf: &mut [u8], hlen: usize) -> usize {
    let crc = digest::crc32c(&buf[..hlen]);
    buf[hlen..hlen + 4].copy_from_slice(&crc.to_le_bytes());
    hlen + 4
}

/// Pack the negotiated digest flags for an ICReq/ICResp `digest` byte.
fn digest_bits(hdgst: bool, ddgst: bool) -> u8 {
    let mut bits = 0;
    if hdgst {
        bits |= DIGEST_HDGST;
    }
    if ddgst {
        bits |= DIGEST_DDGST;
    }
    bits
}

/// Encode an ICReq (host role; used by the test client).
pub fn encode_icreq(buf: &mut [u8], hdgst: bool, ddgst: bool, maxr2t: u32) -> usize {
    let pdu = IcReq {
        hdr: common(pdu_type::ICREQ, 0, 128, 0, 128),
        pfv: U16::new(PFV_1_0),
        hpda: 0,
        digest: digest_bits(hdgst, ddgst),
        maxr2t: U32::new(maxr2t),
        rsvd: [0; 112],
    };
    buf[..128].copy_from_slice(pdu.as_bytes());
    128
}

/// Encode an ICResp granting the negotiated digests.
pub fn encode_icresp(buf: &mut [u8], hdgst: bool, ddgst: bool, maxdata: u32) -> usize {
    let pdu = IcResp {
        hdr: common(pdu_type::ICRESP, 0, 128, 0, 128),
        pfv: U16::new(PFV_1_0),
        cpda: 0,
        digest: digest_bits(hdgst, ddgst),
        maxdata: U32::new(maxdata),
        rsvd: [0; 112],
    };
    buf[..128].copy_from_slice(pdu.as_bytes());
    128
}

/// Encode a command capsule header (test client; in-capsule data is
/// appended by the caller, who must include it in `data_len` and add the
/// DDGST itself when negotiated).
pub fn encode_capsule_cmd(
    buf: &mut [u8],
    sqe: &Sqe,
    hdgst: bool,
    data_len: u32,
    ddgst: bool,
) -> usize {
    let hdgst_len = u32::from(hdgst) * 4;
    let ddgst_len = u32::from(ddgst && data_len > 0) * 4;
    let plen = 72 + hdgst_len + data_len + ddgst_len;
    let mut flags = 0;
    if hdgst {
        flags |= pdu_flags::HDGST;
    }
    if ddgst && data_len > 0 {
        flags |= pdu_flags::DDGST;
    }
    let pdo = if data_len > 0 { 72 + hdgst_len } else { 0 };
    #[allow(clippy::cast_possible_truncation)]
    let pdu = CmdCapsule {
        hdr: common(pdu_type::CAPSULE_CMD, flags, 72, pdo as u8, plen),
        sqe: *sqe,
    };
    buf[..72].copy_from_slice(pdu.as_bytes());
    if hdgst { put_hdgst(buf, 72) } else { 72 }
}

/// Encode a response capsule.
pub fn encode_capsule_resp(buf: &mut [u8], cqe: &Cqe, hdgst: bool) -> usize {
    let plen = 24 + u32::from(hdgst) * 4;
    let flags = if hdgst { pdu_flags::HDGST } else { 0 };
    let pdu = RspCapsule {
        hdr: common(pdu_type::CAPSULE_RESP, flags, 24, 0, plen),
        cqe: *cqe,
    };
    buf[..24].copy_from_slice(pdu.as_bytes());
    if hdgst { put_hdgst(buf, 24) } else { 24 }
}

/// Encode an H2C/C2H DataPdu header (shared 24-byte layout); `base_flags`
/// carries the direction-specific bits (DATA_LAST, DATA_SUCCESS). Payload
/// (and DDGST when enabled) follows.
#[allow(clippy::too_many_arguments)]
fn encode_data_pdu(
    buf: &mut [u8],
    pdu_type: u8,
    base_flags: u8,
    cid: u16,
    ttag: u16,
    data_offset: u32,
    data_length: u32,
    hdgst: bool,
    ddgst: bool,
) -> usize {
    let hdgst_len = u32::from(hdgst) * 4;
    let ddgst_len = u32::from(ddgst) * 4;
    let mut flags = base_flags;
    if hdgst {
        flags |= pdu_flags::HDGST;
    }
    if ddgst {
        flags |= pdu_flags::DDGST;
    }
    #[allow(clippy::cast_possible_truncation)]
    let pdu = DataPdu {
        hdr: common(
            pdu_type,
            flags,
            24,
            (24 + hdgst_len) as u8,
            24 + hdgst_len + data_length + ddgst_len,
        ),
        cid: U16::new(cid),
        ttag: U16::new(ttag),
        data_offset: U32::new(data_offset),
        data_length: U32::new(data_length),
        rsvd: [0; 4],
    };
    buf[..24].copy_from_slice(pdu.as_bytes());
    if hdgst { put_hdgst(buf, 24) } else { 24 }
}

/// Encode a C2HData header; payload (and DDGST when enabled) follows.
#[allow(clippy::too_many_arguments)]
pub fn encode_c2h_data(
    buf: &mut [u8],
    cid: u16,
    data_offset: u32,
    data_length: u32,
    last: bool,
    success: bool,
    hdgst: bool,
    ddgst: bool,
) -> usize {
    let mut base_flags = 0;
    if last {
        base_flags |= pdu_flags::DATA_LAST;
    }
    if success {
        base_flags |= pdu_flags::DATA_SUCCESS;
    }
    encode_data_pdu(
        buf,
        pdu_type::C2H_DATA,
        base_flags,
        cid,
        0,
        data_offset,
        data_length,
        hdgst,
        ddgst,
    )
}

/// Encode an R2T requesting `length` bytes at `offset` for `ttag`.
pub fn encode_r2t(
    buf: &mut [u8],
    cid: u16,
    ttag: u16,
    offset: u32,
    length: u32,
    hdgst: bool,
) -> usize {
    let plen = 24 + u32::from(hdgst) * 4;
    let flags = if hdgst { pdu_flags::HDGST } else { 0 };
    let pdu = DataPdu {
        hdr: common(pdu_type::R2T, flags, 24, 0, plen),
        cid: U16::new(cid),
        ttag: U16::new(ttag),
        data_offset: U32::new(offset),
        data_length: U32::new(length),
        rsvd: [0; 4],
    };
    buf[..24].copy_from_slice(pdu.as_bytes());
    if hdgst { put_hdgst(buf, 24) } else { 24 }
}

/// Encode an H2CData header (test client write path).
#[allow(clippy::too_many_arguments)]
pub fn encode_h2c_data(
    buf: &mut [u8],
    cid: u16,
    ttag: u16,
    data_offset: u32,
    data_length: u32,
    last: bool,
    hdgst: bool,
    ddgst: bool,
) -> usize {
    let base_flags = if last { pdu_flags::DATA_LAST } else { 0 };
    encode_data_pdu(
        buf,
        pdu_type::H2C_DATA,
        base_flags,
        cid,
        ttag,
        data_offset,
        data_length,
        hdgst,
        ddgst,
    )
}

/// Encode a C2HTermReq. We never attach the offending header copy.
pub fn encode_c2h_term(buf: &mut [u8], error: PduError) -> usize {
    let pdu = TermPdu {
        hdr: common(pdu_type::C2H_TERM, 0, 24, 0, 24),
        fes: U16::new(error.fes),
        fei: U32::new(error.fei),
        rsvd: [0; 10],
    };
    buf[..24].copy_from_slice(pdu.as_bytes());
    24
}
