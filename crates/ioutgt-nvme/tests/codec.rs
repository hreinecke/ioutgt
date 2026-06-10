//! PDU codec round-trips, fragmentation torture, and digest vectors.

use ioutgt_nvme::digest;
use ioutgt_nvme::pdu::{self, PduDecoder, PduKind};
use ioutgt_nvme::spec::{Cqe, Sqe};

/// Drive a decoder over `wire` in fragments of `chunk` bytes; collect all
/// decoded PDUs, skipping payload bytes the way a transport would.
fn decode_all(wire: &[u8], chunk: usize, hdr_digest: bool) -> Vec<pdu::DecodedPdu> {
    let mut decoder = PduDecoder::new(hdr_digest);
    let mut out = Vec::new();
    let mut pos = 0;
    let mut skip = 0usize; // payload + ddgst bytes owed to the current PDU
    while pos < wire.len() {
        let end = (pos + chunk).min(wire.len());
        let mut slice = &wire[pos..end];
        pos = end;
        while !slice.is_empty() {
            if skip > 0 {
                let n = skip.min(slice.len());
                skip -= n;
                slice = &slice[n..];
                continue;
            }
            let consumed = decoder.feed(slice).expect("decode error");
            slice = &slice[consumed..];
            if decoder.is_complete() {
                let decoded = decoder.take().expect("take error");
                skip = decoded.data_len as usize + if decoded.ddgst { 4 } else { 0 };
                out.push(decoded);
            } else {
                assert!(slice.is_empty(), "feed stopped early without completion");
            }
        }
    }
    out
}

#[test]
fn crc32c_known_answer() {
    // Standard CRC32C check value.
    assert_eq!(digest::crc32c(b"123456789"), 0xE306_9283);
    let mut inc = digest::Crc32c::new();
    inc.update(b"1234");
    inc.update(b"56789");
    assert_eq!(inc.finalize(), 0xE306_9283);
}

#[test]
fn icreq_icresp_roundtrip() {
    let mut buf = [0u8; 256];
    let n = pdu::encode_icreq(&mut buf, true, true, 4);
    assert_eq!(n, 128);
    let decoded = decode_all(&buf[..n], 128, false);
    assert_eq!(decoded.len(), 1);
    match decoded[0].kind {
        PduKind::IcReq(ic) => {
            assert_eq!(ic.pfv.get(), pdu::PFV_1_0);
            assert_eq!(ic.hpda, 0);
            assert_eq!(ic.digest, pdu::DIGEST_HDGST | pdu::DIGEST_DDGST);
            assert_eq!(ic.maxr2t.get(), 4);
        }
        ref other => panic!("expected ICReq, got {other:?}"),
    }

    let n = pdu::encode_icresp(&mut buf, false, true, 0x40_0000);
    let decoded = decode_all(&buf[..n], 1, false);
    match decoded[0].kind {
        PduKind::IcResp(ic) => {
            assert_eq!(ic.digest, pdu::DIGEST_DDGST);
            assert_eq!(ic.maxdata.get(), 0x40_0000);
        }
        ref other => panic!("expected ICResp, got {other:?}"),
    }
}

#[test]
fn capsule_cmd_with_inline_data_all_fragmentations() {
    let mut sqe = Sqe::zeroed();
    sqe.opcode = 0x01; // write
    sqe.cid.set(0x1234);
    sqe.nsid.set(1);

    let payload = [0xA5u8; 512];
    let mut wire = Vec::new();
    let mut hdr = [0u8; 80];
    let n = pdu::encode_capsule_cmd(&mut hdr, &sqe, true, 512, true);
    wire.extend_from_slice(&hdr[..n]);
    wire.extend_from_slice(&payload);
    let mut ddgst = digest::Crc32c::new();
    ddgst.update(&payload);
    wire.extend_from_slice(&ddgst.finalize().to_le_bytes());

    for chunk in [1, 2, 3, 7, 8, 9, 71, 72, 73, 76, wire.len()] {
        let decoded = decode_all(&wire, chunk, true);
        assert_eq!(decoded.len(), 1, "chunk={chunk}");
        assert_eq!(decoded[0].data_len, 512);
        assert!(decoded[0].ddgst);
        match decoded[0].kind {
            PduKind::CapsuleCmd(got) => {
                assert_eq!(got.opcode, 0x01);
                assert_eq!(got.cid.get(), 0x1234);
                assert_eq!(got.nsid.get(), 1);
            }
            ref other => panic!("expected CapsuleCmd, got {other:?}"),
        }
    }
}

#[test]
fn response_r2t_c2h_data_roundtrip() {
    let mut buf = [0u8; 64];

    let cqe = Cqe::new(0xDEAD_BEEF, 7, 1, 0x42, 0);
    let n = pdu::encode_capsule_resp(&mut buf, &cqe, true);
    let decoded = decode_all(&buf[..n], 1, true);
    match decoded[0].kind {
        PduKind::CapsuleResp(got) => {
            assert_eq!(got.result.get(), 0xDEAD_BEEF);
            assert_eq!(got.sq_head.get(), 7);
            assert_eq!(got.cid.get(), 0x42);
            assert_eq!(got.status.get(), 0);
        }
        ref other => panic!("expected CapsuleResp, got {other:?}"),
    }

    let n = pdu::encode_r2t(&mut buf, 0x11, 9, 8192, 65536, true);
    let decoded = decode_all(&buf[..n], 5, true);
    match decoded[0].kind {
        PduKind::R2T {
            cid,
            ttag,
            offset,
            length,
        } => {
            assert_eq!((cid, ttag, offset, length), (0x11, 9, 8192, 65536));
        }
        ref other => panic!("expected R2T, got {other:?}"),
    }

    // C2HData with payload, last+success set.
    let payload = [0x5Au8; 100];
    let mut wire = Vec::new();
    let n = pdu::encode_c2h_data(&mut buf, 0x33, 0, 100, true, true, true, false);
    wire.extend_from_slice(&buf[..n]);
    wire.extend_from_slice(&payload);
    let decoded = decode_all(&wire, 9, true);
    match decoded[0].kind {
        PduKind::C2HData {
            cid,
            offset,
            length,
            last,
            success,
        } => {
            assert_eq!(
                (cid, offset, length, last, success),
                (0x33, 0, 100, true, true)
            );
        }
        ref other => panic!("expected C2HData, got {other:?}"),
    }
    assert_eq!(decoded[0].data_len, 100);
}

#[test]
fn back_to_back_pdus_across_fragments() {
    // R2T + response + H2CData header concatenated, fed in 3-byte chunks.
    let mut wire = Vec::new();
    let mut buf = [0u8; 64];
    let n = pdu::encode_r2t(&mut buf, 1, 2, 0, 4096, false);
    wire.extend_from_slice(&buf[..n]);
    let n = pdu::encode_capsule_resp(&mut buf, &Cqe::new(0, 1, 1, 1, 0), false);
    wire.extend_from_slice(&buf[..n]);
    let payload = [1u8; 32];
    let n = pdu::encode_h2c_data(&mut buf, 5, 6, 0, 32, true, false, false);
    wire.extend_from_slice(&buf[..n]);
    wire.extend_from_slice(&payload);

    let decoded = decode_all(&wire, 3, false);
    assert_eq!(decoded.len(), 3);
    assert!(matches!(decoded[0].kind, PduKind::R2T { .. }));
    assert!(matches!(decoded[1].kind, PduKind::CapsuleResp(_)));
    match decoded[2].kind {
        PduKind::H2CData {
            cid,
            ttag,
            length,
            last,
            ..
        } => {
            assert_eq!((cid, ttag, length, last), (5, 6, 32, true));
        }
        ref other => panic!("expected H2CData, got {other:?}"),
    }
}

#[test]
fn corrupted_header_digest_rejected() {
    let mut buf = [0u8; 64];
    let n = pdu::encode_r2t(&mut buf, 1, 2, 0, 4096, true);
    buf[10] ^= 0xFF; // corrupt a header byte after the digest was computed
    let mut decoder = PduDecoder::new(true);
    let consumed = decoder
        .feed(&buf[..n])
        .expect("header assembly is digest-agnostic");
    assert_eq!(consumed, n);
    let err = decoder.take().expect_err("corrupted digest must fail");
    assert_eq!(err.fes, pdu::fes::HDR_DIGEST_ERR);
}

#[test]
fn unknown_pdu_type_rejected() {
    let mut decoder = PduDecoder::new(false);
    let bogus = [0xEEu8, 0, 24, 0, 24, 0, 0, 0];
    let err = decoder.feed(&bogus).expect_err("unknown type must fail");
    assert_eq!(err.fes, pdu::fes::INVALID_PDU_HDR);
    assert_eq!(err.fei, 0); // offending field: type at offset 0
}

#[test]
fn truncated_plen_rejected() {
    // CapsuleCmd claiming plen < header size.
    let mut decoder = PduDecoder::new(false);
    let bad = [0x04u8, 0, 72, 0, 16, 0, 0, 0];
    let err = decoder.feed(&bad).expect_err("plen < header must fail");
    assert_eq!(err.fes, pdu::fes::INVALID_PDU_HDR);
}

#[test]
fn term_pdu_roundtrip() {
    let mut buf = [0u8; 64];
    let n = pdu::encode_c2h_term(
        &mut buf,
        pdu::PduError {
            fes: pdu::fes::HDR_DIGEST_ERR,
            fei: 3,
        },
    );
    assert_eq!(n, 24);
    // C2HTerm decodes through the same path as H2CTerm.
    let decoded = decode_all(&buf[..n], 4, false);
    match decoded[0].kind {
        PduKind::H2CTerm { fes, fei } => {
            assert_eq!(fes, pdu::fes::HDR_DIGEST_ERR);
            assert_eq!(fei, 3);
        }
        ref other => panic!("expected term, got {other:?}"),
    }
}

#[test]
fn connect_capsule_layout_matches_spec_offsets() {
    use ioutgt_nvme::fabrics::{ConnectCommand, ConnectData};
    use zerocopy::IntoBytes;

    let mut cmd: ConnectCommand = zerocopy::FromZeros::new_zeroed();
    cmd.opcode = 0x7F;
    cmd.fctype = 0x01;
    cmd.qid.set(0xABCD);
    cmd.sqsize.set(127);
    cmd.kato.set(15000);
    let bytes = cmd.as_bytes();
    assert_eq!(bytes.len(), 64);
    assert_eq!(bytes[0], 0x7F);
    assert_eq!(bytes[4], 0x01);
    assert_eq!(&bytes[42..44], &0xABCDu16.to_le_bytes()); // qid at 42
    assert_eq!(&bytes[44..46], &127u16.to_le_bytes()); // sqsize at 44
    assert_eq!(&bytes[48..52], &15000u32.to_le_bytes()); // kato at 48

    let mut data = ConnectData::zeroed();
    data.cntlid.set(0xFFFF);
    data.subsysnqn[..4].copy_from_slice(b"nqn.");
    let bytes = data.as_bytes();
    assert_eq!(bytes.len(), 1024);
    assert_eq!(&bytes[16..18], &[0xFF, 0xFF]); // cntlid at 16
    assert_eq!(&bytes[256..260], b"nqn."); // subsysnqn at 256
}
