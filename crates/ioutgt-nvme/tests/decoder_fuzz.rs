#![allow(clippy::cast_possible_truncation)] // PRNG-derived test indices, all bounded

//! Deterministic property fuzz of the PDU decoder: arbitrary bytes in
//! arbitrary fragment sizes must never panic, and every accepted header
//! must satisfy the decoder's own invariants. (Scaffolding for a
//! libFuzzer target later; this seeded version runs in every `cargo
//! test`.)

use ioutgt_nvme::pdu::PduDecoder;

struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn byte(&mut self) -> u8 {
        (self.next() >> 32) as u8
    }

    fn range(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

/// Drive a decoder over `data` in random fragments; consume declared
/// payloads as a transport would. Returns the number of PDUs accepted.
fn drive(decoder_hdgst: bool, data: &[u8], rng: &mut XorShift) -> usize {
    let mut decoder = PduDecoder::new(decoder_hdgst);
    let mut accepted = 0;
    let mut pos = 0;
    let mut skip = 0usize;
    while pos < data.len() {
        let chunk = 1 + rng.range(97) as usize;
        let end = (pos + chunk).min(data.len());
        let mut slice = &data[pos..end];
        pos = end;
        while !slice.is_empty() {
            if skip > 0 {
                let take = skip.min(slice.len());
                skip -= take;
                slice = &slice[take..];
                continue;
            }
            match decoder.feed(slice) {
                Ok(consumed) => {
                    slice = &slice[consumed..];
                    if decoder.is_complete() {
                        match decoder.take() {
                            Ok(decoded) => {
                                accepted += 1;
                                // Invariant: payloads are bounded by the
                                // decoder's own PLEN cap.
                                assert!(decoded.data_len <= 32 * 1024 * 1024);
                                skip =
                                    decoded.data_len as usize + if decoded.ddgst { 4 } else { 0 };
                            }
                            Err(_) => return accepted, // decoder rejected: done
                        }
                    } else if slice.is_empty() {
                        break;
                    }
                }
                Err(_) => return accepted,
            }
        }
    }
    accepted
}

#[test]
fn random_bytes_never_panic() {
    let mut rng = XorShift(0x9E37_79B9_7F4A_7C15);
    for round in 0..2_000 {
        let len = 1 + rng.range(4096) as usize;
        let data: Vec<u8> = (0..len).map(|_| rng.byte()).collect();
        let hdgst = round % 2 == 0;
        drive(hdgst, &data, &mut rng);
    }
}

#[test]
fn mutated_valid_frames_never_panic() {
    let mut rng = XorShift(0xDEAD_BEEF_CAFE_F00D);
    // A valid little session: ICReq + capsule cmd with payload + R2T.
    let mut session = Vec::new();
    let mut buf = [0u8; 256];
    let n = ioutgt_nvme::pdu::encode_icreq(&mut buf, true, true, 4);
    session.extend_from_slice(&buf[..n]);
    let sqe = ioutgt_nvme::spec::Sqe::zeroed();
    let n = ioutgt_nvme::pdu::encode_capsule_cmd(&mut buf, &sqe, false, 512, false);
    session.extend_from_slice(&buf[..n]);
    session.extend_from_slice(&[0xA5; 512]);
    let n = ioutgt_nvme::pdu::encode_r2t(&mut buf, 1, 2, 0, 4096, false);
    session.extend_from_slice(&buf[..n]);

    for _ in 0..20_000 {
        let mut mutated = session.clone();
        // 1..=8 random byte mutations.
        for _ in 0..=rng.range(8) {
            let idx = rng.range(mutated.len() as u64) as usize;
            mutated[idx] = rng.byte();
        }
        drive(false, &mutated, &mut rng);
        drive(true, &mutated, &mut rng);
    }
}
