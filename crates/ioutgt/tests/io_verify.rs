#![allow(clippy::cast_possible_truncation)] // test indices bounded by SIZES/BLOCK constants

//! Data-integrity torture: concurrent connections, mixed transfer
//! sizes across both write paths (in-capsule and R2T), interleaved
//! ownership stripes, full read-back verification with per-LBA
//! deterministic patterns — the in-process counterpart of the VM's
//! `fio --verify` stage, able to catch cross-slot and cross-connection
//! corruption that single-stream tests cannot.

mod common;

use common::{Client, NQN, rw_sqe};
use ioutgt_nvme::pdu::PduKind;
use ioutgt_nvme::{spec, status};

const BLOCK: u64 = 512;
/// Transfer sizes covering 1 block, inline max boundary, R2T sizes.
const SIZES: &[u32] = &[512, 4096, 16_384, 20_480, 65_536, 131_072];

/// Deterministic pattern: every 512-byte block is filled from its LBA
/// and a generation tag, so any misplaced or stale block is detected.
fn fill_pattern(buf: &mut [u8], slba: u64, generation: u8) {
    for (block_index, chunk) in buf.chunks_mut(BLOCK as usize).enumerate() {
        let lba = slba + block_index as u64;
        let seed = lba
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(u64::from(generation));
        let bytes = seed.to_le_bytes();
        for (i, b) in chunk.iter_mut().enumerate() {
            *b = bytes[i % 8] ^ (i as u8).wrapping_mul(13);
        }
    }
}

fn write_region(client: &mut Client, cid: u16, slba: u64, len: u32, generation: u8) {
    let mut data = vec![0u8; len as usize];
    fill_pattern(&mut data, slba, generation);
    let nlb0 = u16::try_from(u64::from(len) / BLOCK - 1).unwrap();
    let inline = len <= 16_384;
    let mut sqe = rw_sqe(spec::io_opcode::WRITE, cid, slba, nlb0, len, !inline);
    sqe.nsid.set(1);
    if inline {
        client.send_capsule(&sqe, &data);
    } else {
        // R2T path: wait for the solicitation, deliver in two PDUs to
        // exercise reassembly under concurrency.
        client.send_capsule(&sqe, &[]);
        let (decoded, _) = client.recv_pdu();
        let PduKind::R2T { ttag, length, .. } = decoded.kind else {
            panic!("expected R2T, got {:?}", decoded.kind);
        };
        assert_eq!(length, len);
        let half = (len as usize / 2) & !511;
        client.send_h2c_data(cid, ttag, 0, &data[..half], false);
        client.send_h2c_data(cid, ttag, u32::try_from(half).unwrap(), &data[half..], true);
    }
    let cqe = client.recv_response();
    assert_eq!(
        cqe.status.get() >> 1,
        status::SUCCESS,
        "write slba={slba} len={len}"
    );
}

fn verify_region(client: &mut Client, cid: u16, slba: u64, len: u32, generation: u8) {
    let nlb0 = u16::try_from(u64::from(len) / BLOCK - 1).unwrap();
    let mut sqe = rw_sqe(spec::io_opcode::READ, cid, slba, nlb0, len, true);
    sqe.nsid.set(1);
    client.send_capsule(&sqe, &[]);
    let (decoded, payload) = client.recv_pdu();
    assert!(
        matches!(decoded.kind, PduKind::C2HData { .. }),
        "expected C2HData, got {:?}",
        decoded.kind
    );
    let cqe = client.recv_response();
    assert_eq!(
        cqe.status.get() >> 1,
        status::SUCCESS,
        "read slba={slba} len={len}"
    );
    let mut expect = vec![0u8; len as usize];
    fill_pattern(&mut expect, slba, generation);
    assert_eq!(
        payload, expect,
        "verify failed at slba={slba} len={len} gen={generation}"
    );
}

fn run_verify(hdgst: bool, ddgst: bool) {
    let mut config = ioutgt::TargetConfig::single_memory(NQN, 64);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 2;
    let addr = ioutgt::spawn_target(config).expect("target start");

    let mut admin = Client::handshake(addr, hdgst, ddgst);
    let cntlid = admin.connect(0, 32, 0xFFFF, 1);

    // Two IO queues on (potentially) two different queue threads, each
    // writing an interleaved stripe of the same region: connection A
    // owns even stripes, B owns odd ones. 256 stripes of up to 128K.
    let region_blocks = 131_072 / BLOCK; // stripe pitch: 256 blocks
    let handles: Vec<_> = [(1u16, 0u64), (2u16, 1u64)]
        .into_iter()
        .map(|(qid, parity)| {
            let mut io = Client::handshake(addr, hdgst, ddgst);
            io.connect(qid, 64, cntlid, 1);
            std::thread::spawn(move || {
                let mut cid = 10u16;
                for stripe in 0..128u64 {
                    let index = stripe * 2 + parity;
                    let slba = index * region_blocks;
                    let len = SIZES[(index as usize) % SIZES.len()];
                    write_region(&mut io, cid, slba, len, 1);
                    cid = cid.wrapping_add(1).max(10);
                }
                io
            })
        })
        .collect();
    let mut clients: Vec<Client> = handles
        .into_iter()
        .map(|h| h.join().expect("writer"))
        .collect();

    // Cross-verify: each connection reads back the OTHER's stripes.
    for (reader_index, parity) in [(0usize, 1u64), (1usize, 0u64)] {
        let io = &mut clients[reader_index];
        let mut cid = 200u16;
        for stripe in 0..128u64 {
            let index = stripe * 2 + parity;
            let slba = index * region_blocks;
            let len = SIZES[(index as usize) % SIZES.len()];
            verify_region(io, cid, slba, len, 1);
            cid = cid.wrapping_add(1).max(200);
        }
    }

    // Overwrite a band with a new generation through one connection and
    // confirm both the new data and that neighbours kept generation 1.
    write_region(&mut clients[0], 300, 4 * region_blocks, 131_072, 7);
    verify_region(&mut clients[1], 301, 4 * region_blocks, 131_072, 7);
    let neighbour = 5 * region_blocks;
    let len = SIZES[5 % SIZES.len()];
    verify_region(&mut clients[1], 302, neighbour, len, 1);
}

#[test]
fn concurrent_mixed_size_verify() {
    run_verify(false, false);
}

#[test]
fn concurrent_mixed_size_verify_with_digests() {
    run_verify(true, true);
}
