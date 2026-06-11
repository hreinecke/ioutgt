#![allow(clippy::cast_possible_truncation)] // qd/percentile indices are small and bounded

//! Raw NVMe/TCP load generator for target benchmarking on loopback.
//!
//! fio through the VM rides slirp (userspace NAT) and bottlenecks long
//! before the target does; this client speaks the wire format directly
//! through the sans-io codec, pipelines a fixed queue depth per
//! connection, and reports IOPS plus latency percentiles.
//!
//!   cargo run --release --example loadgen -- \
//!       --addr 127.0.0.1:4420 --conns 4 --qd 32 --bs 4096 --secs 10 --rw randread

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use ioutgt_nvme::fabrics::{ConnectCommand, ConnectData, fctype};
use ioutgt_nvme::pdu::{self, PduDecoder, PduKind};
use ioutgt_nvme::{spec, status};
use zerocopy::{FromBytes, FromZeros, IntoBytes};

const NQN: &str = "nqn.2026-06.io.ioutgt:test";
const HOSTNQN: &str = "nqn.2014-08.org.nvmexpress:uuid:feedface-0000-4000-8000-000000000001";

struct Args {
    addr: String,
    conns: usize,
    qd: usize,
    bs: u32,
    secs: u64,
    write: bool,
}

fn parse_args() -> Args {
    let mut args = Args {
        addr: "127.0.0.1:4420".into(),
        conns: 4,
        qd: 32,
        bs: 4096,
        secs: 10,
        write: false,
    };
    let mut iter = std::env::args().skip(1);
    while let Some(flag) = iter.next() {
        let mut value = || iter.next().expect("flag value");
        match flag.as_str() {
            "--addr" => args.addr = value(),
            "--conns" => args.conns = value().parse().unwrap(),
            "--qd" => args.qd = value().parse().unwrap(),
            "--bs" => args.bs = value().parse().unwrap(),
            "--secs" => args.secs = value().parse().unwrap(),
            "--rw" => args.write = value() == "randwrite",
            other => panic!("unknown flag {other}"),
        }
    }
    args
}

fn handshake(addr: &str) -> TcpStream {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream.set_nodelay(true).unwrap();
    let mut buf = [0u8; 128];
    let n = pdu::encode_icreq(&mut buf, false, false, 4);
    stream.write_all(&buf[..n]).unwrap();
    let mut resp = [0u8; 128];
    stream.read_exact(&mut resp).unwrap();
    stream
}

fn read_pdu(
    stream: &mut TcpStream,
    decoder: &mut PduDecoder,
    scratch: &mut [u8],
) -> pdu::DecodedPdu {
    let mut byte = [0u8; 1];
    loop {
        stream.read_exact(&mut byte).expect("pdu byte");
        decoder.feed(&byte).expect("decode");
        if decoder.is_complete() {
            let decoded = decoder.take().expect("take");
            let mut left = decoded.data_len as usize + if decoded.ddgst { 4 } else { 0 };
            while left > 0 {
                let take = left.min(scratch.len());
                stream.read_exact(&mut scratch[..take]).expect("payload");
                left -= take;
            }
            return decoded;
        }
    }
}

fn nvme_connect(stream: &mut TcpStream, qid: u16, sqsize: u16, cntlid: u16) -> u16 {
    let mut cmd: ConnectCommand = FromZeros::new_zeroed();
    cmd.opcode = spec::admin_opcode::FABRICS;
    cmd.fctype = fctype::CONNECT;
    cmd.cid.set(0);
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
    let mut frame = Vec::new();
    let mut hdr = [0u8; 80];
    let n = pdu::encode_capsule_cmd(&mut hdr, &sqe, false, 1024, false);
    frame.extend_from_slice(&hdr[..n]);
    frame.extend_from_slice(data.as_bytes());
    stream.write_all(&frame).unwrap();

    let mut decoder = PduDecoder::new(false);
    let mut scratch = [0u8; 4096];
    let decoded = read_pdu(stream, &mut decoder, &mut scratch);
    let PduKind::CapsuleResp(cqe) = decoded.kind else {
        panic!("expected connect resp")
    };
    assert_eq!(cqe.status.get() >> 1, status::SUCCESS, "connect qid {qid}");
    u16::try_from(cqe.result.get() & 0xFFFF).unwrap()
}

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
}

#[allow(clippy::too_many_arguments)]
fn worker(
    addr: String,
    qid: u16,
    cntlid: u16,
    qd: usize,
    bs: u32,
    write: bool,
    stop: Arc<AtomicBool>,
    total_ops: Arc<AtomicU64>,
    seed: u64,
) -> Vec<u64> {
    let mut stream = handshake(&addr);
    nvme_connect(&mut stream, qid, 64, cntlid);
    eprintln!("# worker qid={qid} connected");
    let mut rx = stream.try_clone().expect("clone");

    let blocks_per_io = u64::from(bs / 512);
    let device_blocks: u64 = (16 << 20) / 512; // matches loadgen target config
    let nlb0 = u16::try_from(blocks_per_io - 1).unwrap();
    let payload = vec![0xA5u8; bs as usize];
    let mut rng = XorShift(seed | 1);

    // Latency bookkeeping per CID slot.
    let starts: Arc<Vec<AtomicU64>> = Arc::new((0..qd).map(|_| AtomicU64::new(0)).collect());
    let epoch = Instant::now();

    // RX side: drain responses, record latency, signal slot free.
    let free = Arc::new(std::sync::Mutex::new((0..qd as u16).collect::<Vec<u16>>()));
    let condvar = Arc::new(std::sync::Condvar::new());
    let latencies = Arc::new(std::sync::Mutex::new(Vec::<u64>::with_capacity(1 << 20)));

    let rx_thread = {
        let free = Arc::clone(&free);
        let condvar = Arc::clone(&condvar);
        let starts = Arc::clone(&starts);
        let latencies = Arc::clone(&latencies);
        let total_ops = Arc::clone(&total_ops);
        std::thread::spawn(move || {
            // Bulk reads + in-buffer parsing: the byte-at-a-time variant
            // costs ~26 syscalls/op and caps a connection near 40K IOPS,
            // turning the client into the bottleneck under test.
            let mut decoder = PduDecoder::new(false);
            let mut buf = vec![0u8; 256 * 1024];
            let mut skip = 0usize;
            loop {
                let n = match rx.read(&mut buf) {
                    Ok(0) | Err(_) => return,
                    Ok(n) => n,
                };
                let mut slice = &buf[..n];
                while !slice.is_empty() {
                    if skip > 0 {
                        let take = skip.min(slice.len());
                        skip -= take;
                        slice = &slice[take..];
                        continue;
                    }
                    let consumed = decoder.feed(slice).expect("decode");
                    slice = &slice[consumed..];
                    if !decoder.is_complete() {
                        debug_assert!(slice.is_empty());
                        continue;
                    }
                    let decoded = decoder.take().expect("take");
                    skip = decoded.data_len as usize + if decoded.ddgst { 4 } else { 0 };
                    if let PduKind::CapsuleResp(cqe) = decoded.kind {
                        assert_eq!(cqe.status.get() >> 1, 0, "IO failed");
                        let cid = cqe.cid.get();
                        let started = starts[usize::from(cid)].load(Ordering::Relaxed);
                        let now = u64::try_from(epoch.elapsed().as_nanos()).unwrap_or(u64::MAX);
                        latencies.lock().unwrap().push(now - started);
                        total_ops.fetch_add(1, Ordering::Relaxed);
                        free.lock().unwrap().push(cid);
                        condvar.notify_one();
                    }
                }
            }
        })
    };

    // TX side: keep QD outstanding.
    while !stop.load(Ordering::Relaxed) {
        let cid = {
            let mut guard = free.lock().unwrap();
            while guard.is_empty() {
                let (g, timeout) = condvar
                    .wait_timeout(guard, Duration::from_millis(100))
                    .unwrap();
                guard = g;
                if timeout.timed_out() && stop.load(Ordering::Relaxed) {
                    drop(guard);
                    // try_clone'd fds keep the socket open: shut it down
                    // so the blocked RX recv returns.
                    let _ = stream.shutdown(std::net::Shutdown::Both);
                    rx_thread.join().ok();
                    return Arc::try_unwrap(latencies).unwrap().into_inner().unwrap();
                }
            }
            guard.pop().unwrap()
        };
        let max_slba = device_blocks - blocks_per_io;
        let slba = rng.next() % (max_slba + 1);
        let opcode = if write {
            spec::io_opcode::WRITE
        } else {
            spec::io_opcode::READ
        };
        let mut sqe = spec::Sqe::zeroed();
        sqe.opcode = opcode;
        sqe.flags = spec::CMD_FLAGS_SGL_METABUF;
        sqe.cid.set(cid);
        sqe.nsid.set(1);
        #[allow(clippy::cast_possible_truncation)]
        sqe.cdw10.set(slba as u32);
        sqe.cdw11.set(u32::try_from(slba >> 32).unwrap());
        sqe.cdw12.set(u32::from(nlb0));
        sqe.dptr.length.set(bs);
        sqe.dptr.sgl_type = if write && bs <= 16 * 1024 {
            spec::sgl::TYPE_DATA_BLOCK_OFFSET
        } else {
            spec::sgl::TYPE_TRANSPORT_DATA_BLOCK
        };

        let mut frame = Vec::with_capacity(72 + payload.len());
        let mut hdr = [0u8; 80];
        let inline = if write && bs <= 16 * 1024 { bs } else { 0 };
        let n = pdu::encode_capsule_cmd(&mut hdr, &sqe, false, inline, false);
        frame.extend_from_slice(&hdr[..n]);
        if inline > 0 {
            frame.extend_from_slice(&payload);
        }
        let now = u64::try_from(epoch.elapsed().as_nanos()).unwrap_or(u64::MAX);
        starts[usize::from(cid)].store(now, Ordering::Relaxed);
        if stream.write_all(&frame).is_err() {
            break;
        }
        // Large writes: target sends R2T; the RX thread would have to
        // hand it back. Keep the loadgen to reads + inline writes.
        assert!(
            inline > 0 || !write,
            "loadgen supports reads and inline writes only"
        );
    }
    let _ = stream.shutdown(std::net::Shutdown::Both);
    let _ = rx_thread.join();
    Arc::try_unwrap(latencies).unwrap().into_inner().unwrap()
}

fn main() {
    let args = parse_args();
    assert!(
        args.qd <= 60,
        "qd must fit the negotiated sqsize (64) minus headroom"
    );

    // Admin connection holds the controller open.
    let mut admin = handshake(&args.addr);
    let cntlid = nvme_connect(&mut admin, 0, 32, 0xFFFF);
    eprintln!("# admin connected, cntlid={cntlid}");

    let stop = Arc::new(AtomicBool::new(false));
    let total_ops = Arc::new(AtomicU64::new(0));
    let started = Instant::now();

    let workers: Vec<_> = (0..args.conns)
        .map(|i| {
            let addr = args.addr.clone();
            let stop = Arc::clone(&stop);
            let total_ops = Arc::clone(&total_ops);
            let (qd, bs, write) = (args.qd, args.bs, args.write);
            std::thread::spawn(move || {
                worker(
                    addr,
                    u16::try_from(i + 1).unwrap(),
                    cntlid,
                    qd,
                    bs,
                    write,
                    stop,
                    total_ops,
                    0x1234_5678 + i as u64,
                )
            })
        })
        .collect();

    std::thread::sleep(Duration::from_secs(args.secs));
    eprintln!("# stopping");
    stop.store(true, Ordering::Relaxed);

    let mut latencies: Vec<u64> = Vec::new();
    for worker in workers {
        latencies.extend(worker.join().expect("worker"));
    }
    let elapsed = started.elapsed().as_secs_f64();
    let ops = total_ops.load(Ordering::Relaxed);
    latencies.sort_unstable();
    let pct = |p: f64| -> f64 {
        if latencies.is_empty() {
            return 0.0;
        }
        let idx = ((latencies.len() as f64 - 1.0) * p) as usize;
        latencies[idx] as f64 / 1000.0
    };
    println!(
        "ops={ops} iops={:.0} bw={:.1} MiB/s lat_us p50={:.1} p99={:.1} p999={:.1} (conns={} qd={} bs={} rw={})",
        ops as f64 / elapsed,
        ops as f64 / elapsed * f64::from(args.bs) / (1 << 20) as f64,
        pct(0.50),
        pct(0.99),
        pct(0.999),
        args.conns,
        args.qd,
        args.bs,
        if args.write { "randwrite" } else { "randread" },
    );
}
