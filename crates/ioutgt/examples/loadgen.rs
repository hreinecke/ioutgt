#![allow(clippy::cast_possible_truncation)] // qd/percentile indices are small and bounded

//! Raw NVMe/TCP load generator for target benchmarking on loopback.
//!
//! fio through the VM rides slirp (userspace NAT) and bottlenecks long
//! before the target does; this client speaks the wire format directly
//! through the sans-io codec, opens an admin queue plus `--conns` IO
//! queues, pipelines a fixed depth on each, and reports aggregate IOPS,
//! bandwidth, and latency percentiles.
//!
//! # Flags — and the load each shapes
//!
//! - `--addr <ip:port>`  target NVMe/TCP address. Default 127.0.0.1:4420.
//! - `--conns N`  the *width*: N parallel connections, each its own TCP
//!   socket and NVMe IO queue. The target routes qid n to IO thread
//!   `(n-1) % io_threads`, so up to `io_threads` connections run on
//!   distinct threads/CPUs; throughput scales with N until the target's
//!   threads saturate. Default 4. (`--conns 1` isolates one queue / one
//!   IO thread — the per-queue ceiling.)
//! - `--qd N`  the *depth*: N commands kept in flight (pipelined) on each
//!   connection at all times. Total outstanding = `conns × qd`.
//!   Default 32.
//! - `--sqsize N`  the negotiated NVMe queue size (SQSIZE) = the target's
//!   per-queue slot count. Default `0` = auto (= `qd`, one slot per
//!   in-flight command). Must satisfy `qd <= sqsize <= the target's
//!   --io-queue-size` (MAXCMD); a Connect above MAXCMD is rejected. Set
//!   it above `qd` to exercise a large slot allocation under a shallow
//!   in-flight depth.
//! - `--bs N`  block size in bytes per IO. Default 4096 (4 KiB). Served
//!   as: read -> one C2HData PDU; write <= 16 KiB -> in-capsule data;
//!   write > 16 KiB -> transport SGL answered by a single R2T/H2CData
//!   round trip (MAXH2CDATA 16 MiB >> MDTS, so the whole transfer is
//!   solicited at once).
//! - `--secs N`  run duration in seconds. Default 10.
//! - `--rw randread|randwrite`  read or write workload; LBAs are random
//!   within the namespace. Default randread.
//!
//! Net: `conns` connections each pipeline `qd` random `bs`-sized `rw`
//! ops for `secs` seconds.
//!
//! # Examples
//!
//!   # default shape: 4 conns, depth 32, 4 KiB random reads, 10 s
//!   cargo run --release --example loadgen -- \
//!       --addr 127.0.0.1:4420 --conns 4 --qd 32 --bs 4096 --secs 10 --rw randread
//!
//!   # one queue / one IO thread at depth 32 — the single-queue ceiling
//!   cargo run --release --example loadgen -- --conns 1 --qd 32
//!
//!   # deep single queue: 128 in flight (needs target --io-queue-size >= 128)
//!   cargo run --release --example loadgen -- --conns 1 --qd 128
//!
//!   # wide: 16 connections (up to 16 IO threads), depth 32 each
//!   cargo run --release --example loadgen -- --conns 16 --qd 32
//!
//!   # 128 KiB random writes — the large-transfer R2T/H2CData path
//!   cargo run --release --example loadgen -- --bs 131072 --rw randwrite

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use ioutgt_nvme::fabrics::{ConnectCommand, ConnectData, fctype};
use ioutgt_nvme::pdu::{self, PduDecoder, PduKind};
use ioutgt_nvme::{spec, status};
use zerocopy::{FromBytes, FromZeros, IntoBytes};

const NQN: &str = "nqn.2026-06.io.ioutgt:test";
const HOSTNQN: &str = "nqn.2014-08.org.nvmexpress:uuid:feedface-0000-4000-8000-000000000001";

/// Parsed CLI; see the module docs for what each field shapes.
struct Args {
    /// Target `ip:port`.
    addr: String,
    /// Parallel connections / NVMe IO queues (load width).
    conns: usize,
    /// In-flight commands per connection (load depth).
    qd: usize,
    /// Negotiated queue size (slots); `0` = auto (`= qd`).
    sqsize: u16,
    /// Block size in bytes per IO.
    bs: u32,
    /// Run duration in seconds.
    secs: u64,
    /// `true` = randwrite, `false` = randread.
    write: bool,
}

fn parse_args() -> Args {
    let mut args = Args {
        addr: "127.0.0.1:4420".into(),
        conns: 4,
        qd: 32,
        sqsize: 0, // 0 = auto: one slot per in-flight command (= qd)
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
            "--sqsize" => args.sqsize = value().parse().unwrap(),
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
) -> std::io::Result<pdu::DecodedPdu> {
    let mut byte = [0u8; 1];
    loop {
        stream.read_exact(&mut byte)?;
        decoder.feed(&byte).expect("decode");
        if decoder.is_complete() {
            let decoded = decoder.take().expect("take");
            let mut left = decoded.data_len as usize + if decoded.ddgst { 4 } else { 0 };
            while left > 0 {
                let take = left.min(scratch.len());
                stream.read_exact(&mut scratch[..take])?;
                left -= take;
            }
            return Ok(decoded);
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
    let decoded = read_pdu(stream, &mut decoder, &mut scratch).unwrap_or_else(|e| {
        panic!(
            "connect qid={qid} sqsize={sqsize}: target closed the connection ({e}). \
             The negotiated sqsize most likely exceeds the target's advertised \
             --io-queue-size (MAXCMD); lower --qd / --sqsize, or raise the target's \
             --io-queue-size."
        )
    });
    let PduKind::CapsuleResp(cqe) = decoded.kind else {
        panic!("expected connect resp")
    };
    assert_eq!(cqe.status.get() >> 1, status::SUCCESS, "connect qid {qid}");
    u16::try_from(cqe.result.get() & 0xFFFF).unwrap()
}

/// Everything the RX thread hands back to the TX thread. One mpsc
/// channel (instead of two channels or the old Mutex+Condvar free
/// list) keeps the TX loop a single blocking point — `recv_timeout`
/// when idle, `try_recv` drain otherwise — with no busy-wait and no
/// second wakeup primitive. The socket keeps a single writer: R2Ts
/// are decoded on the RX thread but the H2CData answer is written
/// here, so it can never interleave with a capsule mid-PDU.
enum RxEvent {
    /// A CapsuleResp freed this CID slot.
    FreeCid(u16),
    /// The target solicited write data; answer with one H2CData.
    R2t {
        cid: u16,
        ttag: u16,
        offset: u32,
        length: u32,
    },
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
    sqsize: u16,
    bs: u32,
    write: bool,
    stop: Arc<AtomicBool>,
    total_ops: Arc<AtomicU64>,
    seed: u64,
) -> Vec<u64> {
    let mut stream = handshake(&addr);
    nvme_connect(&mut stream, qid, sqsize, cntlid);
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

    // RX side: drain responses, record latency, forward events to TX.
    let (event_tx, event_rx) = mpsc::channel::<RxEvent>();
    let latencies = Arc::new(std::sync::Mutex::new(Vec::<u64>::with_capacity(1 << 20)));

    let rx_thread = {
        let event_tx = event_tx;
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
                    match decoded.kind {
                        PduKind::CapsuleResp(cqe) => {
                            assert_eq!(cqe.status.get() >> 1, 0, "IO failed");
                            let cid = cqe.cid.get();
                            let started = starts[usize::from(cid)].load(Ordering::Relaxed);
                            let now = u64::try_from(epoch.elapsed().as_nanos()).unwrap_or(u64::MAX);
                            latencies.lock().unwrap().push(now - started);
                            total_ops.fetch_add(1, Ordering::Relaxed);
                            if event_tx.send(RxEvent::FreeCid(cid)).is_err() {
                                return;
                            }
                        }
                        PduKind::R2T {
                            cid,
                            ttag,
                            offset,
                            length,
                        } => {
                            // The TX thread owns the socket writes;
                            // forward the solicitation.
                            if event_tx
                                .send(RxEvent::R2t {
                                    cid,
                                    ttag,
                                    offset,
                                    length,
                                })
                                .is_err()
                            {
                                return;
                            }
                        }
                        _ => {}
                    }
                }
            }
        })
    };

    // Answer one R2T: header + payload slice, LAST set, digests off
    // (the icreq negotiated hdgst/ddgst false/false). The target
    // solicits the whole transfer with a single R2T, so one H2CData
    // per command suffices at MDTS sizes.
    fn answer_r2t(
        stream: &mut TcpStream,
        payload: &[u8],
        cid: u16,
        ttag: u16,
        offset: u32,
        length: u32,
    ) -> bool {
        let end = offset as usize + length as usize;
        assert!(end <= payload.len(), "R2T solicits beyond command length");
        // LAST only when this H2CData completes the transfer — fails loud
        // (target's missing-bytes check) if the solicitation strategy
        // ever moves to partial R2Ts.
        let last = end == payload.len();
        let mut hdr = [0u8; 32];
        let n = pdu::encode_h2c_data(&mut hdr, cid, ttag, offset, length, last, false, false);
        let mut frame = Vec::with_capacity(n + length as usize);
        frame.extend_from_slice(&hdr[..n]);
        frame.extend_from_slice(&payload[offset as usize..end]);
        stream.write_all(&frame).is_ok()
    }

    // TX side: keep QD outstanding; also the single socket writer for
    // H2CData answers to R2Ts forwarded by the RX thread.
    //
    // Payload state: loadgen writes one constant 0xA5 pattern, so the
    // shared `payload` buffer serves every in-flight write — no
    // per-CID payload bookkeeping is needed to answer an R2T (the
    // bytes for any CID are identical by construction).
    let mut free: Vec<u16> = (0..qd as u16).collect();
    'tx: while !stop.load(Ordering::Relaxed) {
        // Drain pending events first: an R2T must be answered promptly
        // even when free CIDs are already in hand (the command it
        // belongs to cannot complete until its data is sent).
        loop {
            match event_rx.try_recv() {
                Ok(RxEvent::FreeCid(cid)) => free.push(cid),
                Ok(RxEvent::R2t {
                    cid,
                    ttag,
                    offset,
                    length,
                }) => {
                    if !answer_r2t(&mut stream, &payload, cid, ttag, offset, length) {
                        break 'tx;
                    }
                }
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => break 'tx,
            }
        }
        if free.is_empty() {
            // Queue full: block on the channel (the sole wakeup
            // source) with a timeout so `stop` is still observed.
            match event_rx.recv_timeout(Duration::from_millis(100)) {
                Ok(RxEvent::FreeCid(cid)) => free.push(cid),
                Ok(RxEvent::R2t {
                    cid,
                    ttag,
                    offset,
                    length,
                }) => {
                    if !answer_r2t(&mut stream, &payload, cid, ttag, offset, length) {
                        break;
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
            continue;
        }
        let cid = free.pop().unwrap();
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
    }
    // try_clone'd fds keep the socket open: shut it down so the
    // blocked RX recv returns.
    let _ = stream.shutdown(std::net::Shutdown::Both);
    let _ = rx_thread.join();
    Arc::try_unwrap(latencies).unwrap().into_inner().unwrap()
}

fn main() {
    let mut args = parse_args();
    // Default sqsize auto-fits the depth: the target allocates one slot per
    // negotiated entry and all are usable, so qd outstanding needs sqsize ==
    // qd (this matches the kernel, where nr_tags == MAXCMD). The common case
    // then works against any target whose --io-queue-size (MAXCMD) >= qd,
    // instead of a fixed 64 that a small-MAXCMD target would reject.
    // --sqsize overrides (e.g. a large slot count with a shallow qd).
    if args.sqsize == 0 {
        args.sqsize = u16::try_from(args.qd).expect("qd too large for an NVMe sqsize");
    }
    assert!(
        args.qd <= args.sqsize as usize && args.sqsize >= 2,
        "qd ({}) must fit the negotiated sqsize ({})",
        args.qd,
        args.sqsize
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
            let (qd, sqsize, bs, write) = (args.qd, args.sqsize, args.bs, args.write);
            std::thread::spawn(move || {
                worker(
                    addr,
                    u16::try_from(i + 1).unwrap(),
                    cntlid,
                    qd,
                    sqsize,
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
