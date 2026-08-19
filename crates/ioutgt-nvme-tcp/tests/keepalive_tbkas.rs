//! Traffic-based keep-alive (Identify Controller `CTRATT.TBKAS`): IO on
//! any queue of a controller keeps it alive, so a busy host never has to
//! send a Keep Alive command — and an idle one still gets reclaimed.

mod common;

use std::io::Read;
use std::time::{Duration, Instant};

use common::{Client, NQN, rw_sqe};
use ioutgt_nvme::identify::{IdentifyController, ctratt};
use ioutgt_nvme::pdu::PduKind;
use ioutgt_nvme::{spec, status};
use zerocopy::FromBytes;

/// Short enough to make the test quick, long enough to survive a loaded
/// CI box: the watchdog polls every KATO/2 and expires the controller
/// after KATO×2 + one tick = 2.5 s of silence.
const KATO_MS: u32 = 1_000;

/// A target with one 16 MiB memory namespace.
fn start_target() -> std::net::SocketAddr {
    let mut config = ioutgt_nvme_tcp::TargetConfig::single_memory(NQN, 16);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    ioutgt_nvme_tcp::spawn_target(config).expect("target start")
}

/// Admin queue with a short KATO, controller enabled.
fn connect_admin(addr: std::net::SocketAddr) -> (Client, u16) {
    let mut admin = Client::handshake(addr, false, false);
    let cntlid = admin.connect_with_kato(0, 32, 0xFFFF, 1, KATO_MS);
    assert!(cntlid >= 1);
    admin.enable_controller(2);
    (admin, cntlid)
}

/// One 4K read on an IO queue, drained to its response.
fn io_read(io: &mut Client, cid: u16) {
    let sqe = rw_sqe(spec::io_opcode::READ, cid, 0, 7, 4096, true);
    io.send_capsule(&sqe, &[]);
    let (decoded, _) = io.recv_pdu();
    assert!(matches!(decoded.kind, PduKind::C2HData { .. }), "read data");
    let cqe = io.recv_response();
    assert_eq!(cqe.status.get() >> 1, status::SUCCESS, "io read");
}

#[test]
fn tbkas_advertised_and_io_traffic_keeps_controller_alive() {
    let addr = start_target();
    let (mut admin, cntlid) = connect_admin(addr);

    // The host only skips Keep Alive commands because of this bit, so the
    // advertisement and the behavior below have to travel together.
    let payload = admin.identify(spec::cns::CONTROLLER, 0, 3);
    let id = IdentifyController::ref_from_bytes(&payload[..4096]).unwrap();
    assert_ne!(
        id.ctratt.get() & ctratt::TBKAS,
        0,
        "TCP controllers advertise traffic-based keep-alive"
    );

    let mut io = Client::handshake(addr, false, false);
    io.connect(1, 64, cntlid, 1);

    // Drive IO for well over two expiry windows (2×KATO + tick = 2.5 s)
    // while the admin queue stays completely silent. Without TBKAS the
    // watchdog would have shut this connection down mid-loop.
    let start = Instant::now();
    let mut cid = 0u16;
    while start.elapsed() < Duration::from_secs(6) {
        cid = cid.wrapping_add(1);
        io_read(&mut io, cid);
        std::thread::sleep(Duration::from_millis(200));
    }

    // Still alive: an admin command completes normally. (recv would hit
    // EOF and panic had the watchdog closed the connection.)
    let payload = admin.identify(spec::cns::CONTROLLER, 0, 4);
    let id = IdentifyController::ref_from_bytes(&payload[..4096]).unwrap();
    assert_eq!(id.cntlid.get(), cntlid, "same controller throughout");
}

#[test]
fn idle_io_queue_does_not_keep_controller_alive() {
    // The other half: liveness must be traffic, not the mere existence of
    // an IO queue — a latched flag would leak controllers forever.
    let addr = start_target();
    let (mut admin, cntlid) = connect_admin(addr);
    let mut io = Client::handshake(addr, false, false);
    io.connect(1, 64, cntlid, 1);
    // One command, then silence on both queues.
    io_read(&mut io, 1);

    let start = Instant::now();
    admin
        .stream()
        .set_read_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    let mut buf = [0u8; 64];
    let n = admin.stream().read(&mut buf).unwrap_or(0);
    assert_eq!(n, 0, "server should close the silent connection");
    assert!(
        start.elapsed() < Duration::from_secs(15),
        "keep-alive expiry too slow: {:?}",
        start.elapsed()
    );
}
