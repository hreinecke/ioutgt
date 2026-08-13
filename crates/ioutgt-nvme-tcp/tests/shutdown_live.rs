//! The shutdown handshake: `shutdown()` stops the targets serving IO before
//! it releases what their backends hold, rather than racing the release
//! against in-flight commands. Observable from outside as the connections
//! being dropped and the queue-thread pool gone by the time it returns.
//!
//! Its own test binary, like `shutdown.rs`: the registries `shutdown()`
//! drains are process-wide, so a target another test spawned in the same
//! process would be stopped along with this one's.

mod common;

use std::io::Read;
use std::time::{Duration, Instant};

use common::{Client, NQN, rw_sqe};
use ioutgt_nvme::pdu::PduKind;
use ioutgt_nvme::{spec, status};

/// The queue-thread pool's threads (`ioutgt-admin`, `ioutgt-io<N>`), read
/// from `/proc/self/task` — the control thread is not one of them.
fn pool_threads() -> Vec<String> {
    let Ok(entries) = std::fs::read_dir("/proc/self/task") else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| std::fs::read_to_string(entry.path().join("comm")).ok())
        .map(|comm| comm.trim().to_owned())
        .filter(|name| name == "ioutgt-admin" || name.starts_with("ioutgt-io"))
        .collect()
}

#[test]
fn shutdown_stops_the_io_it_serves() {
    let mut config = ioutgt_nvme_tcp::TargetConfig::single_memory(NQN, 16);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    // No idle reclaim: the pool must still be up at shutdown for its teardown
    // to be what stops these connections.
    config.idle_teardown = None;
    let addr = ioutgt_nvme_tcp::spawn_target(config).expect("target start");

    let mut admin = Client::handshake(addr, false, false);
    let cntlid = admin.connect(0, 32, 0xFFFF, 1);
    admin.enable_controller(2);
    let mut io = Client::handshake(addr, false, false);
    io.connect(1, 32, cntlid, 1);

    // Serve one read, so both queues are demonstrably running commands (and
    // the pool is up) at the moment the handshake starts.
    io.send_capsule(&rw_sqe(spec::io_opcode::READ, 3, 8, 7, 4096, true), &[]);
    let (decoded, _payload) = io.recv_pdu();
    assert!(
        matches!(decoded.kind, PduKind::C2HData { .. }),
        "expected read data, got {:?}",
        decoded.kind
    );
    assert_eq!(io.recv_response().status.get() >> 1, status::SUCCESS);
    assert!(!pool_threads().is_empty(), "pool down while serving IO");

    // Give the reads below a bound: a connection the handshake failed to stop
    // must fail the test, not hang it.
    for client in [&mut admin, &mut io] {
        client
            .stream()
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
    }

    assert_eq!(
        ioutgt_harness::shutdown(),
        1,
        "the target's one namespace was released"
    );

    // Both connections were wound down as part of the handshake — before the
    // release, and without waiting for the process to exit.
    for (name, client) in [("admin", &mut admin), ("io", &mut io)] {
        let mut byte = [0u8; 1];
        assert_eq!(
            client.stream().read(&mut byte).ok(),
            Some(0),
            "{name} connection still open after shutdown"
        );
    }

    // And the queue threads themselves are on their way out: they acked the
    // handshake from inside their mailbox loop, so the OS threads exit a
    // moment later.
    let deadline = Instant::now() + Duration::from_secs(5);
    while !pool_threads().is_empty() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        pool_threads().is_empty(),
        "queue threads still running after shutdown: {:?}",
        pool_threads()
    );
}
