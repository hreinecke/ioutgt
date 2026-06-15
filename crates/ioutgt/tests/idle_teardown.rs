//! The queue-thread pool is reclaimed after an idle grace period (zero
//! active connections across all subsystems) and respawned on the next
//! connection — the teardown counterpart to lazy first-connect spawn.

mod common;

use std::time::{Duration, Instant};

use common::{Client, NQN};
use ioutgt_nvme::spec;

/// Names (from `/proc/self/task/*/comm`) of the queue-thread pool —
/// `ioutgt-admin` plus `ioutgt-io<N>`, excluding the always-present
/// `ioutgt-control` thread.
fn pool_thread_names() -> Vec<String> {
    let mut names = Vec::new();
    let Ok(entries) = std::fs::read_dir("/proc/self/task") else {
        return names;
    };
    for entry in entries.flatten() {
        if let Ok(comm) = std::fs::read_to_string(entry.path().join("comm")) {
            let name = comm.trim();
            if name == "ioutgt-admin" || name.starts_with("ioutgt-io") {
                names.push(name.to_string());
            }
        }
    }
    names.sort();
    names
}

/// Poll until the pool has exactly `target` threads, or the timeout
/// elapses; returns the last observed set.
fn wait_for_pool(target: usize, timeout: Duration) -> Vec<String> {
    let deadline = Instant::now() + timeout;
    loop {
        let names = pool_thread_names();
        if names.len() == target || Instant::now() >= deadline {
            return names;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Connect an admin queue and run one Identify, forcing the pool up.
fn connect_admin(addr: std::net::SocketAddr) -> Client {
    let mut admin = Client::handshake(addr, false, false);
    admin.connect(0, 32, 0xFFFF, 1);
    admin.enable_controller(2);
    let _ = admin.identify(spec::cns::CONTROLLER, 0, 3);
    admin
}

#[test]
fn pool_reclaimed_after_idle_then_respawned() {
    let mut config = ioutgt::TargetConfig::single_memory(NQN, 16);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 2;
    // Short grace so the test is fast (production default is 30s).
    config.idle_teardown = Some(Duration::from_millis(500));
    let addr = ioutgt::spawn_target(config).expect("target start");

    assert!(
        pool_thread_names().is_empty(),
        "pool present before first connect: {:?}",
        pool_thread_names()
    );

    // First connection spawns the whole pool (admin + 2 IO threads).
    {
        let _admin = connect_admin(addr);
        let up = wait_for_pool(3, Duration::from_secs(5));
        assert_eq!(up.len(), 3, "pool not fully up after connect: {up:?}");
        assert!(
            up.contains(&"ioutgt-admin".to_string()),
            "admin missing: {up:?}"
        );
        // _admin drops here → connection closes → active count → 0.
    }

    // After the grace period the idle pool is torn down.
    let down = wait_for_pool(0, Duration::from_secs(5));
    assert!(down.is_empty(), "pool not reclaimed after idle: {down:?}");

    // A fresh connection respawns it, identically to the first connect.
    let _admin = connect_admin(addr);
    let up = wait_for_pool(3, Duration::from_secs(5));
    assert_eq!(up.len(), 3, "pool not respawned after reconnect: {up:?}");
    assert!(
        up.contains(&"ioutgt-admin".to_string()),
        "admin missing after respawn: {up:?}"
    );
}
