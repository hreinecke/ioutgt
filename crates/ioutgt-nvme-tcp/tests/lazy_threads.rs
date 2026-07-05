//! The per-port queue-thread pool (admin + IO threads) is spawned lazily
//! on the first accepted connection, not at `spawn_target()` time. Before
//! any client connects only the control thread exists; the first
//! connection brings up the whole pool.

mod common;

use common::{Client, NQN};
use ioutgt_nvme::spec;

/// Names (from `/proc/self/task/*/comm`) of the queue-thread pool —
/// `ioutgt-admin` plus `ioutgt-io<N>`. Excludes the always-present
/// `ioutgt-control` thread. comm names are truncated to 15 bytes, but
/// every name used here fits.
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

#[test]
fn pool_spawns_lazily_on_first_connection() {
    let mut config = ioutgt_nvme_tcp::TargetConfig::single_memory(NQN, 16);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 2;
    let addr = ioutgt_nvme_tcp::spawn_target(config).expect("target start");

    // The target is bound and the control thread is running, but no
    // client has connected: no admin or IO queue threads exist yet.
    assert!(
        pool_thread_names().is_empty(),
        "queue threads spawned before any client connected: {:?}",
        pool_thread_names()
    );

    // First connection spawns the whole pool (admin + every IO thread),
    // even though this connection only opens the admin queue.
    let mut admin = Client::handshake(addr, false, false);
    admin.connect(0, 32, 0xFFFF, 1);
    admin.enable_controller(2);
    let _ = admin.identify(spec::cns::CONTROLLER, 0, 3);

    // Thread creation is synchronous in the accept loop (before this
    // connection is even routed), but allow a little slack for the comm
    // files to appear under scheduling jitter.
    let mut names = Vec::new();
    for _ in 0..40 {
        names = pool_thread_names();
        if names.len() >= 3 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    assert!(
        names.contains(&"ioutgt-admin".to_string()),
        "admin thread missing after connect: {names:?}"
    );
    assert!(
        names.contains(&"ioutgt-io0".to_string()),
        "io0 thread missing after connect: {names:?}"
    );
    assert!(
        names.contains(&"ioutgt-io1".to_string()),
        "io1 thread missing after connect: {names:?}"
    );
}
