//! Ctrl-C shutdown: the signal wakes `wait_for_shutdown`, which walks the
//! ports this process built and shuts their backends down — where a Sheepdog
//! namespace hands its VDI lock back to the cluster, rather than leaving it
//! for the cluster to reclaim (the release itself is covered against a fake
//! `sheep` by ioutgt-backend's `sheepdog_loopback` suite).
//!
//! Its own test binary on purpose: the port registry is process-wide, so a
//! target spawned by some other test in the same process would land in the
//! walk and throw its count off.

mod common;

use common::NQN;

/// Spawn a one-memory-namespace target on an ephemeral port.
fn spawn() -> std::net::SocketAddr {
    let mut config = ioutgt_nvme_tcp::TargetConfig::single_memory(NQN, 8);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    ioutgt_nvme_tcp::spawn_target(config).expect("target start")
}

#[test]
fn a_signal_wakes_the_shutdown_walk() {
    // Install first: an uncaught SIGINT would take the test binary down.
    ioutgt_harness::install_shutdown_handler().unwrap();
    let _addr = spawn();

    // SAFETY: plain `raise` on this process; the handler installed above
    // catches it and nudges the wait below awake.
    assert_eq!(unsafe { libc::raise(libc::SIGINT) }, 0);
    ioutgt_harness::wait_for_shutdown().unwrap();

    // The walk ran and drained the registry — nothing is released twice.
    assert_eq!(ioutgt_harness::shutdown(), 0, "registry already drained");

    // Draining it did not disable it: the next target registers its own
    // port, and its namespace is what the walk finds.
    let _second = spawn();
    assert_eq!(
        ioutgt_harness::shutdown(),
        1,
        "the second target's namespace was walked"
    );
}
