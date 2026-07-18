//! Host ACLs, nvmet semantics: with `allow_any_host` off, only
//! hostnqns on the subsystem's `allowed_hosts` list may connect;
//! everyone else gets CONNECT_INVALID_HOST. `allow_any_host` on
//! ignores the list entirely.

mod common;

use common::{Client, HOSTNQN, NQN, connect_sqe};
use ioutgt_nvme::status;
use zerocopy::IntoBytes;

/// A single-subsystem target with the given host-ACL settings.
fn start_target(allow_any_host: bool, allowed_hosts: Vec<String>) -> std::net::SocketAddr {
    let mut config = ioutgt_nvme_tcp::TargetConfig::single_memory(NQN, 8);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    config.subsystems[0].allow_any_host = allow_any_host;
    config.subsystems[0].allowed_hosts = allowed_hosts;
    ioutgt_nvme_tcp::spawn_target(config).expect("target start")
}

/// Admin-queue Connect as the fixed test HOSTNQN; returns the
/// (phase-stripped) CQE status.
fn connect_status(addr: std::net::SocketAddr) -> u16 {
    let mut client = Client::handshake(addr, false, false);
    let (sqe, data) = connect_sqe(0, 32, 0xFFFF, 1);
    client.send_capsule(&sqe, data.as_bytes());
    client.recv_response().status.get() >> 1
}

#[test]
fn listed_host_admitted() {
    let addr = start_target(false, vec![HOSTNQN.into()]);
    assert_eq!(connect_status(addr), status::SUCCESS);
}

#[test]
fn unlisted_host_rejected() {
    let addr = start_target(
        false,
        vec!["nqn.2014-08.org.nvmexpress:uuid:someone-else".into()],
    );
    assert_eq!(
        connect_status(addr),
        status::CONNECT_INVALID_HOST | status::DNR
    );
}

#[test]
fn empty_list_rejects_all_hosts() {
    let addr = start_target(false, vec![]);
    assert_eq!(
        connect_status(addr),
        status::CONNECT_INVALID_HOST | status::DNR
    );
}

#[test]
fn allow_any_host_ignores_list() {
    let addr = start_target(
        true,
        vec!["nqn.2014-08.org.nvmexpress:uuid:someone-else".into()],
    );
    assert_eq!(connect_status(addr), status::SUCCESS);
}
