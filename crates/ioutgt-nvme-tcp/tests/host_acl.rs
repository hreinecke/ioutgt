//! Host ACLs, nvmet semantics: with `allow_any_host` off, only
//! hostnqns on the subsystem's `allowed_hosts` list may connect;
//! everyone else gets CONNECT_INVALID_HOST. `allow_any_host` on
//! ignores the list entirely.

mod common;

use common::{HOSTNQN, NQN, connect_status};
use ioutgt_nvme::status;

/// A single-subsystem target with the given host-ACL settings.
fn start_target(allow_any_host: bool, allowed_hosts: Vec<String>) -> std::net::SocketAddr {
    let mut config = ioutgt_nvme_tcp::TargetConfig::single_memory(NQN, 8);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    config.subsystems[0].allow_any_host = allow_any_host;
    config.subsystems[0].allowed_hosts = allowed_hosts;
    ioutgt_nvme_tcp::spawn_target(config).expect("target start")
}

#[test]
fn listed_host_admitted() {
    let addr = start_target(false, vec![HOSTNQN.into()]);
    assert_eq!(connect_status(addr, NQN), status::SUCCESS);
}

#[test]
fn unlisted_host_rejected() {
    let addr = start_target(
        false,
        vec!["nqn.2014-08.org.nvmexpress:uuid:someone-else".into()],
    );
    assert_eq!(
        connect_status(addr, NQN),
        status::CONNECT_INVALID_HOST | status::DNR
    );
}

#[test]
fn empty_list_rejects_all_hosts() {
    let addr = start_target(false, vec![]);
    assert_eq!(
        connect_status(addr, NQN),
        status::CONNECT_INVALID_HOST | status::DNR
    );
}

#[test]
fn allow_any_host_ignores_list() {
    let addr = start_target(
        true,
        vec!["nqn.2014-08.org.nvmexpress:uuid:someone-else".into()],
    );
    assert_eq!(connect_status(addr, NQN), status::SUCCESS);
}
