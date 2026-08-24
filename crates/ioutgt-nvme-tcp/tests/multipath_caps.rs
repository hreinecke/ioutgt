//! ioutgt advertises NVMe multi-path capability like kernel nvmet — CMIC
//! multi-controller (Identify Controller) and NMIC shared (Identify
//! Namespace) — so the host's multipath layer builds a namespace head
//! plus a per-controller path device (`/dev/nvmeXcYnZ`). ANA is reported
//! only where path locality is a real question — a cluster-backed subsystem
//! (`cluster_ana.rs`) — so a local-storage target leaves CMIC bit 3 clear.

mod common;

use common::{Client, NQN};
use ioutgt_nvme::identify::{IdentifyController, IdentifyNamespace, cmic, nsfeat, nmic};
use ioutgt_nvme::spec;
use zerocopy::FromBytes;

fn start_target() -> std::net::SocketAddr {
    let mut config = ioutgt_nvme_tcp::TargetConfig::single_memory(NQN, 16);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    ioutgt_nvme_tcp::spawn_target(config).expect("target start")
}

#[test]
fn advertises_multipath_caps() {
    let addr = start_target();
    let mut admin = Client::handshake(addr, false, false);
    admin.connect(0, 32, 0xFFFF, 1);
    admin.enable_controller(2);

    let ctrl = admin.identify(spec::cns::CONTROLLER, 0, 3);
    let ctrl = IdentifyController::read_from_bytes(&ctrl).expect("identify controller 4096 bytes");
    assert_eq!(
        ctrl.cmic & cmic::MULTI_CTRL,
        cmic::MULTI_CTRL,
        "CMIC must advertise multi-controller so the host enables multipath"
    );
    // ANA (CMIC bit 3) stays clear here: every path to a memory-backed
    // namespace is equally good, so there is nothing to report.
    assert_eq!(
        ctrl.cmic & cmic::ANA_REPORTING,
        0,
        "ANA must not be advertised without cluster storage"
    );

    let ns = admin.identify(spec::cns::NAMESPACE, 1, 4);
    let ns = IdentifyNamespace::read_from_bytes(&ns).expect("identify namespace 4096 bytes");
    assert_eq!(
        ns.nmic & nmic::SHARED,
        nmic::SHARED,
        "NMIC must mark the namespace shared across controllers"
    );
    assert_eq!(
        ns.nsfeat & nsfeat::THINP,
        nsfeat::THINP,
        "NSFEAT must mark the namespace as thin-provisioned across controllers"
    );
}
