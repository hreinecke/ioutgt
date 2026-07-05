//! `--io-queue-size` advertises the configured ceiling as Identify
//! Controller MAXCMD, so the host's IO queues are no longer pinned to the
//! admin queue depth (the kernel clamps every IO queue down to MAXCMD).

mod common;

use common::{Client, NQN};
use ioutgt_nvme::identify::IdentifyController;
use ioutgt_nvme::spec;
use zerocopy::FromBytes;

fn start_target(io_queue_size: u16) -> std::net::SocketAddr {
    let mut config = ioutgt_nvme_tcp::TargetConfig::single_memory(NQN, 16);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    config.io_queue_size = io_queue_size;
    ioutgt_nvme_tcp::spawn_target(config).expect("target start")
}

/// MAXCMD must equal the configured io_queue_size — distinct from the
/// admin queue depth (32) used at connect, proving the advertisement
/// tracks the config and not `ctx.queue.sqsize`.
#[test]
fn identify_maxcmd_reflects_io_queue_size() {
    let addr = start_target(64);
    let mut admin = Client::handshake(addr, false, false);
    admin.connect(0, 32, 0xFFFF, 1);
    admin.enable_controller(2);

    let id = admin.identify(spec::cns::CONTROLLER, 0, 3);
    let ctrl = IdentifyController::read_from_bytes(&id).expect("identify controller is 4096 bytes");
    assert_eq!(
        ctrl.maxcmd.get(),
        64,
        "MAXCMD must equal the configured io_queue_size, not the admin depth"
    );
}
