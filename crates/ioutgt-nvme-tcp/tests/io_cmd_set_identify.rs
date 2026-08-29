//! The NVM Command Set specific Identify Controller data structure (CNS
//! 0x06, CSI 0x00): Dataset Management size limits, derived from the
//! subsystem's namespaces rather than fixed constants — a memory-backed
//! namespace's `io_boundary` (its allocation chunk size) stands in for a
//! Sheepdog VDI's object size here, exercising the same
//! `Subsystem::min_io_boundary` path without a fake cluster.

mod common;

use common::{Client, NQN};
use ioutgt_nvme::identify::IOCSSIdentifyController;
use ioutgt_nvme::{spec, status};
use zerocopy::FromBytes;

fn start_target() -> std::net::SocketAddr {
    let mut config = ioutgt_nvme_tcp::TargetConfig::single_memory(NQN, 16);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    ioutgt_nvme_tcp::spawn_target(config).expect("target start")
}

#[test]
fn reports_dataset_management_limits_from_the_namespace_io_boundary() {
    let addr = start_target();
    let mut admin = Client::handshake(addr, false, false);
    admin.connect(0, 32, 0xFFFF, 1);
    admin.enable_controller(2);

    let data = admin.identify(spec::cns::IO_COMMAND_SET, 0, 3);
    let id = IOCSSIdentifyController::read_from_bytes(&data)
        .expect("io command set specific identify controller is 4096 bytes");

    assert_eq!(
        id.dmrl, 255,
        "DMRL is a fixed ceiling on ranges per command"
    );
    // The memory backend's io_boundary is its 2 MiB allocation chunk over
    // the namespace's 512-byte LBAs: 2097152 / 512 = 4096.
    assert_eq!(
        id.dmrsl.get(),
        4096,
        "DMRSL mirrors the namespace's own io_boundary (object size / LBA size)"
    );
    assert_eq!(
        id.dmsl.get(),
        256 * u64::from(id.dmrsl.get()),
        "DMSL is 256 whole ranges' worth of DMRSL"
    );
}

/// CSI (Command Set Identifier, CDW11 bits 31:24) selects which command
/// set's structure to report; only the NVM command set (0) is implemented.
#[test]
fn rejects_an_unsupported_command_set_identifier() {
    let addr = start_target();
    let mut admin = Client::handshake(addr, false, false);
    admin.connect(0, 32, 0xFFFF, 1);
    admin.enable_controller(2);

    let mut sqe = spec::Sqe::zeroed();
    sqe.opcode = spec::admin_opcode::IDENTIFY;
    sqe.flags = spec::CMD_FLAGS_SGL_METABUF;
    sqe.cid.set(4);
    sqe.cdw10.set(u32::from(spec::cns::IO_COMMAND_SET));
    sqe.cdw11.set(1 << 24); // CSI = 1: not the NVM command set.
    sqe.dptr.length.set(4096);
    sqe.dptr.sgl_type = spec::sgl::TYPE_TRANSPORT_DATA_BLOCK;
    admin.send_capsule(&sqe, &[]);
    let cqe = admin.recv_response();
    assert_eq!(cqe.status.get() >> 1, status::INVALID_FIELD | status::DNR);
}
