//! Discovery log regression test, mirroring the kernel host's behavior:
//! SQ flow control disabled (SUCCESS elision active) and the two-step
//! header-probe + full-read pattern.

mod common;

use common::{Client, NQN};
use ioutgt_nvme::fabrics::{self, ConnectCommand, ConnectData, fctype};
use ioutgt_nvme::pdu::PduKind;
use ioutgt_nvme::{spec, status};
use zerocopy::{FromBytes, FromZeros, IntoBytes};

const HOSTNQN: &str = "nqn.2014-08.org.nvmexpress:uuid:dddddddd-1111-2222-3333-444444444444";

/// Connect to the discovery subsystem with CATTR sq-flow-control
/// disabled, as the Linux host does.
fn connect_discovery(client: &mut Client) {
    let mut cmd: ConnectCommand = FromZeros::new_zeroed();
    cmd.opcode = spec::admin_opcode::FABRICS;
    cmd.fctype = fctype::CONNECT;
    cmd.cid.set(1);
    cmd.qid.set(0);
    cmd.sqsize.set(31);
    cmd.cattr = 1 << 2; // DISABLE_SQFLOW
    cmd.kato.set(120_000);
    cmd.dptr.length.set(1024);
    cmd.dptr.sgl_type = spec::sgl::TYPE_DATA_BLOCK_OFFSET;
    let mut data = ConnectData::zeroed();
    data.cntlid.set(0xFFFF);
    let disc = fabrics::DISCOVERY_NQN.as_bytes();
    data.subsysnqn[..disc.len()].copy_from_slice(disc);
    data.hostnqn[..HOSTNQN.len()].copy_from_slice(HOSTNQN.as_bytes());
    let sqe = spec::Sqe::read_from_bytes(cmd.as_bytes()).unwrap();
    client.send_capsule(&sqe, data.as_bytes());
    let cqe = client.recv_response();
    assert_eq!(cqe.status.get() >> 1, status::SUCCESS, "discovery connect");
}

/// Get Log Page DISCOVERY: returns the payload (data may arrive as
/// C2HData with SUCCESS elision — no response capsule follows).
fn get_disc_log(client: &mut Client, cid: u16, offset: u64, len: u32) -> Vec<u8> {
    let mut sqe = spec::Sqe::zeroed();
    sqe.opcode = spec::admin_opcode::GET_LOG_PAGE;
    sqe.flags = spec::CMD_FLAGS_SGL_METABUF;
    sqe.cid.set(cid);
    let numd = len / 4 - 1;
    sqe.cdw10
        .set(u32::from(spec::log_page::DISCOVERY) | (numd << 16));
    #[allow(clippy::cast_possible_truncation)]
    sqe.cdw12.set(offset as u32);
    sqe.cdw13.set(u32::try_from(offset >> 32).unwrap());
    sqe.dptr.length.set(len);
    sqe.dptr.sgl_type = spec::sgl::TYPE_TRANSPORT_DATA_BLOCK;
    client.send_capsule(&sqe, &[]);
    let (decoded, payload) = client.recv_pdu();
    let PduKind::C2HData { success, last, .. } = decoded.kind else {
        panic!("expected C2HData, got {:?}", decoded.kind);
    };
    assert!(last);
    if !success {
        let cqe = client.recv_response();
        assert_eq!(cqe.status.get() >> 1, status::SUCCESS, "get log page");
    }
    payload
}

#[test]
fn discovery_log_is_intact_with_sqflow_disabled() {
    let mut config = ioutgt_nvme_tcp::TargetConfig::single_memory(NQN, 16);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    let addr = ioutgt_nvme_tcp::spawn_target(config).expect("target start");

    let mut client = Client::handshake(addr, false, false);
    connect_discovery(&mut client);
    client.enable_controller(2);

    // Host pattern: probe the header first, then read the whole log.
    let header = get_disc_log(&mut client, 3, 0, 16);
    let numrec = u64::from_le_bytes(header[8..16].try_into().unwrap());
    assert_eq!(numrec, 1, "one NVM subsystem on this port");

    let log = get_disc_log(&mut client, 4, 0, 2048);
    assert!(log.len() >= 2048);
    let entry = &log[1024..2048];
    assert_eq!(entry[0], fabrics::trtype::TCP, "trtype");
    assert_eq!(entry[1], 1, "adrfam ipv4");
    assert_eq!(entry[2], fabrics::subtype::NVM, "subtype");
    let subnqn = &entry[256..256 + NQN.len()];
    assert_eq!(subnqn, NQN.as_bytes(), "entry subnqn");
}
