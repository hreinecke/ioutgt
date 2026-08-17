//! Discovery log regression test, mirroring the kernel host's behavior:
//! SQ flow control disabled (SUCCESS elision active) and the two-step
//! header-probe + full-read pattern.

mod common;

use common::{Client, NQN, ascii, connect_discovery, get_disc_log};
use ioutgt_nvme::fabrics;

const HOSTNQN: &str = "nqn.2014-08.org.nvmexpress:uuid:dddddddd-1111-2222-3333-444444444444";

#[test]
fn discovery_log_is_intact_with_sqflow_disabled() {
    let mut config = ioutgt_nvme_tcp::TargetConfig::single_memory(NQN, 16);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    let addr = ioutgt_nvme_tcp::spawn_target(config).expect("target start");

    let mut client = Client::handshake(addr, false, false);
    connect_discovery(&mut client, HOSTNQN);
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

/// A subsystem served by several targets — every holder of a Sheepdog ACL's
/// shared lock — is advertised as one entry per path, so a host discovers all
/// of them through whichever one it happened to connect to.
#[test]
fn every_path_to_a_subsystem_gets_its_own_entry() {
    let mut config = ioutgt_nvme_tcp::TargetConfig::single_memory(NQN, 16);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    let addr = ioutgt_nvme_tcp::spawn_target(config).expect("target start");

    // What the ACL's holder list turns into: this target and two peers, one
    // of them on IPv6.
    let paths = [
        (addr.ip().to_string(), addr.port().to_string(), 0u16),
        ("10.9.8.7".to_string(), "4420".to_string(), 1),
        ("fd00::2".to_string(), "4420".to_string(), 2),
    ];
    let port = ioutgt_harness::ports()
        .into_iter()
        .find(|p| p.trsvcid == addr.port().to_string())
        .expect("the port this test just spawned");
    port.subsystem(NQN).expect("test subsystem").set_ports(
        paths
            .iter()
            .map(
                |(traddr, trsvcid, portid)| ioutgt_core::subsystem::SubsystemPort {
                    traddr: traddr.clone(),
                    trsvcid: trsvcid.clone(),
                    trtype: ioutgt_core::subsystem::TransportType::Tcp,
                    portid: *portid,
                },
            )
            .collect(),
    );

    let mut client = Client::handshake(addr, false, false);
    connect_discovery(&mut client, HOSTNQN);
    client.enable_controller(2);

    let header = get_disc_log(&mut client, 3, 0, 16);
    let numrec = u64::from_le_bytes(header[8..16].try_into().unwrap());
    assert_eq!(numrec, 3, "one entry per path to the subsystem");

    let log = get_disc_log(&mut client, 4, 0, 4096);
    for (i, (traddr, trsvcid, portid)) in paths.iter().enumerate() {
        let entry = &log[1024 * (i + 1)..1024 * (i + 2)];
        assert_eq!(entry[0], fabrics::trtype::TCP, "entry {i} trtype");
        // The address family follows the address, not the port's own.
        let adrfam = if traddr.contains(':') { 2 } else { 1 };
        assert_eq!(entry[1], adrfam, "entry {i} adrfam");
        assert_eq!(u16::from_le_bytes([entry[4], entry[5]]), *portid, "portid");
        assert_eq!(ascii(&entry[32..64]), *trsvcid, "entry {i} trsvcid");
        assert_eq!(ascii(&entry[512..768]), *traddr, "entry {i} traddr");
        assert_eq!(
            &entry[256..256 + NQN.len()],
            NQN.as_bytes(),
            "entry {i} subnqn: every path leads to the same subsystem"
        );
    }
}
