//! End-to-end nvmetcli-config compatibility: a target spawned from an
//! nvmetcli-format JSON file (the kernel nvmet save/restore schema)
//! serves exactly what the file says — host ACL, serial number,
//! namespace UUID, and IO through the file-backed namespace.

mod common;

use common::{Client, HOSTNQN, NQN, pattern, rw_sqe};
use ioutgt_harness::TransportType;
use ioutgt_nvme::identify::IdentifyController;
use ioutgt_nvme::{spec, status};
use zerocopy::FromBytes;

const UUID: &str = "00112233-4455-6677-8899-aabbccddeeff";
const UUID_BYTES: [u8; 16] = [
    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
];

#[test]
fn nvmetcli_config_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let disk = dir.path().join("ns1.img");
    std::fs::File::create(&disk)
        .unwrap()
        .set_len(1 << 20)
        .unwrap();

    // The shape `nvmetcli save` writes (trsvcid 0 = ephemeral port,
    // for test isolation).
    let json = format!(
        r#"{{
          "hosts": [ {{ "nqn": "{HOSTNQN}" }} ],
          "ports": [
            {{ "addr": {{ "adrfam": "ipv4", "traddr": "127.0.0.1",
                          "treq": "not specified", "trsvcid": "0", "trtype": "tcp" }},
               "portid": 1, "referrals": [], "subsystems": [ "{NQN}" ] }}
          ],
          "subsystems": [
            {{ "allowed_hosts": [ "{HOSTNQN}" ],
               "attr": {{ "allow_any_host": "0", "serial": "SN1234" }},
               "namespaces": [
                 {{ "device": {{ "path": "{}", "uuid": "{UUID}" }},
                    "enable": 1, "nsid": 1 }}
               ],
               "nqn": "{NQN}" }}
          ]
        }}"#,
        disk.display()
    );
    let path = dir.path().join("config.json");
    std::fs::write(&path, json).unwrap();

    // Flags-then-overlay, as main() composes it: the file replaces the
    // target model, engine settings stay.
    let mut config = ioutgt_nvme_tcp::TargetConfig::single_memory("nqn.overridden", 8);
    config.pin_threads = false;
    config.io_threads = 1;
    config.apply_file(&path, TransportType::Tcp).unwrap();
    let addr = ioutgt_nvme_tcp::spawn_target(config).expect("target start");

    // The file's ACL admits this hostnqn (allow_any_host is off).
    let mut admin = Client::handshake(addr, false, false);
    let cntlid = admin.connect(0, 32, 0xFFFF, 1);
    admin.enable_controller(2);

    // Identity flows from the file: serial, then the namespace UUID in
    // the descriptor list (NIDT 3, NIDL 16).
    let ctrl = admin.identify(spec::cns::CONTROLLER, 0, 3);
    let ctrl = IdentifyController::read_from_bytes(&ctrl).expect("identify controller");
    assert_eq!(&ctrl.sn[..6], b"SN1234");
    let desc = admin.identify(spec::cns::NS_DESC_LIST, 1, 4);
    assert_eq!(desc[0], 3, "NIDT: UUID");
    assert_eq!(desc[1], 16, "NIDL");
    assert_eq!(desc[4..20], UUID_BYTES);

    // IO round-trips through the file-backed namespace.
    let mut io = Client::handshake(addr, false, false);
    io.connect(1, 32, cntlid, 1);
    let data = pattern(4096, 0x5a);
    let mut sqe = rw_sqe(spec::io_opcode::WRITE, 2, 0, 7, 4096, false);
    sqe.nsid.set(1);
    io.send_capsule(&sqe, &data);
    assert_eq!(io.recv_response().status.get() >> 1, status::SUCCESS);
    let mut sqe = rw_sqe(spec::io_opcode::READ, 3, 0, 7, 4096, true);
    sqe.nsid.set(1);
    io.send_capsule(&sqe, &[]);
    let (_, payload) = io.recv_pdu();
    assert_eq!(io.recv_response().status.get() >> 1, status::SUCCESS);
    assert_eq!(payload, data);
}
