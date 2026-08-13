//! Statically-configured multiple namespaces: a subsystem built with
//! several namespaces up front (not hot-added over the control socket —
//! `control.rs` already covers that path) must route IO to the right
//! backend, keep namespaces isolated from one another, and report each
//! one's size and the active-NSID inventory correctly.

mod common;

use common::{Client, NQN, pattern, rw_sqe};
use ioutgt_control::config::{BackendConfig, NamespaceConfig};
use ioutgt_nvme::identify::{IdentifyController, IdentifyNamespace};
use ioutgt_nvme::{spec, status};
use zerocopy::FromBytes;

/// The NSIDs named by an active-NSID-list payload (zero-terminated,
/// 4 bytes little-endian each).
fn active_nsids(payload: &[u8]) -> Vec<u32> {
    payload
        .chunks_exact(4)
        .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
        .take_while(|&n| n != 0)
        .collect()
}

/// A memory-backed namespace config of `size_mb`.
fn mem(nsid: u32, size_mb: u64) -> NamespaceConfig {
    NamespaceConfig {
        nsid,
        backend: BackendConfig::Memory { size_mb },
        uuid: None,
    }
}

/// A target whose single subsystem is configured with exactly
/// `namespaces` from the start, reporting `mnan` (`None`: zero).
fn start_target_mnan(namespaces: Vec<NamespaceConfig>, mnan: Option<u32>) -> std::net::SocketAddr {
    let mut config = ioutgt_nvme_tcp::TargetConfig::single_memory(NQN, 8);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    config.subsystems[0].namespaces = namespaces;
    config.subsystems[0].mnan = mnan;
    ioutgt_nvme_tcp::spawn_target(config).expect("target start")
}

/// [`start_target_mnan`] with no MNAN, as every config-file and flag-built
/// subsystem but a Sheepdog ACL's has it.
fn start_target(namespaces: Vec<NamespaceConfig>) -> std::net::SocketAddr {
    start_target_mnan(namespaces, None)
}

/// Write one 4096-byte block to `nsid` at LBA 0, asserting success.
fn write_ns(io: &mut Client, nsid: u32, cid: u16, data: &[u8]) {
    assert_eq!(data.len(), 4096, "helper writes one 4096-byte block");
    let mut sqe = rw_sqe(spec::io_opcode::WRITE, cid, 0, 7, 4096, false);
    sqe.nsid.set(nsid);
    io.send_capsule(&sqe, data);
    assert_eq!(
        io.recv_response().status.get() >> 1,
        status::SUCCESS,
        "write ns{nsid}"
    );
}

/// Read one 4096-byte block from `nsid` at LBA 0.
fn read_ns(io: &mut Client, nsid: u32, cid: u16) -> Vec<u8> {
    let mut sqe = rw_sqe(spec::io_opcode::READ, cid, 0, 7, 4096, true);
    sqe.nsid.set(nsid);
    io.send_capsule(&sqe, &[]);
    let (_, payload) = io.recv_pdu();
    let _ = io.recv_response();
    payload
}

/// Three namespaces configured up front — two memory of different sizes
/// and one null — exercise inventory, per-namespace identify, isolation,
/// and backend-type routing all at once.
#[test]
fn static_multi_namespace_isolation_and_identify() {
    let addr = start_target(vec![
        mem(1, 8),
        mem(2, 16),
        NamespaceConfig {
            nsid: 3,
            backend: BackendConfig::Null { size_mb: 8 },
            uuid: None,
        },
    ]);

    let mut admin = Client::handshake(addr, false, false);
    let cntlid = admin.connect(0, 32, 0xFFFF, 1);
    admin.enable_controller(2);

    // Inventory lists all three NSIDs, in order, from the first connect —
    // no hot-add involved.
    let list = admin.identify(spec::cns::ACTIVE_NS_LIST, 0, 3);
    assert_eq!(active_nsids(&list), vec![1, 2, 3]);

    // Identify Controller NN reports the highest NSID.
    let ctrl = admin.identify(spec::cns::CONTROLLER, 0, 4);
    let ctrl = IdentifyController::read_from_bytes(&ctrl).expect("identify controller");
    assert_eq!(ctrl.nn.get(), 3, "NN reports the highest NSID");
    assert_eq!(ctrl.mnan.get(), 0, "no storage-supplied count: MNAN is 0");

    // Identify Namespace reports each backend's own size (512-byte blocks,
    // as the config path builds them): 8 MiB → 16384 blocks, 16 MiB → 32768.
    let nsze = |c: &mut Client, nsid: u32, cid: u16| -> u64 {
        let ns = c.identify(spec::cns::NAMESPACE, nsid, cid);
        IdentifyNamespace::read_from_bytes(&ns)
            .expect("identify namespace")
            .nsze
            .get()
    };
    let n1 = nsze(&mut admin, 1, 5);
    let n2 = nsze(&mut admin, 2, 6);
    let n3 = nsze(&mut admin, 3, 7);
    assert_eq!(n1, 16384, "nsid 1 = 8 MiB / 512");
    assert_eq!(n2, 32768, "nsid 2 = 16 MiB / 512");
    assert_eq!(n2, 2 * n1, "each namespace is sized independently");
    assert_eq!(n3, n1, "nsid 3 = 8 MiB null, same block count");

    let mut io = Client::handshake(addr, false, false);
    io.connect(1, 32, cntlid, 1);

    // Isolation: distinct patterns written to nsid 1 and nsid 2 at the
    // same LBA must not bleed across — they are separate backends.
    let a = pattern(4096, 0x11);
    let b = pattern(4096, 0x22);
    write_ns(&mut io, 1, 0x30, &a);
    write_ns(&mut io, 2, 0x31, &b);
    assert_eq!(read_ns(&mut io, 1, 0x32), a, "nsid 1 keeps its own data");
    assert_eq!(read_ns(&mut io, 2, 0x33), b, "nsid 2 keeps its own data");

    // Backend-type routing: nsid 3 is a null backend — its writes are
    // discarded and reads return zeroes, regardless of the write above.
    write_ns(&mut io, 3, 0x34, &pattern(4096, 0x55));
    assert_eq!(
        read_ns(&mut io, 3, 0x35),
        vec![0u8; 4096],
        "null namespace reads zero"
    );

    // An NSID above the highest configured one is rejected.
    let mut sqe = rw_sqe(spec::io_opcode::READ, 0x36, 0, 7, 4096, true);
    sqe.nsid.set(4);
    io.send_capsule(&sqe, &[]);
    assert_eq!(
        io.recv_response().status.get() >> 1,
        status::INVALID_NS | status::DNR,
        "unconfigured NSID rejects IO"
    );
}

/// NSIDs need not be contiguous: a sparse namespace set still reports the
/// highest NSID as NN, lists only the configured NSIDs, and rejects IO to
/// an unconfigured gap NSID that sits *below* the highest.
#[test]
fn noncontiguous_nsids_report_max_nn_and_reject_gap() {
    let addr = start_target(vec![mem(1, 8), mem(3, 8)]);

    let mut admin = Client::handshake(addr, false, false);
    let cntlid = admin.connect(0, 32, 0xFFFF, 1);
    admin.enable_controller(2);

    let list = admin.identify(spec::cns::ACTIVE_NS_LIST, 0, 3);
    assert_eq!(active_nsids(&list), vec![1, 3], "gap NSID omitted");
    let ctrl = admin.identify(spec::cns::CONTROLLER, 0, 4);
    let ctrl = IdentifyController::read_from_bytes(&ctrl).expect("identify controller");
    assert_eq!(ctrl.nn.get(), 3, "NN is the highest NSID, not the count");

    let mut io = Client::handshake(addr, false, false);
    io.connect(1, 32, cntlid, 1);

    // The gap (nsid 2) is unconfigured — IO is rejected even though it is
    // below the highest NSID.
    let mut sqe = rw_sqe(spec::io_opcode::READ, 0x40, 0, 7, 4096, true);
    sqe.nsid.set(2);
    io.send_capsule(&sqe, &[]);
    assert_eq!(
        io.recv_response().status.get() >> 1,
        status::INVALID_NS | status::DNR,
        "gap NSID rejects IO"
    );

    // The real namespaces on either side of the gap still serve IO.
    let data = pattern(4096, 0x77);
    write_ns(&mut io, 3, 0x41, &data);
    assert_eq!(
        read_ns(&mut io, 3, 0x42),
        data,
        "nsid 3 across the gap works"
    );
}

/// A subsystem whose storage carries its own namespace count reports it as
/// MNAN — a Sheepdog ACL object, which sizes its member list with
/// `max_data_id_nr` while each member's NSID is its (large, sparse) vid. NN
/// stays the highest valid NSID, and the inventory and the namespaces
/// themselves are exactly the ones configured.
#[test]
fn a_storage_supplied_namespace_count_is_reported_as_mnan() {
    let addr = start_target_mnan(vec![mem(0x20, 8), mem(0x21, 8)], Some(3));

    let mut admin = Client::handshake(addr, false, false);
    let cntlid = admin.connect(0, 32, 0xFFFF, 1);
    admin.enable_controller(2);

    let ctrl = admin.identify(spec::cns::CONTROLLER, 0, 3);
    let ctrl = IdentifyController::read_from_bytes(&ctrl).expect("identify controller");
    assert_eq!(ctrl.mnan.get(), 3, "MNAN is the storage's count");
    assert_eq!(ctrl.nn.get(), 0x21, "NN stays the highest valid NSID");

    // The host finds the namespaces through the Active Namespace List, which
    // still names the vids themselves.
    let list = admin.identify(spec::cns::ACTIVE_NS_LIST, 0, 4);
    assert_eq!(active_nsids(&list), vec![0x20, 0x21]);

    let mut io = Client::handshake(addr, false, false);
    io.connect(1, 32, cntlid, 1);
    let data = pattern(4096, 0x5a);
    write_ns(&mut io, 0x21, 0x50, &data);
    assert_eq!(read_ns(&mut io, 0x21, 0x51), data, "nsid 0x21 serves IO");
}
