//! M7 exit test: runtime namespace add/remove over the control socket —
//! the connected controller's parked AER completes with the NS_ATTR
//! notice, the changed-NS log reports, identify reflects the new
//! inventory, and IO works on the hot-added namespace.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;

use common::{Client, NQN, pattern, rw_sqe};
use ioutgt_nvme::pdu::PduKind;
use ioutgt_nvme::{spec, status};

fn ctl(socket: &std::path::Path, request: &str) -> serde_json::Value {
    let mut stream = UnixStream::connect(socket).expect("control socket");
    stream.write_all(request.as_bytes()).unwrap();
    stream.write_all(b"\n").unwrap();
    let mut line = String::new();
    BufReader::new(&stream).read_line(&mut line).unwrap();
    serde_json::from_str(&line).expect("json response")
}

fn active_nsids(payload: &[u8]) -> Vec<u32> {
    payload
        .chunks_exact(4)
        .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
        .take_while(|&n| n != 0)
        .collect()
}

#[test]
fn runtime_namespace_add_remove_with_aer() {
    let socket = std::env::temp_dir().join(format!("ioutgt-ctl-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&socket);

    let mut config = ioutgt::TargetConfig::single_memory(NQN, 16);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    config.control_socket = Some(socket.clone());
    let addr = ioutgt::spawn_target(config).expect("target start");

    // Admin connection with a parked AER.
    let mut admin = Client::handshake(addr, false, false);
    let cntlid = admin.connect(0, 32, 0xFFFF, 1);
    admin.enable_controller(8);
    admin.post_aer(9);

    // Baseline inventory: nsid 1 only.
    let list = admin.identify(spec::cns::ACTIVE_NS_LIST, 0, 2);
    assert_eq!(active_nsids(&list), vec![1]);

    // Hot-add nsid 2 over the control socket.
    let resp = ctl(
        &socket,
        r#"{"op":"ADD_NAMESPACE","nsid":2,"backend":{"type":"memory","size_mb":8}}"#,
    );
    assert_eq!(resp["ok"], true, "{resp}");

    // The parked AER must complete with the NS_ATTR notice.
    let cqe = admin.recv_response();
    assert_eq!(cqe.cid.get(), 9, "AER cid");
    assert_eq!(cqe.result.get(), 0x0004_0002, "NS_ATTR_CHANGED notice");

    // Changed-NS log: reports everything changed, then clears.
    let mut sqe = spec::Sqe::zeroed();
    sqe.opcode = spec::admin_opcode::GET_LOG_PAGE;
    sqe.cid.set(10);
    sqe.cdw10
        .set(u32::from(spec::log_page::CHANGED_NS) | (1023 << 16)); // 4096B
    sqe.dptr.length.set(4096);
    sqe.dptr.sgl_type = spec::sgl::TYPE_TRANSPORT_DATA_BLOCK;
    admin.send_capsule(&sqe, &[]);
    let (decoded, payload) = admin.recv_pdu();
    assert!(matches!(decoded.kind, PduKind::C2HData { .. }));
    let _ = admin.recv_response();
    assert_eq!(
        &payload[..4],
        &u32::MAX.to_le_bytes(),
        "changed-ns sentinel"
    );

    // Inventory now lists both.
    let list = admin.identify(spec::cns::ACTIVE_NS_LIST, 0, 11);
    assert_eq!(active_nsids(&list), vec![1, 2]);

    // IO on the hot-added namespace.
    let mut io = Client::handshake(addr, false, false);
    io.connect(1, 32, cntlid, 1);
    let data = pattern(4096, 0x42);
    let mut sqe = rw_sqe(spec::io_opcode::WRITE, 3, 0, 7, 4096, false);
    sqe.nsid.set(2);
    io.send_capsule(&sqe, &data);
    assert_eq!(
        io.recv_response().status.get() >> 1,
        status::SUCCESS,
        "write ns2"
    );
    let mut sqe = rw_sqe(spec::io_opcode::READ, 4, 0, 7, 4096, true);
    sqe.nsid.set(2);
    io.send_capsule(&sqe, &[]);
    let (_, payload) = io.recv_pdu();
    assert_eq!(payload, data, "ns2 readback");
    let _ = io.recv_response();

    // Remove it: a fresh AER fires again, and IO now fails INVALID_NS.
    admin.post_aer(12);
    let resp = ctl(&socket, r#"{"op":"REMOVE_NAMESPACE","nsid":2}"#);
    assert_eq!(resp["ok"], true, "{resp}");
    let cqe = admin.recv_response();
    assert_eq!(cqe.cid.get(), 12);
    assert_eq!(cqe.result.get(), 0x0004_0002);

    let list = admin.identify(spec::cns::ACTIVE_NS_LIST, 0, 13);
    assert_eq!(active_nsids(&list), vec![1]);

    let mut sqe = rw_sqe(spec::io_opcode::READ, 5, 0, 7, 4096, true);
    sqe.nsid.set(2);
    io.send_capsule(&sqe, &[]);
    let cqe = io.recv_response();
    assert_eq!(
        cqe.status.get() >> 1,
        status::INVALID_NS | status::DNR,
        "removed ns rejects IO"
    );

    // Control queries.
    let resp = ctl(&socket, r#"{"op":"LIST_NAMESPACE"}"#);
    assert_eq!(resp["ok"], true);
    assert_eq!(resp["data"]["namespaces"].as_array().unwrap().len(), 1);
    let resp = ctl(&socket, r#"{"op":"GET_STATS"}"#);
    assert_eq!(resp["ok"], true);
    assert_eq!(resp["data"]["controllers"], 1);

    // Bad requests are rejected, connection stays usable.
    let resp = ctl(&socket, r#"{"op":"REMOVE_NAMESPACE","nsid":42}"#);
    assert_eq!(resp["ok"], false);
    let resp = ctl(&socket, r#"{"op":"NOPE"}"#);
    assert_eq!(resp["ok"], false);

    let _ = std::fs::remove_file(&socket);
}

#[test]
fn list_controller_reports_queues_and_namespaces() {
    let socket = std::env::temp_dir().join(format!("ioutgt-lsctrl-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&socket);

    let mut config = ioutgt::TargetConfig::single_memory(NQN, 16);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    config.control_socket = Some(socket.clone());
    let addr = ioutgt::spawn_target(config).expect("target start");

    // Empty registry: ok, pid present, no controllers.
    let resp = ctl(&socket, r#"{"op":"LIST_CONTROLLER"}"#);
    assert_eq!(resp["ok"], true, "{resp}");
    assert_eq!(resp["data"]["pid"], u64::from(std::process::id()));
    assert!(resp["data"]["controllers"].as_array().unwrap().is_empty());

    // Admin connect (Client::connect uses kato 60s on qid 0) + one IO queue.
    let mut admin = Client::handshake(addr, false, false);
    let cntlid = admin.connect(0, 32, 0xFFFF, 1);
    admin.enable_controller(2);
    let mut io = Client::handshake(addr, false, false);
    io.connect(1, 64, cntlid, 1);

    let resp = ctl(&socket, r#"{"op":"LIST_CONTROLLER"}"#);
    let ctrls = resp["data"]["controllers"].as_array().unwrap();
    assert_eq!(ctrls.len(), 1, "{resp}");
    let c = &ctrls[0];
    assert_eq!(c["cntlid"], u64::from(cntlid));
    assert_eq!(c["subsysnqn"], NQN);
    assert_eq!(c["discovery"], false);
    assert_eq!(c["kato_ms"], 60_000);
    let queues = c["queues"].as_array().unwrap();
    assert_eq!(queues.len(), 2, "{resp}");
    assert_eq!(queues[0]["qid"], 0);
    assert_eq!(queues[0]["sqsize"], 32);
    assert_eq!(queues[1]["qid"], 1);
    assert_eq!(queues[1]["sqsize"], 64);
    let admin_tid = queues[0]["tid"].as_i64().unwrap();
    let io_tid = queues[1]["tid"].as_i64().unwrap();
    assert!(admin_tid > 0 && io_tid > 0);
    assert_ne!(
        admin_tid, io_tid,
        "admin and IO queues on different threads"
    );
    assert_eq!(c["namespaces"].as_array().unwrap().len(), 1);
    assert_eq!(c["namespaces"][0]["nsid"], 1);

    // Hot-added namespace appears on the next listing.
    let resp = ctl(
        &socket,
        r#"{"op":"ADD_NAMESPACE","nsid":2,"backend":{"type":"memory","size_mb":8}}"#,
    );
    assert_eq!(resp["ok"], true, "{resp}");
    let resp = ctl(&socket, r#"{"op":"LIST_CONTROLLER"}"#);
    assert_eq!(
        resp["data"]["controllers"][0]["namespaces"]
            .as_array()
            .unwrap()
            .len(),
        2
    );

    // Disconnect reaps the entry (teardown is async; poll briefly).
    drop(io);
    drop(admin);
    for _ in 0..50 {
        let resp = ctl(&socket, r#"{"op":"LIST_CONTROLLER"}"#);
        if resp["data"]["controllers"].as_array().unwrap().is_empty() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    panic!("controller not reaped after disconnect");
}
