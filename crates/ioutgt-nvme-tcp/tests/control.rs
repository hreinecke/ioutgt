//! M7 exit test: runtime namespace add/remove over the control socket —
//! the connected controller's parked AER completes with the NS_ATTR
//! notice, the changed-NS log reports, identify reflects the new
//! inventory, and IO works on the hot-added namespace.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;

use common::{Client, NQN, pattern, rw_sqe};
use ioutgt_nvme::identify::IdentifyController;
use ioutgt_nvme::pdu::PduKind;
use ioutgt_nvme::{spec, status};
use zerocopy::FromBytes;

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

/// Identify Controller TNVMCAP, in bytes.
fn tnvmcap(admin: &mut Client, cid: u16) -> u128 {
    let ctrl = admin.identify(spec::cns::CONTROLLER, 0, cid);
    let ctrl = IdentifyController::read_from_bytes(&ctrl).expect("identify controller");
    u128::from_le_bytes(ctrl.tnvmcap)
}

#[test]
fn runtime_namespace_add_remove_with_aer() {
    let socket = std::env::temp_dir().join(format!("ioutgt-ctl-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&socket);

    let mut config = ioutgt_nvme_tcp::TargetConfig::single_memory(NQN, 16);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    config.control_socket = Some(socket.clone());
    let addr = ioutgt_nvme_tcp::spawn_target(config).expect("target start");

    // Admin connection with a parked AER.
    let mut admin = Client::handshake(addr, false, false);
    let cntlid = admin.connect(0, 32, 0xFFFF, 1);
    admin.enable_controller(8);
    admin.post_aer(9);

    // Baseline inventory: nsid 1 only.
    let list = admin.identify(spec::cns::ACTIVE_NS_LIST, 0, 2);
    assert_eq!(active_nsids(&list), vec![1]);
    assert_eq!(tnvmcap(&mut admin, 20), 16 << 20, "baseline: one 16 MiB ns");

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

    // Inventory now lists both, and TNVMCAP has grown by the new backend.
    let list = admin.identify(spec::cns::ACTIVE_NS_LIST, 0, 11);
    assert_eq!(active_nsids(&list), vec![1, 2]);
    assert_eq!(tnvmcap(&mut admin, 21), 24 << 20, "hot-add grows TNVMCAP");

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
    assert_eq!(tnvmcap(&mut admin, 22), 16 << 20, "removal shrinks TNVMCAP");

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
    let listed = resp["data"]["namespaces"].as_array().unwrap();
    assert_eq!(listed.len(), 1);
    // The same capacity Identify reports as NVMCAP, in bytes.
    assert_eq!(listed[0]["capacity"], 16 << 20, "{resp}");
    let resp = ctl(&socket, r#"{"op":"GET_STATS"}"#);
    assert_eq!(resp["ok"], true);
    assert_eq!(resp["data"]["controllers"], 1);
    // ...and per subsystem, the TNVMCAP total.
    assert_eq!(
        resp["data"]["subsystems"][0]["capacity"],
        16 << 20,
        "{resp}"
    );
    // Every queue thread (admin + io) reports its ring counters.
    let threads = resp["data"]["threads"].as_array().expect("threads array");
    assert!(!threads.is_empty(), "{resp}");
    for thread in threads {
        assert!(thread["ring"]["sqes"].is_u64(), "thread reply: {thread}");
        assert!(thread["tid"].as_i64().unwrap_or(0) > 0, "{thread}");
    }

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

    let mut config = ioutgt_nvme_tcp::TargetConfig::single_memory(NQN, 16);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    config.control_socket = Some(socket.clone());
    let addr = ioutgt_nvme_tcp::spawn_target(config).expect("target start");

    // Empty registry: ok, pid present, no controllers.
    let resp = ctl(&socket, r#"{"op":"LIST_CONTROLLER"}"#);
    assert_eq!(resp["ok"], true, "{resp}");
    assert_eq!(resp["data"]["pid"], u64::from(std::process::id()));
    assert!(resp["data"]["controllers"].as_array().unwrap().is_empty());

    // Discoverable inventory is reported before any host connects.
    // trsvcid is the *bound* port (the target listens on :0 here).
    let ports = resp["data"]["ports"].as_array().unwrap();
    assert_eq!(ports.len(), 1, "{resp}");
    assert_eq!(ports[0]["traddr"], "127.0.0.1", "{resp}");
    assert_eq!(ports[0]["trsvcid"], addr.port().to_string(), "{resp}");
    let subsystems = ports[0]["subsystems"].as_array().unwrap();
    assert_eq!(subsystems.len(), 1);
    assert_eq!(subsystems[0]["nqn"], NQN);
    assert_eq!(subsystems[0]["capacity"], 16 << 20, "{resp}");
    assert_eq!(subsystems[0]["namespaces"][0]["nsid"], 1);
    assert_eq!(
        subsystems[0]["namespaces"][0]["capacity"],
        16 << 20,
        "{resp}"
    );

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
    assert_eq!(queues[0]["depth"], 32);
    assert_eq!(queues[1]["qid"], 1);
    assert_eq!(queues[1]["depth"], 64);
    let admin_tid = queues[0]["tid"].as_i64().unwrap();
    let io_tid = queues[1]["tid"].as_i64().unwrap();
    assert!(admin_tid > 0 && io_tid > 0);
    // Live affinity recorded at Connect; both queues are unpinned
    // in-process, so the values are identical non-empty cpulists.
    let admin_cpus = queues[0]["cpus"].as_str().unwrap();
    assert!(!admin_cpus.is_empty(), "{resp}");
    assert_eq!(queues[0]["cpus"], queues[1]["cpus"], "{resp}");
    assert_ne!(
        admin_tid, io_tid,
        "admin and IO queues on different threads"
    );
    assert_eq!(c["namespaces"].as_array().unwrap().len(), 1);
    assert_eq!(c["namespaces"][0]["nsid"], 1);
    // Port section is present in the connected state too.
    assert_eq!(resp["data"]["ports"][0]["subsystems"][0]["nqn"], NQN);

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
            let _ = std::fs::remove_file(&socket);
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    panic!("controller not reaped after disconnect");
}

/// The per-queue counters in GET_STATS track real IO exactly, and a
/// torn-down queue's counts fold into the thread's retired totals.
#[test]
fn get_stats_counts_ios_and_folds_retired() {
    let socket = std::env::temp_dir().join(format!("ioutgt-stat-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&socket);

    let mut config = ioutgt_nvme_tcp::TargetConfig::single_memory(NQN, 16);
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.io_threads = 1;
    config.control_socket = Some(socket.clone());
    let addr = ioutgt_nvme_tcp::spawn_target(config).expect("target start");

    let mut admin = Client::handshake(addr, false, false);
    let cntlid = admin.connect(0, 32, 0xFFFF, 1);
    admin.enable_controller(2);
    let mut io = Client::handshake(addr, false, false);
    io.connect(1, 32, cntlid, 1);

    const BS: u32 = 4096;
    let data = pattern(BS as usize, 0x37);
    for i in 0..4u16 {
        let sqe = rw_sqe(
            spec::io_opcode::WRITE,
            10 + i,
            u64::from(i) * 8,
            7,
            BS,
            false,
        );
        io.send_capsule(&sqe, &data);
        assert_eq!(io.recv_response().status.get() >> 1, status::SUCCESS);
    }
    for i in 0..3u16 {
        let sqe = rw_sqe(spec::io_opcode::READ, 20 + i, u64::from(i) * 8, 7, BS, true);
        io.send_capsule(&sqe, &[]);
        let (decoded, payload) = io.recv_pdu();
        assert!(matches!(decoded.kind, PduKind::C2HData { .. }));
        assert_eq!(payload, data, "readback {i}");
        let _ = io.recv_response();
    }

    let resp = ctl(&socket, r#"{"op":"GET_STATS"}"#);
    assert_eq!(resp["ok"], true, "{resp}");
    let threads = resp["data"]["threads"].as_array().expect("threads");
    let find_queue = |threads: &[serde_json::Value], qid: u64| -> Option<serde_json::Value> {
        threads.iter().find_map(|t| {
            t["queues"]
                .as_array()?
                .iter()
                .find(|q| {
                    q["cntlid"].as_u64() == Some(u64::from(cntlid))
                        && q["qid"].as_u64() == Some(qid)
                })
                .cloned()
        })
    };
    let q = find_queue(threads, 1).expect("io queue in stats");
    assert_eq!(q["write_cmds"], 4, "{q}");
    assert_eq!(q["read_cmds"], 3, "{q}");
    assert_eq!(q["write_bytes"], 4 * u64::from(BS), "{q}");
    assert_eq!(q["read_bytes"], 3 * u64::from(BS), "{q}");
    assert_eq!(q["errors"], 0, "{q}");
    // The queue's own fabrics Connect is an "other" command.
    assert!(q["other_cmds"].as_u64().unwrap() >= 1, "{q}");
    // Admin queue counters exist too (Connect/enable/keep-alive land
    // in other_cmds), and the serving thread's ring did real work.
    let aq = find_queue(threads, 0).expect("admin queue in stats");
    assert!(aq["other_cmds"].as_u64().unwrap() >= 2, "{aq}");
    let io_thread = threads
        .iter()
        .find(|t| {
            t["queues"]
                .as_array()
                .is_some_and(|qs| qs.iter().any(|q| q["qid"].as_u64() == Some(1)))
        })
        .expect("thread serving qid 1");
    assert!(
        io_thread["ring"]["sqes"].as_u64().unwrap() > 0,
        "{io_thread}"
    );
    // The IO queue recv'd commands and sent responses, so both buckets moved.
    assert!(
        io_thread["ring"]["send_sqes"].as_u64().unwrap() > 0,
        "{io_thread}"
    );
    assert!(
        io_thread["ring"]["recv_sqes"].as_u64().unwrap() > 0,
        "{io_thread}"
    );

    // The response names the controllers the cntlid rows refer to.
    let info = resp["data"]["controller_info"]
        .as_array()
        .expect("controller_info");
    assert!(
        info.iter()
            .any(|c| c["cntlid"].as_u64() == Some(u64::from(cntlid)) && c["subsysnqn"] == NQN),
        "{resp}"
    );

    // clear=true: the clearing request still reports the totals...
    let resp = ctl(&socket, r#"{"op":"GET_STATS","clear":true}"#);
    let q = find_queue(resp["data"]["threads"].as_array().unwrap(), 1).expect("io queue");
    assert_eq!(q["write_cmds"], 4, "clear reports pre-clear totals: {q}");
    // ...the next snapshot starts from zero with identity preserved...
    let resp = ctl(&socket, r#"{"op":"GET_STATS"}"#);
    let q = find_queue(resp["data"]["threads"].as_array().unwrap(), 1).expect("io queue");
    assert_eq!(q["write_cmds"], 0, "cleared: {q}");
    assert_eq!(q["read_bytes"], 0, "cleared: {q}");
    let cleared_thread = resp["data"]["threads"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| {
            t["queues"]
                .as_array()
                .is_some_and(|qs| qs.iter().any(|q| q["qid"].as_u64() == Some(1)))
        })
        .expect("thread serving qid 1");
    assert!(
        cleared_thread["ring"]["sqes"].as_u64().unwrap()
            < io_thread["ring"]["sqes"].as_u64().unwrap(),
        "ring counters cleared too: {cleared_thread}"
    );
    // ...and counting resumes.
    for i in 0..2u16 {
        let sqe = rw_sqe(
            spec::io_opcode::WRITE,
            30 + i,
            u64::from(i) * 8,
            7,
            BS,
            false,
        );
        io.send_capsule(&sqe, &data);
        assert_eq!(io.recv_response().status.get() >> 1, status::SUCCESS);
    }
    let resp = ctl(&socket, r#"{"op":"GET_STATS"}"#);
    let q = find_queue(resp["data"]["threads"].as_array().unwrap(), 1).expect("io queue");
    assert_eq!(q["write_cmds"], 2, "counts resume after clear: {q}");

    // Teardown folds the final (post-clear) counts into retired totals.
    drop(io);
    drop(admin);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let resp = ctl(&socket, r#"{"op":"GET_STATS"}"#);
        let retired_writes: u64 = resp["data"]["threads"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|t| t["retired"]["write_cmds"].as_u64())
            .sum();
        if retired_writes >= 2 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "retired fold timed out: {resp}"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    let _ = std::fs::remove_file(&socket);
}
