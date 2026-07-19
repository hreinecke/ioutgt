//! A multi-port config is served one process per port: the foreground
//! process takes the lowest portid, a forked child each further port,
//! and every port exports only its own subsystems. Exercised against
//! the real binary — the fork happens in main() before any thread, so
//! an in-process spawn cannot cover it.

mod common;

use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use common::{backing_file, connect_status};
use ioutgt_nvme::status;

const NQN_A: &str = "nqn.2026-06.io.ioutgt:port-a";
const NQN_B: &str = "nqn.2026-06.io.ioutgt:port-b";

#[test]
fn two_ports_two_processes_disjoint_subsystems() {
    let dir = tempfile::tempdir().unwrap();
    let disk_a = backing_file(dir.path(), "a.img");
    let disk_b = backing_file(dir.path(), "b.img");
    let config = dir.path().join("config.json");
    std::fs::write(
        &config,
        format!(
            r#"{{ "ports": [
               {{ "addr": {{ "adrfam": "ipv4", "traddr": "127.0.0.1",
                             "trsvcid": "0", "trtype": "tcp" }},
                  "portid": 1, "subsystems": [ "{NQN_A}" ] }},
               {{ "addr": {{ "adrfam": "ipv4", "traddr": "127.0.0.1",
                             "trsvcid": "0", "trtype": "tcp" }},
                  "portid": 2, "subsystems": [ "{NQN_B}" ] }} ],
             "subsystems": [
               {{ "nqn": "{NQN_A}", "attr": {{ "allow_any_host": "1" }},
                  "namespaces": [ {{ "nsid": 1, "device": {{ "path": "{}" }} }} ] }},
               {{ "nqn": "{NQN_B}", "attr": {{ "allow_any_host": "1" }},
                  "namespaces": [ {{ "nsid": 1, "device": {{ "path": "{}" }} }} ] }} ] }}"#,
            disk_a.display(),
            disk_b.display(),
        ),
    )
    .unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_ioutgt-nvme-tcp"))
        .arg("--config")
        .arg(&config)
        .args(["--no-pin", "--io-threads", "1"])
        .arg("--control-socket")
        .arg(dir.path().join("ctl.sock"))
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn target binary");

    // Both processes announce "ioutgt listening on <addr>" on stderr —
    // one line from the foreground port, one from the forked port.
    let stderr = child.stderr.take().expect("piped");
    let (tx, rx) = mpsc::channel::<SocketAddr>();
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines() {
            let Ok(line) = line else { break };
            if let Some((_, addr)) = line.rsplit_once("listening on ")
                && let Ok(addr) = addr.trim().parse()
            {
                let _ = tx.send(addr);
            }
        }
    });
    let mut addrs = Vec::new();
    while addrs.len() < 2 {
        match rx.recv_timeout(Duration::from_secs(20)) {
            Ok(addr) => addrs.push(addr),
            Err(_) => panic!("expected 2 listening ports, got {addrs:?}"),
        }
    }

    // Probe which process serves which subsystem (the probe doubles as
    // the success assertion for A's port), then assert each port
    // exports exactly its own.
    let a_first = connect_status(addrs[0], NQN_A) == status::SUCCESS;
    let (addr_a, addr_b) = if a_first {
        (addrs[0], addrs[1])
    } else {
        (addrs[1], addrs[0])
    };
    if !a_first {
        assert_eq!(connect_status(addr_a, NQN_A), status::SUCCESS, "A nowhere");
    }
    let unknown = status::CONNECT_INVALID_PARAM | status::DNR;
    assert_eq!(connect_status(addr_b, NQN_B), status::SUCCESS);
    assert_eq!(
        connect_status(addr_a, NQN_B),
        unknown,
        "B leaked onto port 1"
    );
    assert_eq!(
        connect_status(addr_b, NQN_A),
        unknown,
        "A leaked onto port 2"
    );

    // Killing the foreground process takes the forked port down with
    // it (PDEATHSIG) — its address must stop accepting.
    child.kill().unwrap();
    child.wait().unwrap();
    let gone = |addr: SocketAddr| {
        for _ in 0..50 {
            if std::net::TcpStream::connect(addr).is_err() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        false
    };
    assert!(gone(addr_a), "foreground port still accepting after kill");
    assert!(gone(addr_b), "forked port outlived its parent");
}
