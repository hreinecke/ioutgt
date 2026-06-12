//! ioutgt — high-performance io_uring-based NVMe/TCP target.

use std::io::{BufRead, BufReader, Write};

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(version, about = "io_uring-based NVMe/TCP target")]
struct Args {
    /// JSON config file (overrides the individual flags below).
    #[arg(long)]
    config: Option<std::path::PathBuf>,

    /// Listen address.
    #[arg(long, default_value = "0.0.0.0:4420")]
    listen: std::net::SocketAddr,

    /// Number of IO queue threads.
    #[arg(long, default_value_t = 2)]
    io_threads: usize,

    /// Refuse header digest negotiation.
    #[arg(long)]
    no_hdgst: bool,

    /// Refuse data digest negotiation.
    #[arg(long)]
    no_ddgst: bool,

    /// Disable topology-aware IO thread pinning (on by default).
    #[arg(long)]
    no_pin: bool,

    /// NVM subsystem NQN.
    #[arg(long, default_value = "nqn.2026-06.io.ioutgt:test")]
    subsys_nqn: String,

    /// Memory/null-backend namespace size in MiB.
    #[arg(long, default_value_t = 64)]
    mem_size_mb: u64,

    /// Namespace backend: memory, null, or a file/blockdev path.
    #[arg(long, default_value = "memory")]
    backend: String,

    /// Unix socket path for the runtime control API.
    #[arg(long, default_value_os_t = default_control_socket())]
    control_socket: std::path::PathBuf,

    #[command(subcommand)]
    command: Option<Command>,
}

/// Default control-socket path, shared by the target and every
/// ctl-style subcommand so out of the box the clients dial the socket
/// the server actually binds: `$XDG_RUNTIME_DIR/ioutgt.sock` (a
/// per-user 0700 directory — no squatting, no cross-user access),
/// falling back to `/tmp/ioutgt.sock` where XDG_RUNTIME_DIR is unset.
fn default_control_socket() -> std::path::PathBuf {
    match std::env::var_os("XDG_RUNTIME_DIR") {
        Some(dir) if !dir.is_empty() => std::path::Path::new(&dir).join("ioutgt.sock"),
        _ => std::path::PathBuf::from("/tmp/ioutgt.sock"),
    }
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Send one JSON request to a running target's control socket.
    Ctl {
        /// Control socket path.
        #[arg(long, default_value_os_t = default_control_socket())]
        socket: std::path::PathBuf,
        /// Request JSON, e.g. '{"op":"LIST_NAMESPACE"}'.
        request: String,
    },
    /// List the target: port inventory plus live controllers
    /// (queues, threads, namespaces).
    #[command(alias = "list-ctrl")]
    List {
        /// Control socket path.
        #[arg(long, default_value_os_t = default_control_socket())]
        socket: std::path::PathBuf,
    },
    /// Per-thread ring and per-queue IO counters from a running target.
    Stat {
        /// Control socket path.
        #[arg(long, default_value_os_t = default_control_socket())]
        socket: std::path::PathBuf,
        /// Repeat every N seconds, printing per-interval rates.
        #[arg(short, long)]
        interval: Option<u64>,
    },
}

/// Send one request line over the control socket; return the raw
/// response line (trailing newline stripped).
fn ctl_request(socket: &std::path::Path, request: &str) -> std::io::Result<String> {
    let mut stream = std::os::unix::net::UnixStream::connect(socket)?;
    stream.write_all(request.as_bytes())?;
    stream.write_all(b"\n")?;
    let mut response = String::new();
    BufReader::new(&stream).read_line(&mut response)?;
    response.truncate(response.trim_end().len());
    Ok(response)
}

/// `ioutgt ctl`: forward one JSON request verbatim, echo the raw
/// response line, exit 1 unless the server said `"ok": true`.
fn ctl(socket: &std::path::Path, request: &str) -> std::io::Result<()> {
    // Validate locally for a friendlier error than the server echo.
    serde_json::from_str::<serde_json::Value>(request)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let response = ctl_request(socket, request)?;
    println!("{response}");
    let parsed = serde_json::from_str::<serde_json::Value>(&response)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    if parsed.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        std::process::exit(1);
    }
    Ok(())
}

/// `ioutgt list`: render the target's inventory and live controllers.
fn list_target(socket: &std::path::Path) -> std::io::Result<()> {
    let raw = ctl_request(socket, r#"{"op":"LIST_CONTROLLER"}"#)?;
    let response = serde_json::from_str::<serde_json::Value>(&raw)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    if response.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        eprintln!("{raw}");
        std::process::exit(1);
    }
    print!("{}", render_ctrl_list(&response["data"]));
    Ok(())
}

/// `ioutgt stat`: one snapshot, or `-i N` for iostat-style rates
/// (client-side deltas of the monotonic counters — the target never
/// computes rates).
fn stat_target(socket: &std::path::Path, interval: Option<u64>) -> std::io::Result<()> {
    let fetch = || -> std::io::Result<serde_json::Value> {
        let raw = ctl_request(socket, r#"{"op":"GET_STATS"}"#)?;
        let v: serde_json::Value = serde_json::from_str(&raw)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        if v.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
            return Err(std::io::Error::other(raw));
        }
        Ok(v["data"].clone())
    };
    let mut prev = fetch()?;
    print!("{}", render_stat(&prev, None));
    let Some(secs) = interval else { return Ok(()) };
    loop {
        std::thread::sleep(std::time::Duration::from_secs(secs));
        let next = fetch()?;
        println!();
        #[allow(clippy::cast_precision_loss)]
        let elapsed = secs as f64;
        print!("{}", render_stat(&next, Some((&prev, elapsed))));
        prev = next;
    }
}

/// Render GET_STATS `data`. With `prev` = (previous snapshot, elapsed
/// seconds), counters print as per-second deltas; deltas saturate at
/// zero so a target restart between samples shows zeros, not garbage.
fn render_stat(data: &serde_json::Value, prev: Option<(&serde_json::Value, f64)>) -> String {
    use std::fmt::Write;

    fn u(v: &serde_json::Value, key: &str) -> u64 {
        v[key].as_u64().unwrap_or(0)
    }
    // Per-second (rounded) when an interval is given, raw total otherwise.
    let val = |cur: u64, before: u64| -> u64 {
        match prev {
            #[allow(
                clippy::cast_precision_loss,
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss
            )]
            Some((_, secs)) if secs > 0.0 => {
                (cur.saturating_sub(before) as f64 / secs).round() as u64
            }
            _ => cur,
        }
    };
    let mib = |bytes: u64| -> String {
        #[allow(clippy::cast_precision_loss)]
        let v = bytes as f64 / f64::from(1u32 << 20);
        format!("{v:.1} MiB")
    };
    let suffix = if prev.is_some() { "/s" } else { "" };

    let find_thread = |name: &str| -> Option<&serde_json::Value> {
        prev?.0["threads"]
            .as_array()?
            .iter()
            .find(|t| t["name"] == name)
    };

    let mut out = String::new();
    for thread in data["threads"].as_array().into_iter().flatten() {
        if let Some(err) = thread["error"].as_str() {
            let _ = writeln!(
                out,
                "thread {}: {err}",
                thread["name"].as_str().unwrap_or("?")
            );
            continue;
        }
        let name = thread["name"].as_str().unwrap_or("?");
        let before = find_thread(name).cloned().unwrap_or_default();
        let ring = &thread["ring"];
        let ring0 = &before["ring"];
        let _ = writeln!(
            out,
            "{name}  tid {}  enters{suffix} {}  parks{suffix} {}  sqes{suffix} {}  cqes{suffix} {}",
            thread["tid"],
            val(u(ring, "enters"), u(ring0, "enters")),
            val(u(ring, "parks"), u(ring0, "parks")),
            val(u(ring, "sqes"), u(ring0, "sqes")),
            val(u(ring, "cqes"), u(ring0, "cqes")),
        );
        for q in thread["queues"].as_array().into_iter().flatten() {
            let q0 = before["queues"]
                .as_array()
                .and_then(|qs| {
                    qs.iter()
                        .find(|p| p["cntlid"] == q["cntlid"] && p["qid"] == q["qid"])
                })
                .cloned()
                .unwrap_or_default();
            let _ = writeln!(
                out,
                "  cntlid {} qid {}   read {}{suffix} ({}{suffix})  write {}{suffix} \
                 ({}{suffix})  flush {}{suffix}  other {}{suffix}  err {}{suffix}",
                q["cntlid"],
                q["qid"],
                val(u(q, "read_cmds"), u(&q0, "read_cmds")),
                mib(val(u(q, "read_bytes"), u(&q0, "read_bytes"))),
                val(u(q, "write_cmds"), u(&q0, "write_cmds")),
                mib(val(u(q, "write_bytes"), u(&q0, "write_bytes"))),
                val(u(q, "flush_cmds"), u(&q0, "flush_cmds")),
                val(u(q, "other_cmds"), u(&q0, "other_cmds")),
                val(u(q, "errors"), u(&q0, "errors")),
            );
        }
        let retired = &thread["retired"];
        let any_retired = [
            "read_cmds",
            "write_cmds",
            "flush_cmds",
            "other_cmds",
            "errors",
        ]
        .iter()
        .any(|k| u(retired, k) > 0);
        if any_retired {
            let r0 = &before["retired"];
            let _ = writeln!(
                out,
                "  retired          read {}{suffix} ({}{suffix})  write {}{suffix} \
                 ({}{suffix})  flush {}{suffix}  other {}{suffix}  err {}{suffix}",
                val(u(retired, "read_cmds"), u(r0, "read_cmds")),
                mib(val(u(retired, "read_bytes"), u(r0, "read_bytes"))),
                val(u(retired, "write_cmds"), u(r0, "write_cmds")),
                mib(val(u(retired, "write_bytes"), u(r0, "write_bytes"))),
                val(u(retired, "flush_cmds"), u(r0, "flush_cmds")),
                val(u(retired, "other_cmds"), u(r0, "other_cmds")),
                val(u(retired, "errors"), u(r0, "errors")),
            );
        }
    }
    out
}

/// One block per controller; NQNs are too long for fixed columns.
fn render_ctrl_list(data: &serde_json::Value) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let _ = writeln!(out, "pid {}", data["pid"]);
    // Discoverable inventory (bound ports + subsystems), shown in
    // every state; skipped silently if the server predates it.
    for port in data["ports"].as_array().into_iter().flatten() {
        let _ = writeln!(
            out,
            "port {}:{}",
            port["traddr"].as_str().unwrap_or("?"),
            port["trsvcid"].as_str().unwrap_or("?")
        );
        for subsys in port["subsystems"].as_array().into_iter().flatten() {
            let _ = writeln!(out, "  subsystem {}", subsys["nqn"].as_str().unwrap_or("?"));
            for ns in subsys["namespaces"].as_array().into_iter().flatten() {
                let blocks = ns["blocks"].as_u64().unwrap_or(0);
                let shift = u32::try_from(ns["block_shift"].as_u64().unwrap_or(0).min(63))
                    .expect("bounded by min(63)");
                let bytes = blocks << shift;
                const GIB: u64 = 1 << 30;
                let size = if bytes > 0 && bytes % GIB == 0 {
                    format!("{} GiB", bytes / GIB)
                } else {
                    format!("{} MiB", bytes >> 20)
                };
                let _ = writeln!(
                    out,
                    "    ns {}: {size} ({}B blocks)",
                    ns["nsid"],
                    1u64 << shift
                );
            }
        }
    }
    let controllers = data["controllers"]
        .as_array()
        .map_or(&[][..], Vec::as_slice);
    if controllers.is_empty() {
        out.push_str("no controllers\n");
        return out;
    }
    for c in controllers {
        let kind = if c["discovery"] == true {
            " (discovery)"
        } else {
            ""
        };
        let _ = writeln!(
            out,
            "controller {}{kind}: {}",
            c["cntlid"],
            c["subsysnqn"].as_str().unwrap_or("?")
        );
        let _ = writeln!(out, "  host:   {}", c["hostnqn"].as_str().unwrap_or("?"));
        let _ = writeln!(out, "  kato:   {} ms", c["kato_ms"]);
        let queues = c["queues"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|q| {
                format!(
                    "{}:{}@{} cpus {}",
                    q["qid"],
                    q["depth"],
                    q["tid"],
                    q["cpus"].as_str().unwrap_or("?")
                )
            })
            .collect::<Vec<_>>()
            .join(" | ");
        let _ = writeln!(out, "  queues: {queues}");
        let nsids = c["namespaces"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|ns| ns["nsid"].to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let _ = writeln!(
            out,
            "  ns:     {}",
            if nsids.is_empty() {
                "-".to_owned()
            } else {
                nsids
            }
        );
    }
    out
}

fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    if let Some(command) = &args.command {
        match command {
            Command::Ctl { socket, request } => return ctl(socket, request),
            Command::List { socket } => return list_target(socket),
            Command::Stat { socket, interval } => return stat_target(socket, *interval),
        }
    }

    let config = match &args.config {
        Some(path) => ioutgt::TargetConfig::from_file(path)?,
        None => {
            let mut config =
                ioutgt::TargetConfig::single_memory(&args.subsys_nqn, args.mem_size_mb);
            config.listen = args.listen;
            config.io_threads = args.io_threads;
            config.allow_hdgst = !args.no_hdgst;
            config.allow_ddgst = !args.no_ddgst;
            config.pin_threads = !args.no_pin;
            config.control_socket = Some(args.control_socket);
            config.subsystems[0].namespaces[0].backend = match args.backend.as_str() {
                "memory" => ioutgt_control::config::BackendConfig::Memory {
                    size_mb: args.mem_size_mb,
                },
                "null" => ioutgt_control::config::BackendConfig::Null {
                    size_mb: args.mem_size_mb,
                },
                path => ioutgt_control::config::BackendConfig::File { path: path.into() },
            };
            config
        }
    };
    let addr = ioutgt::spawn_target(config)?;
    eprintln!("ioutgt listening on {addr}");
    // The target runs on its own threads; park the main thread.
    loop {
        std::thread::park();
    }
}

#[cfg(test)]
mod tests {
    fn sample_port() -> serde_json::Value {
        serde_json::json!({
            "traddr": "0.0.0.0",
            "trsvcid": "14420",
            "subsystems": [{
                "nqn": "nqn.2026-06.io.ioutgt:test",
                "namespaces": [{"nsid": 1, "blocks": 131072, "block_shift": 9}],
            }],
        })
    }

    const PORT_HEADER: &str = "port 0.0.0.0:14420\n\
         \x20 subsystem nqn.2026-06.io.ioutgt:test\n\
         \x20   ns 1: 64 MiB (512B blocks)\n";

    #[test]
    fn render_ctrl_list_formats_controllers() {
        let data = serde_json::json!({
            "pid": 4242,
            "ports": [sample_port()],
            "controllers": [{
                "cntlid": 1,
                "subsysnqn": "nqn.2026-06.io.ioutgt:test",
                "hostnqn": "nqn.2014-08.org.nvmexpress:uuid:abc",
                "discovery": false,
                "kato_ms": 60000,
                "queues": [
                    {"qid": 0, "depth": 32, "tid": 100, "cpus": "*"},
                    {"qid": 1, "depth": 64, "tid": 101, "cpus": "3"},
                ],
                "namespaces": [{"nsid": 1, "blocks": 32768, "block_shift": 9}],
            }],
        });
        let out = super::render_ctrl_list(&data);
        let expected = format!(
            "pid 4242\n{PORT_HEADER}\
             controller 1: nqn.2026-06.io.ioutgt:test\n\
             \x20 host:   nqn.2014-08.org.nvmexpress:uuid:abc\n\
             \x20 kato:   60000 ms\n\
             \x20 queues: 0:32@100 cpus * | 1:64@101 cpus 3\n\
             \x20 ns:     1\n"
        );
        assert_eq!(out, expected);
    }

    #[test]
    fn render_ctrl_list_empty() {
        let data = serde_json::json!({ "pid": 4242, "ports": [sample_port()], "controllers": [] });
        assert_eq!(
            super::render_ctrl_list(&data),
            format!("pid 4242\n{PORT_HEADER}no controllers\n")
        );
    }

    #[test]
    fn render_ctrl_list_gib_sizes() {
        let data = serde_json::json!({
            "pid": 1,
            "ports": [{
                "traddr": "::", "trsvcid": "14420",
                "subsystems": [{
                    "nqn": "nqn.x",
                    // 2 GiB in 4096B blocks.
                    "namespaces": [{"nsid": 7, "blocks": 524288, "block_shift": 12}],
                }],
            }],
            "controllers": [],
        });
        let out = super::render_ctrl_list(&data);
        assert!(out.contains("port :::14420\n"), "{out}");
        assert!(out.contains("ns 7: 2 GiB (4096B blocks)\n"), "{out}");
    }

    #[test]
    fn render_ctrl_list_without_port_section() {
        let data = serde_json::json!({ "pid": 4242, "controllers": [] });
        assert_eq!(super::render_ctrl_list(&data), "pid 4242\nno controllers\n");
    }

    #[test]
    fn render_ctrl_list_discovery() {
        let data = serde_json::json!({
            "pid": 4242,
            "controllers": [{
                "cntlid": 2,
                "subsysnqn": "nqn.2014-08.org.nvmexpress.discovery",
                "hostnqn": "nqn.2014-08.org.nvmexpress:uuid:abc",
                "discovery": true,
                "kato_ms": 120000,
                "queues": [{"qid": 0, "depth": 32, "tid": 100, "cpus": "*"}],
                "namespaces": [],
            }],
        });
        let out = super::render_ctrl_list(&data);
        assert_eq!(
            out,
            "pid 4242\n\
             controller 2 (discovery): nqn.2014-08.org.nvmexpress.discovery\n\
             \x20 host:   nqn.2014-08.org.nvmexpress:uuid:abc\n\
             \x20 kato:   120000 ms\n\
             \x20 queues: 0:32@100 cpus *\n\
             \x20 ns:     -\n"
        );
    }

    fn stat_sample() -> serde_json::Value {
        serde_json::json!({ "threads": [{
            "name": "ioutgt-io0", "tid": 42,
            "ring": { "enters": 100, "parks": 90, "sqes": 5000, "cqes": 5000 },
            "queues": [{ "cntlid": 1, "qid": 1,
                "read_cmds": 3000u64, "write_cmds": 1000u64, "flush_cmds": 0u64,
                "other_cmds": 2u64, "read_bytes": 12_288_000u64,
                "write_bytes": 4_096_000u64, "errors": 0u64 }],
            "retired": { "read_cmds": 0, "write_cmds": 0, "flush_cmds": 0,
                "other_cmds": 0, "read_bytes": 0, "write_bytes": 0, "errors": 0 },
        }]})
    }

    #[test]
    fn render_stat_totals() {
        let out = super::render_stat(&stat_sample(), None);
        assert!(out.contains("ioutgt-io0"), "{out}");
        assert!(out.contains("tid 42"), "{out}");
        assert!(out.contains("5000"), "sqes visible: {out}");
        assert!(out.contains("cntlid 1 qid 1"), "{out}");
        assert!(out.contains("read 3000"), "{out}");
    }

    #[test]
    fn render_stat_interval_rates() {
        let prev = stat_sample();
        let mut next = stat_sample();
        next["threads"][0]["queues"][0]["read_cmds"] = 5000.into();
        next["threads"][0]["ring"]["enters"] = 300.into();
        // 2000 reads over 2 s → 1000/s; 200 enters over 2 s → 100/s.
        let out = super::render_stat(&next, Some((&prev, 2.0)));
        assert!(out.contains("read 1000"), "rate visible: {out}");
        assert!(out.contains("100"), "enter rate visible: {out}");
        // Counters that did not move render as zero rates, not totals.
        assert!(out.contains("write 0"), "{out}");
    }

    #[test]
    fn render_stat_saturates_on_restart() {
        // Target restarted between samples: counters went backwards.
        let prev = stat_sample();
        let mut next = stat_sample();
        next["threads"][0]["queues"][0]["read_cmds"] = 10.into();
        let out = super::render_stat(&next, Some((&prev, 1.0)));
        assert!(out.contains("read 0"), "saturating delta: {out}");
    }

    #[test]
    fn render_stat_unresponsive_thread() {
        let v = serde_json::json!({ "threads": [{ "error": "thread unresponsive" }] });
        let out = super::render_stat(&v, None);
        assert!(out.contains("unresponsive"), "{out}");
    }

    #[test]
    fn render_stat_skips_retired_when_zero() {
        let out = super::render_stat(&stat_sample(), None);
        assert!(!out.contains("retired"), "{out}");
        let mut v = stat_sample();
        v["threads"][0]["retired"]["write_cmds"] = 7.into();
        let out = super::render_stat(&v, None);
        assert!(out.contains("retired"), "{out}");
    }
}
