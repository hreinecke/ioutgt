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

    /// Pin queue threads to cores.
    #[arg(long)]
    pin: bool,

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

/// One block per controller; NQNs are too long for fixed columns.
fn render_ctrl_list(data: &serde_json::Value) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let _ = writeln!(out, "pid {}", data["pid"]);
    // Discoverable inventory (configured port + subsystems), shown in
    // every state; skipped silently if the server predates it.
    if let Some(port) = data["port"].as_object() {
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
            .map(|q| format!("{}:{}@{}", q["qid"], q["sqsize"], q["tid"]))
            .collect::<Vec<_>>()
            .join(" ");
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
            config.pin_threads = args.pin;
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
            "port": sample_port(),
            "controllers": [{
                "cntlid": 1,
                "subsysnqn": "nqn.2026-06.io.ioutgt:test",
                "hostnqn": "nqn.2014-08.org.nvmexpress:uuid:abc",
                "discovery": false,
                "kato_ms": 60000,
                "queues": [
                    {"qid": 0, "sqsize": 32, "tid": 100},
                    {"qid": 1, "sqsize": 64, "tid": 101},
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
             \x20 queues: 0:32@100 1:64@101\n\
             \x20 ns:     1\n"
        );
        assert_eq!(out, expected);
    }

    #[test]
    fn render_ctrl_list_empty() {
        let data = serde_json::json!({ "pid": 4242, "port": sample_port(), "controllers": [] });
        assert_eq!(
            super::render_ctrl_list(&data),
            format!("pid 4242\n{PORT_HEADER}no controllers\n")
        );
    }

    #[test]
    fn render_ctrl_list_gib_sizes() {
        let data = serde_json::json!({
            "pid": 1,
            "port": {
                "traddr": "::", "trsvcid": "14420",
                "subsystems": [{
                    "nqn": "nqn.x",
                    // 2 GiB in 4096B blocks.
                    "namespaces": [{"nsid": 7, "blocks": 524288, "block_shift": 12}],
                }],
            },
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
                "queues": [{"qid": 0, "sqsize": 32, "tid": 100}],
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
             \x20 queues: 0:32@100\n\
             \x20 ns:     -\n"
        );
    }
}
