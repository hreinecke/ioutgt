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
    #[arg(long)]
    control_socket: Option<std::path::PathBuf>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Send one JSON request to a running target's control socket.
    Ctl {
        /// Control socket path.
        #[arg(long, default_value = "/tmp/ioutgt.sock")]
        socket: std::path::PathBuf,
        /// Request JSON, e.g. '{"op":"LIST_NAMESPACE"}'.
        request: String,
    },
    /// List live controllers on a running target (queues, threads,
    /// namespaces).
    ListCtrl {
        /// Control socket path.
        #[arg(long, default_value = "/tmp/ioutgt.sock")]
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

/// `ioutgt list-ctrl`: render LIST_CONTROLLER output for humans.
fn list_ctrl(socket: &std::path::Path) -> std::io::Result<()> {
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
            Command::ListCtrl { socket } => return list_ctrl(socket),
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
            config.control_socket = args.control_socket;
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
    #[test]
    fn render_ctrl_list_formats_controllers() {
        let data = serde_json::json!({
            "pid": 4242,
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
        assert_eq!(
            out,
            "pid 4242\n\
             controller 1: nqn.2026-06.io.ioutgt:test\n\
             \x20 host:   nqn.2014-08.org.nvmexpress:uuid:abc\n\
             \x20 kato:   60000 ms\n\
             \x20 queues: 0:32@100 1:64@101\n\
             \x20 ns:     1\n"
        );
    }

    #[test]
    fn render_ctrl_list_empty() {
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
