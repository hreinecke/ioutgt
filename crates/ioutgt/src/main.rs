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
}

fn ctl(socket: &std::path::Path, request: &str) -> std::io::Result<()> {
    // Validate locally for a friendlier error than the server echo.
    serde_json::from_str::<serde_json::Value>(request)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let mut stream = std::os::unix::net::UnixStream::connect(socket)?;
    stream.write_all(request.as_bytes())?;
    stream.write_all(b"\n")?;
    let mut response = String::new();
    BufReader::new(&stream).read_line(&mut response)?;
    print!("{response}");
    let ok = serde_json::from_str::<serde_json::Value>(&response)
        .ok()
        .and_then(|v| v.get("ok").and_then(serde_json::Value::as_bool))
        .unwrap_or(false);
    if !ok {
        std::process::exit(1);
    }
    Ok(())
}

fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    if let Some(Command::Ctl { socket, request }) = &args.command {
        return ctl(socket, request);
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
