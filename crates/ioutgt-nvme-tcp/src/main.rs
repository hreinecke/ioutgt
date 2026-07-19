//! ioutgt-nvme-tcp — high-performance io_uring-based NVMe/TCP target binary.
//!
//! Runs on the shared `ioutgt-harness` queue-thread pool (admin thread + N IO
//! threads, CPU-pinned), with the runtime control socket (`ctl`/`list`/`stat`
//! served by the harness). The sibling `ioutgt-nvme-rdma` binary mirrors this
//! CLI for the RDMA transport.

use ioutgt_harness::client::{ctl, list_target, stat_target};

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(version, about = "io_uring-based NVMe/TCP target")]
struct Args {
    /// nvmetcli-format JSON config (kernel nvmet's save/restore
    /// schema). Its tcp port supplies the listen address and
    /// subsystems, replacing --listen/--subsys-nqn/--backend; the
    /// engine flags below still apply.
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

    /// Zero-copy sends (SENDMSG_ZC), gating buffer reuse on the
    /// kernel's notification CQE. Experimental: loopback always
    /// copies; a real NIC is needed for any benefit.
    #[arg(long)]
    send_zc: bool,

    /// Max IO queue depth in entries advertised to the host (MAXCMD);
    /// the host uses min(its queue-size, this). The admin queue is
    /// unaffected. Capped at CAP.MQES (256).
    #[arg(long, default_value_t = 128, value_parser = clap::value_parser!(u16).range(2..=256))]
    io_queue_size: u16,

    /// Per-IO-queue data-buffer pool size in MiB. Slots lease their
    /// read/write buffers from this shared arena on demand (4 KiB grain);
    /// deliberately smaller than depth × MDTS. Default 8 MiB.
    #[arg(long, default_value_t = ioutgt_core::pool::DEFAULT_POOL_MB)]
    queue_buf_mb: usize,

    /// Per-CONNECTION receive-ring size in MiB for zero-copy receive; 0 = off
    /// (classic per-recv scratch). Each ring-enabled connection allocates its
    /// own ring, so memory scales as (connections × this). Default 0.
    #[arg(long, default_value_t = 0)]
    recv_buf_mb: usize,

    /// Tear the queue-thread pool down after this many seconds with zero
    /// active connections, respawning it on the next connect; 0 keeps the
    /// pool alive for the process lifetime once spawned.
    #[arg(long, default_value_t = 30)]
    idle_teardown_secs: u64,

    /// NVM subsystem NQN.
    #[arg(long, default_value = "nqn.2026-06.io.ioutgt:test")]
    subsys_nqn: String,

    /// Memory/null-backend namespace size in MiB.
    #[arg(long, default_value_t = 64)]
    mem_size_mb: u64,

    /// Namespace backend: memory, null, or a file/blockdev path.
    #[arg(long, default_value = "memory")]
    backend: String,

    /// Unix socket path for the runtime control API. Also honored by the
    /// client subcommands (`ctl`/`list`/`stat`) when their `--socket` is not
    /// given.
    #[arg(long)]
    control_socket: Option<std::path::PathBuf>,

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
        /// Control socket path (defaults to `--control-socket`, then the
        /// per-user default path).
        #[arg(long)]
        socket: Option<std::path::PathBuf>,
        /// Request JSON, e.g. '{"op":"LIST_NAMESPACE"}'.
        request: String,
    },
    /// List the target: port inventory plus live controllers
    /// (queues, threads, namespaces).
    #[command(alias = "list-ctrl")]
    List {
        /// Control socket path (defaults to `--control-socket`, then the
        /// per-user default path).
        #[arg(long)]
        socket: Option<std::path::PathBuf>,
    },
    /// Per-thread ring and per-queue IO counters from a running target.
    Stat {
        /// Control socket path (defaults to `--control-socket`, then the
        /// per-user default path).
        #[arg(long)]
        socket: Option<std::path::PathBuf>,
        /// Repeat every N seconds, printing per-interval rates.
        #[arg(short, long, value_parser = clap::value_parser!(u64).range(1..))]
        interval: Option<u64>,
        /// Zero all counters after printing this (final) snapshot.
        #[arg(long, conflicts_with = "interval")]
        clear: bool,
    },
}

fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    if let Some(command) = &args.command {
        // A client's socket: its own --socket, else the top-level
        // --control-socket, else the per-user default — so
        // `<bin> --control-socket X stat` reaches the same path a server
        // started with `--control-socket X` is serving.
        let sock = |own: &Option<std::path::PathBuf>| {
            own.clone()
                .or_else(|| args.control_socket.clone())
                .unwrap_or_else(default_control_socket)
        };
        match command {
            Command::Ctl { socket, request } => return ctl(&sock(socket), request),
            Command::List { socket } => return list_target(&sock(socket)),
            Command::Stat {
                socket,
                interval,
                clear,
            } => return stat_target(&sock(socket), *interval, *clear),
        }
    }

    let mut config =
        ioutgt_nvme_tcp::TargetConfig::single_memory(&args.subsys_nqn, args.mem_size_mb);
    config.listen = args.listen;
    config.io_threads = args.io_threads;
    config.allow_hdgst = !args.no_hdgst;
    config.allow_ddgst = !args.no_ddgst;
    config.pin_threads = !args.no_pin;
    config.send_zc = args.send_zc;
    config.io_queue_size = args.io_queue_size;
    config.queue_buf_bytes = args.queue_buf_mb.saturating_mul(1024 * 1024);
    config.recv_buf_bytes = args.recv_buf_mb.saturating_mul(1024 * 1024);
    config.idle_teardown = (args.idle_teardown_secs != 0)
        .then(|| std::time::Duration::from_secs(args.idle_teardown_secs));
    config.control_socket = Some(args.control_socket.unwrap_or_else(default_control_socket));
    config.subsystems[0].namespaces[0].backend = match args.backend.as_str() {
        "memory" => ioutgt_control::config::BackendConfig::Memory {
            size_mb: args.mem_size_mb,
        },
        "null" => ioutgt_control::config::BackendConfig::Null {
            size_mb: args.mem_size_mb,
        },
        path => ioutgt_control::config::BackendConfig::File { path: path.into() },
    };
    // The config file owns the target model (listen + subsystems,
    // replacing the flag-built ones); engine flags above still apply.
    if let Some(path) = &args.config {
        config.apply_file(path, ioutgt_harness::TransportType::Tcp)?;
    }
    let addr = ioutgt_nvme_tcp::spawn_target(config)?;
    ioutgt_harness::announce_listening("ioutgt", addr);
    // The target runs on its own threads; park the main thread.
    loop {
        std::thread::park();
    }
}
