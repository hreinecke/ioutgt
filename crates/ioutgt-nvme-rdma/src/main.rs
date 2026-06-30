//! ioutgt-nvme-rdma — io_uring-based NVMe/RDMA target binary.
//!
//! Runs on the shared `ioutgt-harness` queue-thread pool (admin thread + N IO
//! threads, CPU-pinned), with the runtime control socket (`ctl`/`list`/`stat`
//! served by the harness). The CLI mirrors the transport-neutral subset of the
//! `ioutgt-nvme-tcp` binary; TCP-only knobs (digests, `--send-zc`,
//! `--recv-buf-mb`) are absent.

use clap::Parser;
use ioutgt_control::config::BackendConfig;
use ioutgt_harness::TargetConfig;
use ioutgt_nvme_rdma::transport::RdmaTransport;

#[derive(Parser, Debug)]
#[command(version, about = "io_uring-based NVMe/RDMA target")]
struct Args {
    /// JSON config file (overrides the individual flags below). Shares the
    /// `ioutgt-nvme-tcp` schema; TCP-only fields (digests, `send_zc`,
    /// `recv_buf_mb`) are parsed but ignored.
    #[arg(long)]
    config: Option<std::path::PathBuf>,

    /// Listen address (RoCE addr:port).
    #[arg(long, default_value = "0.0.0.0:4420")]
    listen: std::net::SocketAddr,

    /// Number of IO queue threads (admin thread is implicit).
    #[arg(long, default_value_t = 2)]
    io_threads: usize,

    /// Disable topology-aware IO-thread pinning (on by default).
    #[arg(long)]
    no_pin: bool,

    /// NVM subsystem NQN.
    #[arg(long, default_value = "nqn.2026-06.io.ioutgt:test")]
    subsys_nqn: String,

    /// Namespace backend: memory, null, or a file/blockdev path.
    #[arg(long, default_value = "memory")]
    backend: String,

    /// Memory/null-backend namespace size in MiB.
    #[arg(long, default_value_t = 64)]
    mem_size_mb: u64,

    /// Max IO queue depth advertised to the host (MAXCMD); the host uses
    /// min(its queue-size, this). Capped at CAP.MQES (256).
    #[arg(long, default_value_t = 128, value_parser = clap::value_parser!(u16).range(2..=256))]
    io_queue_size: u16,

    /// Per-IO-queue data-buffer pool size in MiB (slots lease on demand).
    #[arg(long, default_value_t = ioutgt_core::pool::DEFAULT_POOL_MB)]
    queue_buf_mb: usize,

    /// Tear the queue-thread pool down after this many idle seconds (0 = keep
    /// it up for the process lifetime once spawned).
    #[arg(long, default_value_t = 30)]
    idle_teardown_secs: u64,

    /// Unix socket path for the runtime control API.
    #[arg(long, default_value_os_t = default_control_socket())]
    control_socket: std::path::PathBuf,
}

/// `$XDG_RUNTIME_DIR/ioutgt-rdma.sock`, else `/tmp/ioutgt-rdma.sock`.
fn default_control_socket() -> std::path::PathBuf {
    match std::env::var_os("XDG_RUNTIME_DIR") {
        Some(dir) if !dir.is_empty() => std::path::Path::new(&dir).join("ioutgt-rdma.sock"),
        _ => std::path::PathBuf::from("/tmp/ioutgt-rdma.sock"),
    }
}

fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let config = match &args.config {
        Some(path) => {
            let mut config = TargetConfig::from_file(path)?;
            // RDMA has no recv ring; force it off even if the shared config sets
            // recv_buf_mb (which would also flip file backends to O_DIRECT).
            config.recv_buf_bytes = 0;
            config
        }
        None => {
            let mut config = TargetConfig::single_memory(&args.subsys_nqn, args.mem_size_mb);
            config.listen = args.listen;
            config.io_threads = args.io_threads;
            config.pin_threads = !args.no_pin;
            config.io_queue_size = args.io_queue_size;
            config.queue_buf_bytes = args.queue_buf_mb.saturating_mul(1024 * 1024);
            config.recv_buf_bytes = 0; // RDMA has no recv ring.
            config.idle_teardown = (args.idle_teardown_secs != 0)
                .then(|| std::time::Duration::from_secs(args.idle_teardown_secs));
            config.control_socket = Some(args.control_socket);
            config.subsystems[0].namespaces[0].backend = match args.backend.as_str() {
                "memory" => BackendConfig::Memory {
                    size_mb: args.mem_size_mb,
                },
                "null" => BackendConfig::Null {
                    size_mb: args.mem_size_mb,
                },
                path => BackendConfig::File { path: path.into() },
            };
            config
        }
    };

    let addr = ioutgt_harness::spawn::<RdmaTransport>(config)?;
    eprintln!("ioutgt-nvme-rdma listening on {addr}");
    // The target runs on its own threads; park the main thread.
    loop {
        std::thread::park();
    }
}
