//! ioutgt-nvme-rdma — io_uring-based NVMe/RDMA target binary.
//!
//! Focused-v1: a single reactor thread serves the configured subsystem(s) over
//! RDMA. The CLI mirrors the transport-neutral subset of the `ioutgt-nvme-tcp`
//! binary — `--config`, `--listen`, `--subsys-nqn`, `--backend`, `--mem-size-mb`,
//! `--io-queue-size`, `--queue-buf-mb`. The harness-only knobs
//! (`--io-threads`/`--no-pin`/`--control-socket`/`--idle-teardown-secs`) and
//! TCP-only knobs (digests/`--send-zc`/`--recv-buf-mb`) are intentionally absent
//! until the harness integration (RD6).

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use ioutgt_backend::AnyBackend;
use ioutgt_control::config::{BackendConfig, FileConfig, SubsystemConfig};
use ioutgt_control::server::build_backend;
use ioutgt_core::controller::Registry;
use ioutgt_core::subsystem::{Namespace, PortConfig, Subsystem, TransportType};
use ioutgt_uring::{QueueRuntime, RingConfig};

#[derive(Parser, Debug)]
#[command(version, about = "io_uring-based NVMe/RDMA target")]
struct Args {
    /// JSON config file (overrides the individual flags below). Shares the
    /// `ioutgt-nvme-tcp` schema; TCP/harness-only fields (digests, `send_zc`,
    /// `recv_buf_mb`, `io_threads`, `pin_threads`, `control_socket`,
    /// `idle_teardown_secs`) are parsed but ignored by this single-threaded
    /// RDMA target.
    #[arg(long)]
    config: Option<std::path::PathBuf>,

    /// Listen address (RoCE addr:port).
    #[arg(long, default_value = "0.0.0.0:4420")]
    listen: SocketAddr,

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
    /// min(its queue-size, this). The admin queue is unaffected. Capped at
    /// CAP.MQES (256).
    #[arg(long, default_value_t = 128, value_parser = clap::value_parser!(u16).range(2..=256))]
    io_queue_size: u16,

    /// Per-IO-queue data-buffer pool size in MiB. Slots lease their read/write
    /// buffers from this shared arena on demand (4 KiB grain).
    #[arg(long, default_value_t = ioutgt_core::pool::DEFAULT_POOL_MB)]
    queue_buf_mb: usize,
}

/// Build a subsystem (with its namespaces' backends) from a config entry.
fn build_subsystem(spec: &SubsystemConfig) -> Result<Arc<Subsystem<AnyBackend>>, String> {
    let mut namespaces = BTreeMap::new();
    for ns in &spec.namespaces {
        let backend = build_backend(&ns.backend, false)?;
        let mut uuid = [0u8; 16];
        uuid[..4].copy_from_slice(&ns.nsid.to_be_bytes());
        uuid[8] = 0x80;
        namespaces.insert(
            ns.nsid,
            Arc::new(Namespace {
                nsid: ns.nsid,
                backend: Arc::new(backend),
                uuid,
            }),
        );
    }
    // Single reactor thread serves every queue, so one IO queue is advertised.
    Ok(Arc::new(Subsystem::new(
        spec.nqn.clone(),
        spec.serial.clone(),
        spec.model.clone(),
        1,
        spec.allow_any_host,
        namespaces,
    )))
}

/// Assemble the port from the config file or the individual flags.
fn build_port(args: &Args) -> Result<(SocketAddr, Arc<PortConfig<AnyBackend>>), String> {
    let (listen, io_queue_size, queue_buf_bytes, specs): (_, _, _, Vec<SubsystemConfig>) =
        match &args.config {
            Some(path) => {
                // `load` already validates.
                let cfg = FileConfig::load(path)?;
                let listen: SocketAddr = cfg
                    .listen
                    .parse()
                    .map_err(|e| format!("listen {}: {e}", cfg.listen))?;
                let qbb = cfg.queue_buf_mb.saturating_mul(1 << 20);
                (listen, cfg.io_queue_size, qbb, cfg.subsystems)
            }
            None => {
                let backend = match args.backend.as_str() {
                    "memory" => BackendConfig::Memory {
                        size_mb: args.mem_size_mb,
                    },
                    "null" => BackendConfig::Null {
                        size_mb: args.mem_size_mb,
                    },
                    path => BackendConfig::File { path: path.into() },
                };
                let spec = SubsystemConfig {
                    nqn: args.subsys_nqn.clone(),
                    serial: "ioutgt-rdma-0".to_string(),
                    model: "ioutgt-nvme-rdma".to_string(),
                    allow_any_host: true,
                    namespaces: vec![ioutgt_control::config::NamespaceConfig { nsid: 1, backend }],
                };
                let qbb = args.queue_buf_mb.saturating_mul(1 << 20);
                (args.listen, args.io_queue_size, qbb, vec![spec])
            }
        };

    let mut subsystems: BTreeMap<String, Arc<Subsystem<AnyBackend>>> = BTreeMap::new();
    for spec in &specs {
        subsystems.insert(spec.nqn.clone(), build_subsystem(spec)?);
    }
    let port = Arc::new(PortConfig {
        traddr: listen.ip().to_string(),
        trsvcid: listen.port().to_string(),
        trtype: TransportType::Rdma,
        io_queue_size,
        queue_buf_bytes,
        recv_buf_bytes: 0,
        subsystems,
    });
    Ok((listen, port))
}

fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let (listen, port) = build_port(&args).map_err(std::io::Error::other)?;
    let registry = Registry::new();

    let rt = QueueRuntime::new(RingConfig::default())?;
    rt.block_on(ioutgt_nvme_rdma::target::serve(listen, port, registry))
}
