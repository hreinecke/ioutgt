//! Focused-v1 NVMe/RDMA target binary.
//!
//! Serves a single in-memory subsystem over RDMA on one reactor thread, enough
//! to bring up a controller with `nvme connect -t rdma` and run IO against it.
//! The CLI is intentionally minimal; full flag alignment with the
//! `ioutgt-nvme-tcp` binary (backends, control socket, pinning, …) is a later
//! milestone (RD5). All RDMA queues run on this one thread for now.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use ioutgt_backend::{AnyBackend, MemoryBackend};
use ioutgt_core::controller::Registry;
use ioutgt_core::subsystem::{Namespace, PortConfig, Subsystem, TransportType};
use ioutgt_uring::{QueueRuntime, RingConfig};

/// Logical block size (512 B) for the memory namespace.
const BLOCK_SHIFT: u8 = 9;
/// Default advertised IO queue depth.
const IO_QUEUE_SIZE: u16 = 128;
/// Default per-IO-queue data-buffer pool (bytes).
const QUEUE_BUF_BYTES: usize = 8 * 1024 * 1024;

struct Args {
    listen: SocketAddr,
    nqn: String,
    mem_size_mb: u64,
}

fn parse_args() -> Args {
    let mut listen: SocketAddr = "0.0.0.0:4420".parse().expect("default addr");
    let mut nqn = "nqn.2025-01.io.ioutgt:rdma".to_string();
    let mut mem_size_mb: u64 = 256;
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--listen" | "-l" => {
                listen = it
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| fail("--listen expects <ip:port>"));
            }
            "--nqn" | "-n" => nqn = it.next().unwrap_or_else(|| fail("--nqn expects a value")),
            "--mem-size-mb" | "-m" => {
                mem_size_mb = it
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| fail("--mem-size-mb expects an integer"));
            }
            "--help" | "-h" => {
                eprintln!(
                    "ioutgt-nvme-rdma [--listen ip:port] [--nqn NQN] [--mem-size-mb N]\n\
                     defaults: 0.0.0.0:4420, nqn.2025-01.io.ioutgt:rdma, 256 MiB"
                );
                std::process::exit(0);
            }
            other => fail(&format!("unknown argument: {other}")),
        }
    }
    Args {
        listen,
        nqn,
        mem_size_mb,
    }
}

fn fail(msg: &str) -> ! {
    eprintln!("ioutgt-nvme-rdma: {msg}");
    std::process::exit(2);
}

/// Build a one-namespace in-memory subsystem and the port that advertises it.
fn build_port(args: &Args) -> Arc<PortConfig<AnyBackend>> {
    let backend = AnyBackend::Memory(MemoryBackend::new(args.mem_size_mb * 1024 * 1024, BLOCK_SHIFT));
    let mut uuid = [0u8; 16];
    uuid[..4].copy_from_slice(&1u32.to_be_bytes());
    uuid[8] = 0x80;
    let mut namespaces = BTreeMap::new();
    namespaces.insert(
        1u32,
        Arc::new(Namespace {
            nsid: 1,
            backend: Arc::new(backend),
            uuid,
        }),
    );
    let subsystem = Arc::new(Subsystem::new(
        args.nqn.clone(),
        "ioutgt-rdma-0".to_string(),
        "ioutgt-nvme-rdma".to_string(),
        1, // max IO queues (single thread, v1)
        true,
        namespaces,
    ));
    let mut subsystems = BTreeMap::new();
    subsystems.insert(args.nqn.clone(), subsystem);
    Arc::new(PortConfig {
        traddr: args.listen.ip().to_string(),
        trsvcid: args.listen.port().to_string(),
        trtype: TransportType::Rdma,
        io_queue_size: IO_QUEUE_SIZE,
        queue_buf_bytes: QUEUE_BUF_BYTES,
        recv_buf_bytes: 0,
        subsystems,
    })
}

fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = parse_args();
    let port = build_port(&args);
    let registry = Registry::new();
    let listen = args.listen;
    let queue_buf_bytes = port.queue_buf_bytes;

    let rt = QueueRuntime::new(RingConfig::default())?;
    rt.block_on(ioutgt_nvme_rdma::target::serve(
        listen,
        port,
        registry,
        queue_buf_bytes,
    ))
}
