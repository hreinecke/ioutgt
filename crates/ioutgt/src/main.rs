//! ioutgt — high-performance io_uring-based NVMe/TCP target.

use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about = "io_uring-based NVMe/TCP target")]
struct Args {
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

    /// Memory-backend namespace size in MiB.
    #[arg(long, default_value_t = 64)]
    mem_size_mb: u64,
}

fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let config = ioutgt::TargetConfig {
        listen: args.listen,
        io_threads: args.io_threads,
        allow_hdgst: !args.no_hdgst,
        allow_ddgst: !args.no_ddgst,
        pin_threads: args.pin,
        subsys_nqn: args.subsys_nqn,
        mem_size_mb: args.mem_size_mb,
    };
    let addr = ioutgt::spawn_target(config)?;
    eprintln!("ioutgt listening on {addr}");
    // The target runs on its own threads; park the main thread.
    loop {
        std::thread::park();
    }
}
