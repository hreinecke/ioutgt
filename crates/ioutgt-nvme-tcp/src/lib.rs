//! NVMe/TCP transport and target binary.
//!
//! The PDU machinery has two halves, matching the thread model:
//!
//! - [`handshake`]: runs on the control thread over ordinary Tokio
//!   sockets — ICReq/ICResp digest negotiation plus reading the first
//!   (Connect) capsule, whose `qid` decides which queue thread receives
//!   the connection.
//! - [`connection`]: runs on a queue thread over `ioutgt-uring` — the
//!   per-connection recv state machine (PDU → payload → DDGST), the
//!   slot-task pipeline, and the ordered send path.
//!
//! All parsing is the sans-io codec in `ioutgt-nvme`; the transport only
//! moves bytes.
//!
//! [`transport::TcpTransport`] plugs those halves into the harness
//! [`Transport`](ioutgt_harness::Transport) seam, and [`spawn_target`]
//! starts a full target on the shared [`ioutgt_harness`] queue-thread
//! pool. The crate ships the `ioutgt-nvme-tcp` binary (`src/main.rs`) and
//! is also a library so integration tests can start a target in-process
//! on an ephemeral port. The sibling `ioutgt-nvme-rdma` crate has the
//! same shape for the RDMA transport.

use std::io;
use std::net::SocketAddr;

pub mod connection;
pub mod handshake;
pub mod queue;
pub mod recv;
pub mod send;
pub mod transport;

use transport::TcpTransport;

pub use ioutgt_harness::TargetConfig;
pub use recv::H2C_DIRECT_MIN;

/// Connect CATTR bit 2: host requests SQ flow control disabled.
pub(crate) const CONNECT_DISABLE_SQFLOW: u8 = 1 << 2;

/// Start an NVMe/TCP target's control thread; returns the bound address (for
/// ephemeral-port tests). Runs until the process exits.
pub fn spawn_target(config: TargetConfig) -> io::Result<SocketAddr> {
    if config.send_zc {
        // Opt-in experiment: refuse to start rather than silently
        // fall back to the copying path.
        let features = ioutgt_uring::probe()?;
        if !features.sendmsg_zc {
            return Err(io::Error::other(
                "--send-zc requested but the kernel lacks IORING_OP_SENDMSG_ZC (need >= 6.1)",
            ));
        }
    }
    ioutgt_harness::spawn::<TcpTransport>(config)
}

/// Hard ceiling (entries) on a connecting queue's depth. IO queues
/// (qid > 0) are bounded by the configured MAXCMD (`io_queue_size`); the
/// admin queue (qid 0), host-fixed at `NVME_AQ_DEPTH`, keeps the CAP.MQES
/// guard so it is never rejected when `io_queue_size` is set small.
pub(crate) fn sqsize_cap(qid: u16, io_queue_size: u16) -> u16 {
    if qid == 0 {
        ioutgt_nvme::MAX_QUEUE_ENTRIES
    } else {
        io_queue_size
    }
}

/// MAXH2CDATA we advertise in ICResp (16 MiB, as kernel nvmet).
pub const MAX_H2C_DATA: u32 = 0x40_0000 * 4;

#[cfg(test)]
mod tests {
    use super::sqsize_cap;

    #[test]
    fn sqsize_cap_bounds_io_at_config_admin_at_mqes() {
        // IO queues (qid > 0) are capped at the configured ceiling…
        assert_eq!(sqsize_cap(1, 64), 64);
        assert_eq!(sqsize_cap(3, 200), 200);
        // …while the admin queue keeps the hard CAP.MQES guard, so it is
        // never rejected even when io_queue_size is set below the admin
        // depth (NVME_AQ_DEPTH = 32).
        assert_eq!(sqsize_cap(0, 8), ioutgt_nvme::MAX_QUEUE_ENTRIES);
        assert_eq!(sqsize_cap(0, 256), ioutgt_nvme::MAX_QUEUE_ENTRIES);
    }
}
