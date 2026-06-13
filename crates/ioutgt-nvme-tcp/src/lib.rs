//! NVMe/TCP transport.
//!
//! Two halves, matching the thread model:
//!
//! - [`handshake`]: runs on the control thread over ordinary Tokio
//!   sockets — ICReq/ICResp digest negotiation plus reading the first
//!   (Connect) capsule, whose `qid` decides which queue thread receives
//!   the connection.
//! - [`connection`]: runs on a queue thread over `ioutgt-uring` — the
//!   per-connection recv state machine (PDU → payload → DDGST), the
//!   slot-task pipeline, and the ordered send path.
//!
//! All parsing is the sans-io codec in `ioutgt-nvme`; this crate only
//! moves bytes.

pub mod connection;
pub mod handshake;
pub mod queue;

pub use connection::H2C_DIRECT_MIN;

/// MAXH2CDATA we advertise in ICResp (16 MiB, as kernel nvmet).
pub const MAX_H2C_DATA: u32 = 0x40_0000 * 4;

/// In-capsule data limit we advertise via IOCCSZ (16 KiB, as nvmet's
/// default inline_data_size).
pub const INLINE_DATA_SIZE: u32 = 16 * 1024;
