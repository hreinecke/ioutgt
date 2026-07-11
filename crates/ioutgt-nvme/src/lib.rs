//! The NVMe crate: sans-IO protocol codec plus transport-independent
//! command execution.
//!
//! The codec modules — [`spec`], [`pdu`], [`identify`], [`fabrics`],
//! [`status`], [`digest`] — are NVMe wire structures as `repr(C)`
//! zerocopy types, an incremental PDU decoder/encoder operating purely
//! on byte slices, and CRC32C digest helpers. **They perform no IO and
//! own no sockets**: the target data path, the control-thread handshake,
//! the integration-test client, and the decoder fuzz test all share this
//! one codec. All wire integers are little-endian per the NVMe base
//! specification.
//!
//! The execution modules — [`dispatch`], [`fabrics_exec`], [`admin`],
//! [`io`], [`controller`] — are the transport-independent NVMe target:
//! admin/IO command handling and the CC/CSTS register machine, layered
//! on the engine and structural model (subsystem tables, controller
//! registry) in `ioutgt-core`. They mirror the role of
//! `core.c`/`nvmet.h` in the Linux kernel nvmet target.

pub mod admin;
pub mod controller;
pub mod digest;
pub mod dispatch;
pub mod fabrics;
pub mod fabrics_exec;
pub mod identify;
pub mod io;
pub mod pdu;
pub mod spec;
pub mod status;

/// Cap on admin-command response data staged in a slot (identify/log
/// pages). The admin pool is sized `depth × ADMIN_DATA_MAX` so admin
/// leases never block.
pub const ADMIN_DATA_MAX: usize = 8 * 1024;

/// In-capsule data the RDMA transport advertises via IOCCSZ (one page,
/// matching nvmet-rdma's default `inline_data_size`): the host then embeds
/// write payloads up to this size in the command capsule itself, sparing the
/// target a per-write RDMA READ round trip. Must match the RDMA transport's
/// RECV capsule sizing.
pub const RDMA_INLINE_DATA_SIZE: u32 = 4096;

/// In-capsule data we advertise via IOCCSZ (16 KiB, nvmet's default).
pub const INLINE_DATA_SIZE: u32 = 16 * 1024;

/// AEC bit: namespace-attribute-changed notices.
pub const AEN_CFG_NS_ATTR: u32 = 1 << 8;
