//! NVMe status codes as combined `(SCT << 8) | SC` values, matching the
//! Linux kernel's `NVME_SC_*` representation. The CQE encoder shifts the
//! whole value left by one into the wire status field.

#![allow(missing_docs)] // wire-format mirrors: the NVMe spec is the documentation

/// Generic command status (SCT 0).
pub const SUCCESS: u16 = 0x0;
pub const INVALID_OPCODE: u16 = 0x1;
pub const INVALID_FIELD: u16 = 0x2;
pub const CMDID_CONFLICT: u16 = 0x3;
pub const DATA_XFER_ERROR: u16 = 0x4;
pub const INTERNAL: u16 = 0x6;
pub const ABORT_REQ: u16 = 0x7;
pub const SGL_INVALID_LAST: u16 = 0xD;
pub const SGL_INVALID_TYPE: u16 = 0x11;
pub const INVALID_NS: u16 = 0xB;
pub const LBA_RANGE: u16 = 0x80;
pub const CAP_EXCEEDED: u16 = 0x81;
pub const NS_NOT_READY: u16 = 0x82;

/// Command-specific status (SCT 1).
pub const INVALID_QUEUE_TYPE: u16 = 0x101;
pub const INVALID_QUEUE_SIZE: u16 = 0x102;
pub const FEATURE_NOT_SAVEABLE: u16 = 0x10D;
pub const FEATURE_NOT_CHANGEABLE: u16 = 0x10E;
pub const INVALID_LOG_PAGE: u16 = 0x109;

/// Fabrics command-specific status.
pub const CONNECT_FORMAT: u16 = 0x180;
pub const CONNECT_CTRL_BUSY: u16 = 0x181;
pub const CONNECT_INVALID_PARAM: u16 = 0x182;
pub const CONNECT_RESTART_REQ: u16 = 0x183;
pub const CONNECT_INVALID_HOST: u16 = 0x184;

/// Do Not Retry bit.
pub const DNR: u16 = 0x4000;

/// IATTR/IPO encoding for CONNECT_INVALID_PARAM result DW0: byte offset
/// of the offending field in the Connect command or data.
pub fn connect_invalid_param_result(in_data: bool, offset: u16) -> u32 {
    (u32::from(offset) << 16) | u32::from(in_data)
}
