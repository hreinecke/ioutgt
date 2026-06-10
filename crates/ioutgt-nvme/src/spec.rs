//! NVMe base-spec command structures and constants.

#![allow(missing_docs)] // wire-format mirrors: the NVMe spec is the documentation

use zerocopy::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, FromZeros, Immutable, IntoBytes, KnownLayout};

/// Admin command opcodes (subset ioutgt implements).
pub mod admin_opcode {
    pub const GET_LOG_PAGE: u8 = 0x02;
    pub const IDENTIFY: u8 = 0x06;
    pub const SET_FEATURES: u8 = 0x09;
    pub const GET_FEATURES: u8 = 0x0A;
    pub const ASYNC_EVENT: u8 = 0x0C;
    pub const KEEP_ALIVE: u8 = 0x18;
    /// All fabrics commands share this opcode; `fctype` selects.
    pub const FABRICS: u8 = 0x7F;
}

/// NVM (IO) command opcodes.
pub mod io_opcode {
    pub const FLUSH: u8 = 0x00;
    pub const WRITE: u8 = 0x01;
    pub const READ: u8 = 0x02;
    pub const WRITE_ZEROES: u8 = 0x08;
    pub const DSM: u8 = 0x09;
}

/// Identify CNS values.
pub mod cns {
    pub const NAMESPACE: u8 = 0x00;
    pub const CONTROLLER: u8 = 0x01;
    pub const ACTIVE_NS_LIST: u8 = 0x02;
    pub const NS_DESC_LIST: u8 = 0x03;
}

/// Feature identifiers.
pub mod feat {
    pub const NUM_QUEUES: u8 = 0x07;
    pub const ASYNC_EVENT_CONFIG: u8 = 0x0B;
    pub const KATO: u8 = 0x0F;
    pub const HOST_ID: u8 = 0x81;
}

/// Log page identifiers.
pub mod log_page {
    pub const ERROR: u8 = 0x01;
    pub const SMART: u8 = 0x02;
    pub const FW_SLOT: u8 = 0x03;
    pub const CHANGED_NS: u8 = 0x04;
    pub const DISCOVERY: u8 = 0x70;
}

/// PSDT field (bits 7:6 of SQE `flags`): fabrics requires SGL for all
/// commands (`NVME_CMD_SGL_METABUF`).
pub const CMD_FLAGS_SGL_METABUF: u8 = 0x40;

/// SGL descriptor: the only two formats the NVMe/TCP host sends.
pub mod sgl {
    /// Data Block + Offset addressing: in-capsule data (`addr` = offset
    /// past ICDOFF, `length` = bytes inline in the command capsule).
    pub const TYPE_DATA_BLOCK_OFFSET: u8 = 0x0A;
    /// Transport SGL Data Block (subtype 0xA): data stays host-resident,
    /// transferred via R2T/H2CData or C2HData.
    pub const TYPE_TRANSPORT_DATA_BLOCK: u8 = 0x5A;
}

/// 16-byte SGL descriptor as carried in `Sqe::dptr`.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub struct SglDescriptor {
    /// Offset for in-capsule data; 0 for transport data blocks.
    pub addr: U64,
    /// Data length in bytes.
    pub length: U32,
    pub rsvd: [u8; 3],
    /// Type (high nibble) | subtype (low nibble); see [`sgl`].
    pub sgl_type: u8,
}

/// 64-byte submission queue entry, common layout.
///
/// `cdw10`..`cdw15` are command-specific; fabrics commands reinterpret
/// bytes 4..64 entirely (see [`crate::fabrics`]).
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub struct Sqe {
    pub opcode: u8,
    pub flags: u8,
    pub cid: U16,
    pub nsid: U32,
    pub cdw2: U32,
    pub cdw3: U32,
    pub mptr: U64,
    pub dptr: SglDescriptor,
    pub cdw10: U32,
    pub cdw11: U32,
    pub cdw12: U32,
    pub cdw13: U32,
    pub cdw14: U32,
    pub cdw15: U32,
}

impl Sqe {
    /// Zeroed SQE (useful for tests and capsule construction).
    pub fn zeroed() -> Self {
        Self::new_zeroed()
    }
}

/// 16-byte completion queue entry.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub struct Cqe {
    /// Command-specific result (DW0).
    pub result: U32,
    /// DW1 (reserved for the commands we implement).
    pub rsvd: U32,
    pub sq_head: U16,
    pub sq_id: U16,
    pub cid: U16,
    /// Bit 0: phase (unused on fabrics); bits 15:1: status field
    /// (`status_code | sct << 8 | ...` shifted left by one).
    pub status: U16,
}

impl Cqe {
    /// Build a CQE with `status` being the combined (SCT<<8|SC) code as
    /// used throughout this crate; shifted into wire position here.
    pub fn new(result: u32, sq_head: u16, sq_id: u16, cid: u16, status: u16) -> Self {
        Cqe {
            result: U32::new(result),
            rsvd: U32::new(0),
            sq_head: U16::new(sq_head),
            sq_id: U16::new(sq_id),
            cid: U16::new(cid),
            status: U16::new(status << 1),
        }
    }
}

/// Read/write command view of cdw10..cdw13 (NVM command set).
#[derive(Clone, Copy, Debug)]
pub struct RwCommand {
    /// Starting LBA.
    pub slba: u64,
    /// 0-based number of logical blocks.
    pub nlb: u16,
    /// Force Unit Access.
    pub fua: bool,
}

impl RwCommand {
    /// Decode from a generic SQE.
    pub fn parse(sqe: &Sqe) -> RwCommand {
        let slba = u64::from(sqe.cdw10.get()) | (u64::from(sqe.cdw11.get()) << 32);
        let cdw12 = sqe.cdw12.get();
        RwCommand {
            slba,
            #[allow(clippy::cast_possible_truncation)]
            nlb: (cdw12 & 0xFFFF) as u16,
            fua: cdw12 & (1 << 30) != 0,
        }
    }
}

/// DSM range (16 bytes), used by Dataset Management (deallocate).
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub struct DsmRange {
    pub cattr: U32,
    pub nlb: U32,
    pub slba: U64,
}

const _: () = {
    assert!(size_of::<Sqe>() == 64);
    assert!(size_of::<Cqe>() == 16);
    assert!(size_of::<SglDescriptor>() == 16);
    assert!(size_of::<DsmRange>() == 16);
};
