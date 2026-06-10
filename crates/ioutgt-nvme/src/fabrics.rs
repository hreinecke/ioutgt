//! NVMe-oF fabrics command capsules and discovery structures.
//!
//! Fabrics commands share admin opcode 0x7F; bytes 4..64 of the SQE are
//! reinterpreted per `fctype`. Layouts mirror `include/linux/nvme.h`.

#![allow(missing_docs)] // wire-format mirrors: the NVMe spec is the documentation

use zerocopy::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, FromZeros, Immutable, IntoBytes, KnownLayout};

/// Fabrics command types.
pub mod fctype {
    pub const PROPERTY_SET: u8 = 0x00;
    pub const CONNECT: u8 = 0x01;
    pub const PROPERTY_GET: u8 = 0x04;
}

/// Property offsets (fabrics register space).
pub mod prop {
    pub const CAP: u32 = 0x00;
    pub const VS: u32 = 0x08;
    pub const CC: u32 = 0x14;
    pub const CSTS: u32 = 0x1C;
    pub const NSSR: u32 = 0x20;
}

/// Controller Configuration (CC) bits.
pub mod cc {
    pub const EN: u32 = 1 << 0;
    /// Shutdown notification field (bits 15:14).
    pub const SHN_MASK: u32 = 0b11 << 14;
    pub const SHN_NORMAL: u32 = 0b01 << 14;
    /// IO SQ/CQ entry sizes (must be 6 and 4).
    pub const IOSQES_SHIFT: u32 = 16;
    pub const IOCQES_SHIFT: u32 = 20;
}

/// Controller Status (CSTS) bits.
pub mod csts {
    pub const RDY: u32 = 1 << 0;
    pub const CFS: u32 = 1 << 1;
    /// Shutdown status: complete.
    pub const SHST_COMPLETE: u32 = 0b10 << 2;
}

/// The well-known discovery subsystem NQN.
pub const DISCOVERY_NQN: &str = "nqn.2014-08.org.nvmexpress.discovery";

/// Connect command (SQE reinterpretation).
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub struct ConnectCommand {
    pub opcode: u8,
    pub resv1: u8,
    pub cid: U16,
    pub fctype: u8,
    pub resv2: [u8; 19],
    /// SGL1 describing the 1024-byte Connect data (in-capsule).
    pub dptr: crate::spec::SglDescriptor,
    pub recfmt: U16,
    pub qid: U16,
    /// 0-based submission queue size.
    pub sqsize: U16,
    pub cattr: u8,
    pub resv3: u8,
    /// Keep-alive timeout in milliseconds (admin connect only).
    pub kato: U32,
    pub resv4: [u8; 12],
}

/// Connect data capsule payload (1024 bytes).
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct ConnectData {
    pub hostid: [u8; 16],
    /// 0xFFFF on admin connect (dynamic controller allocation).
    pub cntlid: U16,
    pub resv4: [u8; 238],
    pub subsysnqn: [u8; 256],
    pub hostnqn: [u8; 256],
    pub resv5: [u8; 256],
}

impl ConnectData {
    /// Zeroed payload for client/test construction.
    pub fn zeroed() -> Self {
        Self::new_zeroed()
    }
}

/// Property Get/Set command (SQE reinterpretation; `value` is reserved
/// on Get).
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub struct PropertyCommand {
    pub opcode: u8,
    pub resv1: u8,
    pub cid: U16,
    pub fctype: u8,
    pub resv2: [u8; 35],
    /// Bit 2:0 size: 0 = 4 bytes, 1 = 8 bytes.
    pub attrib: u8,
    pub resv3: [u8; 3],
    pub offset: U32,
    pub value: U64,
    pub resv4: [u8; 8],
}

/// Discovery log page header (1024 bytes).
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct DiscoveryLogHeader {
    pub genctr: U64,
    pub numrec: U64,
    pub recfmt: U16,
    pub resv: [u8; 1006],
}

/// One discovery log entry (1024 bytes).
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct DiscoveryLogEntry {
    /// Transport type: 3 = TCP.
    pub trtype: u8,
    /// Address family: 1 = IPv4, 2 = IPv6.
    pub adrfam: u8,
    /// Subsystem type: 2 = NVM subsystem, 3 = current discovery subsystem.
    pub subtype: u8,
    /// Transport requirements (secure channel): 0 = not specified.
    pub treq: u8,
    pub portid: U16,
    /// 0xFFFF = dynamic controller model.
    pub cntlid: U16,
    /// Admin max SQ size.
    pub asqsz: U16,
    pub eflags: U16,
    pub resv12: [u8; 20],
    /// Transport service id (port number as ASCII, space padded).
    pub trsvcid: [u8; 32],
    pub resv64: [u8; 192],
    pub subnqn: [u8; 256],
    /// Transport address (IP as ASCII, space padded).
    pub traddr: [u8; 256],
    pub tsas: [u8; 256],
}

impl DiscoveryLogEntry {
    /// Zeroed entry; fill and space-pad the string fields.
    pub fn zeroed() -> Self {
        Self::new_zeroed()
    }
}

/// Transport type constants for `DiscoveryLogEntry::trtype`.
pub mod trtype {
    pub const RDMA: u8 = 1;
    pub const FC: u8 = 2;
    pub const TCP: u8 = 3;
}

/// Subsystem type constants.
pub mod subtype {
    pub const DISCOVERY: u8 = 3;
    pub const NVM: u8 = 2;
}

const _: () = {
    assert!(size_of::<ConnectCommand>() == 64);
    assert!(size_of::<ConnectData>() == 1024);
    assert!(size_of::<PropertyCommand>() == 64);
    assert!(size_of::<DiscoveryLogHeader>() == 1024);
    assert!(size_of::<DiscoveryLogEntry>() == 1024);
};
