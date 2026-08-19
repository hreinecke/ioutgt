//! Identify data structures (CNS 0x00 / 0x01), 4096 bytes each, with
//! field offsets per NVMe Base Specification 2.0.

#![allow(missing_docs)] // wire-format mirrors: the NVMe spec is the documentation

use zerocopy::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, FromZeros, Immutable, IntoBytes, KnownLayout};

/// Identify Controller (CNS 0x01).
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct IdentifyController {
    pub vid: U16,
    pub ssvid: U16,
    /// Serial number, ASCII space padded.
    pub sn: [u8; 20],
    /// Model number, ASCII space padded.
    pub mn: [u8; 40],
    /// Firmware revision, ASCII space padded.
    pub fr: [u8; 8],
    pub rab: u8,
    pub ieee: [u8; 3],
    pub cmic: u8,
    /// Max data transfer size: 2^mdts * CAP.MPSMIN pages; 0 = unlimited.
    pub mdts: u8,
    pub cntlid: U16,
    pub ver: U32,
    pub rtd3r: U32,
    pub rtd3e: U32,
    pub oaes: U32,
    pub ctratt: U32,
    pub rsvd100: [u8; 11],
    /// 1 = IO controller, 2 = discovery controller.
    pub cntrltype: u8,
    pub fguid: [u8; 16],
    pub rsvd128: [u8; 128],
    pub oacs: U16,
    pub acl: u8,
    pub aerl: u8,
    pub frmw: u8,
    pub lpa: u8,
    pub elpe: u8,
    pub npss: u8,
    pub avscc: u8,
    pub apsta: u8,
    pub wctemp: U16,
    pub cctemp: U16,
    pub rsvd270: [u8; 50],
    /// Keep-alive support granularity in 100ms units.
    pub kas: U16,
    pub rsvd322: [u8; 190],
    /// SQ entry size (required 0x66 on fabrics).
    pub sqes: u8,
    /// CQ entry size (required 0x44 on fabrics).
    pub cqes: u8,
    pub maxcmd: U16,
    /// Number of namespaces (highest valid NSID).
    pub nn: U32,
    pub oncs: U16,
    pub fuses: U16,
    pub fna: u8,
    pub vwc: u8,
    pub awun: U16,
    pub awupf: U16,
    pub nvscc: u8,
    pub nwpc: u8,
    pub acwu: U16,
    pub rsvd534: [u8; 2],
    pub sgls: U32,
    pub mnan: U32,
    pub rsvd544: [u8; 224],
    pub subnqn: [u8; 256],
    pub rsvd1024: [u8; 768],
    /// IO command capsule size in 16-byte units: (64 + inline)/16.
    pub ioccsz: U32,
    /// IO response capsule size in 16-byte units: 1.
    pub iorcsz: U32,
    /// In-capsule data offset in 16-byte units.
    pub icdoff: U16,
    pub ctrattr: u8,
    pub msdbd: u8,
    pub rsvd1804: [u8; 244],
    pub psd: [u8; 1024],
    pub vs: [u8; 1024],
}

impl IdentifyController {
    pub fn zeroed() -> Self {
        Self::new_zeroed()
    }
}

/// ONCS bits.
pub mod oncs {
    pub const DSM: u16 = 1 << 2;
    pub const WRITE_ZEROES: u16 = 1 << 3;
}

/// CTRATT bits (Identify Controller: controller attributes).
pub mod ctratt {
    /// Traffic Based Keep Alive Support: the controller treats command
    /// traffic on any queue as a keep-alive, so a host with IO in flight
    /// need not send Keep Alive commands at all. A Linux host that sees
    /// this bit halves its keep-alive period and then skips the command
    /// whenever it saw a completion in the last interval
    /// (`nvme_keep_alive_work`, `NVME_CTRL_ATTR_TBKAS`) — so a target must
    /// not claim it unless every queue really does feed its keep-alive
    /// watchdog.
    pub const TBKAS: u32 = 1 << 6;
}

/// CMIC bits (Identify Controller: multi-path I/O & namespace sharing).
pub mod cmic {
    /// The NVM subsystem may contain two or more controllers. Setting it
    /// makes the host's NVMe-multipath layer build a namespace head plus a
    /// per-controller path device (`/dev/nvmeXcYnZ`) — see the host gate
    /// `nvme_mpath_alloc_disk` (`NVME_CTRL_CMIC_MULTI_CTRL`).
    pub const MULTI_CTRL: u8 = 1 << 1;
}

/// NMIC bits (Identify Namespace: multi-path I/O & namespace sharing).
pub mod nmic {
    /// The namespace may be attached to two or more controllers at once
    /// (shared namespace; `NVME_NS_NMIC_SHARED`).
    pub const SHARED: u8 = 1 << 0;
}

/// SGLS bits: byte-aligned SGL support (value 1 in bits 1:0).
pub const SGLS_BYTE_ALIGNED: u32 = 1;

/// SGLS bit 2: Keyed SGL Data Block descriptor support. NVMe/RDMA hosts treat
/// this as mandatory (the keyed SGL carries the host's addr+rkey+len).
pub const SGLS_KEYED: u32 = 1 << 2;

/// SGLS bit 20: SGL Address field may specify an offset (in-capsule data).
/// RDMA hosts (`nvme_rdma`'s `NVME_CTRL_SGLS_SAOS` check) refuse to send
/// inline write payloads unless this is set, whatever IOCCSZ says.
pub const SGLS_SAOS: u32 = 1 << 20;

/// LBA format descriptor.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub struct LbaFormat {
    /// Metadata size (0: none).
    pub ms: U16,
    /// LBA data size as a power of two (9 = 512B, 12 = 4K).
    pub lbads: u8,
    /// Relative performance.
    pub rp: u8,
}

/// Identify Namespace (CNS 0x00).
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct IdentifyNamespace {
    /// Namespace size in logical blocks.
    pub nsze: U64,
    pub ncap: U64,
    pub nuse: U64,
    pub nsfeat: u8,
    /// 0-based count of LBA formats.
    pub nlbaf: u8,
    /// Current format index (bits 3:0).
    pub flbas: u8,
    pub mc: u8,
    pub dpc: u8,
    pub dps: u8,
    pub nmic: u8,
    pub rescap: u8,
    pub fpi: u8,
    pub dlfeat: u8,
    pub nawun: U16,
    pub nawupf: U16,
    pub nacwu: U16,
    pub nabsn: U16,
    pub nabo: U16,
    pub nabspf: U16,
    pub noiob: U16,
    pub nvmcap: [u8; 16],
    pub npwg: U16,
    pub npwa: U16,
    pub npdg: U16,
    pub npda: U16,
    pub nows: U16,
    pub rsvd74: [u8; 18],
    pub anagrpid: U32,
    pub rsvd96: [u8; 3],
    pub nsattr: u8,
    pub nvmsetid: U16,
    pub endgid: U16,
    pub nguid: [u8; 16],
    pub eui64: [u8; 8],
    pub lbaf: [LbaFormat; 16],
    pub rsvd192: [u8; 192],
    pub vs: [u8; 3712],
}

impl IdentifyNamespace {
    pub fn zeroed() -> Self {
        Self::new_zeroed()
    }
}

const _: () = {
    assert!(size_of::<IdentifyController>() == 4096);
    assert!(size_of::<IdentifyNamespace>() == 4096);
    assert!(size_of::<LbaFormat>() == 4);
    // Spot-check critical offsets against the spec.
    assert!(core::mem::offset_of!(IdentifyController, cntlid) == 78);
    assert!(core::mem::offset_of!(IdentifyController, kas) == 320);
    assert!(core::mem::offset_of!(IdentifyController, sqes) == 512);
    assert!(core::mem::offset_of!(IdentifyController, sgls) == 536);
    assert!(core::mem::offset_of!(IdentifyController, subnqn) == 768);
    assert!(core::mem::offset_of!(IdentifyController, ioccsz) == 1792);
    assert!(core::mem::offset_of!(IdentifyNamespace, anagrpid) == 92);
    assert!(core::mem::offset_of!(IdentifyNamespace, lbaf) == 128);
};
