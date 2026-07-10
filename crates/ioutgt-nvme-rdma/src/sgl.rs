//! NVMe-oF SGL descriptor parsing: the in-capsule (inline) and keyed
//! (host-resident, RDMA READ/WRITE) data-block descriptor shapes carried in
//! a command SQE's `dptr`, plus the host→controller data-direction and
//! staged-length helpers derived from them. Pure decode — no verbs, no I/O.

use ioutgt_backend::AnyBackend;
use ioutgt_nvme::dispatch::Role;
use ioutgt_nvme::spec::{Sqe, io_opcode};
use zerocopy::IntoBytes;

/// SGL descriptor type byte (dptr offset 15). High nibble `0x4` =
/// `NVME_KEY_SGL_FMT_DATA_DESC` (keyed: host-resident, RDMA READ/WRITE); anything
/// else here is an in-capsule data+offset descriptor (inline).
pub(crate) const SGL_TYPE_OFFSET: usize = 24 + 15;
pub(crate) const KEYED_SGL_TYPE_HI: u8 = 0x4;
/// SGL Data Block descriptor with offset addressing (type nibble 0x0, subtype
/// 0x1): the payload is in the capsule at `addr` bytes past ICDOFF (we set
/// ICDOFF = 0). This is what nvme-rdma hosts send for writes that fit the
/// advertised in-capsule size.
pub(crate) const INLINE_SGL_TYPE: u8 = 0x01;

/// The in-capsule data descriptor: `addr` is the offset into the in-capsule
/// region, `len` the payload length (SGL Data Block: addr 8B, len 4B).
pub(crate) fn parse_inline_sgl(sqe: &Sqe) -> (u64, usize) {
    let b = sqe.as_bytes();
    let off = u64::from_le_bytes(b[24..32].try_into().expect("8 bytes"));
    let len = u32::from_le_bytes(b[32..36].try_into().expect("4 bytes"));
    (off, len as usize)
}
/// SGL descriptor sub-type (type byte low nibble) `0xf` = `NVME_SGL_FMT_INVALIDATE`:
/// the host fast-registered an MR for this transfer and wants the target to invalidate
/// its rkey remotely in the response (`nvme_rdma_map_sg_fr`). Honoring it via
/// `IBV_WR_SEND_WITH_INV` spares the host a per-IO local-invalidate WR + completion.
pub(crate) const SGL_SUBTYPE_MASK: u8 = 0x0f;
pub(crate) const SGL_FMT_INVALIDATE: u8 = 0x0f;

/// The host rkey to remotely invalidate in the response SEND, if the command's keyed
/// SGL requested it. Mirrors nvmet's `rsp->invalidate_rkey`.
pub(crate) fn invalidate_rkey_for(cmd: &Sqe) -> Option<u32> {
    let type_byte = cmd.as_bytes()[SGL_TYPE_OFFSET];
    if type_byte >> 4 == KEYED_SGL_TYPE_HI && type_byte & SGL_SUBTYPE_MASK == SGL_FMT_INVALIDATE {
        Some(parse_keyed_sgl(cmd).rkey)
    } else {
        None
    }
}

/// A host RDMA target region from a command SQE's keyed SGL data block
/// descriptor (NVMe-oF RDMA). Lives in the SQE `dptr` at offset 24:
/// `addr`(le64) `length`(24-bit le) `key`(le32 rkey) `type`.
pub(crate) struct KeyedSgl {
    pub(crate) addr: u64,
    pub(crate) len: u32,
    pub(crate) rkey: u32,
}

/// Whether `opcode` on this queue carries host→controller data the target must
/// pull (RDMA READ) into a pool lease before dispatch. Admin commands in the
/// connect/discovery path carry no host data.
pub(crate) fn host_data_in(role: &Role<AnyBackend>, opcode: u8) -> bool {
    matches!(role, Role::Io(_)) && matches!(opcode, io_opcode::WRITE | io_opcode::DSM)
}

pub(crate) fn parse_keyed_sgl(sqe: &Sqe) -> KeyedSgl {
    let b = sqe.as_bytes();
    let d = &b[24..40];
    let addr = u64::from_le_bytes(d[0..8].try_into().expect("8 bytes"));
    // length is a 24-bit little-endian field at descriptor offset 8.
    let len = u32::from(d[8]) | u32::from(d[9]) << 8 | u32::from(d[10]) << 16;
    let rkey = u32::from_le_bytes(d[11..15].try_into().expect("4 bytes"));
    KeyedSgl { addr, len, rkey }
}

/// The staged transfer length of a validated host-data-in SQE: its keyed-SGL
/// length clamped to MDTS. Recomputed when a pool-deferred command retries.
pub(crate) fn staged_len(sqe: &Sqe) -> usize {
    (parse_keyed_sgl(sqe).len as usize).min(ioutgt_nvme::MDTS_BYTES as usize)
}
