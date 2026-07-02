//! NVMe/RDMA connection-management private data (`linux/nvme-rdma.h`).
//!
//! The connecting host's RDMA-CM `CONNECT_REQUEST` carries a `nvme_rdma_cm_req`
//! in its private data; the target replies on `rdma_accept` with a
//! `nvme_rdma_cm_rep`, or on `rdma_reject` with a `nvme_rdma_cm_rej`. All
//! multi-byte fields are little-endian on the wire. The target reads `qid` to
//! route the connection to a queue thread and `hsqsize` to size the queue.

use std::io;

/// CM private-data record format — only 1.0 (`0`) is defined.
pub const CM_FMT_1_0: u16 = 0;

/// CM reject status codes (`enum nvme_rdma_cm_status`), sent in `cm_rej.sts`.
pub mod reject_status {
    /// Invalid private-data length.
    pub const INVALID_LEN: u16 = 0x01;
    /// Invalid record format.
    pub const INVALID_RECFMT: u16 = 0x02;
    /// Invalid queue ID.
    pub const INVALID_QID: u16 = 0x03;
    /// Invalid host SQ size.
    pub const INVALID_HSQSIZE: u16 = 0x04;
    /// Invalid host RQ size.
    pub const INVALID_HRQSIZE: u16 = 0x05;
    /// Resource not found.
    pub const NO_RSC: u16 = 0x06;
    /// Invalid IRD (initiator read depth).
    pub const INVALID_IRD: u16 = 0x07;
    /// Invalid ORD (outbound read depth).
    pub const INVALID_ORD: u16 = 0x08;
    /// Invalid controller ID.
    pub const INVALID_CNTLID: u16 = 0x09;
}

#[inline]
fn le16(data: &[u8], off: usize) -> u16 {
    u16::from_le_bytes([data[off], data[off + 1]])
}

/// Parsed `nvme_rdma_cm_req` — the host's connect request (32 bytes on the wire:
/// `recfmt|qid|hrqsize|hsqsize|cntlid|rsvd[22]`).
#[derive(Debug, Clone, Copy)]
pub struct CmReq {
    /// Private-data record format (expected [`CM_FMT_1_0`]).
    pub recfmt: u16,
    /// Queue identifier (0 = admin) — routes the connection to a queue thread.
    pub qid: u16,
    /// Host receive queue size to create.
    pub hrqsize: u16,
    /// Host send queue size to create (queue depth − 1).
    pub hsqsize: u16,
    /// Controller id (`0xffff` on the admin-queue connect).
    pub cntlid: u16,
}

impl CmReq {
    /// Wire length of the private data.
    pub const WIRE_LEN: usize = 32;

    /// Parse the host's CONNECT_REQUEST private data. Errors only on a short
    /// buffer; field validation (recfmt/qid/sizes) is the handshake's job, so it
    /// can answer with the right [`reject_status`] code.
    pub fn parse(data: &[u8]) -> io::Result<CmReq> {
        if data.len() < Self::WIRE_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "nvme_rdma_cm_req: {} bytes, need {}",
                    data.len(),
                    Self::WIRE_LEN
                ),
            ));
        }
        Ok(CmReq {
            recfmt: le16(data, 0),
            qid: le16(data, 2),
            hrqsize: le16(data, 4),
            hsqsize: le16(data, 6),
            cntlid: le16(data, 8),
        })
    }

    /// Encode for the host (client) side's `rdma_connect` private data.
    pub fn to_bytes(self) -> [u8; Self::WIRE_LEN] {
        let mut b = [0u8; Self::WIRE_LEN];
        b[0..2].copy_from_slice(&self.recfmt.to_le_bytes());
        b[2..4].copy_from_slice(&self.qid.to_le_bytes());
        b[4..6].copy_from_slice(&self.hrqsize.to_le_bytes());
        b[6..8].copy_from_slice(&self.hsqsize.to_le_bytes());
        b[8..10].copy_from_slice(&self.cntlid.to_le_bytes());
        b
    }
}

/// `nvme_rdma_cm_rep` — the target's accept reply (32 bytes:
/// `recfmt|crqsize|rsvd[28]`).
#[derive(Debug, Clone, Copy)]
pub struct CmRep {
    /// Record format (echo [`CM_FMT_1_0`]).
    pub recfmt: u16,
    /// Controller receive queue size.
    pub crqsize: u16,
}

impl CmRep {
    /// Wire length of the private data.
    pub const WIRE_LEN: usize = 32;

    /// Parse the target's accept-reply private data (from a ConnectResponse).
    pub fn parse(data: &[u8]) -> io::Result<CmRep> {
        if data.len() < Self::WIRE_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "nvme_rdma_cm_rep: {} bytes, need {}",
                    data.len(),
                    Self::WIRE_LEN
                ),
            ));
        }
        Ok(CmRep {
            recfmt: le16(data, 0),
            crqsize: le16(data, 2),
        })
    }

    /// Encode for the accept reply private data.
    pub fn to_bytes(self) -> [u8; Self::WIRE_LEN] {
        let mut b = [0u8; Self::WIRE_LEN];
        b[0..2].copy_from_slice(&self.recfmt.to_le_bytes());
        b[2..4].copy_from_slice(&self.crqsize.to_le_bytes());
        b
    }
}

/// `nvme_rdma_cm_rej` — the target's reject reply (4 bytes: `recfmt|sts`).
#[derive(Debug, Clone, Copy)]
pub struct CmRej {
    /// Record format (echo [`CM_FMT_1_0`]).
    pub recfmt: u16,
    /// Reject reason — a [`reject_status`] code.
    pub sts: u16,
}

impl CmRej {
    /// Wire length of the private data.
    pub const WIRE_LEN: usize = 4;

    /// Build a reject with [`CM_FMT_1_0`] and the given status.
    pub fn new(sts: u16) -> CmRej {
        CmRej {
            recfmt: CM_FMT_1_0,
            sts,
        }
    }

    /// Encode for `Identifier::reject`.
    pub fn to_bytes(self) -> [u8; Self::WIRE_LEN] {
        let mut b = [0u8; Self::WIRE_LEN];
        b[0..2].copy_from_slice(&self.recfmt.to_le_bytes());
        b[2..4].copy_from_slice(&self.sts.to_le_bytes());
        b
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cm_req_parses_little_endian_fields_at_the_right_offsets() {
        let mut data = [0u8; 32];
        data[0..2].copy_from_slice(&CM_FMT_1_0.to_le_bytes()); // recfmt
        data[2..4].copy_from_slice(&3u16.to_le_bytes()); // qid
        data[4..6].copy_from_slice(&128u16.to_le_bytes()); // hrqsize
        data[6..8].copy_from_slice(&127u16.to_le_bytes()); // hsqsize
        data[8..10].copy_from_slice(&0xffffu16.to_le_bytes()); // cntlid
        let req = CmReq::parse(&data).unwrap();
        assert_eq!(req.recfmt, CM_FMT_1_0);
        assert_eq!(req.qid, 3);
        assert_eq!(req.hrqsize, 128);
        assert_eq!(req.hsqsize, 127);
        assert_eq!(req.cntlid, 0xffff);
    }

    #[test]
    fn cm_req_rejects_short_private_data() {
        assert!(CmReq::parse(&[0u8; 16]).is_err());
        assert!(CmReq::parse(&[]).is_err());
    }

    #[test]
    fn cm_req_ignores_trailing_private_data() {
        // A longer buffer (extra vendor bytes) still parses the leading record.
        let mut data = vec![0u8; 64];
        data[2..4].copy_from_slice(&5u16.to_le_bytes());
        assert_eq!(CmReq::parse(&data).unwrap().qid, 5);
    }

    #[test]
    fn cm_rep_encodes_recfmt_and_crqsize() {
        let bytes = CmRep {
            recfmt: CM_FMT_1_0,
            crqsize: 127,
        }
        .to_bytes();
        assert_eq!(bytes.len(), 32);
        assert_eq!(le16(&bytes, 0), CM_FMT_1_0);
        assert_eq!(le16(&bytes, 2), 127);
        assert!(bytes[4..].iter().all(|&b| b == 0));
    }

    #[test]
    fn cm_req_round_trips() {
        let req = CmReq {
            recfmt: CM_FMT_1_0,
            qid: 9,
            hrqsize: 128,
            hsqsize: 127,
            cntlid: 0xabcd,
        };
        let back = CmReq::parse(&req.to_bytes()).unwrap();
        assert_eq!(
            (
                back.recfmt,
                back.qid,
                back.hrqsize,
                back.hsqsize,
                back.cntlid
            ),
            (req.recfmt, req.qid, req.hrqsize, req.hsqsize, req.cntlid)
        );
    }

    #[test]
    fn cm_rep_round_trips() {
        let rep = CmRep {
            recfmt: CM_FMT_1_0,
            crqsize: 127,
        };
        let back = CmRep::parse(&rep.to_bytes()).unwrap();
        assert_eq!((back.recfmt, back.crqsize), (rep.recfmt, rep.crqsize));
        assert!(CmRep::parse(&[0u8; 8]).is_err());
    }

    #[test]
    fn cm_rej_encodes_status() {
        let bytes = CmRej::new(reject_status::INVALID_QID).to_bytes();
        assert_eq!(le16(&bytes, 0), CM_FMT_1_0);
        assert_eq!(le16(&bytes, 2), reject_status::INVALID_QID);
    }
}
