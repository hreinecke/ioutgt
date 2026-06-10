//! Sans-io NVMe protocol layer.
//!
//! NVMe spec structures (SQE/CQE/Identify/log pages/fabrics capsules) as
//! `repr(C)` zerocopy types with compile-time size assertions, NVMe/TCP PDU
//! definitions, an incremental PDU decoder/encoder that operates purely on
//! byte slices, and CRC32C digest helpers.
//!
//! This crate performs no IO and owns no sockets: the target data path, the
//! control-thread handshake, the integration-test client, and the fuzzer all
//! share this one codec.
//!
//! All wire integers are little-endian per the NVMe base specification;
//! the zerocopy types use explicit `U16`/`U32`/`U64` little-endian wrappers
//! so reinterpreting received bytes is endian-correct on any host.

pub mod digest;
pub mod fabrics;
pub mod identify;
pub mod pdu;
pub mod spec;
pub mod status;
