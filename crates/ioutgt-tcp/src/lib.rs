//! NVMe/TCP transport.
//!
//! Implements the connection-level recv state machine (PDU header -> data ->
//! data digest), the ordered send side (C2HData / R2T / response capsules),
//! ICReq/ICResp digest negotiation, and termination (C2HTermReq) paths,
//! mirroring `drivers/nvme/target/tcp.c`. Protocol parsing is delegated to
//! the sans-io codec in `ioutgt-nvme`; all socket IO goes through
//! `ioutgt-uring`.
