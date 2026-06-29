//! Thin wrappers over the libibverbs primitives the target needs, built on the
//! safe `sideway` bindings. This is where the RDMA resource model (device,
//! PD, MR, CQ + completion channel, QP) is assembled for the transport.

use sideway::ibverbs::device::{DeviceInfo, DeviceList};

/// The RDMA devices the host exposes, by name. Empty when no provider is
/// present — no HCA and no soft-RoCE (`rxe`) configured — which is the expected
/// state on a plain dev box, so callers must treat an empty list as "RDMA
/// unavailable", not an error.
pub fn rdma_devices() -> Vec<String> {
    match DeviceList::new() {
        Ok(list) => list.iter().map(|dev| dev.name()).collect(),
        Err(_) => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enumerate_devices_links_and_runs() {
        // Exercises the sideway -> system libibverbs path end to end. The count
        // is environment dependent (0 on a box without an HCA or a configured
        // rxe device), so we assert only that the call links, runs, and frees
        // cleanly.
        let devices = rdma_devices();
        println!("rdma devices: {devices:?}");
    }
}
