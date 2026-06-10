//! ioutgt — high-performance io_uring-based NVMe/TCP target.
//!
//! Binary entry point: parses CLI/JSON config, spawns the control thread,
//! the admin queue thread, and the pinned IO queue threads, and wires
//! connection handoff between them.

fn main() {
    println!("ioutgt: NVMe/TCP target (bootstrap skeleton)");
}
