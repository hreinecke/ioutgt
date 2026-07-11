//! Locality-aware even CPU grouping for IO-thread pinning.
//!
//! [`spread_cpus`] partitions all possible CPUs into `n` groups that
//! respect NUMA / cluster / SMT boundaries and spread present CPUs as
//! evenly as possible, so pinning IO-queue thread `i` into group `i`
//! places threads the way locality-aware IRQ spreading places host
//! nvme queues. The algorithm is ioutgt's own (see `spread.rs` for the
//! full contract); its exact assignments are not guaranteed to match
//! any particular kernel's managed-IRQ grouping — only the locality
//! and evenness properties are.
//!
//! Like `ioutgt-nvme`, this crate is a pure leaf: [`spread_cpus`] only
//! consumes a [`CpuTopology`] value; sysfs access is confined to
//! [`CpuTopology::from_sysfs`].

mod cpuset;
mod spread;
pub mod thread;
mod topology;

pub use cpuset::CpuSet;
pub use spread::spread_cpus;
pub use topology::CpuTopology;
