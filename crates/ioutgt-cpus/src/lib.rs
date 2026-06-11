//! Userspace port of the kernel's `group_cpus_evenly()`
//! (`lib/group_cpus.c`).
//!
//! Groups all possible CPUs evenly per NUMA / cluster / SMT locality:
//! present CPUs are spread first, then possible-but-not-present ones,
//! groups are apportioned to NUMA nodes by CPU-count ratio, kept
//! cluster-aligned when possible, and filled SMT-sibling-first. The
//! result is the same grouping the kernel computes for managed IRQ
//! spreading (and thus what `nvme-tcp` queues see), so pinning queue
//! thread `i` into group `i` aligns target threads with host queues.
//!
//! Like `ioutgt-nvme`, this crate is a pure leaf: the algorithm
//! ([`group_cpus_evenly`]) only consumes a [`CpuTopology`] value;
//! sysfs access is confined to [`CpuTopology::from_sysfs`].

mod cpuset;
mod group;
mod topology;

pub use cpuset::CpuSet;
pub use group::group_cpus_evenly;
pub use topology::CpuTopology;
