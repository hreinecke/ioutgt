//! CPU topology snapshot read from sysfs — the userspace stand-in for
//! the kernel topology masks consumed by `group_cpus_evenly()`.

use std::io;
use std::path::Path;

use crate::CpuSet;

/// CPU topology snapshot: the masks `group_cpus_evenly` consumes.
///
/// Normally built from sysfs via [`CpuTopology::from_sysfs`]; tests
/// construct synthetic instances directly.
#[derive(Clone, Debug)]
pub struct CpuTopology {
    /// `possible.last() + 1` — exclusive upper bound on CPU ids.
    pub nr_cpu_ids: usize,
    /// Possible CPUs (`cpu/possible`).
    pub possible: CpuSet,
    /// Present CPUs (`cpu/present`).
    pub present: CpuSet,
    /// Online CPUs (`cpu/online`) — not used by the grouping itself,
    /// but needed to select a pinnable CPU out of a group.
    pub online: CpuSet,
    /// CPUs of each NUMA node, indexed by node id (`node/nodeN/cpulist`).
    /// Possible CPUs not claimed by any node are folded into node 0,
    /// matching `cpu_to_node()` for never-onlined CPUs.
    pub node_to_cpus: Vec<CpuSet>,
    /// SMT siblings of each CPU, indexed by CPU id
    /// (`cpuN/topology/core_cpus_list`, kernel `topology_sibling_cpumask`).
    pub sibling: Vec<CpuSet>,
    /// Cluster mask of each CPU, indexed by CPU id
    /// (`cpuN/topology/cluster_cpus_list`, kernel `topology_cluster_cpumask`).
    pub cluster: Vec<CpuSet>,
}

fn read_cpulist(path: &Path) -> io::Result<CpuSet> {
    CpuSet::from_cpulist(&std::fs::read_to_string(path)?)
}

/// Missing files read as the empty set: not-present CPUs have no
/// `cpuN/` directory and offline CPUs may lack `topology/`, exactly
/// the cases where the kernel sees empty topology masks.
fn read_cpulist_opt(path: &Path) -> io::Result<CpuSet> {
    match std::fs::read_to_string(path) {
        Ok(s) => CpuSet::from_cpulist(&s),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(CpuSet::new()),
        Err(err) => Err(err),
    }
}

impl CpuTopology {
    /// Read the live machine topology from `/sys`.
    pub fn from_sysfs() -> io::Result<CpuTopology> {
        CpuTopology::from_sysfs_root(Path::new("/sys"))
    }

    /// Read a topology from an alternate sysfs root (fixture trees in
    /// tests).
    pub fn from_sysfs_root(root: &Path) -> io::Result<CpuTopology> {
        let cpu_dir = root.join("devices/system/cpu");
        let possible = read_cpulist(&cpu_dir.join("possible"))?;
        let present = read_cpulist(&cpu_dir.join("present"))?;
        let online = read_cpulist(&cpu_dir.join("online"))?;
        let nr_cpu_ids = possible.last().map_or(0, |c| c + 1);

        let node_dir = root.join("devices/system/node");
        let mut node_to_cpus: Vec<CpuSet> = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&node_dir) {
            for entry in entries {
                let entry = entry?;
                let name = entry.file_name();
                let Some(id) = name
                    .to_str()
                    .and_then(|n| n.strip_prefix("node"))
                    .and_then(|n| n.parse::<usize>().ok())
                else {
                    continue;
                };
                let cpus = read_cpulist_opt(&entry.path().join("cpulist"))?;
                if id >= node_to_cpus.len() {
                    node_to_cpus.resize(id + 1, CpuSet::new());
                }
                node_to_cpus[id] = cpus;
            }
        }
        if node_to_cpus.is_empty() {
            node_to_cpus.push(CpuSet::new());
        }
        // Fold unclaimed possible CPUs (e.g. never onlined, so absent
        // from every node's cpulist) into node 0, like cpu_to_node().
        let claimed = node_to_cpus.iter().fold(CpuSet::new(), |acc, n| acc.or(n));
        node_to_cpus[0] = node_to_cpus[0].or(&possible.andnot(&claimed));

        let mut sibling = vec![CpuSet::new(); nr_cpu_ids];
        let mut cluster = vec![CpuSet::new(); nr_cpu_ids];
        for cpu in possible.iter() {
            let topo_dir = cpu_dir.join(format!("cpu{cpu}/topology"));
            sibling[cpu] = read_cpulist_opt(&topo_dir.join("core_cpus_list"))?;
            cluster[cpu] = read_cpulist_opt(&topo_dir.join("cluster_cpus_list"))?;
        }

        Ok(CpuTopology {
            nr_cpu_ids,
            possible,
            present,
            online,
            node_to_cpus,
            sibling,
            cluster,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(root: &Path, rel: &str, contents: &str) {
        let path = root.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, contents).unwrap();
    }

    #[test]
    fn fixture_tree() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "devices/system/cpu/possible", "0-7\n");
        write(root, "devices/system/cpu/present", "0-5\n");
        write(root, "devices/system/cpu/online", "0-4\n");
        write(root, "devices/system/node/node0/cpulist", "0-2\n");
        write(root, "devices/system/node/node2/cpulist", "3-5\n");
        for cpu in 0..6 {
            let sib = if cpu % 2 == 0 { cpu } else { cpu - 1 };
            write(
                root,
                &format!("devices/system/cpu/cpu{cpu}/topology/core_cpus_list"),
                &format!("{}-{}\n", sib, sib + 1),
            );
            // no cluster_cpus_list for cpu5: must read as empty
            if cpu != 5 {
                write(
                    root,
                    &format!("devices/system/cpu/cpu{cpu}/topology/cluster_cpus_list"),
                    "0-5\n",
                );
            }
        }

        let topo = CpuTopology::from_sysfs_root(root).unwrap();
        assert_eq!(topo.nr_cpu_ids, 8);
        assert_eq!(topo.possible.to_string(), "0-7");
        assert_eq!(topo.present.to_string(), "0-5");
        assert_eq!(topo.online.to_string(), "0-4");
        // sparse node ids: node1 exists but is empty
        assert_eq!(topo.node_to_cpus.len(), 3);
        assert!(topo.node_to_cpus[1].is_empty());
        assert_eq!(topo.node_to_cpus[2].to_string(), "3-5");
        // CPUs 6-7 are possible but in no node: folded into node 0
        assert_eq!(topo.node_to_cpus[0].to_string(), "0-2,6-7");
        assert_eq!(topo.sibling[4].to_string(), "4-5");
        assert!(topo.cluster[5].is_empty());
        assert!(topo.sibling[6].is_empty()); // no cpu6 directory
    }

    #[test]
    fn live_sysfs_smoke() {
        let topo = CpuTopology::from_sysfs().unwrap();
        assert!(!topo.possible.is_empty());
        assert!(topo.nr_cpu_ids > 0);
        // every present CPU must be claimed by some node
        let claimed = topo
            .node_to_cpus
            .iter()
            .fold(CpuSet::new(), |acc, n| acc.or(n));
        assert!(topo.present.andnot(&claimed).is_empty());
    }
}
