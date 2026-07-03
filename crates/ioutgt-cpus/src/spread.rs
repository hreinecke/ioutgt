//! NUMA/cluster/SMT-aware even partition of CPUs into pinning groups.
//!
//! Written from ioutgt's own requirements as an independent algorithm —
//! not a port of kernel code, and the exact group assignments are not
//! guaranteed to match the kernel's managed-IRQ spread; only the
//! locality and evenness properties below are.
//!
//! Contract of [`spread_cpus`] (`n` = requested group count):
//! - exactly `n` groups come back, pairwise disjoint, and their union
//!   is every possible CPU (trailing groups may be empty when `n`
//!   exceeds the CPU count);
//! - each group stays inside one NUMA node whenever `n >=` the number
//!   of non-empty nodes, and every such node gets at least one group;
//!   with fewer groups than nodes, groups are unions of whole nodes;
//! - present CPUs are spread as evenly as the node quotas allow, so
//!   never-onlined possible CPUs do not skew the pinnable load;
//! - cluster and SMT siblings land in the same group whenever the
//!   group sizes allow.
//!
//! Shape of the algorithm: group *seats* are apportioned to nodes by
//! largest remainder on present-CPU weight, with a floor of one seat
//! per non-empty node; each node is then packed sequentially in
//! cluster-major, SMT-atom order — present CPUs first against
//! per-group present quotas, then the remaining possible CPUs, each
//! preferring a group that already holds one of its SMT siblings.
//! With fewer groups than nodes, whole nodes are instead placed
//! heaviest-first into the currently lightest group.

use crate::{CpuSet, CpuTopology};

/// Partition all possible CPUs of `topo` into `ngroups` groups, evenly
/// per NUMA / cluster / SMT locality (see the module docs for the full
/// contract). Always returns exactly `ngroups` sets; trailing sets may
/// be empty when there are more groups than CPUs.
pub fn spread_cpus(ngroups: usize, topo: &CpuTopology) -> Vec<CpuSet> {
    if ngroups == 0 {
        return Vec::new();
    }
    // Non-empty nodes in node-id order, clipped to the possible mask.
    let nodes: Vec<CpuSet> = topo
        .node_to_cpus
        .iter()
        .map(|n| n.and(&topo.possible))
        .filter(|n| !n.is_empty())
        .collect();
    if nodes.is_empty() {
        return vec![CpuSet::new(); ngroups];
    }
    // Seat weight = present CPUs per node; a machine with no present
    // CPUs at all (synthetic topologies) falls back to possible CPUs.
    let mut weights: Vec<usize> = nodes
        .iter()
        .map(|n| n.and(&topo.present).weight())
        .collect();
    if weights.iter().all(|&w| w == 0) {
        weights = nodes.iter().map(CpuSet::weight).collect();
    }
    if ngroups < nodes.len() {
        return pack_whole_nodes(ngroups, &nodes, &weights);
    }
    let seats = apportion(ngroups, &weights);
    nodes
        .iter()
        .zip(&seats)
        .flat_map(|(node, &n)| pack_node(node, n, topo))
        .collect()
}

/// Largest-remainder apportionment of `seats` to `weights.len()` nodes
/// with a floor of one seat each. Requires `seats >= weights.len()`
/// and a positive total weight. Ties go to the lower node index, so
/// the result is deterministic.
fn apportion(seats: usize, weights: &[usize]) -> Vec<usize> {
    let spare = seats - weights.len();
    let total: usize = weights.iter().sum();
    let mut out: Vec<usize> = weights.iter().map(|w| 1 + spare * w / total).collect();
    let assigned: usize = out.iter().sum();
    let mut order: Vec<usize> = (0..weights.len()).collect();
    order.sort_by_key(|&i| (std::cmp::Reverse(spare * weights[i] % total), i));
    for &i in order.iter().take(seats - assigned) {
        out[i] += 1;
    }
    out
}

/// Fewer groups than nodes: keep nodes whole and pack them
/// heaviest-first into the currently lightest group (LPT greedy), so
/// every group is a union of complete nodes with near-even load.
fn pack_whole_nodes(ngroups: usize, nodes: &[CpuSet], weights: &[usize]) -> Vec<CpuSet> {
    let mut order: Vec<usize> = (0..nodes.len()).collect();
    order.sort_by(|&a, &b| weights[b].cmp(&weights[a]).then(a.cmp(&b)));
    let mut groups = vec![CpuSet::new(); ngroups];
    let mut loads = vec![0usize; ngroups];
    for i in order {
        let g = (0..ngroups).min_by_key(|&g| (loads[g], g)).unwrap();
        groups[g] = groups[g].or(&nodes[i]);
        loads[g] += weights[i];
    }
    groups
}

/// Split one node's CPUs into `seats` groups: walk the node in
/// locality order (see [`fill_order`]), placing present CPUs first
/// against per-group present quotas, then topping groups up to their
/// total size with the not-present possible CPUs — each of those
/// preferring a group that already holds one of its SMT siblings.
fn pack_node(node: &CpuSet, seats: usize, topo: &CpuTopology) -> Vec<CpuSet> {
    if seats == 0 {
        return Vec::new();
    }
    let order = fill_order(node, topo);
    let (present, absent): (Vec<usize>, Vec<usize>) =
        order.iter().copied().partition(|&c| topo.present.test(c));
    // Both quota vectors give the `+1` remainders to the leading
    // groups, which keeps `quota_present <= quota_total` elementwise —
    // the present pass can never eat a later CPU's total capacity.
    let quota_present = split_even(present.len(), seats);
    let quota_total = split_even(order.len(), seats);
    let mut groups = vec![CpuSet::new(); seats];
    let mut fill = vec![0usize; seats];

    let mut g = 0;
    for cpu in present {
        while fill[g] == quota_present[g] {
            g += 1;
        }
        groups[g].set(cpu);
        fill[g] += 1;
    }
    let empty = CpuSet::new();
    for cpu in absent {
        let sib = topo.sibling.get(cpu).unwrap_or(&empty);
        let j = (0..seats)
            .filter(|&j| fill[j] < quota_total[j])
            .find(|&j| groups[j].intersects(sib))
            .or_else(|| (0..seats).find(|&j| fill[j] < quota_total[j]))
            .expect("total quotas cover every CPU of the node");
        groups[j].set(cpu);
        fill[j] += 1;
    }
    groups
}

/// `n` split into `k` near-even parts, the `n % k` remainder going to
/// the leading parts.
fn split_even(n: usize, k: usize) -> Vec<usize> {
    (0..k).map(|i| n / k + usize::from(i < n % k)).collect()
}

/// Every CPU of `node` exactly once, in locality order: clusters by
/// their lowest CPU id, whole SMT sibling sets contiguous inside their
/// cluster, ascending ids inside a sibling set. CPUs whose cluster
/// mask is empty fall back to their sibling set, then to themselves,
/// so sparse topology data degrades to plain id order.
fn fill_order(node: &CpuSet, topo: &CpuTopology) -> Vec<usize> {
    let mut order = Vec::with_capacity(node.weight());
    let mut visited = CpuSet::new();
    for cpu in node.iter() {
        if visited.test(cpu) {
            continue;
        }
        let mask = |v: &[CpuSet]| v.get(cpu).cloned().unwrap_or_default();
        let mut cluster = mask(&topo.cluster);
        if cluster.is_empty() {
            cluster = mask(&topo.sibling);
        }
        let mut cluster = cluster.and(node).andnot(&visited);
        if cluster.is_empty() {
            cluster.set(cpu);
        }
        for c in cluster.iter() {
            if visited.test(c) {
                continue;
            }
            let mut atom = topo
                .sibling
                .get(c)
                .cloned()
                .unwrap_or_default()
                .and(&cluster)
                .andnot(&visited);
            if atom.is_empty() {
                atom.set(c);
            }
            for member in atom.iter() {
                order.push(member);
                visited.set(member);
            }
        }
    }
    order
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Synthetic topology: all-present == online, no SMT/cluster data.
    fn mk(possible: &str, present: &str, nodes: &[&str]) -> CpuTopology {
        let possible = CpuSet::from_cpulist(possible).unwrap();
        let present = CpuSet::from_cpulist(present).unwrap();
        let nr_cpu_ids = possible.last().map_or(0, |c| c + 1);
        CpuTopology {
            nr_cpu_ids,
            online: present.clone(),
            present,
            node_to_cpus: nodes
                .iter()
                .map(|s| CpuSet::from_cpulist(s).unwrap())
                .collect(),
            sibling: vec![CpuSet::new(); nr_cpu_ids],
            cluster: vec![CpuSet::new(); nr_cpu_ids],
            possible,
        }
    }

    /// Install `lists` as the per-CPU masks of every listed member.
    fn set_masks(masks: &mut [CpuSet], lists: &[&str]) {
        for list in lists {
            let set = CpuSet::from_cpulist(list).unwrap();
            for cpu in set.iter() {
                masks[cpu] = set.clone();
            }
        }
    }

    fn same(a: &CpuSet, b: &CpuSet) -> bool {
        a.andnot(b).is_empty() && b.andnot(a).is_empty()
    }

    /// The unconditional contract: exact length, disjoint, covering.
    fn check(groups: &[CpuSet], topo: &CpuTopology, ngroups: usize) {
        assert_eq!(groups.len(), ngroups);
        let mut seen = CpuSet::new();
        for g in groups {
            assert!(!seen.intersects(g), "groups overlap: {groups:?}");
            seen = seen.or(g);
        }
        assert!(
            same(&seen, &topo.possible),
            "groups {groups:?} do not cover possible {}",
            topo.possible
        );
    }

    #[test]
    fn single_node_even() {
        let t = mk("0-7", "0-7", &["0-7"]);
        let groups = spread_cpus(4, &t);
        check(&groups, &t, 4);
        assert!(groups.iter().all(|g| g.weight() == 2), "{groups:?}");
    }

    #[test]
    fn uneven_remainder() {
        let t = mk("0-9", "0-9", &["0-9"]);
        let groups = spread_cpus(4, &t);
        check(&groups, &t, 4);
        let mut weights: Vec<usize> = groups.iter().map(CpuSet::weight).collect();
        weights.sort_unstable();
        assert_eq!(weights, vec![2, 2, 3, 3]);
    }

    #[test]
    fn two_nodes_node_pure() {
        let t = mk("0-15", "0-15", &["0-7", "8-15"]);
        let groups = spread_cpus(4, &t);
        check(&groups, &t, 4);
        let mut per_node = [0usize; 2];
        for g in &groups {
            assert_eq!(g.weight(), 4);
            let homes: Vec<usize> = (0..2)
                .filter(|&n| g.intersects(&t.node_to_cpus[n]))
                .collect();
            assert_eq!(homes.len(), 1, "group {g} spans nodes");
            per_node[homes[0]] += 1;
        }
        assert_eq!(per_node, [2, 2]);
    }

    #[test]
    fn groups_match_nodes_exactly() {
        let nodes = ["0-3", "4-7", "8-11", "12-15"];
        let t = mk("0-15", "0-15", &nodes);
        let groups = spread_cpus(4, &t);
        check(&groups, &t, 4);
        for node in &t.node_to_cpus {
            assert!(
                groups.iter().any(|g| same(g, node)),
                "no group equals node {node}"
            );
        }
    }

    #[test]
    fn fewer_groups_keep_nodes_whole() {
        let t = mk("0-11", "0-11", &["0-3", "4-7", "8-11"]);
        let groups = spread_cpus(2, &t);
        check(&groups, &t, 2);
        // Every node lands whole inside exactly one group.
        for node in &t.node_to_cpus {
            let holders: Vec<&CpuSet> = groups.iter().filter(|g| g.intersects(node)).collect();
            assert_eq!(holders.len(), 1, "node {node} split across groups");
            assert!(node.andnot(holders[0]).is_empty());
        }
    }

    #[test]
    fn more_groups_than_cpus() {
        let t = mk("0-2", "0-2", &["0-2"]);
        let groups = spread_cpus(5, &t);
        check(&groups, &t, 5);
        let mut weights: Vec<usize> = groups.iter().map(CpuSet::weight).collect();
        weights.sort_unstable();
        assert_eq!(weights, vec![0, 0, 1, 1, 1]);
    }

    #[test]
    fn smt_siblings_stay_together() {
        let mut t = mk("0-7", "0-7", &["0-7"]);
        set_masks(&mut t.sibling, &["0,4", "1,5", "2,6", "3,7"]);
        let groups = spread_cpus(4, &t);
        check(&groups, &t, 4);
        for g in &groups {
            let first = g.first().unwrap();
            assert!(same(g, &t.sibling[first]), "group {g} splits an SMT pair");
        }
    }

    #[test]
    fn clusters_stay_together() {
        let mut t = mk("0-15", "0-15", &["0-15"]);
        set_masks(&mut t.cluster, &["0-3", "4-7", "8-11", "12-15"]);
        let groups = spread_cpus(4, &t);
        check(&groups, &t, 4);
        for g in &groups {
            let first = g.first().unwrap();
            assert!(same(g, &t.cluster[first]), "group {g} splits a cluster");
        }
    }

    #[test]
    fn present_spread_evenly() {
        // Half the possible CPUs are present: each group must get an
        // equal share of the present ones, not just of the total.
        let t = mk("0-7", "0-3", &["0-7"]);
        let groups = spread_cpus(2, &t);
        check(&groups, &t, 2);
        for g in &groups {
            assert_eq!(g.weight(), 4);
            assert_eq!(g.and(&t.present).weight(), 2, "present skewed: {g}");
        }
    }

    #[test]
    fn seats_follow_present_weight() {
        // node0: 4 present of 4; node1: 2 present of 8. Present
        // weighting gives the third group to node0; possible weighting
        // would hand it to node1.
        let t = mk("0-11", "0-5", &["0-3", "4-11"]);
        let groups = spread_cpus(3, &t);
        check(&groups, &t, 3);
        let in_node0 = groups
            .iter()
            .filter(|g| !g.is_empty() && g.andnot(&t.node_to_cpus[0]).is_empty())
            .count();
        let in_node1 = groups
            .iter()
            .filter(|g| !g.is_empty() && g.andnot(&t.node_to_cpus[1]).is_empty())
            .count();
        assert_eq!((in_node0, in_node1), (2, 1), "{groups:?}");
    }

    #[test]
    fn zero_groups() {
        let t = mk("0-3", "0-3", &["0-3"]);
        assert!(spread_cpus(0, &t).is_empty());
    }

    #[test]
    fn empty_topology() {
        let t = CpuTopology {
            nr_cpu_ids: 0,
            possible: CpuSet::new(),
            present: CpuSet::new(),
            online: CpuSet::new(),
            node_to_cpus: vec![CpuSet::new()],
            sibling: Vec::new(),
            cluster: Vec::new(),
        };
        let groups = spread_cpus(3, &t);
        assert_eq!(groups.len(), 3);
        assert!(groups.iter().all(CpuSet::is_empty));
    }

    #[test]
    fn invariants_hold_across_group_counts() {
        let mut t = mk("0-15", "0-15", &["0-7", "8-15"]);
        set_masks(
            &mut t.sibling,
            &["0-1", "2-3", "4-5", "6-7", "8-9", "10-11", "12-13", "14-15"],
        );
        set_masks(&mut t.cluster, &["0-3", "4-7", "8-11", "12-15"]);
        for n in 1..=10 {
            let groups = spread_cpus(n, &t);
            check(&groups, &t, n);
            if n >= 2 {
                // At least as many groups as nodes: node purity.
                for g in groups.iter().filter(|g| !g.is_empty()) {
                    assert!(
                        t.node_to_cpus.iter().any(|nd| g.andnot(nd).is_empty()),
                        "group {g} spans nodes at n={n}"
                    );
                }
            }
        }
    }
}
