// SPDX-License-Identifier: GPL-2.0-only
//
// Derived from the Linux kernel's lib/group_cpus.c:
// Copyright (C) 2016 Thomas Gleixner.
// Copyright (C) 2016-2017 Christoph Hellwig.
// Rust port Copyright (C) 2026 Ming Lei.

//! Userspace port of the kernel's `group_cpus_evenly()`
//! (`lib/group_cpus.c`): group CPUs evenly per NUMA / cluster / SMT
//! locality.

use crate::{CpuSet, CpuTopology};

/// Per-node (or per-cluster) group allocation record — kernel
/// `struct node_groups`, with the `ngroups`/`ncpus` union split into
/// two fields.
struct NodeGroups {
    id: usize,
    ncpus: usize,
    ngroups: usize,
}

/// Kernel `grp_spread_init_one()`: move `cpus_per_grp` CPUs from
/// `nmsk` into `grpmsk`, draining each chosen CPU's SMT siblings
/// first so hyperthreads land in the same group.
fn grp_spread_init_one(
    grpmsk: &mut CpuSet,
    nmsk: &mut CpuSet,
    mut cpus_per_grp: usize,
    sibling: &[CpuSet],
) {
    while cpus_per_grp > 0 {
        let Some(cpu) = nmsk.first() else { return };
        nmsk.clear(cpu);
        grpmsk.set(cpu);
        cpus_per_grp -= 1;

        /* If the cpu has siblings, use them first */
        let Some(siblmsk) = sibling.get(cpu) else {
            continue;
        };
        for sibl in siblmsk.iter() {
            if cpus_per_grp == 0 {
                break;
            }
            if nmsk.test(sibl) {
                nmsk.clear(sibl);
                grpmsk.set(sibl);
                cpus_per_grp -= 1;
            }
        }
    }
}

/// Kernel `alloc_groups_to_nodes()`: apportion `numgrps` groups over
/// the entries by CPU-count ratio, smallest node first, so every
/// entry gets `1 <= ngroups <= ncpus` (see the proof in the kernel
/// source).
fn alloc_groups_to_nodes(mut numgrps: usize, mut remaining_ncpus: usize, nodes: &mut [NodeGroups]) {
    nodes.sort_by_key(|n| n.ncpus);
    for node in nodes {
        let ngroups = (numgrps * node.ncpus / remaining_ncpus).max(1);
        debug_assert!(ngroups <= node.ncpus);
        node.ngroups = ngroups;
        remaining_ncpus -= node.ncpus;
        numgrps -= ngroups;
    }
}

/// Kernel `assign_cpus_to_groups()`: spread `ngroups` groups over the
/// CPUs in `nmsk`, advancing (and wrapping) the shared group cursor.
fn assign_cpus_to_groups(
    ncpus: usize,
    nmsk: &mut CpuSet,
    ngroups: usize,
    sibling: &[CpuSet],
    masks: &mut [CpuSet],
    curgrp: &mut usize,
) {
    let last_grp = masks.len();
    /* Account for rounding errors */
    let mut extra_grps = ncpus - ngroups * (ncpus / ngroups);

    for _ in 0..ngroups {
        let mut cpus_per_grp = ncpus / ngroups;
        if extra_grps > 0 {
            cpus_per_grp += 1;
            extra_grps -= 1;
        }
        // wrapping has to be considered given startgrp may start anywhere
        if *curgrp >= last_grp {
            *curgrp = 0;
        }
        grp_spread_init_one(&mut masks[*curgrp], nmsk, cpus_per_grp, sibling);
        *curgrp += 1;
    }
}

/// Kernel `__try_group_cluster_cpus()` (+ `alloc_cluster_groups()`):
/// group the node's CPUs cluster-aligned when each group can stay
/// within one cluster. Returns false when the node has no cluster
/// info or clusters outnumber the node's groups.
fn try_group_cluster_cpus(
    topo: &CpuTopology,
    ncpus: usize,
    ngroups: usize,
    node_cpumask: &CpuSet,
    masks: &mut [CpuSet],
    curgrp: &mut usize,
) -> bool {
    /* Probe how many clusters in this node. */
    let mut msk = node_cpumask.clone();
    let mut clusters: Vec<CpuSet> = Vec::new();
    while let Some(cpu) = msk.first() {
        let Some(cluster_mask) = topo.cluster.get(cpu).filter(|m| !m.is_empty()) else {
            return false;
        };
        /* Clean out CPUs on the same cluster. */
        msk = msk.andnot(cluster_mask);
        clusters.push(cluster_mask.clone());
    }

    /* If ngroups < nclusters, cross cluster is inevitable, skip. */
    if clusters.is_empty() || clusters.len() > ngroups {
        return false;
    }

    let mut cluster_groups: Vec<NodeGroups> = clusters
        .iter()
        .enumerate()
        .map(|(id, cluster)| NodeGroups {
            id,
            ncpus: cluster.and(node_cpumask).weight(),
            ngroups: 0,
        })
        .collect();

    alloc_groups_to_nodes(ngroups, ncpus, &mut cluster_groups);

    for nv in &cluster_groups {
        /* Get the cpus on this cluster. */
        let mut nmsk = node_cpumask.and(&clusters[nv.id]);
        let nc = nmsk.weight();
        if nc == 0 {
            continue;
        }
        assign_cpus_to_groups(nc, &mut nmsk, nv.ngroups, &topo.sibling, masks, curgrp);
    }
    true
}

/// Kernel `__group_cpus_evenly()`: one spread stage over `cpu_mask`.
/// Returns the number of groups allocated by this stage.
fn group_evenly_stage(
    topo: &CpuTopology,
    startgrp: usize,
    cpu_mask: &CpuSet,
    masks: &mut [CpuSet],
) -> usize {
    let numgrps = masks.len();
    let mut curgrp = startgrp;

    if cpu_mask.is_empty() {
        return 0;
    }

    // Nodes intersecting cpu_mask, in ascending node-id order.
    let active_nodes: Vec<usize> = (0..topo.node_to_cpus.len())
        .filter(|&n| cpu_mask.intersects(&topo.node_to_cpus[n]))
        .collect();

    /*
     * If the number of nodes in the mask is greater than or equal the
     * number of groups we just spread the groups across the nodes.
     */
    if numgrps <= active_nodes.len() {
        for &n in &active_nodes {
            let nmsk = cpu_mask.and(&topo.node_to_cpus[n]);
            masks[curgrp] = masks[curgrp].or(&nmsk);
            curgrp += 1;
            if curgrp == numgrps {
                curgrp = 0;
            }
        }
        return numgrps;
    }

    /* allocate group number for each node */
    let mut numcpus = 0;
    let mut node_groups: Vec<NodeGroups> = Vec::new();
    for &n in &active_nodes {
        let ncpus = cpu_mask.and(&topo.node_to_cpus[n]).weight();
        numcpus += ncpus;
        node_groups.push(NodeGroups {
            id: n,
            ncpus,
            ngroups: 0,
        });
    }
    alloc_groups_to_nodes(numcpus.min(numgrps), numcpus, &mut node_groups);

    let mut done = 0;
    for nv in &node_groups {
        /* Get the cpus on this node which are in the mask */
        let mut nmsk = cpu_mask.and(&topo.node_to_cpus[nv.id]);
        let ncpus = nmsk.weight();
        debug_assert!(nv.ngroups <= ncpus);

        if try_group_cluster_cpus(topo, ncpus, nv.ngroups, &nmsk, masks, &mut curgrp) {
            done += nv.ngroups;
            continue;
        }

        assign_cpus_to_groups(
            ncpus,
            &mut nmsk,
            nv.ngroups,
            &topo.sibling,
            masks,
            &mut curgrp,
        );
        done += nv.ngroups;
    }
    done
}

/// Group all CPUs evenly per NUMA/CPU locality — userspace port of
/// the kernel's `group_cpus_evenly()`.
///
/// Returns the initialized group masks; the result length can be less
/// than `numgrps` when there are fewer possible CPUs than groups.
/// Two-stage spread: present CPUs are distributed first, then
/// possible-but-not-present CPUs, so the grouping covers every
/// possible CPU exactly once.
pub fn group_cpus_evenly(numgrps: usize, topo: &CpuTopology) -> Vec<CpuSet> {
    if numgrps == 0 {
        return Vec::new();
    }
    let mut masks = vec![CpuSet::new(); numgrps];

    /* grouping present CPUs first */
    let present = topo.present.and(&topo.possible);
    let nr_present = group_evenly_stage(topo, 0, &present, &mut masks);

    /*
     * Allocate non present CPUs starting from the next group to be
     * handled. If the grouping of present CPUs already exhausted the
     * group space, assign the non present CPUs to the already
     * allocated out groups.
     */
    let curgrp = if nr_present >= numgrps { 0 } else { nr_present };
    let npresmsk = topo.possible.andnot(&present);
    let nr_others = group_evenly_stage(topo, curgrp, &npresmsk, &mut masks);

    masks.truncate((nr_present + nr_others).min(numgrps));
    masks
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Synthetic topology: `nodes` is the per-node cpulist; present /
    /// online default to all possible; sibling defaults to self-only;
    /// no clusters.
    fn topo(nodes: &[&str]) -> CpuTopology {
        let node_to_cpus: Vec<CpuSet> = nodes
            .iter()
            .map(|s| CpuSet::from_cpulist(s).unwrap())
            .collect();
        let possible = node_to_cpus.iter().fold(CpuSet::new(), |acc, n| acc.or(n));
        let nr_cpu_ids = possible.last().map_or(0, |c| c + 1);
        CpuTopology {
            nr_cpu_ids,
            present: possible.clone(),
            online: possible.clone(),
            possible,
            node_to_cpus,
            sibling: (0..nr_cpu_ids).map(|c| [c].into_iter().collect()).collect(),
            cluster: vec![CpuSet::new(); nr_cpu_ids],
        }
    }

    /// The documented invariant: groups are pairwise disjoint and
    /// their union is exactly the possible mask.
    fn check_disjoint_cover(topo: &CpuTopology, groups: &[CpuSet]) {
        let mut seen = CpuSet::new();
        for g in groups {
            assert!(!seen.intersects(g), "groups overlap: {groups:?}");
            seen = seen.or(g);
        }
        assert_eq!(
            seen, topo.possible,
            "groups do not cover possible: {groups:?}"
        );
    }

    #[test]
    fn single_node_even_split() {
        let t = topo(&["0-7"]);
        let groups = group_cpus_evenly(4, &t);
        assert_eq!(groups.len(), 4);
        for g in &groups {
            assert_eq!(g.weight(), 2);
        }
        check_disjoint_cover(&t, &groups);
    }

    #[test]
    fn single_node_with_remainder() {
        let t = topo(&["0-9"]);
        let groups = group_cpus_evenly(4, &t);
        let mut weights: Vec<usize> = groups.iter().map(CpuSet::weight).collect();
        weights.sort_unstable();
        assert_eq!(weights, vec![2, 2, 3, 3]);
        check_disjoint_cover(&t, &groups);
    }

    #[test]
    fn two_nodes_node_pure_groups() {
        let t = topo(&["0-7", "8-15"]);
        let groups = group_cpus_evenly(4, &t);
        assert_eq!(groups.len(), 4);
        check_disjoint_cover(&t, &groups);
        let mut per_node = [0, 0];
        for g in &groups {
            assert_eq!(g.weight(), 4);
            let node = usize::from(g.first().unwrap() >= 8);
            assert!(
                g.andnot(&t.node_to_cpus[node]).is_empty(),
                "group {g} crosses nodes"
            );
            per_node[node] += 1;
        }
        assert_eq!(per_node, [2, 2]);
    }

    #[test]
    fn unbalanced_nodes_get_at_least_one_group() {
        // node0 has 2 CPUs, node1 has 14: with 4 groups node0 still
        // gets one group (smallest-node-first allocation).
        let t = topo(&["0-1", "2-15"]);
        let groups = group_cpus_evenly(4, &t);
        assert_eq!(groups.len(), 4);
        check_disjoint_cover(&t, &groups);
        assert!(groups.iter().any(|g| g == &t.node_to_cpus[0]));
    }

    #[test]
    fn more_nodes_than_groups_wraps() {
        let t = topo(&["0-3", "4-7", "8-11"]);
        let groups = group_cpus_evenly(2, &t);
        assert_eq!(groups.len(), 2);
        check_disjoint_cover(&t, &groups);
        // node0 and node2 share group 0 (wrap), node1 gets group 1
        assert_eq!(groups[0].to_string(), "0-3,8-11");
        assert_eq!(groups[1].to_string(), "4-7");
    }

    #[test]
    fn more_groups_than_cpus_truncates() {
        let t = topo(&["0-2"]);
        let groups = group_cpus_evenly(8, &t);
        assert_eq!(groups.len(), 3);
        for g in &groups {
            assert_eq!(g.weight(), 1);
        }
        check_disjoint_cover(&t, &groups);
    }

    #[test]
    fn smt_siblings_stay_together() {
        // 8 CPUs, siblings (0,4) (1,5) (2,6) (3,7) — like x86 SMT.
        let mut t = topo(&["0-7"]);
        for c in 0..8 {
            t.sibling[c] = [c % 4, c % 4 + 4].into_iter().collect();
        }
        let groups = group_cpus_evenly(4, &t);
        check_disjoint_cover(&t, &groups);
        for g in &groups {
            let lo = g.first().unwrap();
            assert_eq!(g.iter().collect::<Vec<_>>(), vec![lo, lo + 4], "group {g}");
        }
    }

    #[test]
    fn cluster_pure_groups() {
        // one node, 4 clusters of 4 CPUs, 4 groups: each group must be
        // exactly one cluster.
        let mut t = topo(&["0-15"]);
        for c in 0..16 {
            t.cluster[c] =
                CpuSet::from_cpulist(&format!("{}-{}", c / 4 * 4, c / 4 * 4 + 3)).unwrap();
        }
        let groups = group_cpus_evenly(4, &t);
        check_disjoint_cover(&t, &groups);
        for g in &groups {
            assert_eq!(g.weight(), 4);
            assert_eq!(g.first().unwrap() % 4, 0, "group {g} is not cluster-pure");
        }
    }

    #[test]
    fn clusters_outnumbering_groups_fall_back() {
        // 4 clusters but only 2 groups: cluster pass must be skipped,
        // plain spread still covers everything evenly.
        let mut t = topo(&["0-15"]);
        for c in 0..16 {
            t.cluster[c] =
                CpuSet::from_cpulist(&format!("{}-{}", c / 4 * 4, c / 4 * 4 + 3)).unwrap();
        }
        let groups = group_cpus_evenly(2, &t);
        check_disjoint_cover(&t, &groups);
        assert!(groups.iter().all(|g| g.weight() == 8));
    }

    #[test]
    fn two_stage_present_then_possible() {
        // 8 possible CPUs but only 0-3 present: present CPUs spread
        // over groups 0-3 first, non-present CPUs over the same groups
        // starting back at 0 (4 groups exhausted by stage 1).
        let mut t = topo(&["0-7"]);
        t.present = CpuSet::from_cpulist("0-3").unwrap();
        let groups = group_cpus_evenly(4, &t);
        assert_eq!(groups.len(), 4);
        check_disjoint_cover(&t, &groups);
        for g in &groups {
            assert_eq!(g.weight(), 2);
            assert!(g.intersects(&t.present), "group {g} has no present CPU");
        }
    }

    #[test]
    fn invariant_matrix() {
        // Sweep group counts over assorted topologies; the
        // disjoint-and-complete invariant must always hold.
        let mut topos = vec![
            topo(&["0"]),
            topo(&["0-63"]),
            topo(&["0-7", "8-15", "16-23", "24-31"]),
            topo(&["0-1", "2-30", "31"]),
        ];
        let mut t = topo(&["0-11", "12-23"]);
        t.present = CpuSet::from_cpulist("0-2,12-17").unwrap();
        for c in 0..24 {
            t.cluster[c] =
                CpuSet::from_cpulist(&format!("{}-{}", c / 3 * 3, c / 3 * 3 + 2)).unwrap();
            t.sibling[c] = [c ^ 1].into_iter().collect();
        }
        topos.push(t);
        for t in &topos {
            for numgrps in 1..=(t.possible.weight() + 2) {
                let groups = group_cpus_evenly(numgrps, t);
                assert_eq!(groups.len(), numgrps.min(t.possible.weight()));
                check_disjoint_cover(t, &groups);
            }
        }
    }
}
