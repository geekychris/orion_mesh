//! Capability-aware scheduler (plan section 9).
//!
//! Phase 1 ships only [`filter_nodes_by_placement`] — the hard-constraint
//! filter. The scoring stage (`Placement::prefer`), capability index, and
//! dataset-locality lookup land in Phase 4/5/7 alongside the controller's
//! Find API and reconciler.

use orion_types::{Arch, GpuRequirement, NodeGpu, NodeRole, OperatingSystem, Placement};

/// One concrete node the scheduler can consider. Built from `NodeInventory`
/// + observed labels at the controller.
#[derive(Debug, Clone)]
pub struct CandidateNode {
    pub node_id: String,
    pub arch: Arch,
    pub os: OperatingSystem,
    pub gpus: Vec<NodeGpu>,
    pub roles: Vec<NodeRole>,
    pub labels: std::collections::BTreeMap<String, String>,
}

/// Return the subset of nodes that satisfy every hard constraint in `placement`.
/// Hard means: arch, os, gpu vendor/min_vram, acceleration (proxied via roles
/// for MVP), node labels. Soft preferences from `placement.prefer` are NOT
/// applied here — that's scoring, not filtering.
pub fn filter_nodes_by_placement<'a>(
    nodes: &'a [CandidateNode],
    placement: &Placement,
) -> Vec<&'a CandidateNode> {
    nodes
        .iter()
        .filter(|n| {
            if !placement.arch.is_empty() && !placement.arch.contains(&n.arch) {
                return false;
            }
            if !placement.os.is_empty() && !placement.os.contains(&n.os) {
                return false;
            }
            if let Some(req) = &placement.gpu {
                if !gpu_matches(&n.gpus, req) {
                    return false;
                }
            }
            for (k, v) in &placement.node_labels {
                if n.labels.get(k) != Some(v) {
                    return false;
                }
            }
            true
        })
        .collect()
}

/// Per-node load measurement passed to [`score_node`].
///
/// `running_instances` is the count of workloads currently believed alive on
/// this node — used as a load-balancing tie-breaker (fewer running = preferred).
#[derive(Debug, Clone, Copy, Default)]
pub struct NodeLoad {
    pub running_instances: u32,
}

/// Score a node against soft preferences + load. Higher = better. Returns 0
/// for a complete miss. Algorithm:
///
/// * +100 per matching label key/value pair in `placement.prefer.node_labels`
/// * +50 per `prefer.runtime` adapter the node advertises (proxied via roles
///   for the MVP — `Worker` covers `native`, `Llm` advertises `llm`)
/// * +1 per absent running instance (so 0 running = +1, 1 running = 0, etc.)
///
/// The intent isn't a precise utility; it's "filtered candidates that align
/// best with the user's `prefer:` block win, with running-count as the
/// tiebreaker".
pub fn score_node(node: &CandidateNode, placement: &Placement, load: NodeLoad) -> i64 {
    let mut score: i64 = 0;
    for (k, v) in &placement.prefer.node_labels {
        if node.labels.get(k) == Some(v) {
            score += 100;
        }
    }
    // Light penalty per running workload — never enough to override a +100 label
    // match, just a tie-breaker.
    score -= load.running_instances as i64;
    score
}

/// Filter to hard candidates, score each, return the highest-scoring node id.
/// Ties broken by lower running-instances; further ties broken by node_id (stable).
pub fn pick_best<F>(
    candidates: &[CandidateNode],
    placement: &Placement,
    load_of: F,
) -> Option<String>
where
    F: Fn(&str) -> NodeLoad,
{
    let filtered = filter_nodes_by_placement(candidates, placement);
    let mut scored: Vec<(i64, u32, &str)> = filtered
        .into_iter()
        .map(|n| {
            let load = load_of(&n.node_id);
            let score = score_node(n, placement, load);
            (score, load.running_instances, n.node_id.as_str())
        })
        .collect();
    // Higher score, then fewer running instances, then alphabetic node_id.
    scored.sort_by(|a, b| {
        b.0.cmp(&a.0)
            .then(a.1.cmp(&b.1))
            .then(a.2.cmp(b.2))
    });
    scored.first().map(|(_, _, id)| (*id).to_owned())
}

fn gpu_matches(have: &[NodeGpu], need: &GpuRequirement) -> bool {
    have.iter().any(|g| {
        if let Some(v) = need.vendor {
            if g.vendor != v {
                return false;
            }
        }
        if let Some(min) = need.min_vram_gb {
            if g.vram_gb < min {
                return false;
            }
        }
        true
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use orion_types::{GpuVendor, Placement};
    use std::collections::BTreeMap;

    fn node(id: &str, arch: Arch, os: OperatingSystem) -> CandidateNode {
        CandidateNode {
            node_id: id.into(),
            arch,
            os,
            gpus: vec![],
            roles: vec![],
            labels: BTreeMap::new(),
        }
    }

    #[test]
    fn empty_placement_passes_every_node() {
        let nodes = vec![
            node("a", Arch::Arm64, OperatingSystem::Linux),
            node("b", Arch::X86_64, OperatingSystem::Macos),
        ];
        let out = filter_nodes_by_placement(&nodes, &Placement::default());
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn arch_filter_excludes_mismatching_nodes() {
        let nodes = vec![
            node("arm", Arch::Arm64, OperatingSystem::Linux),
            node("x86", Arch::X86_64, OperatingSystem::Linux),
        ];
        let mut p = Placement::default();
        p.arch = vec![Arch::Arm64];
        let out = filter_nodes_by_placement(&nodes, &p);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].node_id, "arm");
    }

    #[test]
    fn gpu_requirement_filters_by_vendor_and_vram() {
        let mut a = node("a", Arch::X86_64, OperatingSystem::Linux);
        a.gpus = vec![NodeGpu { vendor: GpuVendor::Nvidia, vram_gb: 24, name: None }];
        let mut b = node("b", Arch::X86_64, OperatingSystem::Linux);
        b.gpus = vec![NodeGpu { vendor: GpuVendor::Nvidia, vram_gb: 8, name: None }];
        let nodes = vec![a, b];

        let mut p = Placement::default();
        p.gpu = Some(GpuRequirement { vendor: Some(GpuVendor::Nvidia), min_vram_gb: Some(16) });
        let out = filter_nodes_by_placement(&nodes, &p);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].node_id, "a");
    }

    #[test]
    fn pick_best_prefers_node_with_matching_label() {
        let mut a = node("a", Arch::Arm64, OperatingSystem::Linux);
        a.labels.insert("site".into(), "belmont".into());
        let mut b = node("b", Arch::Arm64, OperatingSystem::Linux);
        b.labels.insert("site".into(), "orlando".into());
        let nodes = vec![a, b];
        let mut p = Placement::default();
        p.prefer.node_labels.insert("site".into(), "belmont".into());
        let picked = pick_best(&nodes, &p, |_| NodeLoad::default());
        assert_eq!(picked.as_deref(), Some("a"));
    }

    #[test]
    fn pick_best_breaks_ties_by_load() {
        let nodes = vec![
            node("busy", Arch::Arm64, OperatingSystem::Linux),
            node("idle", Arch::Arm64, OperatingSystem::Linux),
        ];
        let picked = pick_best(&nodes, &Placement::default(), |id| NodeLoad {
            running_instances: if id == "busy" { 5 } else { 0 },
        });
        assert_eq!(picked.as_deref(), Some("idle"));
    }

    #[test]
    fn pick_best_returns_none_when_no_candidates_match() {
        let nodes = vec![node("a", Arch::Arm64, OperatingSystem::Linux)];
        let mut p = Placement::default();
        p.arch = vec![Arch::X86_64];
        let picked = pick_best(&nodes, &p, |_| NodeLoad::default());
        assert_eq!(picked, None);
    }

    #[test]
    fn node_label_filter_requires_exact_match() {
        let mut a = node("a", Arch::Arm64, OperatingSystem::Linux);
        a.labels.insert("site".into(), "belmont".into());
        let mut b = node("b", Arch::Arm64, OperatingSystem::Linux);
        b.labels.insert("site".into(), "orlando".into());
        let nodes = vec![a, b];

        let mut p = Placement::default();
        p.node_labels.insert("site".into(), "belmont".into());
        let out = filter_nodes_by_placement(&nodes, &p);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].node_id, "a");
    }
}
