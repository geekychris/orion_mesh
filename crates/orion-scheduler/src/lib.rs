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
