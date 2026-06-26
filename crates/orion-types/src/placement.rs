//! Placement constraints and node hardware descriptors.
//!
//! Two distinct GPU types per plan section 9.2:
//! - [`NodeGpu`] — what a node *has* (vendor, vram, model name)
//! - [`GpuRequirement`] — what a workload *needs* (min_vram_gb, optional vendor filter)
//!
//! The scheduler matches `GpuRequirement` against `NodeGpu`.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Arch {
    X86_64,
    Arm64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OperatingSystem {
    Linux,
    Macos,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GpuVendor {
    Nvidia,
    Amd,
    Apple,
    Intel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Acceleration {
    None,
    Metal,
    Cuda,
    Rocm,
    Coreml,
}

/// What a node has.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeGpu {
    pub vendor: GpuVendor,
    pub vram_gb: u32,
    /// Marketing name (`RTX 4090`, `M2 Max`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// What a workload needs.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct GpuRequirement {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vendor: Option<GpuVendor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_vram_gb: Option<u32>,
}

/// Placement constraints. All fields optional; empty = matches anything.
/// Lists are ANY-of; multiple keys are AND-ed.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Placement {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub arch: Vec<Arch>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub os: Vec<OperatingSystem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gpu: Option<GpuRequirement>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acceleration: Option<Acceleration>,
    /// Node labels (`site: belmont`, `power: mains`). AND-ed with the above.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub node_labels: BTreeMap<String, String>,
    /// Soft scoring preferences. Hard constraints stay in the fields above.
    #[serde(skip_serializing_if = "PlacementPreferences::is_empty")]
    pub prefer: PlacementPreferences,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct PlacementPreferences {
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub node_labels: BTreeMap<String, String>,
    /// Prefer nodes that already hold a copy of the referenced dataset.
    /// Used together with `TaskSpec::prefer_data_locality`.
    pub data_locality: bool,
}

impl PlacementPreferences {
    pub fn is_empty(&self) -> bool {
        self.node_labels.is_empty() && !self.data_locality
    }
}
