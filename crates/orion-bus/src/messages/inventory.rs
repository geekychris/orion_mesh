use orion_types::{Acceleration, Arch, NodeGpu, NodeId, NodeRole, OperatingSystem};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Full structural snapshot of a node. Published on first connect and on change.
/// Separate from `Heartbeat` so the controller can keep heartbeats small.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeInventory {
    pub node_id: NodeId,
    pub agent_version: String,
    pub arch: Arch,
    pub os: OperatingSystem,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceleration: Option<Acceleration>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub gpus: Vec<NodeGpu>,
    pub cpu_cores: u32,
    pub mem_total_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_gb: Option<u32>,
    /// Runtime adapters that loaded successfully on this node.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runtimes: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub roles: Vec<NodeRole>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
}
