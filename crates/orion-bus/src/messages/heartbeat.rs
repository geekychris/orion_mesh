use orion_types::{Acceleration, Arch, Gpu, NodeId, OperatingSystem};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Periodic agent → controller liveness + node inventory.
/// Published every ~5s on `orion.heartbeat`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Heartbeat {
    pub node_id: NodeId,
    pub agent_version: String,
    pub uptime_seconds: u64,
    pub arch: Arch,
    pub os: OperatingSystem,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gpu: Option<Gpu>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acceleration: Option<Acceleration>,
    pub cpu_cores: u32,
    pub mem_total_bytes: u64,
    pub mem_used_bytes: u64,
    pub load_avg_1m: f32,
    /// Free-form node labels (`site=belmont`, `power=mains`, etc.).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}
