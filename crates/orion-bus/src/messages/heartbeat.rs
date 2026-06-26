use orion_types::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Compact liveness + per-tick metrics. Published every ~5s on `orion.heartbeat`.
/// Heavy/structural details (gpus, runtimes) live on `orion.node.inventory`,
/// not here — heartbeats should stay small.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Heartbeat {
    pub node_id: NodeId,
    pub agent_version: String,
    pub uptime_seconds: u64,
    pub cpu_load_1m: f32,
    pub mem_used_bytes: u64,
    pub mem_total_bytes: u64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}
