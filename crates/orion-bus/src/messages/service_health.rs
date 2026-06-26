use orion_types::{NodeId, ResourceName};
use serde::{Deserialize, Serialize};

/// Periodic health probe result for a running service instance.
/// Distinct from register/unregister: those are lifecycle events; this is liveness.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceHealth {
    pub node_id: NodeId,
    pub service: ResourceName,
    pub instance_id: String,
    pub status: HealthStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Consecutive failure count; reset to 0 on success. Drives the
    /// `failure_threshold` check at the controller.
    #[serde(default)]
    pub consecutive_failures: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    Healthy,
    Unhealthy,
    Unknown,
}
