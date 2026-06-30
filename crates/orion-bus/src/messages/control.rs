use orion_types::{HealthCheck, ResourceName, Runtime};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Controller → agent: start a workload. Published to `orion.control.{node_id}.run`.
///
/// When `replicas > 1` the agent launches that many copies, each with its own
/// distinct `instance_id` derived from this base id. The 0-th instance reuses
/// `instance_id` exactly so single-replica callers don't change behaviour.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControlRun {
    pub instance_id: Uuid,
    /// `Service` or `Task` — what kind of workload this is.
    pub kind: WorkloadKind,
    pub name: ResourceName,
    pub runtime: Runtime,
    /// Generation of the resource at the time of dispatch. Echoed back in
    /// status updates so the controller knows if it's stale.
    #[serde(default)]
    pub generation: u64,
    /// How many copies of the workload to launch. Defaults to 1.
    #[serde(default = "default_one_replica")]
    pub replicas: u32,
    /// Optional health-check spec — when set, the agent runs the probe in a
    /// loop and publishes ServiceHealth envelopes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health_check: Option<HealthCheck>,
}

fn default_one_replica() -> u32 {
    1
}

/// Controller → agent: stop a workload instance. `orion.control.{node_id}.stop`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControlStop {
    pub instance_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grace_seconds: Option<u32>,
}

/// Controller → agent: restart a service instance. `orion.control.{node_id}.restart`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControlRestart {
    pub instance_id: Uuid,
}

/// Controller → agent: drain — finish current work, accept no new dispatches.
/// `orion.control.{node_id}.drain`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControlDrain {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkloadKind {
    Service,
    Task,
}
