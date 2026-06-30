//! OrionMesh shared resource model.
//!
//! The wire shape is always:
//! ```yaml
//! apiVersion: orionmesh.dev/v1
//! kind: <Kind>
//! metadata: { name, namespace?, labels?, annotations?, generation? }
//! spec: { ... per-kind ... }
//! status: { phase, conditions[], observedGeneration?, node?, message? }
//! ```
//!
//! Parsed via [`Resource::from_yaml`] / [`Resource::from_json`] and re-emitted
//! via [`Resource::to_yaml`] / [`Resource::to_json`]. Round-trip stability is
//! guaranteed by the `roundtrip_*` tests in [`tests`].

pub mod capability;
pub mod metadata;
pub mod placement;
pub mod resource;
pub mod runtime;
pub mod specs;
pub mod status;

pub use capability::{AttrChecks, AttrMatch, AttrOp, Capability, CapabilitySelector};
pub use metadata::{Metadata, NodeId, ResourceName};
pub use placement::{
    Acceleration, Arch, GpuRequirement, GpuVendor, NodeGpu, OperatingSystem, Placement,
    PlacementPreferences,
};
pub use resource::{API_VERSION, Resource, ResourceBody};
pub use runtime::{Runtime, RuntimeKind};
pub use specs::{
    CapabilityResourceSpec, DatasetAccess, DatasetLocation, DatasetSpec, HealthCheck,
    IntegrationSpec, JobSpec, ModelSpec, ModelVariant, NetworkSpec, NodeResources, NodeRole,
    NodeSpec, PolicySpec, PortProtocol, PortSpec, ProjectBuild, ProjectService, ProjectSpec,
    QueueSpec, QueueType, RestartPolicy, RetryPolicy, RuntimeResourceSpec, ScheduleSpec,
    SecretSpec, ServiceSpec, TaskSpec, VolumeSpec, WorkflowSpec, WorkflowStep,
};
pub use status::{Condition, ConditionStatus, Phase, Status};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ResourceError {
    #[error("invalid resource yaml: {0}")]
    Yaml(#[from] serde_yml::Error),
    #[error("invalid resource json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("schedule must set exactly one of `task` or `taskTemplate`")]
    ScheduleAmbiguous,
    #[error("queue name {0:?} is invalid: must match [a-zA-Z0-9_-]+ (no dots, slashes, spaces)")]
    QueueNameInvalid(String),
}

impl Resource {
    pub fn from_yaml(s: &str) -> Result<Self, ResourceError> {
        Ok(serde_yml::from_str(s)?)
    }

    pub fn to_yaml(&self) -> Result<String, ResourceError> {
        Ok(serde_yml::to_string(self)?)
    }

    pub fn from_json(s: &str) -> Result<Self, ResourceError> {
        Ok(serde_json::from_str(s)?)
    }

    pub fn to_json(&self) -> Result<String, ResourceError> {
        Ok(serde_json::to_string_pretty(self)?)
    }

    /// Lightweight semantic checks that serde can't catch.
    pub fn validate(&self) -> Result<(), ResourceError> {
        if let ResourceBody::Schedule { spec, .. } = &self.body {
            match (&spec.task, &spec.task_template) {
                (Some(_), None) | (None, Some(_)) => {}
                _ => return Err(ResourceError::ScheduleAmbiguous),
            }
        }
        if let ResourceBody::Queue { .. } = &self.body {
            let name = &self.metadata.name.0;
            if name.is_empty()
                || !name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
            {
                return Err(ResourceError::QueueNameInvalid(name.clone()));
            }
        }
        Ok(())
    }
}

/// Default subject for a named queue: `orion.queue.<name>`.
pub fn default_queue_subject(name: &str) -> String {
    format!("orion.queue.{name}")
}

/// Default JetStream stream name for a queue: uppercased + `-`/`.` → `_`.
pub fn default_queue_stream(name: &str) -> String {
    let mut s = String::with_capacity(name.len() + 12);
    s.push_str("ORION_QUEUE_");
    for c in name.chars() {
        match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' => s.push(c.to_ascii_uppercase()),
            _ => s.push('_'),
        }
    }
    s
}

#[cfg(test)]
mod tests;
