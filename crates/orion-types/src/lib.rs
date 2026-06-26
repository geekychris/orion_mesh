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
    RestartPolicy, RetryPolicy, RuntimeResourceSpec, ScheduleSpec, SecretSpec, ServiceSpec,
    TaskSpec, VolumeSpec,
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
        Ok(())
    }
}

#[cfg(test)]
mod tests;
