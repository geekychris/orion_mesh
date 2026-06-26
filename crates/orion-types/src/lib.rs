//! OrionMesh shared resource model.
//!
//! The `Resource` enum is the K8s-style `kind:` discriminator the desired-state
//! YAML uses. Every variant carries metadata + a spec; specs are crate-private
//! types that derive serde.

pub mod capability;
pub mod metadata;
pub mod placement;
pub mod resource;
pub mod runtime;

pub use capability::{Capability, CapabilitySelector};
pub use metadata::{Metadata, NodeId, ResourceName};
pub use placement::{Acceleration, Arch, Gpu, OperatingSystem, Placement};
pub use resource::{
    DatasetSpec, ModelSpec, NetworkSpec, NodeSpec, ProjectSpec, Resource, ScheduleSpec,
    SecretSpec, ServiceSpec, TaskSpec, VolumeSpec,
};
pub use runtime::{Runtime, RuntimeKind};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ResourceError {
    #[error("invalid resource yaml: {0}")]
    Yaml(#[from] serde_yml::Error),
    #[error("invalid resource json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("resource kind mismatch: expected {expected}, got {actual}")]
    KindMismatch { expected: &'static str, actual: String },
}

impl Resource {
    /// Parse a single resource document from YAML.
    pub fn from_yaml(s: &str) -> Result<Self, ResourceError> {
        Ok(serde_yml::from_str(s)?)
    }

    /// Serialize this resource to YAML.
    pub fn to_yaml(&self) -> Result<String, ResourceError> {
        Ok(serde_yml::to_string(self)?)
    }
}
