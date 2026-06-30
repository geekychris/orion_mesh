//! `Resource` — the top-level wire shape of every desired-state document.
//!
//! Wire form (YAML/JSON), plan section 7.1:
//! ```yaml
//! apiVersion: orionmesh.dev/v1
//! kind: Service
//! metadata: { name: amiga-search, labels: { site: belmont } }
//! spec:   { ... }
//! status: { phase: Running, conditions: [...], observedGeneration: 3 }
//! ```
//!
//! Internally we wrap a tagged enum (`ResourceBody`) inside a struct that
//! carries the apiVersion + metadata so they sit at the top level after serde's
//! `flatten`. The `kind` field is the body's discriminator and gets merged in.

use crate::{metadata::Metadata, specs::*, status::Status};
use serde::{Deserialize, Serialize};

pub const API_VERSION: &str = "orionmesh.dev/v1";

fn default_api_version() -> String {
    API_VERSION.to_owned()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Resource {
    #[serde(rename = "apiVersion", default = "default_api_version")]
    pub api_version: String,
    pub metadata: Metadata,
    #[serde(flatten)]
    pub body: ResourceBody,
}

/// `kind:`-discriminated union over every resource kind.
/// Adding a variant: add it here, add a `spec` type in [`crate::specs`], and
/// extend [`ResourceBody::kind_str`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ResourceBody {
    Node {
        spec: NodeSpec,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<Status>,
    },
    Service {
        spec: ServiceSpec,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<Status>,
    },
    Task {
        spec: TaskSpec,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<Status>,
    },
    Job {
        spec: JobSpec,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<Status>,
    },
    Schedule {
        spec: ScheduleSpec,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<Status>,
    },
    Dataset {
        spec: DatasetSpec,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<Status>,
    },
    Model {
        spec: ModelSpec,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<Status>,
    },
    Project {
        spec: ProjectSpec,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<Status>,
    },
    Secret {
        spec: SecretSpec,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<Status>,
    },
    Volume {
        spec: VolumeSpec,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<Status>,
    },
    Network {
        spec: NetworkSpec,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<Status>,
    },
    Queue {
        spec: QueueSpec,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<Status>,
    },
    /// Peer runtime catalog entry (OrionMesh, KQueue, ...).
    Runtime {
        spec: RuntimeResourceSpec,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<Status>,
    },
    /// Declared capability schema.
    Capability {
        spec: CapabilityResourceSpec,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<Status>,
    },
    Policy {
        spec: PolicySpec,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<Status>,
    },
    Integration {
        spec: IntegrationSpec,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<Status>,
    },
}

impl ResourceBody {
    pub fn kind_str(&self) -> &'static str {
        match self {
            ResourceBody::Node { .. } => "Node",
            ResourceBody::Service { .. } => "Service",
            ResourceBody::Task { .. } => "Task",
            ResourceBody::Job { .. } => "Job",
            ResourceBody::Schedule { .. } => "Schedule",
            ResourceBody::Dataset { .. } => "Dataset",
            ResourceBody::Model { .. } => "Model",
            ResourceBody::Project { .. } => "Project",
            ResourceBody::Secret { .. } => "Secret",
            ResourceBody::Volume { .. } => "Volume",
            ResourceBody::Network { .. } => "Network",
            ResourceBody::Queue { .. } => "Queue",
            ResourceBody::Runtime { .. } => "Runtime",
            ResourceBody::Capability { .. } => "Capability",
            ResourceBody::Policy { .. } => "Policy",
            ResourceBody::Integration { .. } => "Integration",
        }
    }
}

impl Resource {
    pub fn kind_str(&self) -> &'static str {
        self.body.kind_str()
    }

    pub fn name(&self) -> &str {
        &self.metadata.name.0
    }
}
