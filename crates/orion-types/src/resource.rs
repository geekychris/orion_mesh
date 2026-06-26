use crate::{
    capability::{Capability, CapabilitySelector},
    metadata::{Metadata, NodeId},
    placement::{Acceleration, Arch, Gpu, OperatingSystem, Placement},
    runtime::Runtime,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// K8s-style discriminated union over every resource the controller manages.
///
/// Wire form (YAML/JSON):
/// ```yaml
/// kind: Service
/// metadata: { name: amiga-search }
/// spec:
///   runtime: { kind: docker, image: ... }
///   placement: { arch: [arm64, x86_64], os: [linux] }
///   requires:
///     dataset: amiga_schematics
///   replicas: 1
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum Resource {
    Node {
        metadata: Metadata,
        spec: NodeSpec,
    },
    Service {
        metadata: Metadata,
        spec: ServiceSpec,
    },
    Task {
        metadata: Metadata,
        spec: TaskSpec,
    },
    Schedule {
        metadata: Metadata,
        spec: ScheduleSpec,
    },
    Dataset {
        metadata: Metadata,
        spec: DatasetSpec,
    },
    Model {
        metadata: Metadata,
        spec: ModelSpec,
    },
    Project {
        metadata: Metadata,
        spec: ProjectSpec,
    },
    Secret {
        metadata: Metadata,
        spec: SecretSpec,
    },
    Volume {
        metadata: Metadata,
        spec: VolumeSpec,
    },
    Network {
        metadata: Metadata,
        spec: NetworkSpec,
    },
}

impl Resource {
    pub fn kind_str(&self) -> &'static str {
        match self {
            Resource::Node { .. } => "Node",
            Resource::Service { .. } => "Service",
            Resource::Task { .. } => "Task",
            Resource::Schedule { .. } => "Schedule",
            Resource::Dataset { .. } => "Dataset",
            Resource::Model { .. } => "Model",
            Resource::Project { .. } => "Project",
            Resource::Secret { .. } => "Secret",
            Resource::Volume { .. } => "Volume",
            Resource::Network { .. } => "Network",
        }
    }

    pub fn metadata(&self) -> &Metadata {
        match self {
            Resource::Node { metadata, .. }
            | Resource::Service { metadata, .. }
            | Resource::Task { metadata, .. }
            | Resource::Schedule { metadata, .. }
            | Resource::Dataset { metadata, .. }
            | Resource::Model { metadata, .. }
            | Resource::Project { metadata, .. }
            | Resource::Secret { metadata, .. }
            | Resource::Volume { metadata, .. }
            | Resource::Network { metadata, .. } => metadata,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct NodeSpec {
    pub node_id: NodeId,
    pub arch: Option<Arch>,
    pub os: Option<OperatingSystem>,
    pub gpu: Option<Gpu>,
    pub acceleration: Option<Acceleration>,
    pub address: Option<String>,
    pub labels: BTreeMap<String, String>,
}

impl Default for NodeSpec {
    fn default() -> Self {
        Self {
            node_id: NodeId(String::new()),
            arch: None,
            os: None,
            gpu: None,
            acceleration: None,
            address: None,
            labels: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ServiceSpec {
    pub runtime: Option<Runtime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replicas: Option<u32>,
    #[serde(skip_serializing_if = "is_default_placement")]
    pub placement: Placement,
    /// `requires:` block. Resolved against advertised Capabilities.
    #[serde(skip_serializing_if = "is_default_selector")]
    pub requires: CapabilitySelector,
    /// What this service *itself* advertises once running.
    #[serde(skip_serializing_if = "Capability::is_empty")]
    pub capabilities: Capability,
}

impl Capability {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

fn is_default_placement(p: &Placement) -> bool {
    p == &Placement::default()
}

fn is_default_selector(s: &CapabilitySelector) -> bool {
    s == &CapabilitySelector::default()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TaskSpec {
    pub runtime: Option<Runtime>,
    #[serde(skip_serializing_if = "is_default_placement")]
    pub placement: Placement,
    #[serde(skip_serializing_if = "is_default_selector")]
    pub requires: CapabilitySelector,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ScheduleSpec {
    /// 5-field cron expression.
    pub cron: String,
    /// Name of the Task resource this schedule fires.
    pub task: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct DatasetSpec {
    pub uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    /// Nodes that hold a local copy (capability-aware scheduling).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub located_on: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ModelSpec {
    pub model_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters_b: Option<f32>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub served_by: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ProjectSpec {
    /// Dev Portal asset id, when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub asset_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repo_url: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub services: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct SecretSpec {
    /// Reference into SecureVault. Plaintext never sits in this struct.
    pub vault_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct VolumeSpec {
    pub path: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub mounted_on: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct NetworkSpec {
    pub cidr: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sites: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::ResourceName;
    use crate::placement::{Arch, OperatingSystem};

    #[test]
    fn roundtrips_amiga_search_example() {
        // The canonical example from OrionMesh_Architecture_Plan.md.
        let yaml = r#"
kind: Service
metadata:
  name: amiga-search
spec:
  runtime:
    kind: docker
    image: amiga-search:latest
  replicas: 1
  placement:
    arch: [arm64, x86_64]
    os: [linux]
  requires:
    dataset: amiga_schematics
"#;
        let r = Resource::from_yaml(yaml).unwrap();
        assert_eq!(r.kind_str(), "Service");
        assert_eq!(r.metadata().name, ResourceName::from("amiga-search"));
        match r {
            Resource::Service { spec, .. } => {
                assert_eq!(spec.replicas, Some(1));
                assert!(spec.placement.arch.contains(&Arch::Arm64));
                assert!(spec.placement.os.contains(&OperatingSystem::Linux));
                assert_eq!(
                    spec.requires.require.get("dataset").map(String::as_str),
                    Some("amiga_schematics"),
                );
            }
            _ => panic!("expected Service"),
        }
    }
}
