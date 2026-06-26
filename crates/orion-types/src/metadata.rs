use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

/// DNS-1123-style resource name. Validated lightly at the boundary.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct ResourceName(pub String);

impl fmt::Display for ResourceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for ResourceName {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

impl From<String> for ResourceName {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Stable identifier for a node. Set at agent first-boot; persisted locally.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct NodeId(pub String);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for NodeId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

impl From<String> for NodeId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Common metadata block shared by every resource kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Metadata {
    pub name: ResourceName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
    /// Monotonic counter bumped by the controller on every spec mutation.
    /// `Status::observed_generation` is compared against this for staleness checks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
}
