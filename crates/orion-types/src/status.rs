//! Observed state shared by every Resource variant.
//!
//! Plan section 10.3 places `status:` next to `spec:` and uses K8s-style
//! condition arrays. We mirror that, with a typed `Phase` enum and freeform
//! `Condition::type_` so callers can introduce new condition types
//! (`Available`, `Healthy`, `Reconciling`, ...) without a schema bump.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Status {
    pub phase: Phase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
    /// Node hosting this resource (for Service/Task/Job/Schedule).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<crate::NodeId>,
    /// Free-form per-kind state. Keep small; long state goes on the wire/store.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum Phase {
    #[default]
    Pending,
    Running,
    Succeeded,
    Failed,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Condition {
    /// `Available`, `Healthy`, `Reconciling`, etc. Freeform string.
    #[serde(rename = "type")]
    pub type_: String,
    pub status: ConditionStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub last_transition: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum ConditionStatus {
    True,
    False,
    Unknown,
}
