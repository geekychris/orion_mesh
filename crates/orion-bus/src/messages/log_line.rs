use orion_types::{NodeId, ResourceName};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// One log line streamed from a service running on a node.
/// Subject: `orion.logs.{node_id}`. Subscribers filter by `service` field-side.
///
/// `instance_id` and `replica_index` let consumers distinguish output from N
/// replicas of the same Service/Task running on the same (or different) nodes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogLine {
    pub node_id: NodeId,
    pub service: ResourceName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<Uuid>,
    #[serde(default)]
    pub replica_index: u32,
    pub stream: LogStream,
    pub line: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogStream {
    Stdout,
    Stderr,
}
