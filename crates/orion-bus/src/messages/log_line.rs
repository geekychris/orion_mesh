use orion_types::{NodeId, ResourceName};
use serde::{Deserialize, Serialize};

/// One log line streamed from a service running on a node.
/// Subject: `orion.logs.{node_id}`. Subscribers filter by `service` field-side.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogLine {
    pub node_id: NodeId,
    pub service: ResourceName,
    pub stream: LogStream,
    pub line: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogStream {
    Stdout,
    Stderr,
}
