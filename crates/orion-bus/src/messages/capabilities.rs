use orion_types::{Capability, NodeId, ResourceName};
use serde::{Deserialize, Serialize};

/// "Service X on node N can do these things". One message per (service, node);
/// re-emitted on change.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Capabilities {
    pub node_id: NodeId,
    pub service: ResourceName,
    pub capabilities: Vec<Capability>,
}
