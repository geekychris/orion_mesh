use orion_types::{Capability, NodeId, ResourceName};
use serde::{Deserialize, Serialize};

/// Capability advertisement — "service `X` on node `N` can do these things".
/// One message per (service, node) pair; re-sent on change.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capabilities {
    pub node_id: NodeId,
    pub service: ResourceName,
    pub capabilities: Capability,
}
