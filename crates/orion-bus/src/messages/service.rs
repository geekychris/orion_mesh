use orion_types::{NodeId, ResourceName, Runtime};
use serde::{Deserialize, Serialize};

/// A service instance came online.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceRegister {
    pub node_id: NodeId,
    pub service: ResourceName,
    pub instance_id: String,
    pub runtime: Runtime,
    /// Reachable endpoint(s) — `tcp://host:port`, `unix:/path`, `http://host:port`.
    pub endpoints: Vec<String>,
}

/// A service instance went away (clean shutdown).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceUnregister {
    pub node_id: NodeId,
    pub service: ResourceName,
    pub instance_id: String,
    pub reason: String,
}
