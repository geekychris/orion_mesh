//! NATS contract for OrionMesh — hybrid wide control plane + consolidated data plane.
//!
//! Adding a new topic or message:
//! 1. Add to [`Topic`] (subject naming convention: `orion.<area>.<verb>`).
//! 2. Add a payload type in [`messages`].
//! 3. Re-export from [`messages::*`] and from the crate root.
//! 4. Bump [`PROTOCOL_VERSION`] only on **incompatible** envelope shape changes.

pub mod messages;
pub mod topics;

pub use messages::{
    Capabilities, ControlDrain, ControlRestart, ControlRun, ControlStop, Envelope, HealthStatus,
    Heartbeat, LogLine, LogStream, Metric, MetricSample, NodeInventory, ServiceHealth,
    ServiceRegister, ServiceUnregister, TaskEvent, TaskOutcome, TaskSubmit, WorkloadKind,
};
pub use topics::Topic;

pub const PROTOCOL_VERSION: u32 = 1;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum BusError {
    #[error("json encode/decode: {0}")]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests;
