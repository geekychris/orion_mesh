//! NATS contract for OrionMesh.
//!
//! Every inter-component message rides one of the topics in [`topics`].
//! Payloads are wrapped in an [`Envelope`] that carries id, time, and source node.
//! Adding a new topic is a deliberate act: bump [`PROTOCOL_VERSION`] if the
//! envelope shape changes incompatibly.

pub mod messages;
pub mod topics;

pub use messages::{
    Capabilities, Envelope, Heartbeat, LogLine, Metric, ServiceRegister, ServiceUnregister,
    TaskEvent, TaskSubmit,
};
pub use topics::Topic;

/// Bumped when the envelope wire format changes incompatibly.
pub const PROTOCOL_VERSION: u32 = 1;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum BusError {
    #[error("json encode/decode: {0}")]
    Json(#[from] serde_json::Error),
}
