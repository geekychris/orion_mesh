use crate::PROTOCOL_VERSION;
use chrono::{DateTime, Utc};
use orion_types::NodeId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

mod heartbeat;
mod capabilities;
mod service;
mod task;
mod log_line;
mod metric;

pub use capabilities::Capabilities;
pub use heartbeat::Heartbeat;
pub use log_line::LogLine;
pub use metric::Metric;
pub use service::{ServiceRegister, ServiceUnregister};
pub use task::{TaskEvent, TaskOutcome, TaskSubmit};

/// Common envelope for every NATS message. Payload is generic so each topic
/// statically knows its body type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope<P> {
    /// Bumps when the envelope wire format changes.
    pub protocol: u32,
    /// Unique per-message id (uuid v4).
    pub id: Uuid,
    /// Originating node. May be `None` for controller-emitted control messages.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<NodeId>,
    pub at: DateTime<Utc>,
    pub payload: P,
}

impl<P> Envelope<P> {
    pub fn new(source: Option<NodeId>, payload: P) -> Self {
        Self {
            protocol: PROTOCOL_VERSION,
            id: Uuid::new_v4(),
            source,
            at: Utc::now(),
            payload,
        }
    }
}
