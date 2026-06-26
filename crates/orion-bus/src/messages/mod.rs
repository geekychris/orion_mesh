use crate::PROTOCOL_VERSION;
use chrono::{DateTime, Utc};
use orion_types::NodeId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

mod capabilities;
mod control;
mod heartbeat;
mod inventory;
mod log_line;
mod metric;
mod service;
mod service_health;
mod task;

pub use capabilities::Capabilities;
pub use control::{ControlDrain, ControlRestart, ControlRun, ControlStop, WorkloadKind};
pub use heartbeat::Heartbeat;
pub use inventory::NodeInventory;
pub use log_line::{LogLine, LogStream};
pub use metric::{Metric, MetricSample};
pub use service::{ServiceRegister, ServiceUnregister};
pub use service_health::{HealthStatus, ServiceHealth};
pub use task::{TaskEvent, TaskOutcome, TaskSubmit};

/// Common envelope for every NATS message. Payload is generic so each topic
/// statically knows its body type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope<P> {
    pub protocol: u32,
    pub id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
