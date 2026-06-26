//! NATS subject namespace.
//!
//! Naming convention (matches NATS idiom): dot-separated, lowercase, optional
//! token slots use `{token}` in docs and a literal node-id / resource-name on the wire.
//!
//! Persistence tier:
//! - `core` — NATS Core pub/sub, ephemeral, fire-and-forget.
//! - `jetstream` — JetStream durable stream, at-least-once delivery.

/// Strongly-typed handle to a subject. `Topic::heartbeat()` returns the literal string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Topic {
    /// `orion.heartbeat` — every agent publishes here every N seconds. Core.
    Heartbeat,
    /// `orion.capabilities` — capability advertisements (one per service per node). Core.
    Capabilities,
    /// `orion.service.register` — service comes online. JetStream.
    ServiceRegister,
    /// `orion.service.unregister` — service goes away cleanly. JetStream.
    ServiceUnregister,
    /// `orion.task.submit` — controller asks an agent to run a Task. JetStream.
    TaskSubmit,
    /// `orion.task.events` — agent reports task lifecycle (started/finished/failed). JetStream.
    TaskEvents,
    /// `orion.logs.{node_id}` — line-delimited log forwarding. Core.
    Logs,
    /// `orion.metrics.{node_id}` — periodic node/service metric snapshots. Core.
    Metrics,
}

impl Topic {
    /// The literal NATS subject string for fixed-name topics.
    /// For [`Topic::Logs`] and [`Topic::Metrics`], use [`Topic::for_node`].
    pub fn as_str(&self) -> &'static str {
        match self {
            Topic::Heartbeat => "orion.heartbeat",
            Topic::Capabilities => "orion.capabilities",
            Topic::ServiceRegister => "orion.service.register",
            Topic::ServiceUnregister => "orion.service.unregister",
            Topic::TaskSubmit => "orion.task.submit",
            Topic::TaskEvents => "orion.task.events",
            Topic::Logs => "orion.logs.*",
            Topic::Metrics => "orion.metrics.*",
        }
    }

    /// Concrete subject for a per-node topic.
    pub fn for_node(&self, node_id: &str) -> String {
        match self {
            Topic::Logs => format!("orion.logs.{node_id}"),
            Topic::Metrics => format!("orion.metrics.{node_id}"),
            _ => self.as_str().to_owned(),
        }
    }

    /// Whether this topic requires JetStream durability.
    pub const fn requires_jetstream(&self) -> bool {
        matches!(
            self,
            Topic::ServiceRegister
                | Topic::ServiceUnregister
                | Topic::TaskSubmit
                | Topic::TaskEvents,
        )
    }
}
