//! NATS subject namespace — hybrid wide control plane + consolidated data plane.
//!
//! Decision (CLAUDE.md): wider control-side namespace (per-node control subjects,
//! split inventory from heartbeat, service.health) but consolidated data-plane
//! (one `task.events` carrying `TaskOutcome`, one `logs.<node>` carrying service in payload).
//!
//! Persistence tier:
//! - `core` — NATS Core pub/sub, ephemeral.
//! - `jetstream` — durable, at-least-once.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Topic {
    // ---------- data plane (consolidated) ----------
    /// `orion.heartbeat` — every agent every N seconds. Core.
    Heartbeat,
    /// `orion.node.inventory` — full hardware/runtime snapshot on connect + on change. Core.
    NodeInventory,
    /// `orion.capabilities` — capability advertisements. Core.
    Capabilities,
    /// `orion.service.register` — service comes online. JetStream.
    ServiceRegister,
    /// `orion.service.unregister` — service goes away cleanly. JetStream.
    ServiceUnregister,
    /// `orion.service.health` — periodic health updates per service instance. Core.
    ServiceHealth,
    /// `orion.task.events` — task lifecycle (TaskOutcome variants in payload). JetStream.
    TaskEvents,
    /// `orion.logs.{node_id}` — line-delimited log forwarding. Core.
    Logs,
    /// `orion.metrics.{node_id}` — periodic metric snapshots. Core.
    Metrics,

    // ---------- control plane (per-node, wide namespace) ----------
    /// `orion.control.{node_id}.run` — controller asks an agent to run a service/task.
    ControlRun,
    /// `orion.control.{node_id}.stop` — stop a running instance.
    ControlStop,
    /// `orion.control.{node_id}.restart` — restart a service instance.
    ControlRestart,
    /// `orion.control.{node_id}.drain` — graceful node drain; stop accepting new work.
    ControlDrain,
}

impl Topic {
    /// Returns the literal subject when fixed; a wildcard for per-node topics.
    /// Use [`Topic::for_node`] to build the concrete subject for those.
    pub fn as_str(&self) -> &'static str {
        match self {
            Topic::Heartbeat => "orion.heartbeat",
            Topic::NodeInventory => "orion.node.inventory",
            Topic::Capabilities => "orion.capabilities",
            Topic::ServiceRegister => "orion.service.register",
            Topic::ServiceUnregister => "orion.service.unregister",
            Topic::ServiceHealth => "orion.service.health",
            Topic::TaskEvents => "orion.task.events",
            Topic::Logs => "orion.logs.*",
            Topic::Metrics => "orion.metrics.*",
            Topic::ControlRun => "orion.control.*.run",
            Topic::ControlStop => "orion.control.*.stop",
            Topic::ControlRestart => "orion.control.*.restart",
            Topic::ControlDrain => "orion.control.*.drain",
        }
    }

    pub fn for_node(&self, node_id: &str) -> String {
        match self {
            Topic::Logs => format!("orion.logs.{node_id}"),
            Topic::Metrics => format!("orion.metrics.{node_id}"),
            Topic::ControlRun => format!("orion.control.{node_id}.run"),
            Topic::ControlStop => format!("orion.control.{node_id}.stop"),
            Topic::ControlRestart => format!("orion.control.{node_id}.restart"),
            Topic::ControlDrain => format!("orion.control.{node_id}.drain"),
            _ => self.as_str().to_owned(),
        }
    }

    /// All control subjects an agent should subscribe to (one wildcard per node).
    pub fn control_subjects_for_node(node_id: &str) -> [String; 4] {
        [
            Topic::ControlRun.for_node(node_id),
            Topic::ControlStop.for_node(node_id),
            Topic::ControlRestart.for_node(node_id),
            Topic::ControlDrain.for_node(node_id),
        ]
    }

    pub const fn requires_jetstream(&self) -> bool {
        matches!(
            self,
            Topic::ServiceRegister
                | Topic::ServiceUnregister
                | Topic::TaskEvents
                | Topic::ControlRun
                | Topic::ControlStop
                | Topic::ControlRestart
                | Topic::ControlDrain
        )
    }
}
