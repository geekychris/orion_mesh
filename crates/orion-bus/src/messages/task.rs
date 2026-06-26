use orion_types::{NodeId, ResourceName, Runtime};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Controller asks a specific node to execute a task.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskSubmit {
    pub task_id: Uuid,
    pub task: ResourceName,
    pub assigned_to: NodeId,
    pub runtime: Runtime,
    /// Deadline by which the task must start; absent means no deadline.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deadline_seconds: Option<u64>,
}

/// Lifecycle event for an in-flight task.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskEvent {
    pub task_id: Uuid,
    pub node_id: NodeId,
    pub outcome: TaskOutcome,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TaskOutcome {
    Accepted,
    Started,
    Progress { percent: u8 },
    Succeeded { exit_code: i32 },
    Failed { exit_code: i32, message: String },
    Cancelled { reason: String },
}
