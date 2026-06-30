//! Per-kind `spec:` types referenced by [`crate::ResourceBody`].
//!
//! Each variant of `ResourceBody` carries one of these. Keeping them in a single
//! module instead of one-file-per-kind avoids twelve modules with five lines each.

use crate::{
    capability::{Capability, CapabilitySelector},
    metadata::{NodeId, ResourceName},
    placement::{Acceleration, Arch, NodeGpu, OperatingSystem, Placement},
    runtime::Runtime,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// ============================================================================
// Node
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct NodeSpec {
    pub node_id: NodeId,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub roles: Vec<NodeRole>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arch: Option<Arch>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub os: Option<OperatingSystem>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub gpus: Vec<NodeGpu>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acceleration: Option<Acceleration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<NodeResources>,
    /// Runtime adapters this node can host (`docker`, `python`, `llm`, ...).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub runtimes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    Controller,
    Worker,
    Workstation,
    Edge,
    Llm,
    Storage,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct NodeResources {
    pub cpu_cores: u32,
    pub memory_gb: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_gb: Option<u32>,
}

// ============================================================================
// Service
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ServiceSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime: Option<Runtime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replicas: Option<u32>,
    #[serde(skip_serializing_if = "Placement_is_default")]
    pub placement: Placement,
    #[serde(skip_serializing_if = "Selector_is_default")]
    pub requires: CapabilitySelector,
    /// What this service advertises once running.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<Capability>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<PortSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health: Option<HealthCheck>,
    pub restart_policy: RestartPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortSpec {
    pub name: String,
    pub port: u16,
    #[serde(default)]
    pub protocol: PortProtocol,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PortProtocol {
    #[default]
    Tcp,
    Udp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum HealthCheck {
    Http {
        path: String,
        port: u16,
        #[serde(default = "default_health_interval")]
        interval_seconds: u32,
        #[serde(default = "default_failure_threshold")]
        failure_threshold: u32,
    },
    Tcp {
        port: u16,
        #[serde(default = "default_health_interval")]
        interval_seconds: u32,
        #[serde(default = "default_failure_threshold")]
        failure_threshold: u32,
    },
    Exec {
        command: Vec<String>,
        #[serde(default = "default_health_interval")]
        interval_seconds: u32,
        #[serde(default = "default_failure_threshold")]
        failure_threshold: u32,
    },
}

fn default_health_interval() -> u32 {
    10
}
fn default_failure_threshold() -> u32 {
    3
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestartPolicy {
    #[default]
    Always,
    OnFailure,
    Never,
}

// ============================================================================
// Task
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TaskSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime: Option<Runtime>,
    #[serde(skip_serializing_if = "Placement_is_default")]
    pub placement: Placement,
    #[serde(skip_serializing_if = "Selector_is_default")]
    pub requires: CapabilitySelector,
    /// Lift dataset locality from the optimization layer into the spec.
    /// When true, the scheduler scores nodes that hold a referenced Dataset higher.
    pub prefer_data_locality: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryPolicy>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff_seconds: Option<u32>,
}

// ============================================================================
// Schedule
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ScheduleSpec {
    /// 5-field cron expression.
    pub cron: String,
    /// Reference to an existing Task resource by name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<ResourceName>,
    /// Inline task definition. Mutually exclusive with `task`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_template: Option<TaskSpec>,
}

// ============================================================================
// Dataset
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct DatasetSpec {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub locations: Vec<DatasetLocation>,
    /// File formats present (`parquet`, `jsonl`, `arrow`, ...).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub formats: Vec<String>,
    /// Capability names this dataset enables (`search`, `embeddings`, ...).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetLocation {
    pub node: NodeId,
    pub path: String,
    #[serde(default)]
    pub access: DatasetAccess,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DatasetAccess {
    #[default]
    Ro,
    Rw,
    Wo,
}

// ============================================================================
// Model
// ============================================================================

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ModelSpec {
    pub model_id: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub variants: Vec<ModelVariant>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub served_by: Vec<NodeId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelVariant {
    /// `gguf`, `safetensors`, `onnx`, `mlx`, ...
    pub format: String,
    /// `q4_k_m`, `fp16`, `int8`, ...
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quant: Option<String>,
    /// Approx memory needed to serve this variant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_gb: Option<f32>,
    /// Max context window in tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window: Option<u32>,
    /// Preferred runtime adapter (`ollama`, `llama.cpp`, `vllm`, ...).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preferred_runtime: Option<String>,
}

// ============================================================================
// Project
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ProjectSpec {
    /// Dev Portal asset id, when registered.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub asset_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repo_url: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub languages: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub build: Option<ProjectBuild>,
    /// Runtime adapters that can run this project's services.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub runtimes: Vec<String>,
    /// Services exposed by this project. Each references a Service resource.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub services: Vec<ProjectService>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectBuild {
    /// `cargo`, `maven`, `gradle`, `pnpm`, `make`, ...
    #[serde(rename = "type")]
    pub type_: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectService {
    pub name: ResourceName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
}

// ============================================================================
// Secret
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct SecretSpec {
    /// Resolver URI: `plaintext://<path>`, `vaultrix://<key>`, `op://...`, `age://...`.
    /// Resolved at consumption time by an `orion_runtime::SecretResolver`.
    pub vault_ref: String,
}

// ============================================================================
// Volume
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct VolumeSpec {
    pub path: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub mounted_on: Vec<NodeId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_gb: Option<u32>,
}

// ============================================================================
// Queue — named NATS/JetStream queue with declared semantics
// ============================================================================

/// Delivery semantics for a [`QueueSpec`].
///
/// Both backends use the same JetStream stream; the difference is enforced on
/// the *subscriber* side: a `work` queue requires consumers to share a durable
/// name (so each message lands at exactly one consumer); a `topic` queue
/// forbids it (so every subscriber sees every message via its own consumer).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueueType {
    /// Broadcast — every subscriber receives every message.
    Topic,
    /// Work distribution — each message is delivered to exactly one of the
    /// subscribers sharing the consumer durable.
    #[default]
    Work,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct QueueSpec {
    #[serde(rename = "type")]
    pub queue_type: QueueType,
    /// Override the default `orion.queue.<name>` subject.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    /// Override the default `ORION_QUEUE_<NAME_UPPER>` stream name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<String>,
    /// Maximum age of a message before JetStream drops it. None = unlimited.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_age_seconds: Option<u64>,
    /// Maximum messages retained on the stream. None = unlimited.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_msgs: Option<u64>,
    /// Maximum bytes retained on the stream. None = unlimited.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
    /// JetStream stream replicas (durability factor). Default = 1.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replicas: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

// ============================================================================
// Workflow — DAG of Task references with depends_on edges
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct WorkflowSpec {
    /// Steps in the workflow. Each one names a Task resource that already
    /// exists; the workflow runner dispatches them in dependency order.
    pub steps: Vec<WorkflowStep>,
    /// When a step fails, should downstream steps run anyway? Default: false
    /// (fail-fast). Set to true for "best effort" workflows where each step
    /// is independent and you want max coverage.
    pub continue_on_error: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkflowStep {
    /// Step name — unique within the workflow.
    pub name: String,
    /// The Task resource to dispatch.
    pub task: ResourceName,
    /// Step names that must finish (successfully, unless continue_on_error)
    /// before this one starts. Empty = root step.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
}

// ============================================================================
// Network
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct NetworkSpec {
    pub cidr: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sites: Vec<String>,
}

// ============================================================================
// Job — historical record of a completed Task run
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobSpec {
    pub task: ResourceName,
    pub node: NodeId,
    pub started_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub attempt: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// ============================================================================
// Runtime resource — peer runtime catalog entry
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct RuntimeResourceSpec {
    /// `orionmesh`, `kqueue`, `devportal-local`, ...
    pub runtime_kind: String,
    pub base_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub admin_ui_url: Option<String>,
    /// Peer-specific config — `natsUrl` for OrionMesh, `jetstreamPrefix` for KQueue, etc.
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    pub config: serde_json::Value,
}

// ============================================================================
// Capability resource — declared capability schema
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct CapabilityResourceSpec {
    pub capability: String,
    /// JSON Schema (loose) describing the attributes this capability uses.
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    pub attribute_schema: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

// ============================================================================
// Policy — placeholder (plan section 7, vague)
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct PolicySpec {
    /// `placement`, `access`, `quota`, ...
    pub policy_kind: String,
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    pub rules: serde_json::Value,
}

// ============================================================================
// Integration — placeholder (plan section 7, vague)
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct IntegrationSpec {
    /// `home-assistant`, `telegram`, `mcp`, ...
    pub integration_kind: String,
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    pub config: serde_json::Value,
}

// ============================================================================
// helpers used by ServiceSpec / TaskSpec skip-if predicates
// ============================================================================

#[allow(non_snake_case)]
fn Placement_is_default(p: &Placement) -> bool {
    p == &Placement::default()
}

#[allow(non_snake_case)]
fn Selector_is_default(s: &CapabilitySelector) -> bool {
    s == &CapabilitySelector::default()
}
