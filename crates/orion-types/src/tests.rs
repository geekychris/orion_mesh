//! Comprehensive serde + validation tests for the resource model.
//!
//! Every Resource kind has a roundtrip test (parse YAML → re-emit → parse again
//! and assert structural equality). Edge cases for apiVersion default, status
//! optionality, capability attribute matching, and Schedule semantic validation
//! get dedicated tests.

use crate::*;
use serde_json::json;

fn parse(yaml: &str) -> Resource {
    Resource::from_yaml(yaml).unwrap_or_else(|e| panic!("parse failed: {e}\n---\n{yaml}"))
}

fn roundtrip(yaml: &str) -> Resource {
    let first = parse(yaml);
    let re_emitted = first.to_yaml().expect("to_yaml");
    let second = Resource::from_yaml(&re_emitted)
        .unwrap_or_else(|e| panic!("re-parse failed: {e}\n---\n{re_emitted}"));
    assert_eq!(first, second, "roundtrip changed semantics");
    first
}

// ============================================================================
// Top-level: apiVersion + kind + metadata + status
// ============================================================================

#[test]
fn api_version_defaults_to_v1_when_missing() {
    let r = parse(
        r#"
kind: Volume
metadata: { name: scratch }
spec: { path: /mnt/scratch }
"#,
    );
    assert_eq!(r.api_version, "orionmesh.dev/v1");
}

#[test]
fn api_version_round_trips_explicitly() {
    let r = parse(
        r#"
apiVersion: orionmesh.dev/v1
kind: Volume
metadata: { name: scratch }
spec: { path: /mnt/scratch }
"#,
    );
    let out = r.to_yaml().unwrap();
    assert!(out.contains("apiVersion"), "apiVersion missing from output:\n{out}");
}

#[test]
fn status_is_optional() {
    let r = parse(
        r#"
kind: Service
metadata: { name: svc }
spec: { runtime: { kind: native, exec: /bin/true } }
"#,
    );
    match r.body {
        ResourceBody::Service { status, .. } => assert!(status.is_none()),
        _ => unreachable!(),
    }
}

#[test]
fn status_roundtrips_with_phase_and_conditions() {
    let yaml = r#"
kind: Service
metadata: { name: svc, generation: 5 }
spec: { runtime: { kind: native, exec: /bin/true } }
status:
  phase: Running
  observed_generation: 5
  conditions:
    - type: Available
      status: "True"
      last_transition: "2026-06-25T12:00:00Z"
      reason: ServiceRegistered
"#;
    let r = roundtrip(yaml);
    match r.body {
        ResourceBody::Service { status: Some(s), .. } => {
            assert_eq!(s.phase, Phase::Running);
            assert_eq!(s.observed_generation, Some(5));
            assert_eq!(s.conditions.len(), 1);
            assert_eq!(s.conditions[0].type_, "Available");
            assert_eq!(s.conditions[0].status, ConditionStatus::True);
        }
        _ => unreachable!(),
    }
}

// ============================================================================
// Service — the canonical example
// ============================================================================

#[test]
fn service_amiga_search_roundtrip() {
    let yaml = r#"
apiVersion: orionmesh.dev/v1
kind: Service
metadata:
  name: amiga-search
  labels: { site: belmont }
spec:
  runtime:
    kind: docker
    image: amiga-search:latest
  replicas: 1
  placement:
    arch: [arm64, x86_64]
    os: [linux]
  requires:
    search:
      dataset: amiga_schematics
  capabilities:
    - name: search
      attributes:
        dataset: amiga_schematics
        protocol: http
"#;
    let r = roundtrip(yaml);
    assert_eq!(r.name(), "amiga-search");
    let ResourceBody::Service { spec, .. } = r.body else { panic!() };
    assert_eq!(spec.replicas, Some(1));
    assert!(spec.placement.arch.contains(&Arch::Arm64));
    assert_eq!(spec.capabilities.len(), 1);
    assert_eq!(spec.capabilities[0].name, "search");
    assert_eq!(spec.capabilities[0].attributes["protocol"], "http");
}

#[test]
fn service_health_check_http() {
    let yaml = r#"
kind: Service
metadata: { name: svc }
spec:
  runtime: { kind: docker, image: nginx }
  health:
    kind: http
    path: /healthz
    port: 8080
    interval_seconds: 5
    failure_threshold: 2
  restart_policy: on_failure
  ports:
    - { name: http, port: 8080 }
    - { name: metrics, port: 9090, protocol: tcp }
"#;
    let r = roundtrip(yaml);
    let ResourceBody::Service { spec, .. } = r.body else { panic!() };
    assert!(matches!(
        spec.health,
        Some(HealthCheck::Http { ref path, port: 8080, interval_seconds: 5, failure_threshold: 2 })
            if path == "/healthz"
    ));
    assert_eq!(spec.restart_policy, RestartPolicy::OnFailure);
    assert_eq!(spec.ports.len(), 2);
    assert_eq!(spec.ports[0].name, "http");
}

// ============================================================================
// Task
// ============================================================================

#[test]
fn task_with_retry_and_data_locality() {
    let yaml = r#"
kind: Task
metadata: { name: train }
spec:
  runtime: { kind: python, module: train }
  prefer_data_locality: true
  retry: { max_attempts: 3, backoff_seconds: 30 }
"#;
    let r = roundtrip(yaml);
    let ResourceBody::Task { spec, .. } = r.body else { panic!() };
    assert!(spec.prefer_data_locality);
    assert_eq!(spec.retry.unwrap().max_attempts, 3);
}

// ============================================================================
// Schedule — inline vs reference; semantic validation
// ============================================================================

#[test]
fn schedule_with_task_reference_validates() {
    let yaml = r#"
kind: Schedule
metadata: { name: nightly }
spec: { cron: "0 2 * * *", task: train }
"#;
    let r = roundtrip(yaml);
    assert!(r.validate().is_ok());
}

#[test]
fn schedule_with_inline_template_validates() {
    let yaml = r#"
kind: Schedule
metadata: { name: nightly }
spec:
  cron: "0 2 * * *"
  task_template:
    runtime: { kind: native, exec: /usr/local/bin/snapshot }
"#;
    let r = roundtrip(yaml);
    assert!(r.validate().is_ok());
}

#[test]
fn schedule_with_both_fails_validation() {
    let r = parse(
        r#"
kind: Schedule
metadata: { name: nightly }
spec:
  cron: "0 2 * * *"
  task: train
  task_template:
    runtime: { kind: native, exec: /bin/true }
"#,
    );
    assert!(matches!(r.validate(), Err(ResourceError::ScheduleAmbiguous)));
}

#[test]
fn schedule_with_neither_fails_validation() {
    let r = parse(
        r#"
kind: Schedule
metadata: { name: empty }
spec: { cron: "0 0 * * *" }
"#,
    );
    assert!(matches!(r.validate(), Err(ResourceError::ScheduleAmbiguous)));
}

// ============================================================================
// Dataset — locations, formats, capabilities
// ============================================================================

#[test]
fn dataset_with_locations_roundtrips() {
    let yaml = r#"
kind: Dataset
metadata: { name: amiga-schematics }
spec:
  locations:
    - { node: pi5, path: /data/amiga, access: ro }
    - { node: mac-studio, path: /Volumes/data/amiga, access: rw }
  formats: [pdf, png]
  capabilities: [search]
  size_bytes: 12345678
"#;
    let r = roundtrip(yaml);
    let ResourceBody::Dataset { spec, .. } = r.body else { panic!() };
    assert_eq!(spec.locations.len(), 2);
    assert_eq!(spec.locations[0].access, DatasetAccess::Ro);
    assert_eq!(spec.locations[1].access, DatasetAccess::Rw);
    assert_eq!(spec.formats, vec!["pdf", "png"]);
}

// ============================================================================
// Model — variants with quant/memory/context
// ============================================================================

#[test]
fn model_with_variants_roundtrips() {
    let yaml = r#"
kind: Model
metadata: { name: qwen-coder }
spec:
  model_id: qwen2.5-coder-32b
  variants:
    - { format: gguf, quant: q4_k_m, memory_gb: 22.0, context_window: 32768, preferred_runtime: "llama.cpp" }
    - { format: mlx,  quant: int8,   memory_gb: 36.0, context_window: 32768, preferred_runtime: mlx }
  served_by: [mac-studio]
"#;
    let r = roundtrip(yaml);
    let ResourceBody::Model { spec, .. } = r.body else { panic!() };
    assert_eq!(spec.variants.len(), 2);
    assert_eq!(spec.variants[0].quant.as_deref(), Some("q4_k_m"));
    assert_eq!(spec.variants[1].memory_gb, Some(36.0));
}

// ============================================================================
// Node — roles, gpus, runtimes
// ============================================================================

#[test]
fn node_with_gpus_and_roles_roundtrips() {
    let yaml = r#"
kind: Node
metadata: { name: gpu-rig }
spec:
  node_id: gpu-rig
  roles: [worker, llm]
  arch: x86_64
  os: linux
  gpus:
    - { vendor: nvidia, vram_gb: 24, name: "RTX 4090" }
  acceleration: cuda
  resources: { cpu_cores: 16, memory_gb: 64, disk_gb: 2000 }
  runtimes: [native, docker, python, llm]
  labels: { site: belmont, power: mains }
"#;
    let r = roundtrip(yaml);
    let ResourceBody::Node { spec, .. } = r.body else { panic!() };
    assert_eq!(spec.roles, vec![NodeRole::Worker, NodeRole::Llm]);
    assert_eq!(spec.gpus.len(), 1);
    assert_eq!(spec.gpus[0].vendor, GpuVendor::Nvidia);
    assert_eq!(spec.gpus[0].vram_gb, 24);
    assert_eq!(spec.runtimes.len(), 4);
}

// ============================================================================
// New kinds: Job, Runtime, Capability, Policy, Integration
// ============================================================================

#[test]
fn job_roundtrips() {
    let yaml = r#"
kind: Job
metadata: { name: train-123 }
spec:
  task: train
  node: pi5
  started_at: "2026-06-25T01:00:00Z"
  finished_at: "2026-06-25T01:42:00Z"
  exit_code: 0
  attempt: 1
"#;
    let r = roundtrip(yaml);
    let ResourceBody::Job { spec, .. } = r.body else { panic!() };
    assert_eq!(spec.exit_code, Some(0));
}

#[test]
fn runtime_peer_catalog_entry_roundtrips() {
    let yaml = r#"
kind: Runtime
metadata: { name: orionmesh-belmont }
spec:
  runtime_kind: orionmesh
  base_url: "http://controller.belmont.local:7878"
  admin_ui_url: "http://controller.belmont.local:7879"
  config:
    natsUrl: "nats://nats.belmont.local:4222"
"#;
    let r = roundtrip(yaml);
    let ResourceBody::Runtime { spec, .. } = r.body else { panic!() };
    assert_eq!(spec.runtime_kind, "orionmesh");
    assert_eq!(spec.config["natsUrl"], "nats://nats.belmont.local:4222");
}

#[test]
fn capability_schema_resource_roundtrips() {
    let yaml = r#"
kind: Capability
metadata: { name: search }
spec:
  capability: search
  description: "Full-text or vector lookup over a dataset"
  attribute_schema:
    type: object
    properties:
      dataset: { type: string }
      protocol: { type: string, enum: [http, grpc] }
"#;
    let r = roundtrip(yaml);
    let ResourceBody::Capability { spec, .. } = r.body else { panic!() };
    assert_eq!(spec.capability, "search");
}

#[test]
fn policy_stub_roundtrips() {
    let yaml = r#"
kind: Policy
metadata: { name: gpu-quota }
spec:
  policy_kind: quota
  rules:
    gpu_hours_per_day: 12
"#;
    let r = roundtrip(yaml);
    let ResourceBody::Policy { spec, .. } = r.body else { panic!() };
    assert_eq!(spec.policy_kind, "quota");
}

#[test]
fn integration_stub_roundtrips() {
    let yaml = r#"
kind: Integration
metadata: { name: ha }
spec:
  integration_kind: home-assistant
  config:
    base_url: "http://ha.belmont.local:8123"
"#;
    let r = roundtrip(yaml);
    let ResourceBody::Integration { spec, .. } = r.body else { panic!() };
    assert_eq!(spec.integration_kind, "home-assistant");
}

// ============================================================================
// Capability + Selector — the matching layer
// ============================================================================

#[test]
fn capability_attributes_can_be_nested_json() {
    let cap = Capability::with_attributes(
        "llm",
        json!({ "model": "qwen-coder", "gpu": { "vendor": "nvidia", "min_vram_gb": 24 } }),
    );
    let s = serde_json::to_string(&cap).unwrap();
    let cap2: Capability = serde_json::from_str(&s).unwrap();
    assert_eq!(cap, cap2);
    assert_eq!(cap2.attributes["gpu"]["min_vram_gb"], 24);
}

#[test]
fn selector_supports_equals_oneof_and_op_forms() {
    let yaml = r#"
search:
  dataset: amiga_schematics      # Equals (bare value)
  format: [pdf, png]             # OneOf
llm:
  gpu:
    min_vram_gb: { gte: 24 }     # Op
"#;
    let sel: CapabilitySelector = serde_yml::from_str(&format!("requirements:\n  {yaml}"))
        .or_else(|_| {
            // The transparent wrapper means the YAML *is* the inner map.
            serde_yml::from_str(yaml)
        })
        .unwrap();
    let search = sel.requirements.get("search").unwrap();
    let dataset = search.0.get("dataset").unwrap();
    assert!(matches!(dataset, AttrMatch::Equals(_)));
    let format = search.0.get("format").unwrap();
    assert!(matches!(format, AttrMatch::OneOf(_)));
    let llm = sel.requirements.get("llm").unwrap();
    let gpu = llm.0.get("gpu").unwrap();
    // Nested object -> Equals(serde_json::Value::Object). Op form is one level deeper.
    assert!(matches!(gpu, AttrMatch::Equals(_)));
}

// ============================================================================
// Placement — Gpu split: NodeGpu vs GpuRequirement
// ============================================================================

#[test]
fn gpu_requirement_is_distinct_from_node_gpu() {
    let req = GpuRequirement {
        vendor: Some(GpuVendor::Nvidia),
        min_vram_gb: Some(24),
    };
    let node_gpu = NodeGpu {
        vendor: GpuVendor::Nvidia,
        vram_gb: 24,
        name: Some("RTX 4090".into()),
    };
    // No `From` impls between them — they are intentionally different types.
    // The check is by-construction: this test compiles only if the types exist.
    let _ = (req, node_gpu);
}

#[test]
fn placement_with_prefer_block_roundtrips() {
    let yaml = r#"
kind: Task
metadata: { name: t }
spec:
  placement:
    arch: [arm64]
    os: [linux]
    gpu: { vendor: nvidia, min_vram_gb: 24 }
    acceleration: cuda
    node_labels: { site: belmont }
    prefer:
      node_labels: { power: mains }
      data_locality: true
"#;
    let r = roundtrip(yaml);
    let ResourceBody::Task { spec, .. } = r.body else { panic!() };
    assert_eq!(spec.placement.gpu.as_ref().unwrap().min_vram_gb, Some(24));
    assert!(spec.placement.prefer.data_locality);
    assert_eq!(spec.placement.prefer.node_labels.get("power"), Some(&"mains".to_owned()));
}

// ============================================================================
// JSON wire form — same as YAML, exercised separately
// ============================================================================

#[test]
fn json_round_trips_identically() {
    let yaml = r#"
kind: Volume
metadata: { name: scratch, labels: { tier: hot } }
spec: { path: /mnt/scratch, size_gb: 500 }
"#;
    let from_yaml = parse(yaml);
    let as_json = from_yaml.to_json().unwrap();
    let from_json = Resource::from_json(&as_json).unwrap();
    assert_eq!(from_yaml, from_json);
}
