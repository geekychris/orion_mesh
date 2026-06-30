//! Serde + Topic tests for orion-bus.
//!
//! Goal: every envelope kind round-trips through JSON; subject helpers produce
//! valid NATS subjects; JetStream flag matches the persistence-tier decision.

use crate::*;
use chrono::Utc;
use orion_types::{Arch, Capability, NodeGpu, NodeId, OperatingSystem, ResourceName, Runtime};
use serde_json::json;
use uuid::Uuid;

fn nid() -> NodeId {
    NodeId("pi5".into())
}

fn rt() -> Runtime {
    Runtime::Native {
        exec: "/bin/true".into(),
        args: vec![],
        env: Default::default(),
    }
}

fn assert_roundtrips<P>(payload: P)
where
    P: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug + Clone,
{
    let env = Envelope::new(Some(nid()), payload.clone());
    let bytes = serde_json::to_vec(&env).expect("encode");
    let back: Envelope<P> = serde_json::from_slice(&bytes).expect("decode");
    assert_eq!(env.payload, back.payload);
    assert_eq!(env.protocol, PROTOCOL_VERSION);
    assert_eq!(back.source, Some(nid()));
}

// ----------------------------------------------------------------- topic helpers

#[test]
fn topic_strings_use_orion_prefix() {
    for t in [
        Topic::Heartbeat,
        Topic::NodeInventory,
        Topic::Capabilities,
        Topic::ServiceRegister,
        Topic::ServiceUnregister,
        Topic::ServiceHealth,
        Topic::TaskEvents,
        Topic::Logs,
        Topic::Metrics,
        Topic::ControlRun,
        Topic::ControlStop,
        Topic::ControlRestart,
        Topic::ControlDrain,
    ] {
        let s = t.as_str();
        assert!(s.starts_with("orion."), "{s}");
        // No empty segments or trailing dots.
        assert!(!s.contains(".."), "{s}");
        assert!(!s.ends_with('.'), "{s}");
    }
}

#[test]
fn per_node_subjects_substitute_node_id() {
    assert_eq!(Topic::Logs.for_node("pi5"), "orion.logs.pi5");
    assert_eq!(Topic::Metrics.for_node("pi5"), "orion.metrics.pi5");
    assert_eq!(Topic::ControlRun.for_node("pi5"), "orion.control.pi5.run");
    assert_eq!(Topic::ControlStop.for_node("pi5"), "orion.control.pi5.stop");
    assert_eq!(Topic::ControlRestart.for_node("pi5"), "orion.control.pi5.restart");
    assert_eq!(Topic::ControlDrain.for_node("pi5"), "orion.control.pi5.drain");
}

#[test]
fn fixed_topics_ignore_for_node() {
    // Heartbeat doesn't include node id in the subject — node id is in the payload.
    assert_eq!(Topic::Heartbeat.for_node("pi5"), Topic::Heartbeat.as_str());
}

#[test]
fn control_subjects_for_node_returns_all_four() {
    let subs = Topic::control_subjects_for_node("pi5");
    assert_eq!(subs.len(), 4);
    assert!(subs.contains(&"orion.control.pi5.run".to_owned()));
    assert!(subs.contains(&"orion.control.pi5.drain".to_owned()));
}

#[test]
fn jetstream_flag_matches_durability_decision() {
    // Durable: register/unregister + task events + every control subject.
    for t in [
        Topic::ServiceRegister,
        Topic::ServiceUnregister,
        Topic::TaskEvents,
        Topic::ControlRun,
        Topic::ControlStop,
        Topic::ControlRestart,
        Topic::ControlDrain,
    ] {
        assert!(t.requires_jetstream(), "{} should be JS", t.as_str());
    }
    // Ephemeral: heartbeat, inventory, capabilities, health, logs, metrics.
    for t in [
        Topic::Heartbeat,
        Topic::NodeInventory,
        Topic::Capabilities,
        Topic::ServiceHealth,
        Topic::Logs,
        Topic::Metrics,
    ] {
        assert!(!t.requires_jetstream(), "{} should be Core", t.as_str());
    }
}

// ----------------------------------------------------------------- payload roundtrips

#[test]
fn heartbeat_roundtrips() {
    assert_roundtrips(Heartbeat {
        node_id: nid(),
        agent_version: "0.1.0".into(),
        uptime_seconds: 42,
        cpu_load_1m: 0.3,
        mem_used_bytes: 1_000_000,
        mem_total_bytes: 8_000_000,
        labels: Default::default(),
    });
}

#[test]
fn node_inventory_roundtrips() {
    assert_roundtrips(NodeInventory {
        node_id: nid(),
        agent_version: "0.1.0".into(),
        arch: Arch::Arm64,
        os: OperatingSystem::Linux,
        acceleration: None,
        gpus: vec![NodeGpu {
            vendor: orion_types::GpuVendor::Apple,
            vram_gb: 96,
            name: Some("M2 Ultra".into()),
        }],
        cpu_cores: 12,
        mem_total_bytes: 64_000_000_000,
        disk_gb: Some(2000),
        runtimes: vec!["native".into(), "docker".into()],
        roles: vec![orion_types::NodeRole::Worker],
        labels: Default::default(),
        address: None,
    });
}

#[test]
fn capabilities_roundtrips_with_nested_attrs() {
    assert_roundtrips(Capabilities {
        node_id: nid(),
        service: ResourceName::from("llm"),
        capabilities: vec![Capability::with_attributes(
            "llm",
            json!({ "model": "qwen-coder", "gpu": { "vendor": "apple", "min_vram_gb": 24 } }),
        )],
    });
}

#[test]
fn service_register_and_unregister_roundtrip() {
    assert_roundtrips(ServiceRegister {
        node_id: nid(),
        service: ResourceName::from("amiga-search"),
        instance_id: "abc".into(),
        runtime: rt(),
        endpoints: vec!["http://pi5:8080".into()],
    });
    assert_roundtrips(ServiceUnregister {
        node_id: nid(),
        service: ResourceName::from("amiga-search"),
        instance_id: "abc".into(),
        reason: "shutdown".into(),
    });
}

#[test]
fn service_health_roundtrips() {
    assert_roundtrips(ServiceHealth {
        node_id: nid(),
        service: ResourceName::from("svc"),
        instance_id: "abc".into(),
        status: HealthStatus::Healthy,
        message: None,
        consecutive_failures: 0,
    });
}

#[test]
fn task_submit_and_event_roundtrip() {
    assert_roundtrips(TaskSubmit {
        task_id: Uuid::new_v4(),
        task: ResourceName::from("train"),
        assigned_to: nid(),
        runtime: rt(),
        deadline_seconds: Some(3600),
    });
    for outcome in [
        TaskOutcome::Accepted,
        TaskOutcome::Started,
        TaskOutcome::Progress { percent: 42 },
        TaskOutcome::Succeeded { exit_code: 0 },
        TaskOutcome::Failed {
            exit_code: 1,
            message: "oops".into(),
        },
        TaskOutcome::Cancelled {
            reason: "user".into(),
        },
    ] {
        assert_roundtrips(TaskEvent {
            task_id: Uuid::new_v4(),
            node_id: nid(),
            outcome,
        });
    }
}

#[test]
fn log_line_roundtrips() {
    assert_roundtrips(LogLine {
        node_id: nid(),
        service: ResourceName::from("svc"),
        instance_id: None,
        replica_index: 0,
        stream: LogStream::Stdout,
        line: "hello\n".into(),
    });
}

#[test]
fn metric_roundtrips_with_samples() {
    assert_roundtrips(Metric {
        node_id: nid(),
        samples: vec![MetricSample {
            name: "cpu.load_1m".into(),
            value: 0.5,
            service: None,
            labels: vec![("site".into(), "belmont".into())],
        }],
    });
}

#[test]
fn control_messages_roundtrip() {
    assert_roundtrips(ControlRun {
        instance_id: Uuid::new_v4(),
        kind: WorkloadKind::Service,
        name: ResourceName::from("svc"),
        runtime: rt(),
        generation: 3,
        replicas: 1,
    });
    assert_roundtrips(ControlStop {
        instance_id: Uuid::new_v4(),
        reason: Some("update".into()),
        grace_seconds: Some(10),
    });
    assert_roundtrips(ControlRestart {
        instance_id: Uuid::new_v4(),
    });
    assert_roundtrips(ControlDrain {
        reason: Some("reboot".into()),
    });
}

// ----------------------------------------------------------------- envelope properties

#[test]
fn envelope_carries_source_and_time() {
    let env = Envelope::new(Some(nid()), Heartbeat {
        node_id: nid(),
        agent_version: "0.1.0".into(),
        uptime_seconds: 0,
        cpu_load_1m: 0.0,
        mem_used_bytes: 0,
        mem_total_bytes: 0,
        labels: Default::default(),
    });
    assert!(env.at <= Utc::now());
    assert_eq!(env.source, Some(nid()));
    assert_eq!(env.protocol, PROTOCOL_VERSION);
}
