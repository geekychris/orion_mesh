//! OrionMesh node agent.
//!
//! Phase 2 scope: connect to NATS with cluster auth, publish a NodeInventory
//! on connect, publish slim Heartbeats on a ticker, subscribe to the per-node
//! control plane (Run/Stop/Restart/Drain), forward child stdout/stderr as
//! orion.logs.{node} envelopes.

use anyhow::{Context, Result};
use async_nats::Client;
use clap::Parser;
use futures::StreamExt;
use orion_auth::AuthMode;
use orion_bus::{
    ControlRun, ControlStop, Envelope, Heartbeat, HealthStatus, LogLine, LogStream, NodeInventory,
    ServiceHealth, TaskEvent, TaskOutcome, Topic, WorkloadKind,
};
use orion_runtime::{
    DockerAdapter, ExitNotice, ExitSink, LaunchSpec, LogSink, NativeAdapter, OutStream,
    RuntimeAdapter, RuntimeRegistry,
};
use orion_types::{Arch, HealthCheck, NodeId, OperatingSystem, ResourceName};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use sysinfo::System;
use tokio::sync::mpsc;
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "orion-agent", version, about = "OrionMesh node agent")]
struct Args {
    #[arg(long, env = "ORION_NATS_URL", default_value = "nats://127.0.0.1:4222")]
    nats_url: String,

    #[arg(long, env = "ORION_NODE_ID")]
    node_id: Option<String>,

    #[arg(long, default_value_t = 5)]
    heartbeat_interval: u64,

    /// Re-publish NodeInventory every N heartbeats. Defaults to ~30s at the
    /// default heartbeat interval. Keeps the controller current even after a
    /// controller restart (the inventory snapshot isn't durable on NATS Core).
    #[arg(long, default_value_t = 6)]
    inventory_every_n_heartbeats: u32,
}

/// Per-instance metadata tracked by the agent. Populated when the agent
/// receives a `ControlRun`; used to label outgoing `LogLine`s and to answer
/// /v1/instances queries from the controller.
#[derive(Clone)]
struct InstanceMeta {
    kind: WorkloadKind,
    name: ResourceName,
    replica_index: u32,
    started_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Default)]
struct InstanceRegistry {
    by_id: Mutex<HashMap<Uuid, InstanceMeta>>,
}

impl InstanceRegistry {
    fn record(&self, id: Uuid, meta: InstanceMeta) {
        self.by_id.lock().unwrap().insert(id, meta);
    }

    fn get(&self, id: &Uuid) -> Option<InstanceMeta> {
        self.by_id.lock().unwrap().get(id).cloned()
    }

    fn forget(&self, id: &Uuid) {
        self.by_id.lock().unwrap().remove(id);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let node_id = NodeId(
        args.node_id
            .clone()
            .unwrap_or_else(|| hostname().unwrap_or_else(|| "unknown-node".into())),
    );
    let auth = AuthMode::from_env().context("loading cluster auth")?;
    let started = std::time::Instant::now();

    info!(node_id = %node_id, nats_url = %args.nats_url, disabled = auth.is_disabled(), "orion-agent starting");

    let mut reg = RuntimeRegistry::new();
    reg.register(Arc::new(NativeAdapter::new()));
    let docker = Arc::new(DockerAdapter::new());
    if docker.available().await {
        reg.register(docker);
        info!("docker adapter advertised");
    } else {
        info!("docker adapter not advertised (daemon unreachable)");
    }
    let registry = Arc::new(reg);
    let instances = Arc::new(InstanceRegistry::default());

    let nats = orion_auth::nats::connect_options(&auth)
        .name("orion-agent")
        .connect(&args.nats_url)
        .await
        .context("connecting to NATS")?;
    info!("connected to NATS");

    // Exit forwarder: process exits come in via `exit_rx` and get published as
    // Envelope<TaskEvent> on orion.task.events. The controller's reconciler
    // subscribes there and applies restart_policy.
    let (exit_tx, mut exit_rx) = mpsc::unbounded_channel::<ExitNotice>();
    {
        let nats = nats.clone();
        let node_id = node_id.clone();
        let instances = instances.clone();
        tokio::spawn(async move {
            let subject = Topic::TaskEvents.as_str().to_owned();
            while let Some(notice) = exit_rx.recv().await {
                let meta = match instances.get(&notice.instance_id) {
                    Some(m) => m,
                    None => continue,
                };
                let outcome = match notice.exit_code {
                    Some(0) => TaskOutcome::Succeeded { exit_code: 0 },
                    Some(c) => TaskOutcome::Failed {
                        exit_code: c,
                        message: notice.message.clone(),
                    },
                    None => TaskOutcome::Failed {
                        exit_code: -1,
                        message: notice.message.clone(),
                    },
                };
                let payload = TaskEvent {
                    task_id: notice.instance_id,
                    node_id: node_id.clone(),
                    outcome,
                };
                instances.forget(&notice.instance_id);
                info!(
                    instance_id = %notice.instance_id,
                    name = %meta.name,
                    exit_code = ?notice.exit_code,
                    "instance exited",
                );
                let env = Envelope::new(Some(node_id.clone()), payload);
                match serde_json::to_vec(&env) {
                    Ok(bytes) => {
                        if let Err(e) = nats.publish(subject.clone(), bytes.into()).await {
                            warn!(error = ?e, "exit publish failed");
                        }
                    }
                    Err(e) => warn!(error = ?e, "exit encode failed"),
                }
            }
        });
    }

    // Log forwarder: per-process stdout/stderr lines come in via `log_rx` and
    // get published as Envelope<LogLine> on orion.logs.{node_id}.
    let (log_tx, mut log_rx) = mpsc::unbounded_channel::<(Uuid, OutStream, String)>();
    {
        let nats = nats.clone();
        let node_id = node_id.clone();
        let instances = instances.clone();
        tokio::spawn(async move {
            let subject = Topic::Logs.for_node(&node_id.0);
            while let Some((id, stream, line)) = log_rx.recv().await {
                let Some(meta) = instances.get(&id) else { continue };
                let payload = LogLine {
                    node_id: node_id.clone(),
                    service: meta.name,
                    instance_id: Some(id),
                    replica_index: meta.replica_index,
                    stream: match stream {
                        OutStream::Stdout => LogStream::Stdout,
                        OutStream::Stderr => LogStream::Stderr,
                    },
                    line,
                };
                let env = Envelope::new(Some(node_id.clone()), payload);
                match serde_json::to_vec(&env) {
                    Ok(bytes) => {
                        if let Err(e) = nats.publish(subject.clone(), bytes.into()).await {
                            warn!(error = ?e, "log publish failed");
                        }
                    }
                    Err(e) => warn!(error = ?e, "log encode failed"),
                }
            }
        });
    }

    let mut sys = System::new_all();
    sys.refresh_all();
    publish_inventory(&nats, &node_id, &sys, &registry).await;

    // Subscribe to per-node control subjects.
    for subject in Topic::control_subjects_for_node(&node_id.0) {
        let mut sub = nats.subscribe(subject.clone()).await?;
        let registry = registry.clone();
        let instances = instances.clone();
        let log_tx = log_tx.clone();
        let exit_tx = exit_tx.clone();
        let subject_for_log = subject.clone();
        let nats_clone = nats.clone();
        let node_id_clone = node_id.clone();
        tokio::spawn(async move {
            info!(subject = %subject_for_log, "subscribed to control subject");
            while let Some(msg) = sub.next().await {
                if let Err(e) = dispatch_control(
                    &subject_for_log,
                    &msg.payload,
                    &registry,
                    &instances,
                    &log_tx,
                    &exit_tx,
                    &nats_clone,
                    &node_id_clone,
                )
                .await
                {
                    warn!(subject = %subject_for_log, error = ?e, "control dispatch failed");
                }
            }
        });
    }

    let mut ticker = tokio::time::interval(Duration::from_secs(args.heartbeat_interval));
    let mut tick_count: u32 = 0;
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                sys.refresh_cpu_usage();
                sys.refresh_memory();
                tick_count = tick_count.wrapping_add(1);

                if args.inventory_every_n_heartbeats > 0
                    && tick_count.is_multiple_of(args.inventory_every_n_heartbeats)
                {
                    publish_inventory(&nats, &node_id, &sys, &registry).await;
                }

                let hb = Heartbeat {
                    node_id: node_id.clone(),
                    agent_version: env!("CARGO_PKG_VERSION").to_owned(),
                    uptime_seconds: started.elapsed().as_secs(),
                    cpu_load_1m: System::load_average().one as f32,
                    mem_used_bytes: sys.used_memory(),
                    mem_total_bytes: sys.total_memory(),
                    labels: BTreeMap::new(),
                };
                let env = Envelope::new(Some(node_id.clone()), hb);
                match serde_json::to_vec(&env) {
                    Ok(bytes) => {
                        if let Err(e) = nats.publish(Topic::Heartbeat.as_str().to_owned(), bytes.into()).await {
                            warn!(error = ?e, "heartbeat publish failed");
                        }
                    }
                    Err(e) => warn!(error = ?e, "heartbeat encode failed"),
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("ctrl-c received, shutting down");
                break;
            }
        }
    }

    Ok(())
}

async fn publish_inventory(
    nats: &Client,
    node_id: &NodeId,
    sys: &System,
    registry: &RuntimeRegistry,
) {
    let inventory = build_inventory(node_id, sys, registry);
    let env = Envelope::new(Some(node_id.clone()), inventory);
    match serde_json::to_vec(&env) {
        Ok(bytes) => {
            if let Err(e) = nats
                .publish(Topic::NodeInventory.as_str().to_owned(), bytes.into())
                .await
            {
                warn!(error = ?e, "inventory publish failed");
            }
        }
        Err(e) => warn!(error = ?e, "inventory encode failed"),
    }
}

fn hostname() -> Option<String> {
    std::env::var("HOSTNAME").ok().or_else(|| {
        std::process::Command::new("hostname")
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| s.trim().to_owned())
    })
}

fn detect_arch() -> Arch {
    if cfg!(target_arch = "aarch64") {
        Arch::Arm64
    } else {
        Arch::X86_64
    }
}

fn detect_os() -> OperatingSystem {
    if cfg!(target_os = "macos") {
        OperatingSystem::Macos
    } else {
        OperatingSystem::Linux
    }
}

fn build_inventory(
    node_id: &NodeId,
    sys: &System,
    registry: &RuntimeRegistry,
) -> NodeInventory {
    NodeInventory {
        node_id: node_id.clone(),
        agent_version: env!("CARGO_PKG_VERSION").to_owned(),
        arch: detect_arch(),
        os: detect_os(),
        acceleration: None,
        gpus: vec![],
        cpu_cores: sys.cpus().len() as u32,
        mem_total_bytes: sys.total_memory(),
        disk_gb: None,
        runtimes: registry.names(),
        roles: vec![],
        labels: BTreeMap::new(),
        address: None,
    }
}

async fn dispatch_control(
    subject: &str,
    payload: &[u8],
    registry: &RuntimeRegistry,
    instances: &Arc<InstanceRegistry>,
    log_tx: &LogSink,
    exit_tx: &ExitSink,
    nats: &async_nats::Client,
    node_id: &NodeId,
) -> anyhow::Result<()> {
    if subject.ends_with(".run") {
        let env: Envelope<ControlRun> = serde_json::from_slice(payload)?;
        let spec = env.payload;
        let replicas = spec.replicas.max(1);
        info!(?spec.kind, %spec.name, instance = %spec.instance_id, replicas, "control: run");

        let adapter_name = runtime_adapter_name(&spec.runtime);
        let adapter = registry.get(adapter_name).ok_or_else(|| {
            anyhow::anyhow!(
                "runtime kind '{adapter_name}' has no adapter registered on this agent. \
                 OrionMesh is native-first; only `kind: native` is implemented today \
                 (docker / python / java / node / spark / llm / homeassistant / wasm \
                 adapters are Phase 5+ on the roadmap — see CLAUDE.md). \
                 For now: wrap any binary as `runtime: {{ kind: native, exec: <path>, args: [...] }}`. \
                 Examples: examples/01-services/native-sleeper.yaml, \
                 examples/10-queues/service-yamls/processor-work-python.yaml \
                 (Python and Java *processes* launched via `kind: native exec: python|java`)."
            )
        })?;

        for idx in 0..replicas {
            // 0-th instance reuses the controller-supplied id; siblings get fresh ids
            // so per-instance tracking + per-line attribution stays unambiguous.
            let id = if idx == 0 { spec.instance_id } else { Uuid::new_v4() };
            instances.record(
                id,
                InstanceMeta {
                    kind: spec.kind,
                    name: spec.name.clone(),
                    replica_index: idx,
                    started_at: chrono::Utc::now(),
                },
            );
            // Each replica gets its own ORION_REPLICA_INDEX env var so the workload
            // can read it and join the right NATS queue group, pick a worker slot, etc.
            let mut runtime = spec.runtime.clone();
            inject_replica_env(&mut runtime, idx, replicas);
            adapter
                .launch(LaunchSpec {
                    instance_id: id,
                    name: spec.name.clone(),
                    runtime,
                    log_sink: Some(log_tx.clone()),
                    exit_sink: Some(exit_tx.clone()),
                })
                .await?;
            // If the Service declared a health probe, spawn a per-instance
            // probe loop. It runs until the instance is forgotten (after exit).
            if let Some(hc) = spec.health_check.clone() {
                let nats = nats.clone();
                let node_id = node_id.clone();
                let name = spec.name.clone();
                let instances = instances.clone();
                tokio::spawn(probe_loop(nats, node_id, name, id, hc, instances));
            }
        }
    } else if subject.ends_with(".stop") {
        let env: Envelope<ControlStop> = serde_json::from_slice(payload)?;
        let spec = env.payload;
        info!(instance_id = %spec.instance_id, "control: stop");
        for name in registry.names() {
            if let Some(a) = registry.get(&name) {
                let _ = a.stop(spec.instance_id).await;
            }
        }
        instances.forget(&spec.instance_id);
    }
    Ok(())
}

/// Adds ORION_REPLICA_INDEX + ORION_REPLICA_COUNT to the workload's env
/// (for runtimes that have an env map). Not all runtimes do — silently no-ops
/// for `peer`, `homeassistant`, etc.
/// Periodic health probe loop for a service instance. Probes on the
/// configured interval; publishes ServiceHealth to `orion.service.health`.
/// Exits when the instance is forgotten (i.e. after process exit).
async fn probe_loop(
    nats: async_nats::Client,
    node_id: NodeId,
    service: ResourceName,
    instance_id: Uuid,
    hc: HealthCheck,
    instances: Arc<InstanceRegistry>,
) {
    let (interval, threshold) = match &hc {
        HealthCheck::Http { interval_seconds, failure_threshold, .. }
        | HealthCheck::Tcp { interval_seconds, failure_threshold, .. }
        | HealthCheck::Exec { interval_seconds, failure_threshold, .. } => {
            (*interval_seconds as u64, *failure_threshold)
        }
    };
    let subject = Topic::ServiceHealth.as_str().to_owned();
    let mut ticker = tokio::time::interval(Duration::from_secs(interval.max(1)));
    let mut consecutive_failures: u32 = 0;
    // Give the workload a beat to start listening before the first probe.
    tokio::time::sleep(Duration::from_secs(interval.min(2))).await;
    loop {
        ticker.tick().await;
        if instances.get(&instance_id).is_none() {
            // Instance has exited and been forgotten — stop probing.
            return;
        }
        let (status, message) = match run_probe(&hc).await {
            Ok(()) => {
                consecutive_failures = 0;
                (HealthStatus::Healthy, None)
            }
            Err(e) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                let msg = format!("{e}");
                if consecutive_failures >= threshold {
                    (HealthStatus::Unhealthy, Some(msg))
                } else {
                    (HealthStatus::Unknown, Some(msg))
                }
            }
        };
        let payload = ServiceHealth {
            node_id: node_id.clone(),
            service: service.clone(),
            instance_id: instance_id.to_string(),
            status,
            message,
            consecutive_failures,
        };
        let env = Envelope::new(Some(node_id.clone()), payload);
        if let Ok(bytes) = serde_json::to_vec(&env) {
            let _ = nats.publish(subject.clone(), bytes.into()).await;
        }
    }
}

async fn run_probe(hc: &HealthCheck) -> Result<()> {
    match hc {
        HealthCheck::Http { path, port, .. } => {
            let url = format!("http://127.0.0.1:{port}{path}");
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(3))
                .build()?;
            let resp = client.get(&url).send().await?;
            if !resp.status().is_success() {
                anyhow::bail!("http {url} → {}", resp.status());
            }
            Ok(())
        }
        HealthCheck::Tcp { port, .. } => {
            let _stream = tokio::time::timeout(
                Duration::from_secs(3),
                tokio::net::TcpStream::connect(("127.0.0.1", *port)),
            )
            .await??;
            Ok(())
        }
        HealthCheck::Exec { command, .. } => {
            if command.is_empty() {
                anyhow::bail!("exec health check has empty command");
            }
            let mut cmd = tokio::process::Command::new(&command[0]);
            cmd.args(&command[1..]);
            cmd.stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null());
            let status =
                tokio::time::timeout(Duration::from_secs(5), cmd.status()).await??;
            if !status.success() {
                anyhow::bail!("exec exit {status:?}");
            }
            Ok(())
        }
    }
}

fn inject_replica_env(rt: &mut orion_types::Runtime, idx: u32, count: u32) {
    use orion_types::Runtime;
    let envs = match rt {
        Runtime::Native { env, .. } => env,
        Runtime::Docker { env, .. } => env,
        _ => return,
    };
    envs.insert("ORION_REPLICA_INDEX".into(), idx.to_string());
    envs.insert("ORION_REPLICA_COUNT".into(), count.to_string());
}

fn runtime_adapter_name(r: &orion_types::Runtime) -> &'static str {
    use orion_types::Runtime;
    match r {
        Runtime::Native { .. } => "native",
        Runtime::Docker { .. } => "docker",
        Runtime::Python { .. } => "python",
        Runtime::Java { .. } => "java",
        Runtime::Node { .. } => "node",
        Runtime::Spark { .. } => "spark",
        Runtime::Llm { .. } => "llm",
        Runtime::HomeAssistant { .. } => "homeassistant",
        Runtime::Wasm { .. } => "wasm",
        Runtime::Peer { .. } => "peer",
    }
}
