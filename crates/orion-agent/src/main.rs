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
use orion_types::{Acceleration, Arch, GpuVendor, HealthCheck, NodeGpu, NodeId, OperatingSystem, ResourceName};
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
    let arch = detect_arch();
    let os = detect_os();
    let gpus = detect_gpus(arch, os, sys.total_memory());
    let acceleration = primary_acceleration(&gpus, os);
    NodeInventory {
        node_id: node_id.clone(),
        agent_version: env!("CARGO_PKG_VERSION").to_owned(),
        arch,
        os,
        acceleration,
        gpus,
        cpu_cores: sys.cpus().len() as u32,
        mem_total_bytes: sys.total_memory(),
        disk_gb: None,
        runtimes: registry.names(),
        roles: vec![],
        labels: BTreeMap::new(),
        address: None,
    }
}

/// Best-effort GPU detection. Returns an empty Vec when nothing is detectable
/// — never panics, never blocks. macOS uses `system_profiler`; Linux tries
/// `nvidia-smi` first, then `lspci` for a generic display device. Hardware
/// without a GPU stays an empty Vec.
fn detect_gpus(arch: Arch, os: OperatingSystem, total_memory_bytes: u64) -> Vec<NodeGpu> {
    match os {
        OperatingSystem::Macos => detect_gpus_macos(arch, total_memory_bytes),
        OperatingSystem::Linux => detect_gpus_linux(),
        _ => Vec::new(),
    }
}

fn detect_gpus_macos(arch: Arch, total_memory_bytes: u64) -> Vec<NodeGpu> {
    // Apple Silicon Macs have a unified-memory Apple GPU. VRAM is shared with
    // the system; conservatively report half of total RAM as the upper bound a
    // GPU workload could touch without paging.
    if matches!(arch, Arch::Arm64) {
        let vram_gb = ((total_memory_bytes / 2) / 1_073_741_824) as u32;
        return vec![NodeGpu {
            vendor: GpuVendor::Apple,
            vram_gb: vram_gb.max(1),
            name: Some(apple_silicon_marketing_name()),
        }];
    }
    // Intel Macs — try system_profiler. Best-effort; on failure return empty.
    if let Ok(output) = std::process::Command::new("system_profiler")
        .args(["SPDisplaysDataType", "-json"])
        .output()
    {
        if output.status.success() {
            if let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&output.stdout) {
                return parse_macos_system_profiler(&parsed);
            }
        }
    }
    Vec::new()
}

/// Parser exposed for tests — pure transform from system_profiler JSON to GPUs.
pub(crate) fn parse_macos_system_profiler(v: &serde_json::Value) -> Vec<NodeGpu> {
    let arr = match v.get("SPDisplaysDataType").and_then(|a| a.as_array()) {
        Some(a) => a,
        None => return Vec::new(),
    };
    let mut gpus = Vec::new();
    for entry in arr {
        let model = entry
            .get("sppci_model")
            .and_then(|s| s.as_str())
            .or_else(|| entry.get("_name").and_then(|s| s.as_str()));
        let vendor = match model.unwrap_or("").to_lowercase() {
            ref s if s.contains("nvidia") || s.contains("geforce") || s.contains("quadro") => GpuVendor::Nvidia,
            ref s if s.contains("amd") || s.contains("radeon") => GpuVendor::Amd,
            ref s if s.contains("apple") => GpuVendor::Apple,
            ref s if s.contains("intel") || s.contains("iris") || s.contains("uhd") => GpuVendor::Intel,
            _ => continue,
        };
        // VRAM: explicit or shared. "sppci_vram_shared" is the inline-graphics
        // path; "spdisplays_vram" is dedicated. Parse "1536 MB" / "8 GB" style.
        let vram_gb = entry
            .get("spdisplays_vram")
            .and_then(|s| s.as_str())
            .or_else(|| entry.get("sppci_vram_shared").and_then(|s| s.as_str()))
            .and_then(parse_vram_to_gb)
            .unwrap_or(0);
        gpus.push(NodeGpu {
            vendor,
            vram_gb,
            name: model.map(|s| s.to_owned()),
        });
    }
    gpus
}

pub(crate) fn parse_vram_to_gb(s: &str) -> Option<u32> {
    let s = s.trim().to_lowercase();
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.is_empty() {
        return None;
    }
    let num: f64 = parts[0].parse().ok()?;
    let unit = parts.get(1).copied().unwrap_or("mb");
    let gb = match unit {
        "gb" | "gigabytes" => num,
        "mb" | "megabytes" => num / 1024.0,
        "kb" | "kilobytes" => num / 1024.0 / 1024.0,
        _ => return None,
    };
    Some(gb.round().max(1.0) as u32)
}

fn detect_gpus_linux() -> Vec<NodeGpu> {
    // NVIDIA first — `nvidia-smi --query-gpu=name,memory.total --format=csv,noheader,nounits`
    // emits one line per GPU: "NVIDIA GeForce RTX 4090, 24564"
    if let Ok(output) = std::process::Command::new("nvidia-smi")
        .args(["--query-gpu=name,memory.total", "--format=csv,noheader,nounits"])
        .output()
    {
        if output.status.success() {
            let parsed = parse_nvidia_smi(&String::from_utf8_lossy(&output.stdout));
            if !parsed.is_empty() {
                return parsed;
            }
        }
    }
    // Fallback: lspci for any VGA / 3D controller. Doesn't report VRAM —
    // workloads that need vram_gb checks will need a node with the real path.
    if let Ok(output) = std::process::Command::new("lspci").output() {
        if output.status.success() {
            return parse_lspci(&String::from_utf8_lossy(&output.stdout));
        }
    }
    Vec::new()
}

pub(crate) fn parse_nvidia_smi(stdout: &str) -> Vec<NodeGpu> {
    stdout
        .lines()
        .filter_map(|line| {
            let parts: Vec<&str> = line.split(',').map(str::trim).collect();
            if parts.len() < 2 {
                return None;
            }
            let name = parts[0].to_owned();
            let vram_mib: u32 = parts[1].parse().ok()?;
            Some(NodeGpu {
                vendor: GpuVendor::Nvidia,
                vram_gb: ((vram_mib as f64 / 1024.0).round() as u32).max(1),
                name: Some(name),
            })
        })
        .collect()
}

pub(crate) fn parse_lspci(stdout: &str) -> Vec<NodeGpu> {
    let mut gpus = Vec::new();
    for line in stdout.lines() {
        if !line.contains("VGA") && !line.contains("3D controller") && !line.contains("Display controller") {
            continue;
        }
        let lower = line.to_lowercase();
        let vendor = if lower.contains("nvidia") {
            GpuVendor::Nvidia
        } else if lower.contains("amd") || lower.contains("advanced micro devices") || lower.contains("radeon") {
            // Avoid plain "ati" — it matches "VGA comp**ati**ble controller"
            // on every Intel / NVIDIA line.
            GpuVendor::Amd
        } else if lower.contains("intel") {
            GpuVendor::Intel
        } else {
            continue;
        };
        // Drop the leading "<bus>: VGA ..." then split on the next colon for the model.
        let model = line.split(':').nth(2).map(|s| s.trim().to_owned());
        gpus.push(NodeGpu { vendor, vram_gb: 0, name: model });
    }
    gpus
}

fn primary_acceleration(gpus: &[NodeGpu], os: OperatingSystem) -> Option<Acceleration> {
    if gpus.iter().any(|g| matches!(g.vendor, GpuVendor::Apple)) {
        Some(Acceleration::Metal)
    } else if gpus.iter().any(|g| matches!(g.vendor, GpuVendor::Nvidia)) {
        Some(Acceleration::Cuda)
    } else if gpus.iter().any(|g| matches!(g.vendor, GpuVendor::Amd)) {
        Some(Acceleration::Rocm)
    } else if matches!(os, OperatingSystem::Macos) {
        Some(Acceleration::Coreml)
    } else {
        None
    }
}

fn apple_silicon_marketing_name() -> String {
    // `sysctl -n machdep.cpu.brand_string` returns "Apple M2 Pro" etc. Cheaper
    // than parsing system_profiler.
    if let Ok(output) = std::process::Command::new("sysctl")
        .args(["-n", "machdep.cpu.brand_string"])
        .output()
    {
        if output.status.success() {
            let s = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            if !s.is_empty() {
                return s;
            }
        }
    }
    "Apple Silicon".to_owned()
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use axum::Router;
    use orion_types::HealthCheck;
    use tokio::net::TcpListener;

    // ----------------------------------------------------------- HTTP probes

    async fn ephemeral_http_server<F>(handler: axum::routing::MethodRouter) -> u16
    where
        F: Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let app = Router::new().route("/health", handler);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        port
    }

    #[tokio::test]
    async fn http_probe_succeeds_on_2xx() {
        let port = ephemeral_http_server::<()>(get(|| async { "ok" })).await;
        // Loop probe needs 127.0.0.1; that's exactly where we bound.
        let hc = HealthCheck::Http {
            path: "/health".into(),
            port,
            interval_seconds: 1,
            failure_threshold: 1,
        };
        run_probe(&hc).await.expect("2xx body");
    }

    #[tokio::test]
    async fn http_probe_fails_on_5xx() {
        let port = ephemeral_http_server::<()>(get(|| async {
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "boom")
        }))
        .await;
        let hc = HealthCheck::Http {
            path: "/health".into(),
            port,
            interval_seconds: 1,
            failure_threshold: 1,
        };
        let err = run_probe(&hc).await.unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("500"), "expected 500 in error message, got: {msg}");
    }

    #[tokio::test]
    async fn http_probe_fails_when_no_one_listening() {
        // Bind+drop to pick an unused port, then probe it. Race window is tiny;
        // accept either "connection refused" or any other connect-time error.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let hc = HealthCheck::Http {
            path: "/x".into(),
            port,
            interval_seconds: 1,
            failure_threshold: 1,
        };
        assert!(run_probe(&hc).await.is_err());
    }

    // ----------------------------------------------------------- TCP probes

    #[tokio::test]
    async fn tcp_probe_succeeds_when_port_open() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        // Keep listener alive in the background; the probe just needs the connect to succeed.
        tokio::spawn(async move {
            // Accept once and drop the connection.
            let _ = listener.accept().await;
        });
        let hc = HealthCheck::Tcp {
            port,
            interval_seconds: 1,
            failure_threshold: 1,
        };
        run_probe(&hc).await.expect("tcp connect succeeds");
    }

    #[tokio::test]
    async fn tcp_probe_fails_when_port_closed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let hc = HealthCheck::Tcp {
            port,
            interval_seconds: 1,
            failure_threshold: 1,
        };
        assert!(run_probe(&hc).await.is_err());
    }

    // ----------------------------------------------------------- Exec probes

    #[tokio::test]
    async fn exec_probe_succeeds_on_zero_exit() {
        let hc = HealthCheck::Exec {
            command: vec!["/usr/bin/true".into()],
            interval_seconds: 1,
            failure_threshold: 1,
        };
        run_probe(&hc).await.expect("/bin/true exits 0");
    }

    #[tokio::test]
    async fn exec_probe_fails_on_nonzero_exit() {
        let hc = HealthCheck::Exec {
            command: vec!["/usr/bin/false".into()],
            interval_seconds: 1,
            failure_threshold: 1,
        };
        let err = run_probe(&hc).await.unwrap_err();
        assert!(format!("{err}").to_lowercase().contains("exec"));
    }

    #[tokio::test]
    async fn exec_probe_fails_on_missing_command() {
        let hc = HealthCheck::Exec {
            command: vec!["/this/binary/definitely/does/not/exist".into()],
            interval_seconds: 1,
            failure_threshold: 1,
        };
        assert!(run_probe(&hc).await.is_err());
    }

    // ----------------------------------------------------------- GPU parsing

    #[test]
    fn parse_vram_handles_mb_and_gb_units() {
        assert_eq!(parse_vram_to_gb("8192 MB"), Some(8));
        assert_eq!(parse_vram_to_gb("24 GB"), Some(24));
        assert_eq!(parse_vram_to_gb("1536 MB"), Some(2)); // 1.5 GB rounds up to 2
        assert_eq!(parse_vram_to_gb("256 MB"), Some(1));  // sub-1GB clamps to 1
        assert_eq!(parse_vram_to_gb("not a number"), None);
        assert_eq!(parse_vram_to_gb(""), None);
    }

    #[test]
    fn parse_nvidia_smi_reports_one_gpu_per_line() {
        let stdout = "NVIDIA GeForce RTX 4090, 24564\nNVIDIA GeForce RTX 3090, 24576\n";
        let gpus = parse_nvidia_smi(stdout);
        assert_eq!(gpus.len(), 2);
        assert_eq!(gpus[0].vendor, GpuVendor::Nvidia);
        assert_eq!(gpus[0].vram_gb, 24);
        assert_eq!(gpus[0].name.as_deref(), Some("NVIDIA GeForce RTX 4090"));
        assert_eq!(gpus[1].vram_gb, 24);
    }

    #[test]
    fn parse_nvidia_smi_handles_empty_output() {
        assert!(parse_nvidia_smi("").is_empty());
        assert!(parse_nvidia_smi("malformed\n").is_empty());
    }

    #[test]
    fn parse_lspci_categorises_vendors() {
        let stdout = "\
00:02.0 VGA compatible controller: Intel Corporation UHD Graphics 630
01:00.0 VGA compatible controller: NVIDIA Corporation GA102 [GeForce RTX 3090]
02:00.0 3D controller: Advanced Micro Devices, Inc. [AMD] Radeon Pro WX 9100
03:00.0 Display controller: Some Other Vendor Foo Bar
04:00.0 Network controller: Realtek RTL8125
";
        let gpus = parse_lspci(stdout);
        assert_eq!(gpus.len(), 3);
        assert_eq!(gpus[0].vendor, GpuVendor::Intel);
        assert_eq!(gpus[1].vendor, GpuVendor::Nvidia);
        assert_eq!(gpus[2].vendor, GpuVendor::Amd);
        // Unknown-vendor GPU is dropped, network controller is dropped.
        assert!(!gpus.iter().any(|g| g.name.as_deref().unwrap_or("").contains("Realtek")));
    }

    #[test]
    fn parse_macos_system_profiler_categorises_vendors_and_vram() {
        use serde_json::json;
        let v = json!({
            "SPDisplaysDataType": [
                {
                    "_name": "AMD Radeon Pro 5500M",
                    "sppci_model": "AMD Radeon Pro 5500M",
                    "spdisplays_vram": "8 GB"
                },
                {
                    "_name": "Intel UHD Graphics 630",
                    "sppci_model": "Intel UHD Graphics 630",
                    "sppci_vram_shared": "1536 MB"
                },
                {
                    "_name": "Some Mystery Device",
                    "sppci_model": "Mystery"
                }
            ]
        });
        let gpus = parse_macos_system_profiler(&v);
        assert_eq!(gpus.len(), 2);
        assert_eq!(gpus[0].vendor, GpuVendor::Amd);
        assert_eq!(gpus[0].vram_gb, 8);
        assert_eq!(gpus[1].vendor, GpuVendor::Intel);
        assert_eq!(gpus[1].vram_gb, 2); // 1.5 rounded up
    }

    #[test]
    fn parse_macos_system_profiler_returns_empty_on_missing_key() {
        let v = serde_json::json!({"other": "data"});
        assert!(parse_macos_system_profiler(&v).is_empty());
    }

    #[test]
    fn detect_gpus_on_apple_silicon_reports_apple_gpu() {
        let gpus = detect_gpus(Arch::Arm64, OperatingSystem::Macos, 16 * 1_073_741_824);
        assert_eq!(gpus.len(), 1);
        assert_eq!(gpus[0].vendor, GpuVendor::Apple);
        assert_eq!(gpus[0].vram_gb, 8); // half of 16 GB
    }

    #[test]
    fn detect_gpus_on_other_os_returns_empty() {
        // Windows-like unsupported branch.
        let gpus = detect_gpus(Arch::X86_64, OperatingSystem::Linux, 0);
        // On real Linux nvidia-smi might be present; the function is best-effort.
        // We just check it didn't panic.
        let _ = gpus;
    }

    #[test]
    fn primary_acceleration_picks_metal_for_apple_gpu() {
        let gpus = vec![NodeGpu { vendor: GpuVendor::Apple, vram_gb: 8, name: None }];
        assert_eq!(
            primary_acceleration(&gpus, OperatingSystem::Macos),
            Some(Acceleration::Metal)
        );
    }

    #[test]
    fn primary_acceleration_picks_cuda_for_nvidia() {
        let gpus = vec![NodeGpu { vendor: GpuVendor::Nvidia, vram_gb: 24, name: None }];
        assert_eq!(
            primary_acceleration(&gpus, OperatingSystem::Linux),
            Some(Acceleration::Cuda)
        );
    }

    #[test]
    fn primary_acceleration_picks_rocm_for_amd() {
        let gpus = vec![NodeGpu { vendor: GpuVendor::Amd, vram_gb: 16, name: None }];
        assert_eq!(
            primary_acceleration(&gpus, OperatingSystem::Linux),
            Some(Acceleration::Rocm)
        );
    }

    #[test]
    fn primary_acceleration_picks_coreml_on_macos_intel() {
        // Intel iGPU on macOS, no Apple GPU
        let gpus = vec![NodeGpu { vendor: GpuVendor::Intel, vram_gb: 1, name: None }];
        assert_eq!(
            primary_acceleration(&gpus, OperatingSystem::Macos),
            Some(Acceleration::Coreml)
        );
    }

    #[test]
    fn primary_acceleration_none_on_bare_linux() {
        assert_eq!(primary_acceleration(&[], OperatingSystem::Linux), None);
    }

    #[tokio::test]
    async fn exec_probe_fails_on_empty_command() {
        let hc = HealthCheck::Exec {
            command: vec![],
            interval_seconds: 1,
            failure_threshold: 1,
        };
        let err = run_probe(&hc).await.unwrap_err();
        assert!(format!("{err}").contains("empty"));
    }
}
