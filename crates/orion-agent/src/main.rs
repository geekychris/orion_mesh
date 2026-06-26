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
    ControlRun, ControlStop, Envelope, Heartbeat, LogLine, LogStream, NodeInventory, Topic,
    WorkloadKind,
};
use orion_runtime::{LaunchSpec, LogSink, NativeAdapter, OutStream, RuntimeRegistry};
use orion_types::{Arch, NodeId, OperatingSystem, ResourceName};
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

/// In-memory map: instance_id → (kind, name). Populated when the agent
/// receives a `ControlRun`; used to label outgoing `LogLine`s.
#[derive(Default)]
struct InstanceRegistry {
    by_id: Mutex<HashMap<Uuid, (WorkloadKind, ResourceName)>>,
}

impl InstanceRegistry {
    fn record(&self, id: Uuid, kind: WorkloadKind, name: ResourceName) {
        self.by_id.lock().unwrap().insert(id, (kind, name));
    }

    fn get(&self, id: &Uuid) -> Option<(WorkloadKind, ResourceName)> {
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
    let registry = Arc::new(reg);
    let instances = Arc::new(InstanceRegistry::default());

    let nats = orion_auth::nats::connect_options(&auth)
        .name("orion-agent")
        .connect(&args.nats_url)
        .await
        .context("connecting to NATS")?;
    info!("connected to NATS");

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
                let Some((_kind, name)) = instances.get(&id) else { continue };
                let payload = LogLine {
                    node_id: node_id.clone(),
                    service: name,
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
        let subject_for_log = subject.clone();
        tokio::spawn(async move {
            info!(subject = %subject_for_log, "subscribed to control subject");
            while let Some(msg) = sub.next().await {
                if let Err(e) =
                    dispatch_control(&subject_for_log, &msg.payload, &registry, &instances, &log_tx)
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
) -> anyhow::Result<()> {
    if subject.ends_with(".run") {
        let env: Envelope<ControlRun> = serde_json::from_slice(payload)?;
        let spec = env.payload;
        info!(?spec.kind, %spec.name, instance = %spec.instance_id, "control: run");

        instances.record(spec.instance_id, spec.kind, spec.name.clone());

        let adapter_name = runtime_adapter_name(&spec.runtime);
        let adapter = registry
            .get(adapter_name)
            .ok_or_else(|| anyhow::anyhow!("no adapter for kind '{adapter_name}'"))?;
        adapter
            .launch(LaunchSpec {
                instance_id: spec.instance_id,
                name: spec.name,
                runtime: spec.runtime,
                log_sink: Some(log_tx.clone()),
            })
            .await?;
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
