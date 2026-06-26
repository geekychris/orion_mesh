//! OrionMesh node agent.
//!
//! Phase 1+ scope: connect to NATS with cluster auth, publish a NodeInventory
//! on connect, publish slim Heartbeats on a ticker, subscribe to the per-node
//! control plane (Run/Stop/Restart/Drain). The control handler is wired but
//! delegates to a runtime registry that ships with only the Native adapter.

use anyhow::{Context, Result};
use clap::Parser;
use futures::StreamExt;
use orion_auth::AuthMode;
use orion_bus::{
    ControlRun, ControlStop, Envelope, Heartbeat, NodeInventory, Topic, WorkloadKind,
};
use orion_runtime::{LaunchSpec, NativeAdapter, RuntimeRegistry};
use orion_types::{Arch, NodeId, OperatingSystem};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use sysinfo::System;
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(name = "orion-agent", version, about = "OrionMesh node agent")]
struct Args {
    #[arg(long, env = "ORION_NATS_URL", default_value = "nats://127.0.0.1:4222")]
    nats_url: String,

    #[arg(long, env = "ORION_NODE_ID")]
    node_id: Option<String>,

    #[arg(long, default_value_t = 5)]
    heartbeat_interval: u64,
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

    // Build runtime registry. Native adapter ships by default.
    let mut reg = RuntimeRegistry::new();
    reg.register(Arc::new(NativeAdapter::new()));
    let registry = Arc::new(reg);

    // Connect to NATS using the configured auth mode.
    let nats = orion_auth::nats::connect_options(&auth)
        .name("orion-agent")
        .connect(&args.nats_url)
        .await
        .context("connecting to NATS")?;
    info!("connected to NATS");

    // Publish inventory snapshot once.
    let mut sys = System::new_all();
    sys.refresh_all();
    let inventory = build_inventory(&node_id, &sys, &registry);
    let inv_env = Envelope::new(Some(node_id.clone()), inventory);
    if let Err(e) = nats
        .publish(Topic::NodeInventory.as_str().to_owned(), serde_json::to_vec(&inv_env)?.into())
        .await
    {
        warn!(error = ?e, "inventory publish failed");
    }

    // Subscribe to per-node control subjects (Run / Stop / Restart / Drain).
    for subject in Topic::control_subjects_for_node(&node_id.0) {
        let mut sub = nats.subscribe(subject.clone()).await?;
        let registry = registry.clone();
        let subject_for_log = subject.clone();
        tokio::spawn(async move {
            info!(subject = %subject_for_log, "subscribed to control subject");
            while let Some(msg) = sub.next().await {
                if let Err(e) = dispatch_control(&subject_for_log, &msg.payload, &registry).await {
                    warn!(subject = %subject_for_log, error = ?e, "control dispatch failed");
                }
            }
        });
    }

    let mut ticker = tokio::time::interval(Duration::from_secs(args.heartbeat_interval));
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                sys.refresh_cpu_usage();
                sys.refresh_memory();
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
) -> anyhow::Result<()> {
    if subject.ends_with(".run") {
        let env: Envelope<ControlRun> = serde_json::from_slice(payload)?;
        let spec = env.payload;
        info!(?spec.kind, %spec.name, "control: run");
        let adapter_name = match spec.kind {
            WorkloadKind::Service | WorkloadKind::Task => runtime_adapter_name(&spec.runtime),
        };
        let adapter = registry
            .get(adapter_name)
            .ok_or_else(|| anyhow::anyhow!("no adapter for kind '{adapter_name}'"))?;
        adapter
            .launch(LaunchSpec {
                instance_id: spec.instance_id,
                name: spec.name,
                runtime: spec.runtime,
            })
            .await?;
    } else if subject.ends_with(".stop") {
        let env: Envelope<ControlStop> = serde_json::from_slice(payload)?;
        let spec = env.payload;
        info!(instance_id = %spec.instance_id, "control: stop");
        // We don't track which adapter owns which instance yet (Phase 2 work).
        // For MVP: every adapter is asked to stop; only the owner does anything.
        for name in registry.names() {
            if let Some(a) = registry.get(&name) {
                let _ = a.stop(spec.instance_id).await;
            }
        }
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
