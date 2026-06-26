//! OrionMesh node agent.
//!
//! Phase 1 scope: connect to NATS, publish heartbeats. Everything else (runtime
//! adapters, service registration, log forwarding, metrics) lights up in later phases.

use anyhow::Result;
use clap::Parser;
use orion_bus::{Envelope, Heartbeat, Topic};
use orion_types::{Arch, NodeId, OperatingSystem};
use std::time::Duration;
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(name = "orion-agent", version, about = "OrionMesh node agent")]
struct Args {
    /// NATS server URL (also reads ORION_NATS_URL env var).
    #[arg(long, env = "ORION_NATS_URL", default_value = "nats://127.0.0.1:4222")]
    nats_url: String,

    /// Node id. Defaults to the machine hostname when omitted.
    #[arg(long, env = "ORION_NODE_ID")]
    node_id: Option<String>,

    /// Heartbeat interval in seconds.
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

    info!(node_id = %node_id, nats_url = %args.nats_url, "orion-agent starting");

    let client = async_nats::connect(&args.nats_url).await?;
    info!("connected to NATS");

    let mut ticker = tokio::time::interval(Duration::from_secs(args.heartbeat_interval));
    let started = std::time::Instant::now();

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let hb = Heartbeat {
                    node_id: node_id.clone(),
                    agent_version: env!("CARGO_PKG_VERSION").to_owned(),
                    uptime_seconds: started.elapsed().as_secs(),
                    arch: detect_arch(),
                    os: detect_os(),
                    gpu: None,
                    acceleration: None,
                    cpu_cores: num_cpus(),
                    mem_total_bytes: 0,
                    mem_used_bytes: 0,
                    load_avg_1m: 0.0,
                    labels: Default::default(),
                };
                let env = Envelope::new(Some(node_id.clone()), hb);
                match serde_json::to_vec(&env) {
                    Ok(bytes) => {
                        if let Err(e) = client.publish(Topic::Heartbeat.as_str().to_owned(), bytes.into()).await {
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

fn num_cpus() -> u32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1)
}
