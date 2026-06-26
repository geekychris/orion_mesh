//! OrionMesh controller.
//!
//! Phase 1 scope: connect to NATS, subscribe to heartbeats, track live nodes in
//! memory, expose `/health` and `/v1/nodes` over HTTP. Reconciliation, scheduling,
//! and persistence (`redb`) light up in later phases.

use anyhow::Result;
use axum::{Json, Router, extract::State, routing::get};
use chrono::{DateTime, Utc};
use clap::Parser;
use futures::StreamExt;
use orion_bus::{Envelope, Heartbeat, Topic};
use orion_types::NodeId;
use serde::Serialize;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(name = "orion-controller", version, about = "OrionMesh controller")]
struct Args {
    /// NATS server URL.
    #[arg(long, env = "ORION_NATS_URL", default_value = "nats://127.0.0.1:4222")]
    nats_url: String,

    /// HTTP bind address.
    #[arg(long, env = "ORION_HTTP_BIND", default_value = "127.0.0.1:7878")]
    bind: SocketAddr,
}

#[derive(Clone, Default)]
struct AppState {
    nodes: Arc<Mutex<HashMap<NodeId, NodeView>>>,
}

#[derive(Clone, Serialize)]
struct NodeView {
    node_id: NodeId,
    agent_version: String,
    last_seen: DateTime<Utc>,
    uptime_seconds: u64,
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
    info!(nats_url = %args.nats_url, bind = %args.bind, "orion-controller starting");

    let state = AppState::default();

    let nats = async_nats::connect(&args.nats_url).await?;
    info!("connected to NATS");

    let heartbeat_state = state.clone();
    let nats_subscriber = nats.clone();
    tokio::spawn(async move {
        if let Err(e) = subscribe_heartbeats(nats_subscriber, heartbeat_state).await {
            warn!(error = ?e, "heartbeat subscriber exited");
        }
    });

    let router = Router::new()
        .route("/health", get(health))
        .route("/v1/nodes", get(list_nodes))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    axum::serve(listener, router).await?;
    Ok(())
}

async fn subscribe_heartbeats(client: async_nats::Client, state: AppState) -> Result<()> {
    let mut sub = client.subscribe(Topic::Heartbeat.as_str().to_owned()).await?;
    info!("subscribed to {}", Topic::Heartbeat.as_str());
    while let Some(msg) = sub.next().await {
        match serde_json::from_slice::<Envelope<Heartbeat>>(&msg.payload) {
            Ok(env) => {
                let hb = env.payload;
                let view = NodeView {
                    node_id: hb.node_id.clone(),
                    agent_version: hb.agent_version,
                    last_seen: env.at,
                    uptime_seconds: hb.uptime_seconds,
                };
                state.nodes.lock().unwrap().insert(hb.node_id, view);
            }
            Err(e) => warn!(error = ?e, "malformed heartbeat envelope"),
        }
    }
    Ok(())
}

async fn health() -> &'static str {
    "ok"
}

async fn list_nodes(State(state): State<AppState>) -> Json<Vec<NodeView>> {
    let nodes = state.nodes.lock().unwrap().values().cloned().collect();
    Json(nodes)
}
