//! OrionMesh controller.
//!
//! Phase 2 additions over Phase 1:
//!   - Subscribes to `orion.logs.*` (all per-node log subjects) and keeps a
//!     ring buffer of the last ~500 lines per (kind, name) in memory.
//!   - `POST /v1/dispatch/:kind/:name` actually publishes a `ControlRun`
//!     envelope to a chosen node's `orion.control.{node}.run` subject.
//!     Phase 5 will replace the "pick the first live node" heuristic with the
//!     real scheduler.

use anyhow::{Context, Result};
use async_nats::Client;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    middleware::from_fn_with_state,
    response::IntoResponse,
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use clap::Parser;
use futures::StreamExt;
use orion_auth::AuthMode;
use orion_bus::{
    ControlRun, Envelope, Heartbeat, LogLine, NodeInventory, Topic, WorkloadKind,
};
use orion_store::Store;
use orion_types::{Resource, ResourceBody, Runtime};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tower_http::cors::{Any, CorsLayer};
use tracing::{info, warn};
use uuid::Uuid;

const LOG_RING_CAPACITY: usize = 500;

#[derive(Parser, Debug)]
#[command(name = "orion-controller", version, about = "OrionMesh controller")]
struct Args {
    #[arg(long, env = "ORION_NATS_URL", default_value = "nats://127.0.0.1:4222")]
    nats_url: String,

    #[arg(long, env = "ORION_HTTP_BIND", default_value = "127.0.0.1:7878")]
    bind: SocketAddr,

    /// SQLite file path. `sqlite::memory:` for an ephemeral in-memory store.
    #[arg(long, env = "ORION_STORE_PATH", default_value = "./orion-state.sqlite")]
    store_path: String,
}

/// In-memory ring buffer per workload, keyed by (kind, name).
#[derive(Default)]
struct LogBuffer {
    rings: Mutex<HashMap<(String, String), VecDeque<LogEntry>>>,
}

#[derive(Clone, Serialize)]
struct LogEntry {
    at: DateTime<Utc>,
    node_id: String,
    stream: String, // "stdout" or "stderr"
    line: String,
}

impl LogBuffer {
    fn push(&self, kind: &str, name: &str, e: LogEntry) {
        let mut rings = self.rings.lock().unwrap();
        let ring = rings.entry((kind.to_owned(), name.to_owned())).or_default();
        if ring.len() >= LOG_RING_CAPACITY {
            ring.pop_front();
        }
        ring.push_back(e);
    }

    fn snapshot(&self, kind: &str, name: &str, since_seq: usize) -> (Vec<LogEntry>, usize) {
        let rings = self.rings.lock().unwrap();
        match rings.get(&(kind.to_owned(), name.to_owned())) {
            Some(ring) => {
                // Treat the front of the ring as the oldest; since_seq is a logical
                // total counter that we approximate with ring length.
                let total = ring.len();
                let start = since_seq.min(total);
                let entries: Vec<_> = ring.iter().skip(start).cloned().collect();
                (entries, total)
            }
            None => (vec![], 0),
        }
    }
}

#[derive(Clone)]
struct AppState {
    store: Arc<Store>,
    nats: Client,
    node_id: NodeIdRegistry,
    logs: Arc<LogBuffer>,
    #[allow(dead_code)]
    auth: AuthMode,
}

/// Tracks the most recent live node id (for dispatch). Phase 5 will replace
/// "first observed node" with the real scheduler.
#[derive(Clone, Default)]
struct NodeIdRegistry {
    last: Arc<Mutex<Option<String>>>,
}

impl NodeIdRegistry {
    fn set(&self, id: &str) {
        *self.last.lock().unwrap() = Some(id.to_owned());
    }
    fn get(&self) -> Option<String> {
        self.last.lock().unwrap().clone()
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
    let auth = AuthMode::from_env().context("loading cluster auth")?;
    info!(
        nats_url = %args.nats_url,
        bind = %args.bind,
        store = %args.store_path,
        auth_disabled = auth.is_disabled(),
        "orion-controller starting"
    );

    let store = Arc::new(Store::open(&args.store_path).await.context("opening store")?);
    let nats = orion_auth::nats::connect_options(&auth)
        .name("orion-controller")
        .connect(&args.nats_url)
        .await
        .context("connecting to NATS")?;
    info!("connected to NATS");

    let state = AppState {
        store: store.clone(),
        nats: nats.clone(),
        node_id: NodeIdRegistry::default(),
        logs: Arc::new(LogBuffer::default()),
        auth: auth.clone(),
    };

    // ----- subscribers
    tokio::spawn({
        let nats = nats.clone();
        let store = store.clone();
        let nodes = state.node_id.clone();
        async move {
            if let Err(e) = subscribe_heartbeats(nats, store, nodes).await {
                warn!(error = ?e, "heartbeat subscriber exited");
            }
        }
    });
    tokio::spawn({
        let nats = nats.clone();
        let store = store.clone();
        async move {
            if let Err(e) = subscribe_inventory(nats, store).await {
                warn!(error = ?e, "inventory subscriber exited");
            }
        }
    });
    tokio::spawn({
        let nats = nats.clone();
        let logs = state.logs.clone();
        async move {
            if let Err(e) = subscribe_logs(nats, logs).await {
                warn!(error = ?e, "log subscriber exited");
            }
        }
    });

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let router = Router::new()
        .route("/v1/nodes", get(list_nodes))
        .route("/v1/kinds", get(list_kinds))
        .route("/v1/resources/:kind", get(list_resources))
        .route("/v1/resources/:kind/:name", get(get_resource).delete(delete_resource))
        .route("/v1/resources/apply", post(apply_resource))
        .route("/v1/dispatch/:kind/:name", post(dispatch_resource))
        .route("/v1/logs/:kind/:name", get(get_logs))
        .layer(from_fn_with_state(auth, orion_auth::http::require_bearer))
        .route("/health", get(health))
        .layer(cors)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    axum::serve(listener, router).await?;
    Ok(())
}

async fn subscribe_heartbeats(
    client: Client,
    store: Arc<Store>,
    nodes: NodeIdRegistry,
) -> Result<()> {
    let mut sub = client.subscribe(Topic::Heartbeat.as_str().to_owned()).await?;
    info!("subscribed to {}", Topic::Heartbeat.as_str());
    while let Some(msg) = sub.next().await {
        match serde_json::from_slice::<Envelope<Heartbeat>>(&msg.payload) {
            Ok(env) => {
                let hb = env.payload;
                nodes.set(&hb.node_id.0);
                if let Err(e) = store.touch_node(&hb.node_id.0, &hb.agent_version).await {
                    warn!(error = ?e, "store touch_node failed");
                }
            }
            Err(e) => warn!(error = ?e, "malformed heartbeat envelope"),
        }
    }
    Ok(())
}

async fn subscribe_inventory(client: Client, store: Arc<Store>) -> Result<()> {
    let mut sub = client.subscribe(Topic::NodeInventory.as_str().to_owned()).await?;
    info!("subscribed to {}", Topic::NodeInventory.as_str());
    while let Some(msg) = sub.next().await {
        match serde_json::from_slice::<Envelope<NodeInventory>>(&msg.payload) {
            Ok(env) => {
                let inv = env.payload;
                let inv_json = match serde_json::to_string(&inv) {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(error = ?e, "inventory re-encode failed");
                        continue;
                    }
                };
                if let Err(e) = store
                    .set_node_inventory(&inv.node_id.0, &inv.agent_version, &inv_json)
                    .await
                {
                    warn!(error = ?e, "store set_node_inventory failed");
                }
            }
            Err(e) => warn!(error = ?e, "malformed inventory envelope"),
        }
    }
    Ok(())
}

async fn subscribe_logs(client: Client, logs: Arc<LogBuffer>) -> Result<()> {
    // Wildcard: every per-node logs subject (orion.logs.<node>) feeds into here.
    let mut sub = client.subscribe(Topic::Logs.as_str().to_owned()).await?;
    info!("subscribed to {}", Topic::Logs.as_str());
    while let Some(msg) = sub.next().await {
        match serde_json::from_slice::<Envelope<LogLine>>(&msg.payload) {
            Ok(env) => {
                let line = env.payload;
                let stream = match line.stream {
                    orion_bus::LogStream::Stdout => "stdout",
                    orion_bus::LogStream::Stderr => "stderr",
                };
                // The kind isn't on the LogLine; we don't track instance→kind on
                // the controller yet, so dispatch by name into BOTH Service and
                // Task rings — the UI picks the right one based on which kind
                // tab is open.
                let entry = LogEntry {
                    at: env.at,
                    node_id: line.node_id.0.clone(),
                    stream: stream.to_owned(),
                    line: line.line.clone(),
                };
                logs.push("Service", &line.service.0, entry.clone());
                logs.push("Task", &line.service.0, entry);
            }
            Err(e) => warn!(error = ?e, "malformed log envelope"),
        }
    }
    Ok(())
}

// ============================================================ HTTP handlers

async fn health() -> &'static str {
    "ok"
}

#[derive(Serialize)]
struct NodeView {
    node_id: String,
    agent_version: String,
    last_seen_at: String,
    inventory: Option<serde_json::Value>,
}

async fn list_nodes(State(state): State<AppState>) -> Result<Json<Vec<NodeView>>, ApiError> {
    let nodes = state.store.list_nodes().await.map_err(ApiError::store)?;
    let view = nodes
        .into_iter()
        .map(|n| NodeView {
            node_id: n.node_id,
            agent_version: n.agent_version,
            last_seen_at: n.last_seen_at.to_rfc3339(),
            inventory: n
                .inventory_json
                .and_then(|s| serde_json::from_str(&s).ok()),
        })
        .collect();
    Ok(Json(view))
}

async fn list_resources(
    State(state): State<AppState>,
    Path(kind): Path<String>,
) -> Result<Json<Vec<Resource>>, ApiError> {
    let rs = state.store.list_by_kind(&kind).await.map_err(ApiError::store)?;
    Ok(Json(rs))
}

async fn get_resource(
    State(state): State<AppState>,
    Path((kind, name)): Path<(String, String)>,
) -> Result<Json<Resource>, ApiError> {
    match state
        .store
        .get_resource(&kind, "_", &name)
        .await
        .map_err(ApiError::store)?
    {
        Some(r) => Ok(Json(r)),
        None => Err(ApiError::not_found(format!("{kind}/{name} not found"))),
    }
}

async fn delete_resource(
    State(state): State<AppState>,
    Path((kind, name)): Path<(String, String)>,
) -> Result<Json<DeleteOutcome>, ApiError> {
    let removed = state
        .store
        .delete_resource(&kind, "_", &name)
        .await
        .map_err(ApiError::store)?;
    if !removed {
        return Err(ApiError::not_found(format!("{kind}/{name} not found")));
    }
    Ok(Json(DeleteOutcome { kind, name, deleted: true }))
}

#[derive(Serialize)]
struct KindsView {
    kinds: &'static [&'static str],
}

async fn list_kinds() -> Json<KindsView> {
    Json(KindsView {
        kinds: &[
            "Node", "Service", "Task", "Job", "Schedule", "Dataset", "Model",
            "Project", "Secret", "Volume", "Network", "Runtime", "Capability",
            "Policy", "Integration",
        ],
    })
}

#[derive(Deserialize)]
struct ApplyQuery {
    #[serde(default)]
    dry_run: Option<String>,
}

impl ApplyQuery {
    fn is_dry_run(&self) -> bool {
        matches!(
            self.dry_run.as_deref(),
            Some("1" | "true" | "TRUE" | "yes" | "YES" | "on" | "")
        )
    }
}

async fn apply_resource(
    State(state): State<AppState>,
    Query(q): Query<ApplyQuery>,
    body: String,
) -> Result<Json<ApplyOutcome>, ApiError> {
    let r = Resource::from_yaml(&body)
        .map_err(|e| ApiError::bad_request(format!("yaml parse: {e}")))?;
    r.validate()
        .map_err(|e| ApiError::bad_request(format!("validate: {e}")))?;
    let kind = r.kind_str().to_owned();
    let name = r.metadata.name.0.clone();

    if q.is_dry_run() {
        return Ok(Json(ApplyOutcome { kind, name, generation: 0, dry_run: true }));
    }

    let generation = state
        .store
        .upsert_resource(&r)
        .await
        .map_err(ApiError::store)?;
    Ok(Json(ApplyOutcome { kind, name, generation, dry_run: false }))
}

#[derive(Serialize)]
struct DispatchOutcome {
    kind: String,
    name: String,
    node: String,
    instance_id: Uuid,
}

async fn dispatch_resource(
    State(state): State<AppState>,
    Path((kind, name)): Path<(String, String)>,
) -> Result<Json<DispatchOutcome>, ApiError> {
    if kind != "Service" && kind != "Task" {
        return Err(ApiError::bad_request(format!(
            "dispatch is only defined for Service and Task; got {kind}"
        )));
    }
    let resource = state
        .store
        .get_resource(&kind, "_", &name)
        .await
        .map_err(ApiError::store)?
        .ok_or_else(|| ApiError::not_found(format!("{kind}/{name} not found")))?;

    let runtime = match &resource.body {
        ResourceBody::Service { spec, .. } => spec
            .runtime
            .clone()
            .ok_or_else(|| ApiError::bad_request("Service has no runtime"))?,
        ResourceBody::Task { spec, .. } => spec
            .runtime
            .clone()
            .ok_or_else(|| ApiError::bad_request("Task has no runtime"))?,
        _ => unreachable!(),
    };

    let node = state.node_id.get().ok_or_else(|| {
        ApiError::bad_request("no live nodes — start an agent first")
    })?;

    let workload_kind = if kind == "Service" {
        WorkloadKind::Service
    } else {
        WorkloadKind::Task
    };

    let instance_id = Uuid::new_v4();
    let envelope = Envelope::new(
        None,
        ControlRun {
            instance_id,
            kind: workload_kind,
            name: resource.metadata.name.clone(),
            runtime,
            generation: resource.metadata.generation.unwrap_or(1),
        },
    );
    let payload = serde_json::to_vec(&envelope).expect("encode ControlRun");
    let subject = Topic::ControlRun.for_node(&node);
    state
        .nats
        .publish(subject, payload.into())
        .await
        .map_err(|e| ApiError::internal(format!("publish control.run: {e}")))?;

    Ok(Json(DispatchOutcome {
        kind,
        name,
        node,
        instance_id,
    }))
}

#[derive(Deserialize)]
struct LogsQuery {
    #[serde(default)]
    since: Option<usize>,
}

#[derive(Serialize)]
struct LogsView {
    kind: String,
    name: String,
    total: usize,
    entries: Vec<LogEntry>,
}

async fn get_logs(
    State(state): State<AppState>,
    Path((kind, name)): Path<(String, String)>,
    Query(q): Query<LogsQuery>,
) -> Json<LogsView> {
    let since = q.since.unwrap_or(0);
    let (entries, total) = state.logs.snapshot(&kind, &name, since);
    Json(LogsView { kind, name, total, entries })
}

#[derive(Serialize)]
struct ApplyOutcome {
    kind: String,
    name: String,
    generation: u64,
    dry_run: bool,
}

#[derive(Serialize)]
struct DeleteOutcome {
    kind: String,
    name: String,
    deleted: bool,
}

// ============================================================ error mapping

struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(msg: impl Into<String>) -> Self {
        Self { status: StatusCode::BAD_REQUEST, message: msg.into() }
    }
    fn not_found(msg: impl Into<String>) -> Self {
        Self { status: StatusCode::NOT_FOUND, message: msg.into() }
    }
    fn internal(msg: impl Into<String>) -> Self {
        Self { status: StatusCode::INTERNAL_SERVER_ERROR, message: msg.into() }
    }
    fn store(e: orion_store::StoreError) -> Self {
        Self { status: StatusCode::INTERNAL_SERVER_ERROR, message: e.to_string() }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (self.status, self.message).into_response()
    }
}
