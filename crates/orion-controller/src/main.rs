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
    ControlRun, ControlStop, Envelope, Heartbeat, LogLine, NodeInventory, Topic, WorkloadKind,
};
use orion_store::Store;
use orion_types::{Resource, ResourceBody, ResourceName, Runtime};
use std::str::FromStr;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
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

    fn line_count(&self) -> usize {
        let rings = self.rings.lock().unwrap();
        rings.values().map(|r| r.len()).sum()
    }

    /// Substring search across every workload's buffer. Returns at most `limit`
    /// matches with the workload identity attached. `kind_filter` empty = any.
    fn search(
        &self,
        query: &str,
        kind_filter: Option<&str>,
        name_filter: Option<&str>,
        limit: usize,
    ) -> Vec<SearchHit> {
        let rings = self.rings.lock().unwrap();
        let mut hits: Vec<SearchHit> = Vec::new();
        for ((kind, name), ring) in rings.iter() {
            if let Some(k) = kind_filter {
                if k != kind {
                    continue;
                }
            }
            if let Some(n) = name_filter {
                if n != name {
                    continue;
                }
            }
            for entry in ring.iter() {
                if entry.line.contains(query) {
                    hits.push(SearchHit {
                        kind: kind.clone(),
                        name: name.clone(),
                        at: entry.at,
                        node_id: entry.node_id.clone(),
                        stream: entry.stream.clone(),
                        line: entry.line.clone(),
                    });
                }
            }
        }
        // Newest first.
        hits.sort_by(|a, b| b.at.cmp(&a.at));
        hits.truncate(limit);
        hits
    }
}

#[derive(Clone, Serialize)]
struct SearchHit {
    kind: String,
    name: String,
    at: DateTime<Utc>,
    node_id: String,
    stream: String,
    line: String,
}

#[derive(Clone)]
struct AppState {
    store: Arc<Store>,
    nats: Client,
    nats_url: String,
    node_id: NodeIdRegistry,
    logs: Arc<LogBuffer>,
    schedules: Arc<ScheduleRegistry>,
    instances: Arc<InstanceRegistry>,
    health: Arc<HealthRegistry>,
    workflows: Arc<WorkflowRegistry>,
    auth: AuthMode,
    started_at: DateTime<Utc>,
}

/// Live instance index. Learned from two sources:
///   - on POST /v1/dispatch: we record (kind, name, node, replicas, dispatched_at)
///   - on LogLine envelopes: we tag instance_id with first_seen_at + last_seen_at
#[derive(Default)]
struct InstanceRegistry {
    by_id: Mutex<HashMap<Uuid, InstanceRecord>>,
    /// Index from (kind, name) → set of instance ids, for the /v1/instances/{kind}/{name} endpoint.
    by_workload: Mutex<HashMap<(String, String), Vec<Uuid>>>,
}

#[derive(Clone, Serialize)]
struct InstanceRecord {
    instance_id: Uuid,
    kind: String,
    name: String,
    node: Option<String>,
    /// 0..replicas-1; defaults to 0 if we can't tell.
    replica_index: u32,
    dispatched_at: Option<DateTime<Utc>>,
    first_seen_at: Option<DateTime<Utc>>,
    last_seen_at: Option<DateTime<Utc>>,
    line_count: u32,
    /// When the agent reported a `TaskOutcome` for this instance. None = still
    /// believed alive.
    #[serde(skip_serializing_if = "Option::is_none")]
    exited_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exit_code: Option<i32>,
    /// "succeeded" | "failed" | None
    #[serde(skip_serializing_if = "Option::is_none")]
    exit_kind: Option<String>,
}

impl InstanceRegistry {
    fn record_dispatch(
        &self,
        instance_id: Uuid,
        kind: &str,
        name: &str,
        node: Option<&str>,
        replicas: u32,
    ) {
        // The 0-th instance keeps the controller-supplied id; later replicas get
        // their own at the agent. We pre-populate replica 0 here and let logs
        // fill in the others as they arrive.
        let now = Utc::now();
        let mut by_id = self.by_id.lock().unwrap();
        let mut by_workload = self.by_workload.lock().unwrap();
        by_id
            .entry(instance_id)
            .and_modify(|r| {
                r.dispatched_at = Some(now);
                r.node = node.map(|s| s.to_owned()).or(r.node.clone());
            })
            .or_insert(InstanceRecord {
                instance_id,
                kind: kind.to_owned(),
                name: name.to_owned(),
                node: node.map(|s| s.to_owned()),
                replica_index: 0,
                dispatched_at: Some(now),
                first_seen_at: None,
                last_seen_at: None,
                line_count: 0,
                exited_at: None,
                exit_code: None,
                exit_kind: None,
            });
        let key = (kind.to_owned(), name.to_owned());
        let ids = by_workload.entry(key).or_default();
        if !ids.contains(&instance_id) {
            ids.push(instance_id);
        }
        let _ = replicas; // replica fan-out is observed via LogLine instance_ids
    }

    /// Called from subscribe_logs() when a LogLine carries an instance_id.
    fn note_line(
        &self,
        instance_id: Uuid,
        kind: &str,
        name: &str,
        node: &str,
        replica_index: u32,
        at: DateTime<Utc>,
    ) {
        let mut by_id = self.by_id.lock().unwrap();
        let mut by_workload = self.by_workload.lock().unwrap();
        by_id
            .entry(instance_id)
            .and_modify(|r| {
                r.first_seen_at.get_or_insert(at);
                r.last_seen_at = Some(at);
                r.line_count = r.line_count.saturating_add(1);
                r.replica_index = replica_index;
                if r.node.is_none() {
                    r.node = Some(node.to_owned());
                }
            })
            .or_insert(InstanceRecord {
                instance_id,
                kind: kind.to_owned(),
                name: name.to_owned(),
                node: Some(node.to_owned()),
                replica_index,
                dispatched_at: None,
                first_seen_at: Some(at),
                last_seen_at: Some(at),
                line_count: 1,
                exited_at: None,
                exit_code: None,
                exit_kind: None,
            });
        let key = (kind.to_owned(), name.to_owned());
        let ids = by_workload.entry(key).or_default();
        if !ids.contains(&instance_id) {
            ids.push(instance_id);
        }
    }

    fn snapshot_for(&self, kind: &str, name: &str) -> Vec<InstanceRecord> {
        let by_workload = self.by_workload.lock().unwrap();
        let ids = by_workload
            .get(&(kind.to_owned(), name.to_owned()))
            .cloned()
            .unwrap_or_default();
        let by_id = self.by_id.lock().unwrap();
        let mut out: Vec<_> = ids.iter().filter_map(|i| by_id.get(i).cloned()).collect();
        out.sort_by_key(|r| r.replica_index);
        out
    }

    fn snapshot_all(&self) -> Vec<InstanceRecord> {
        let by_id = self.by_id.lock().unwrap();
        let mut out: Vec<_> = by_id.values().cloned().collect();
        out.sort_by(|a, b| (a.kind.clone(), a.name.clone(), a.replica_index).cmp(&(b.kind.clone(), b.name.clone(), b.replica_index)));
        out
    }

    fn drain_for(&self, kind: &str, name: &str) -> Vec<InstanceRecord> {
        let mut by_workload = self.by_workload.lock().unwrap();
        let ids = by_workload
            .remove(&(kind.to_owned(), name.to_owned()))
            .unwrap_or_default();
        let mut by_id = self.by_id.lock().unwrap();
        ids.iter().filter_map(|i| by_id.remove(i)).collect()
    }

    /// Find the workload kind ("Service" or "Task") for a given name.
    /// Used by subscribe_logs so each LogLine is stored under the correct
    /// kind exactly once, instead of double-pushed under both rings.
    fn kind_for_name(&self, name: &str) -> Option<String> {
        let by_id = self.by_id.lock().unwrap();
        by_id
            .values()
            .find(|r| r.name == name)
            .map(|r| r.kind.clone())
    }

    /// Called when the agent reports a TaskOutcome. Marks the instance Exited
    /// without removing it (so the reconciler can read the exit_code + kind
    /// to decide whether to re-dispatch).
    fn record_exit(&self, instance_id: Uuid, exit_code: i32, kind: &str) -> Option<InstanceRecord> {
        let now = Utc::now();
        let mut by_id = self.by_id.lock().unwrap();
        by_id.get_mut(&instance_id).map(|r| {
            r.exited_at = Some(now);
            r.exit_code = Some(exit_code);
            r.exit_kind = Some(kind.to_owned());
            r.clone()
        })
    }

    /// Count instances for (kind, name) that haven't reported an exit yet.
    fn count_alive(&self, kind: &str, name: &str) -> u32 {
        let by_workload = self.by_workload.lock().unwrap();
        let ids = by_workload
            .get(&(kind.to_owned(), name.to_owned()))
            .cloned()
            .unwrap_or_default();
        let by_id = self.by_id.lock().unwrap();
        ids.iter()
            .filter_map(|i| by_id.get(i))
            .filter(|r| r.exited_at.is_none())
            .count() as u32
    }

    /// Remove an instance entirely (used after the reconciler re-dispatches
    /// a slot — we don't need to keep the dead exit record around forever).
    fn purge(&self, instance_id: Uuid) {
        let mut by_id = self.by_id.lock().unwrap();
        by_id.remove(&instance_id);
        let mut by_workload = self.by_workload.lock().unwrap();
        for ids in by_workload.values_mut() {
            ids.retain(|i| *i != instance_id);
        }
    }
}

/// Observed state of every Schedule the controller has seen.
/// Keyed by Schedule resource name. Phase-2-lite: in-memory only.
#[derive(Default)]
struct ScheduleRegistry {
    by_name: Mutex<HashMap<String, ScheduleObservation>>,
}

#[derive(Clone, Serialize)]
struct ScheduleObservation {
    /// When the controller first started tracking this Schedule.
    armed_at: DateTime<Utc>,
    last_fired_at: Option<DateTime<Utc>>,
    last_instance_id: Option<Uuid>,
    /// Next time the cron will fire (computed at the last tick).
    next_fire_at: Option<DateTime<Utc>>,
    /// Most recent error from a fire attempt; cleared on success.
    last_error: Option<String>,
    fire_count: u32,
}

impl ScheduleRegistry {
    fn observe<F: FnOnce(&mut ScheduleObservation)>(&self, name: &str, f: F) {
        let mut map = self.by_name.lock().unwrap();
        let entry = map.entry(name.to_owned()).or_insert_with(|| ScheduleObservation {
            armed_at: Utc::now(),
            last_fired_at: None,
            last_instance_id: None,
            next_fire_at: None,
            last_error: None,
            fire_count: 0,
        });
        f(entry);
    }

    fn snapshot(&self) -> HashMap<String, ScheduleObservation> {
        self.by_name.lock().unwrap().clone()
    }
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
        nats_url: args.nats_url.clone(),
        node_id: NodeIdRegistry::default(),
        logs: Arc::new(LogBuffer::default()),
        schedules: Arc::new(ScheduleRegistry::default()),
        instances: Arc::new(InstanceRegistry::default()),
        health: Arc::new(HealthRegistry::default()),
        workflows: Arc::new(WorkflowRegistry::default()),
        auth: auth.clone(),
        started_at: Utc::now(),
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
        let instances = state.instances.clone();
        let store = state.store.clone();
        async move {
            if let Err(e) = subscribe_logs(nats, logs, instances, store).await {
                warn!(error = ?e, "log subscriber exited");
            }
        }
    });
    tokio::spawn({
        let state = state.clone();
        async move {
            scheduler_tick_loop(state).await;
        }
    });
    tokio::spawn({
        let state = state.clone();
        async move {
            if let Err(e) = subscribe_task_events(state).await {
                warn!(error = ?e, "task.events subscriber exited");
            }
        }
    });
    tokio::spawn({
        let state = state.clone();
        async move {
            if let Err(e) = subscribe_service_health(state).await {
                warn!(error = ?e, "service.health subscriber exited");
            }
        }
    });
    tokio::spawn({
        let state = state.clone();
        async move {
            reconcile_loop(state).await;
        }
    });
    tokio::spawn({
        let state = state.clone();
        async move {
            workflow_loop(state).await;
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
        .route("/v1/logs/search", get(search_logs))
        .route("/v1/instances", get(list_all_instances))
        .route("/v1/instances/:kind/:name", get(get_instances))
        .route("/v1/control/:kind/:name/stop", post(stop_workload))
        .route("/v1/control/:kind/:name/restart", post(restart_workload))
        .route("/v1/diag/system", get(diag_system))
        .route("/v1/diag/jetstream", get(diag_jetstream))
        .route("/v1/schedules/observed", get(list_schedule_observations))
        .route("/v1/health/instances", get(list_health))
        .route("/v1/find", post(find_services))
        .route("/v1/logs-archive/:kind/:name", get(get_logs_archive))
        .route("/metrics", get(prometheus_metrics))
        .route("/v1/workflows/observed", get(list_workflow_progress))
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

async fn subscribe_logs(
    client: Client,
    logs: Arc<LogBuffer>,
    instances: Arc<InstanceRegistry>,
    store: Arc<Store>,
) -> Result<()> {
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
                // The LogLine doesn't carry kind, but the InstanceRegistry knows
                // the kind for each workload (set at record_dispatch). Look it
                // up so each line lands in exactly ONE ring instead of being
                // double-pushed under both "Service" and "Task". Default to
                // "Service" when the registry hasn't seen the workload yet
                // (e.g. early log lines that arrive before record_dispatch).
                let kind = instances
                    .kind_for_name(&line.service.0)
                    .unwrap_or_else(|| "Service".to_owned());
                let entry = LogEntry {
                    at: env.at,
                    node_id: line.node_id.0.clone(),
                    stream: stream.to_owned(),
                    line: line.line.clone(),
                };
                logs.push(&kind, &line.service.0, entry);
                // Best-effort archive write — never block the hot path on it.
                let store_for_write = store.clone();
                let archive_kind = kind.clone();
                let archive_name = line.service.0.clone();
                let archive_node = line.node_id.0.clone();
                let archive_stream = stream.to_owned();
                let archive_line = line.line.clone();
                tokio::spawn(async move {
                    let _ = store_for_write
                        .append_log(
                            &archive_kind,
                            &archive_name,
                            &archive_node,
                            &archive_stream,
                            &archive_line,
                        )
                        .await;
                });
                if let Some(iid) = line.instance_id {
                    instances.note_line(
                        iid,
                        &kind,
                        &line.service.0,
                        &line.node_id.0,
                        line.replica_index,
                        env.at,
                    );
                }
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
            "Project", "Secret", "Volume", "Network", "Queue", "Workflow", "Runtime",
            "Capability", "Policy", "Integration",
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

    let (runtime, replicas) = match &resource.body {
        ResourceBody::Service { spec, .. } => (
            spec.runtime
                .clone()
                .ok_or_else(|| ApiError::bad_request("Service has no runtime"))?,
            spec.replicas.unwrap_or(1).max(1),
        ),
        ResourceBody::Task { spec, .. } => (
            spec.runtime
                .clone()
                .ok_or_else(|| ApiError::bad_request("Task has no runtime"))?,
            1, // Tasks are one-shot — replicas only meaningful for Services.
        ),
        _ => unreachable!(),
    };

    let workload_kind = if kind == "Service" { WorkloadKind::Service } else { WorkloadKind::Task };
    let generation = resource.metadata.generation.unwrap_or(1);

    let (node, instance_id) = dispatch_workload(
        &state,
        workload_kind,
        resource.metadata.name.clone(),
        runtime,
        generation,
        replicas,
    )
    .await?;

    state
        .instances
        .record_dispatch(instance_id, &kind, &name, Some(&node), replicas);

    Ok(Json(DispatchOutcome { kind, name, node, instance_id }))
}

async fn get_instances(
    State(state): State<AppState>,
    Path((kind, name)): Path<(String, String)>,
) -> Json<Vec<InstanceRecord>> {
    Json(state.instances.snapshot_for(&kind, &name))
}

/// Shared dispatch path used by POST /v1/dispatch and by the scheduler tick.
/// Picks a node via the placement-aware scheduler, generates the base
/// instance_id, publishes a ControlRun envelope carrying `replicas`. The
/// agent fans out into N copies (each with its own derived id).
async fn dispatch_workload(
    state: &AppState,
    kind: WorkloadKind,
    name: ResourceName,
    runtime: Runtime,
    generation: u64,
    replicas: u32,
) -> Result<(String, Uuid), ApiError> {
    let node = pick_node_for_dispatch(state, &name, &kind)
        .await
        .ok_or_else(|| ApiError::bad_request("no live nodes match the workload's placement"))?;
    let instance_id = Uuid::new_v4();
    // Pull the health-check from the resource (Service only) so the agent can
    // run periodic probes and publish ServiceHealth.
    let health_check = if matches!(kind, WorkloadKind::Service) {
        state
            .store
            .get_resource("Service", "_", &name.0)
            .await
            .ok()
            .flatten()
            .and_then(|r| match r.body {
                ResourceBody::Service { spec, .. } => spec.health,
                _ => None,
            })
    } else {
        None
    };
    let envelope = Envelope::new(
        None,
        ControlRun {
            instance_id,
            kind,
            name,
            runtime,
            generation,
            replicas,
            health_check,
        },
    );
    let payload = serde_json::to_vec(&envelope).expect("encode ControlRun");
    let subject = Topic::ControlRun.for_node(&node);
    state
        .nats
        .publish(subject, payload.into())
        .await
        .map_err(|e| ApiError::internal(format!("publish control.run: {e}")))?;
    Ok((node, instance_id))
}

// ============================================================ scheduler

/// Look up the workload's Placement (if it's a Service or Task with one) and
/// pick the best live node via orion-scheduler. Returns None if no node passes
/// the hard filter.
async fn pick_node_for_dispatch(
    state: &AppState,
    name: &ResourceName,
    kind: &WorkloadKind,
) -> Option<String> {
    // Pull placement from the resource (if present).
    let resource_kind = match kind {
        WorkloadKind::Service => "Service",
        WorkloadKind::Task => "Task",
    };
    let placement = state
        .store
        .get_resource(resource_kind, "_", &name.0)
        .await
        .ok()
        .flatten()
        .and_then(|r| match r.body {
            ResourceBody::Service { spec, .. } => Some(spec.placement),
            ResourceBody::Task { spec, .. } => Some(spec.placement),
            _ => None,
        })
        .unwrap_or_default();

    // Build candidate list from observed_node inventory.
    let observed = state.store.list_nodes().await.ok().unwrap_or_default();
    let cutoff = Utc::now() - chrono::Duration::seconds(30);
    let mut candidates = Vec::new();
    for n in &observed {
        if n.last_seen_at < cutoff {
            continue; // node hasn't heartbeat in 30s — treat as dead
        }
        let inv: Option<orion_bus::NodeInventory> = n
            .inventory_json
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok());
        let inv = match inv {
            Some(i) => i,
            None => continue,
        };
        candidates.push(orion_scheduler::CandidateNode {
            node_id: n.node_id.clone(),
            arch: inv.arch,
            os: inv.os,
            gpus: vec![], // NodeInventory doesn't carry GPUs today — Phase 5 follow-up
            roles: vec![],
            labels: Default::default(),
        });
    }

    if candidates.is_empty() {
        // Backwards-compat fallback: if no inventory has landed yet, use the
        // most-recent heartbeat node so single-node dev still works.
        return state.node_id.get();
    }

    let instances = state.instances.snapshot_all();
    orion_scheduler::pick_best(&candidates, &placement, |id| orion_scheduler::NodeLoad {
        running_instances: instances
            .iter()
            .filter(|r| r.node.as_deref() == Some(id) && r.exited_at.is_none())
            .count() as u32,
    })
}

// ============================================================ workflow runner

/// Tracks per-workflow step state. In-memory — same lifecycle as the
/// reconciler; rebuilds on controller restart from the resources + recent
/// task.events.
#[derive(Default)]
struct WorkflowRegistry {
    by_name: Mutex<HashMap<String, WorkflowProgress>>,
}

#[derive(Clone, Serialize)]
struct WorkflowProgress {
    /// step name → status ("pending" | "running" | "succeeded" | "failed")
    steps: HashMap<String, StepStatus>,
    started_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum StepStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
}

impl WorkflowRegistry {
    fn snapshot(&self) -> HashMap<String, WorkflowProgress> {
        self.by_name.lock().unwrap().clone()
    }
}

const WORKFLOW_TICK_SECONDS: u64 = 3;

async fn workflow_loop(state: AppState) {
    let mut ticker = tokio::time::interval(Duration::from_secs(WORKFLOW_TICK_SECONDS));
    ticker.tick().await;
    loop {
        ticker.tick().await;
        if let Err(e) = workflow_tick_once(&state).await {
            warn!(error = ?e, "workflow tick failed");
        }
    }
}

async fn workflow_tick_once(state: &AppState) -> Result<()> {
    use orion_types::WorkflowSpec;

    let workflows = state.store.list_by_kind("Workflow").await?;
    for wf in workflows {
        let name = wf.metadata.name.0.clone();
        let spec: WorkflowSpec = match wf.body {
            ResourceBody::Workflow { spec, .. } => spec,
            _ => continue,
        };
        let mut progress = state
            .workflows
            .by_name
            .lock()
            .unwrap()
            .entry(name.clone())
            .or_insert_with(|| WorkflowProgress {
                steps: spec
                    .steps
                    .iter()
                    .map(|s| (s.name.clone(), StepStatus::Pending))
                    .collect(),
                started_at: Utc::now(),
                finished_at: None,
            })
            .clone();
        if progress.finished_at.is_some() {
            continue;
        }

        // Apply observed instance state to step status.
        let instances = state.instances.snapshot_all();
        for step in &spec.steps {
            let status = progress.steps.entry(step.name.clone()).or_insert(StepStatus::Pending);
            if matches!(status, StepStatus::Pending) {
                continue;
            }
            // If we marked it Running, look at the task's instance to see if it exited.
            let task_inst = instances
                .iter()
                .find(|r| r.kind == "Task" && r.name == step.task.0);
            if let Some(rec) = task_inst {
                if rec.exit_kind.as_deref() == Some("succeeded") {
                    *status = StepStatus::Succeeded;
                } else if rec.exit_kind.as_deref() == Some("failed") {
                    *status = StepStatus::Failed;
                }
            }
        }

        // For each pending step whose deps are satisfied, dispatch the Task.
        for step in &spec.steps {
            let cur = *progress.steps.get(&step.name).unwrap_or(&StepStatus::Pending);
            if !matches!(cur, StepStatus::Pending) {
                continue;
            }
            let deps_ready = step.depends_on.iter().all(|d| {
                let s = progress.steps.get(d).copied().unwrap_or(StepStatus::Pending);
                matches!(s, StepStatus::Succeeded)
                    || (spec.continue_on_error && matches!(s, StepStatus::Failed))
            });
            let blocked_by_failure = step.depends_on.iter().any(|d| {
                let s = progress.steps.get(d).copied().unwrap_or(StepStatus::Pending);
                !spec.continue_on_error && matches!(s, StepStatus::Failed)
            });
            if blocked_by_failure {
                progress.steps.insert(step.name.clone(), StepStatus::Failed);
                continue;
            }
            if !deps_ready {
                continue;
            }
            // Look up the referenced Task and dispatch it.
            let task = match state
                .store
                .get_resource("Task", "_", &step.task.0)
                .await
                .ok()
                .flatten()
            {
                Some(t) => t,
                None => {
                    warn!(workflow = %name, step = %step.name, task = %step.task, "workflow step references missing Task");
                    progress.steps.insert(step.name.clone(), StepStatus::Failed);
                    continue;
                }
            };
            let runtime = match task.body {
                ResourceBody::Task { spec, .. } => spec.runtime,
                _ => None,
            };
            let runtime = match runtime {
                Some(r) => r,
                None => {
                    warn!(workflow = %name, step = %step.name, "task has no runtime");
                    progress.steps.insert(step.name.clone(), StepStatus::Failed);
                    continue;
                }
            };
            match dispatch_workload(
                state,
                WorkloadKind::Task,
                step.task.clone(),
                runtime,
                0,
                1,
            )
            .await
            {
                Ok((node, id)) => {
                    info!(
                        workflow = %name,
                        step = %step.name,
                        task = %step.task,
                        instance = %id,
                        node = %node,
                        "workflow: step dispatched"
                    );
                    state.instances.record_dispatch(id, "Task", &step.task.0, Some(&node), 1);
                    progress.steps.insert(step.name.clone(), StepStatus::Running);
                }
                Err(e) => {
                    warn!(workflow = %name, step = %step.name, error = %e, "workflow dispatch failed");
                    progress.steps.insert(step.name.clone(), StepStatus::Failed);
                }
            }
        }

        // Workflow done when every step is Succeeded or Failed.
        let all_terminal = progress
            .steps
            .values()
            .all(|s| matches!(s, StepStatus::Succeeded | StepStatus::Failed));
        if all_terminal {
            progress.finished_at = Some(Utc::now());
        }

        state
            .workflows
            .by_name
            .lock()
            .unwrap()
            .insert(name.clone(), progress);
    }
    Ok(())
}

// ============================================================ scheduler tick

const SCHEDULER_TICK_SECONDS: u64 = 5;
const RECONCILE_TICK_SECONDS: u64 = 5;

// ============================================================ task events

async fn subscribe_task_events(state: AppState) -> Result<()> {
    let subject = Topic::TaskEvents.as_str().to_owned();
    let mut sub = state.nats.subscribe(subject.clone()).await?;
    info!("subscribed to {subject}");
    while let Some(msg) = sub.next().await {
        let env: Envelope<orion_bus::TaskEvent> = match serde_json::from_slice(&msg.payload) {
            Ok(e) => e,
            Err(e) => {
                warn!(error = ?e, "malformed task.event envelope");
                continue;
            }
        };
        let ev = env.payload;
        let (code, kind_str) = match ev.outcome {
            orion_bus::TaskOutcome::Succeeded { exit_code } => (exit_code, "succeeded"),
            orion_bus::TaskOutcome::Failed { exit_code, .. } => (exit_code, "failed"),
            orion_bus::TaskOutcome::Cancelled { .. } => (-1, "cancelled"),
            _ => continue,
        };
        if let Some(rec) = state.instances.record_exit(ev.task_id, code, kind_str) {
            info!(
                instance = %ev.task_id,
                workload = %rec.name,
                kind_str,
                exit_code = code,
                "task event observed"
            );
        }
    }
    Ok(())
}

/// POST /v1/find — capability-aware service discovery.
///
/// Request body: a YAML/JSON CapabilitySelector. Returns the subset of
/// Services whose advertised `capabilities:` match the selector.
async fn find_services(
    State(state): State<AppState>,
    body: String,
) -> Result<Json<Vec<Resource>>, ApiError> {
    // Accept either YAML or JSON — serde_json handles both for plain map/value
    // shapes; for richer YAML the apply endpoint sits one Resource layer up.
    let selector: orion_types::CapabilitySelector = serde_json::from_str(&body)
        .map_err(|e| ApiError::bad_request(format!("selector parse (json): {e}")))?;
    let services = state
        .store
        .list_by_kind("Service")
        .await
        .map_err(ApiError::store)?;
    let mut out = Vec::new();
    for svc in services {
        let advertised = match &svc.body {
            ResourceBody::Service { spec, .. } => spec.capabilities.clone(),
            _ => continue,
        };
        if capabilities_match(&advertised, &selector) {
            out.push(svc);
        }
    }
    Ok(Json(out))
}

fn capabilities_match(
    advertised: &[orion_types::Capability],
    selector: &orion_types::CapabilitySelector,
) -> bool {
    use orion_types::AttrMatch;
    for (cap_name, checks) in &selector.requirements {
        let cap = match advertised.iter().find(|c| &c.name == cap_name) {
            Some(c) => c,
            None => return false,
        };
        for (attr_key, attr_match) in &checks.0 {
            let actual = match cap.attributes.get(attr_key) {
                Some(v) => v,
                None => return false,
            };
            match attr_match {
                AttrMatch::Equals(v) => {
                    if actual != v {
                        return false;
                    }
                }
                AttrMatch::OneOf(values) => {
                    if !values.iter().any(|v| v == actual) {
                        return false;
                    }
                }
                AttrMatch::Op(ops) => {
                    if !attr_op_matches(ops, actual) {
                        return false;
                    }
                }
            }
        }
    }
    true
}

fn attr_op_matches(op: &orion_types::AttrOp, actual: &serde_json::Value) -> bool {
    if let Some(v) = &op.eq {
        if actual != v {
            return false;
        }
    }
    if let Some(v) = &op.ne {
        if actual == v {
            return false;
        }
    }
    let actual_num = actual.as_f64();
    let cmp = |lhs: &Option<serde_json::Number>,
               op: fn(f64, f64) -> bool|
     -> bool {
        match (lhs, actual_num) {
            (Some(n), Some(a)) => n.as_f64().map_or(true, |x| op(a, x)),
            (Some(_), None) => false,
            (None, _) => true,
        }
    };
    cmp(&op.gt, |a, x| a > x)
        && cmp(&op.gte, |a, x| a >= x)
        && cmp(&op.lt, |a, x| a < x)
        && cmp(&op.lte, |a, x| a <= x)
}

#[derive(Deserialize)]
struct ArchiveQuery {
    #[serde(default)]
    since: Option<String>,
    #[serde(default = "default_archive_limit")]
    limit: i64,
}

fn default_archive_limit() -> i64 {
    1000
}

async fn get_logs_archive(
    State(state): State<AppState>,
    Path((kind, name)): Path<(String, String)>,
    Query(q): Query<ArchiveQuery>,
) -> Result<Json<Vec<orion_store::LogArchiveEntry>>, ApiError> {
    let since = q
        .since
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.with_timezone(&Utc));
    let entries = state
        .store
        .read_logs(&kind, &name, since, q.limit)
        .await
        .map_err(ApiError::store)?;
    Ok(Json(entries))
}

/// `/metrics` — Prometheus text exposition. Scrape from any Prometheus-compatible
/// pull system (Prometheus itself, Grafana Agent, VictoriaMetrics vmagent).
async fn list_workflow_progress(State(state): State<AppState>) -> Json<HashMap<String, WorkflowProgress>> {
    Json(state.workflows.snapshot())
}

async fn prometheus_metrics(State(state): State<AppState>) -> String {
    let mut out = String::with_capacity(2048);
    let now_uptime = (Utc::now() - state.started_at).num_seconds().max(0);

    let agents = state.store.list_nodes().await.unwrap_or_default();
    let live_agents = {
        let cutoff = Utc::now() - chrono::Duration::seconds(30);
        agents.iter().filter(|n| n.last_seen_at >= cutoff).count()
    };
    let total_agents = agents.len();

    let instances = state.instances.snapshot_all();
    let alive = instances.iter().filter(|r| r.exited_at.is_none()).count();
    let exited = instances.iter().filter(|r| r.exited_at.is_some()).count();
    let failed = instances
        .iter()
        .filter(|r| r.exit_kind.as_deref() == Some("failed"))
        .count();

    let healthy = state
        .health
        .snapshot_all()
        .values()
        .filter(|h| h.status == "healthy")
        .count();
    let unhealthy = state
        .health
        .snapshot_all()
        .values()
        .filter(|h| h.status == "unhealthy")
        .count();

    let schedules = state.schedules.snapshot();
    let total_fires: u32 = schedules.values().map(|o| o.fire_count).sum();

    use std::fmt::Write;
    let _ = writeln!(
        out,
        "# HELP orion_controller_uptime_seconds Seconds since controller start"
    );
    let _ = writeln!(out, "# TYPE orion_controller_uptime_seconds gauge");
    let _ = writeln!(out, "orion_controller_uptime_seconds {now_uptime}");
    let _ = writeln!(out, "# HELP orion_agents_total Agents the controller has seen ever");
    let _ = writeln!(out, "# TYPE orion_agents_total gauge");
    let _ = writeln!(out, "orion_agents_total {total_agents}");
    let _ = writeln!(out, "# HELP orion_agents_live Agents whose last heartbeat was within 30s");
    let _ = writeln!(out, "# TYPE orion_agents_live gauge");
    let _ = writeln!(out, "orion_agents_live {live_agents}");
    let _ = writeln!(out, "# HELP orion_instances_alive Workload instances believed alive");
    let _ = writeln!(out, "# TYPE orion_instances_alive gauge");
    let _ = writeln!(out, "orion_instances_alive {alive}");
    let _ = writeln!(out, "# HELP orion_instances_exited Workload instances that have exited");
    let _ = writeln!(out, "# TYPE orion_instances_exited counter");
    let _ = writeln!(out, "orion_instances_exited {exited}");
    let _ = writeln!(out, "# HELP orion_instances_failed Workload instances that exited non-zero");
    let _ = writeln!(out, "# TYPE orion_instances_failed counter");
    let _ = writeln!(out, "orion_instances_failed {failed}");
    let _ = writeln!(out, "# HELP orion_health_status Instances reporting a health status");
    let _ = writeln!(out, "# TYPE orion_health_status gauge");
    let _ = writeln!(out, "orion_health_status{{status=\"healthy\"}} {healthy}");
    let _ = writeln!(out, "orion_health_status{{status=\"unhealthy\"}} {unhealthy}");
    let _ = writeln!(out, "# HELP orion_schedule_fires_total Total Schedule fires since controller start");
    let _ = writeln!(out, "# TYPE orion_schedule_fires_total counter");
    let _ = writeln!(out, "orion_schedule_fires_total {total_fires}");

    out
}

async fn list_health(State(state): State<AppState>) -> Json<HashMap<String, HealthSnapshot>> {
    let snap = state.health.snapshot_all();
    Json(snap.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}

// ============================================================ service.health

#[derive(Default)]
struct HealthRegistry {
    by_instance: Mutex<HashMap<Uuid, HealthSnapshot>>,
}

#[derive(Clone, Serialize)]
struct HealthSnapshot {
    service: String,
    status: String,
    consecutive_failures: u32,
    last_at: DateTime<Utc>,
    message: Option<String>,
}

impl HealthRegistry {
    fn record(&self, instance_id: Uuid, snap: HealthSnapshot) {
        self.by_instance.lock().unwrap().insert(instance_id, snap);
    }
    fn snapshot_all(&self) -> HashMap<Uuid, HealthSnapshot> {
        self.by_instance.lock().unwrap().clone()
    }
}

async fn subscribe_service_health(state: AppState) -> Result<()> {
    let subject = Topic::ServiceHealth.as_str().to_owned();
    let mut sub = state.nats.subscribe(subject.clone()).await?;
    info!("subscribed to {subject}");
    while let Some(msg) = sub.next().await {
        let env: Envelope<orion_bus::ServiceHealth> = match serde_json::from_slice(&msg.payload) {
            Ok(e) => e,
            Err(e) => {
                warn!(error = ?e, "malformed service.health envelope");
                continue;
            }
        };
        let h = env.payload;
        let instance_id = match Uuid::parse_str(&h.instance_id) {
            Ok(u) => u,
            Err(_) => continue,
        };
        let status = format!("{:?}", h.status).to_lowercase();
        state.health.record(
            instance_id,
            HealthSnapshot {
                service: h.service.0.clone(),
                status,
                consecutive_failures: h.consecutive_failures,
                last_at: Utc::now(),
                message: h.message,
            },
        );
    }
    Ok(())
}

// ============================================================ reconciler

async fn reconcile_loop(state: AppState) {
    let mut ticker = tokio::time::interval(Duration::from_secs(RECONCILE_TICK_SECONDS));
    ticker.tick().await; // skip immediate fire
    loop {
        ticker.tick().await;
        if let Err(e) = reconcile_once(&state).await {
            warn!(error = ?e, "reconcile tick failed");
        }
    }
}

/// One reconciliation pass.
///
/// For every Service the store knows about:
///   1. Count alive instances (exited_at == None).
///   2. If alive < `replicas` and the service has never been dispatched, dispatch
///      it (the agent fan-out covers the replica count).
///   3. For each Exited instance:
///        - `restart_policy: Always`     → re-dispatch + purge the dead record
///        - `restart_policy: OnFailure`  → re-dispatch iff exit_code != 0
///        - `restart_policy: Never`      → leave the record alone (Failed terminal state)
///
/// Tasks are NOT auto-restarted by the reconciler — Task is a one-shot resource,
/// re-firing it is the Schedule subsystem's job.
async fn reconcile_once(state: &AppState) -> Result<()> {
    use orion_types::{RestartPolicy, ServiceSpec};

    let services = state.store.list_by_kind("Service").await?;

    for svc in services {
        let name = svc.metadata.name.0.clone();
        let generation = svc.metadata.generation;
        let (spec, runtime): (ServiceSpec, _) = match &svc.body {
            ResourceBody::Service { spec, .. } => {
                let rt = match &spec.runtime {
                    Some(r) => r.clone(),
                    None => continue, // nothing to launch
                };
                (spec.clone(), rt)
            }
            _ => continue,
        };
        let desired = spec.replicas.unwrap_or(1).max(1);
        let alive = state.instances.count_alive("Service", &name);

        // Inspect every dead replica and classify it as "should restart" or
        // "terminal" (Never policy, or OnFailure + exit_code==0).
        let dead: Vec<InstanceRecord> = {
            let by_workload = state.instances.by_workload.lock().unwrap();
            let ids = by_workload
                .get(&("Service".to_owned(), name.clone()))
                .cloned()
                .unwrap_or_default();
            let by_id = state.instances.by_id.lock().unwrap();
            ids.iter()
                .filter_map(|i| by_id.get(i))
                .filter(|r| r.exited_at.is_some())
                .cloned()
                .collect()
        };
        let mut want_restart = 0u32;
        let mut terminal = 0u32;
        for d in &dead {
            let restart = match spec.restart_policy {
                RestartPolicy::Always => true,
                RestartPolicy::OnFailure => d.exit_code.unwrap_or(0) != 0,
                RestartPolicy::Never => false,
            };
            if restart {
                want_restart += 1;
                state.instances.purge(d.instance_id);
                info!(
                    workload = %name,
                    instance = %d.instance_id,
                    exit_code = ?d.exit_code,
                    policy = ?spec.restart_policy,
                    "reconciler: restarting replica"
                );
            } else {
                terminal += 1;
            }
        }

        // "Missing" = desired - alive - terminal. Slots that terminated under
        // a no-restart policy count as filled (the workload is complete on
        // that slot — re-running it would violate the user's policy).
        let alive_or_terminal = alive.saturating_add(terminal);
        let missing = desired.saturating_sub(alive_or_terminal);
        let to_launch = missing + want_restart;
        if to_launch == 0 {
            continue;
        }

        // Phase-5 placeholder: re-dispatch the whole Service. The agent
        // already fans out into `replicas` copies. Future revision should
        // dispatch *just* the missing slots, but that needs richer per-slot
        // tracking. For now: only re-dispatch if NO alive replicas remain
        // (the common "everything crashed" case).
        if alive == 0 && terminal < desired {
            info!(
                workload = %name,
                desired,
                "reconciler: re-dispatching Service (no alive replicas)"
            );
            match dispatch_workload(
                state,
                WorkloadKind::Service,
                ResourceName::from(name.as_str()),
                runtime,
                generation.unwrap_or(0),
                desired,
            )
            .await
            {
                Ok((node, id)) => {
                    state.instances.record_dispatch(id, "Service", &name, Some(&node), desired);
                }
                Err(e) => warn!(workload = %name, error = %e, "reconcile dispatch failed"),
            }
        } else if to_launch > 0 {
            // Some replicas alive, others dead — Phase 5 partial-fanout work.
            // Log so we know the reconciler noticed without doing the wrong thing.
            warn!(
                workload = %name,
                alive,
                desired,
                want_restart,
                "reconciler: partial under-provision not yet supported (Phase 5 follow-up)"
            );
        }
    }
    Ok(())
}

async fn scheduler_tick_loop(state: AppState) {
    let mut ticker = tokio::time::interval(Duration::from_secs(SCHEDULER_TICK_SECONDS));
    // Skip the immediate fire on startup.
    ticker.tick().await;
    loop {
        ticker.tick().await;
        if let Err(e) = scheduler_tick_once(&state).await {
            warn!(error = ?e, "scheduler tick failed");
        }
    }
}

async fn scheduler_tick_once(state: &AppState) -> Result<()> {
    let schedules = state.store.list_by_kind("Schedule").await?;
    let now = Utc::now();

    for sched in schedules {
        let name = sched.metadata.name.0.clone();
        let spec = match &sched.body {
            ResourceBody::Schedule { spec, .. } => spec.clone(),
            _ => continue,
        };

        // Parse cron — the `cron` crate expects 6 fields (with seconds).
        // Users author the 5-field POSIX form; prepend "0 " to align.
        let cron_expr = if spec.cron.split_whitespace().count() == 5 {
            format!("0 {}", spec.cron)
        } else {
            spec.cron.clone()
        };
        let parsed = match cron::Schedule::from_str(&cron_expr) {
            Ok(s) => s,
            Err(e) => {
                state.schedules.observe(&name, |o| {
                    o.last_error = Some(format!("cron parse: {e}"));
                    o.next_fire_at = None;
                });
                continue;
            }
        };

        // Find next fire after the last_fired_at (or armed_at if never).
        let armed = state
            .schedules
            .by_name
            .lock()
            .unwrap()
            .get(&name)
            .map(|o| o.armed_at);
        let after = match armed {
            Some(armed_at) => state
                .schedules
                .by_name
                .lock()
                .unwrap()
                .get(&name)
                .and_then(|o| o.last_fired_at)
                .unwrap_or(armed_at),
            None => {
                // First time seeing this Schedule.
                state.schedules.observe(&name, |o| {
                    o.next_fire_at = parsed.after(&now).next();
                });
                continue;
            }
        };

        let next = parsed.after(&after).next();
        state.schedules.observe(&name, |o| o.next_fire_at = next);

        if let Some(t) = next {
            if t > now {
                continue;
            }

            // Time to fire. Resolve task → runtime.
            let (workload_name, runtime) = match resolve_schedule_target(state, &spec).await {
                Ok(x) => x,
                Err(e) => {
                    state.schedules.observe(&name, |o| {
                        o.last_error = Some(e);
                    });
                    continue;
                }
            };

            match dispatch_workload(state, WorkloadKind::Task, workload_name, runtime, 1, 1).await {
                Ok((_node, id)) => {
                    state.schedules.observe(&name, |o| {
                        o.last_fired_at = Some(now);
                        o.last_instance_id = Some(id);
                        o.last_error = None;
                        o.fire_count += 1;
                        o.next_fire_at = parsed.after(&now).next();
                    });
                    info!(schedule = %name, instance = %id, "schedule fired");
                }
                Err(e) => {
                    state.schedules.observe(&name, |o| {
                        o.last_error = Some(e.message.clone());
                        // Don't bump last_fired_at on failure; we'll try again next tick.
                    });
                }
            }
        }
    }
    Ok(())
}

async fn resolve_schedule_target(
    state: &AppState,
    spec: &orion_types::ScheduleSpec,
) -> Result<(ResourceName, Runtime), String> {
    if let Some(template) = &spec.task_template {
        let rt = template
            .runtime
            .clone()
            .ok_or_else(|| "task_template has no runtime".to_owned())?;
        return Ok((ResourceName::from("inline-task"), rt));
    }
    if let Some(task_name) = &spec.task {
        let task = state
            .store
            .get_resource("Task", "_", &task_name.0)
            .await
            .map_err(|e| format!("store: {e}"))?
            .ok_or_else(|| format!("referenced Task '{}' not found", task_name.0))?;
        let rt = match task.body {
            ResourceBody::Task { spec, .. } => spec
                .runtime
                .clone()
                .ok_or_else(|| "referenced Task has no runtime".to_owned())?,
            _ => return Err("referenced resource is not a Task".to_owned()),
        };
        return Ok((task_name.clone(), rt));
    }
    Err("schedule has no task or task_template (should not happen if validate() ran)".to_owned())
}

async fn list_schedule_observations(
    State(state): State<AppState>,
) -> Json<HashMap<String, ScheduleObservation>> {
    Json(state.schedules.snapshot())
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

// ============================================================ diagnostics

#[derive(Deserialize)]
struct LogSearchQuery {
    q: String,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default = "default_search_limit")]
    limit: usize,
}

fn default_search_limit() -> usize {
    200
}

async fn search_logs(
    State(state): State<AppState>,
    Query(q): Query<LogSearchQuery>,
) -> Json<Vec<SearchHit>> {
    Json(state.logs.search(
        &q.q,
        q.kind.as_deref(),
        q.name.as_deref(),
        q.limit,
    ))
}

async fn list_all_instances(State(state): State<AppState>) -> Json<Vec<InstanceRecord>> {
    Json(state.instances.snapshot_all())
}

#[derive(Serialize)]
struct StopOutcome {
    kind: String,
    name: String,
    stopped: u32,
    nodes: Vec<String>,
}

async fn stop_workload(
    State(state): State<AppState>,
    Path((kind, name)): Path<(String, String)>,
) -> Result<Json<StopOutcome>, ApiError> {
    let instances = state.instances.drain_for(&kind, &name);
    if instances.is_empty() {
        return Err(ApiError::not_found(format!(
            "no live instances of {kind}/{name}"
        )));
    }
    let mut nodes: Vec<String> = Vec::new();
    let mut stopped: u32 = 0;
    for rec in &instances {
        let Some(node) = rec.node.clone() else { continue };
        let envelope = Envelope::new(
            None,
            ControlStop {
                instance_id: rec.instance_id,
                reason: Some(format!("stop_workload {}/{}", kind, name)),
                grace_seconds: Some(5),
            },
        );
        let bytes = serde_json::to_vec(&envelope).expect("encode ControlStop");
        let subject = Topic::ControlStop.for_node(&node);
        if state.nats.publish(subject, bytes.into()).await.is_ok() {
            stopped += 1;
            if !nodes.contains(&node) {
                nodes.push(node);
            }
        }
    }
    Ok(Json(StopOutcome {
        kind,
        name,
        stopped,
        nodes,
    }))
}

#[derive(Serialize)]
struct RestartOutcome {
    kind: String,
    name: String,
    stopped: u32,
    redispatched: bool,
    node: Option<String>,
    instance_id: Option<Uuid>,
}

async fn restart_workload(
    State(state): State<AppState>,
    Path((kind, name)): Path<(String, String)>,
) -> Result<Json<RestartOutcome>, ApiError> {
    let stop_outcome = stop_workload(State(state.clone()), Path((kind.clone(), name.clone())))
        .await
        .ok();
    let stopped = stop_outcome
        .as_ref()
        .map(|o| o.0.stopped)
        .unwrap_or(0);
    // Brief pause so the agent can reap the children before we re-dispatch.
    tokio::time::sleep(Duration::from_millis(300)).await;
    // Re-dispatch via the existing route handler — share the same path.
    let outcome = dispatch_resource(
        State(state),
        Path((kind.clone(), name.clone())),
    )
    .await?;
    Ok(Json(RestartOutcome {
        kind,
        name,
        stopped,
        redispatched: true,
        node: Some(outcome.0.node),
        instance_id: Some(outcome.0.instance_id),
    }))
}

#[derive(Serialize)]
struct DiagSystem {
    controller: ControllerDiag,
    agents: usize,
    nodes: Vec<DiagNode>,
    instances: InstanceStats,
    schedules: ScheduleStats,
    logs: LogStats,
    nats: NatsDiag,
}

#[derive(Serialize)]
struct ControllerDiag {
    started_at: DateTime<Utc>,
    uptime_seconds: i64,
    nats_url: String,
    auth_disabled: bool,
    version: &'static str,
}

#[derive(Serialize)]
struct DiagNode {
    node_id: String,
    agent_version: String,
    last_seen_at: String,
    seconds_since_seen: i64,
}

#[derive(Serialize)]
struct InstanceStats {
    total: usize,
    by_workload: Vec<WorkloadInstanceCount>,
}

#[derive(Serialize)]
struct WorkloadInstanceCount {
    kind: String,
    name: String,
    instance_count: usize,
}

#[derive(Serialize)]
struct ScheduleStats {
    armed: usize,
    fired_total: u32,
}

#[derive(Serialize)]
struct LogStats {
    buffered_lines: usize,
    workloads_with_logs: usize,
}

#[derive(Serialize)]
struct NatsDiag {
    connected: bool,
    url: String,
    monitoring_url: Option<String>,
    server_info: Option<serde_json::Value>,
}

async fn diag_system(State(state): State<AppState>) -> Result<Json<DiagSystem>, ApiError> {
    let now = Utc::now();
    let nodes_raw = state.store.list_nodes().await.map_err(ApiError::store)?;
    let nodes = nodes_raw
        .into_iter()
        .map(|n| {
            let secs = (now - n.last_seen_at).num_seconds();
            DiagNode {
                node_id: n.node_id,
                agent_version: n.agent_version,
                last_seen_at: n.last_seen_at.to_rfc3339(),
                seconds_since_seen: secs,
            }
        })
        .collect::<Vec<_>>();

    let inst = state.instances.snapshot_all();
    let mut by_key: HashMap<(String, String), usize> = HashMap::new();
    for r in &inst {
        *by_key.entry((r.kind.clone(), r.name.clone())).or_default() += 1;
    }
    let mut by_workload: Vec<_> = by_key
        .into_iter()
        .map(|((k, n), c)| WorkloadInstanceCount { kind: k, name: n, instance_count: c })
        .collect();
    by_workload.sort_by(|a, b| (a.kind.clone(), a.name.clone()).cmp(&(b.kind.clone(), b.name.clone())));

    let sched_snap = state.schedules.snapshot();
    let schedules = ScheduleStats {
        armed: sched_snap.len(),
        fired_total: sched_snap.values().map(|o| o.fire_count).sum(),
    };

    let logs = LogStats {
        buffered_lines: state.logs.line_count(),
        workloads_with_logs: state.logs.rings.lock().unwrap().len(),
    };

    let monitoring_url = derive_nats_monitoring_url(&state.nats_url);
    let server_info = if let Some(url) = &monitoring_url {
        fetch_nats_varz(url).await.ok()
    } else {
        None
    };
    let nats = NatsDiag {
        connected: server_info.is_some(),
        url: state.nats_url.clone(),
        monitoring_url,
        server_info,
    };

    let agents = nodes.iter().filter(|n| n.seconds_since_seen < 30).count();
    let controller = ControllerDiag {
        started_at: state.started_at,
        uptime_seconds: (now - state.started_at).num_seconds(),
        nats_url: state.nats_url.clone(),
        auth_disabled: state.auth.is_disabled(),
        version: env!("CARGO_PKG_VERSION"),
    };
    Ok(Json(DiagSystem {
        controller,
        agents,
        nodes,
        instances: InstanceStats {
            total: inst.len(),
            by_workload,
        },
        schedules,
        logs,
        nats,
    }))
}

fn derive_nats_monitoring_url(nats_url: &str) -> Option<String> {
    // nats://host:4222 → http://host:8222 (the conventional monitoring port)
    let host = nats_url
        .strip_prefix("nats://")
        .unwrap_or(nats_url)
        .split('/')
        .next()
        .unwrap_or("");
    let host = host.split(':').next().unwrap_or("");
    if host.is_empty() {
        return None;
    }
    Some(format!("http://{host}:8222"))
}

async fn fetch_nats_varz(monitoring_url: &str) -> Result<serde_json::Value, ApiError> {
    let url = format!("{monitoring_url}/varz");
    let resp = reqwest::get(&url)
        .await
        .map_err(|e| ApiError::internal(format!("nats /varz: {e}")))?;
    if !resp.status().is_success() {
        return Err(ApiError::internal(format!(
            "nats /varz status {}",
            resp.status()
        )));
    }
    resp.json::<serde_json::Value>()
        .await
        .map_err(|e| ApiError::internal(format!("nats /varz parse: {e}")))
}

#[derive(Serialize)]
struct DiagJetStream {
    monitoring_url: Option<String>,
    accounts: Option<serde_json::Value>,
    streams: Vec<JsStream>,
    consumers: Vec<JsConsumer>,
    raw_jsz: Option<serde_json::Value>,
}

#[derive(Serialize)]
struct JsStream {
    name: String,
    subjects: Vec<String>,
    messages: u64,
    bytes: u64,
    first_seq: u64,
    last_seq: u64,
    consumer_count: u32,
}

#[derive(Serialize)]
struct JsConsumer {
    stream: String,
    name: String,
    num_pending: u64,
    num_ack_pending: u64,
    delivered: u64,
    last_ack_floor: u64,
}

async fn diag_jetstream(
    State(state): State<AppState>,
) -> Result<Json<DiagJetStream>, ApiError> {
    let Some(monitoring_url) = derive_nats_monitoring_url(&state.nats_url) else {
        return Ok(Json(DiagJetStream {
            monitoring_url: None,
            accounts: None,
            streams: vec![],
            consumers: vec![],
            raw_jsz: None,
        }));
    };
    let url = format!("{monitoring_url}/jsz?accounts=true&streams=true&consumers=true&config=true");
    let resp = reqwest::get(&url)
        .await
        .map_err(|e| ApiError::internal(format!("nats /jsz: {e}")))?;
    if !resp.status().is_success() {
        return Err(ApiError::internal(format!(
            "nats /jsz status {}",
            resp.status()
        )));
    }
    let raw: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| ApiError::internal(format!("nats /jsz parse: {e}")))?;

    let mut streams: Vec<JsStream> = Vec::new();
    let mut consumers: Vec<JsConsumer> = Vec::new();

    // jsz layout: { account_details: [ { name, stream_detail: [ { config: { name, subjects }, state: {...}, consumer_detail: [...] }, ... ] } ] }
    if let Some(accounts) = raw.get("account_details").and_then(|v| v.as_array()) {
        for acct in accounts {
            let stream_detail = acct
                .get("stream_detail")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            for s in &stream_detail {
                let config = s.get("config").cloned().unwrap_or(serde_json::Value::Null);
                let state_obj = s.get("state").cloned().unwrap_or(serde_json::Value::Null);
                let name = config.get("name").and_then(|v| v.as_str()).unwrap_or("").to_owned();
                let subjects = config
                    .get("subjects")
                    .and_then(|v| v.as_array())
                    .map(|a| a.iter().filter_map(|s| s.as_str().map(String::from)).collect())
                    .unwrap_or_default();
                let messages = state_obj.get("messages").and_then(|v| v.as_u64()).unwrap_or(0);
                let bytes = state_obj.get("bytes").and_then(|v| v.as_u64()).unwrap_or(0);
                let first_seq = state_obj.get("first_seq").and_then(|v| v.as_u64()).unwrap_or(0);
                let last_seq = state_obj.get("last_seq").and_then(|v| v.as_u64()).unwrap_or(0);
                let cons_arr = s
                    .get("consumer_detail")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                let consumer_count = cons_arr.len() as u32;
                streams.push(JsStream {
                    name: name.clone(),
                    subjects,
                    messages,
                    bytes,
                    first_seq,
                    last_seq,
                    consumer_count,
                });
                for c in &cons_arr {
                    let cname = c.get("name").and_then(|v| v.as_str()).unwrap_or("").to_owned();
                    let num_pending = c.get("num_pending").and_then(|v| v.as_u64()).unwrap_or(0);
                    let num_ack_pending = c.get("num_ack_pending").and_then(|v| v.as_u64()).unwrap_or(0);
                    let delivered = c
                        .get("delivered")
                        .and_then(|v| v.get("consumer_seq"))
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    let last_ack_floor = c
                        .get("ack_floor")
                        .and_then(|v| v.get("consumer_seq"))
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    consumers.push(JsConsumer {
                        stream: name.clone(),
                        name: cname,
                        num_pending,
                        num_ack_pending,
                        delivered,
                        last_ack_floor,
                    });
                }
            }
        }
    }

    Ok(Json(DiagJetStream {
        monitoring_url: Some(monitoring_url),
        accounts: raw.get("account_details").cloned(),
        streams,
        consumers,
        raw_jsz: Some(raw),
    }))
}

// ============================================================ error mapping

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.status, self.message)
    }
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
