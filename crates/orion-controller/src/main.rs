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
        async move {
            if let Err(e) = subscribe_logs(nats, logs, instances).await {
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
            "Project", "Secret", "Volume", "Network", "Queue", "Runtime", "Capability",
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
/// Picks a node (currently: most recent live), generates the base instance_id,
/// publishes a ControlRun envelope carrying `replicas`. The agent fans out
/// into N copies (each with its own derived id).
async fn dispatch_workload(
    state: &AppState,
    kind: WorkloadKind,
    name: ResourceName,
    runtime: Runtime,
    generation: u64,
    replicas: u32,
) -> Result<(String, Uuid), ApiError> {
    let node = state.node_id.get().ok_or_else(|| {
        ApiError::bad_request("no live nodes — start an agent first")
    })?;
    let instance_id = Uuid::new_v4();
    let envelope = Envelope::new(
        None,
        ControlRun {
            instance_id,
            kind,
            name,
            runtime,
            generation,
            replicas,
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

// ============================================================ scheduler tick

const SCHEDULER_TICK_SECONDS: u64 = 5;

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
