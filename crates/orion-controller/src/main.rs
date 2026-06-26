//! OrionMesh controller.
//!
//! - Subscribes to `orion.heartbeat` and `orion.node.inventory`; updates the
//!   observed-node cache in SQLite.
//! - Serves `/health`, `/v1/nodes`, `/v1/resources/{kind}`, `POST /v1/resources/apply`.
//! - HTTP authenticated via shared cluster token (or open in dev mode).
//!
//! Reconciler, scheduler-driven dispatch, and the Find API land in later phases.

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    middleware::from_fn_with_state,
    response::IntoResponse,
    routing::{get, post},
};
use serde::Deserialize;
use tower_http::cors::{Any, CorsLayer};
use clap::Parser;
use futures::StreamExt;
use orion_auth::AuthMode;
use orion_bus::{Envelope, Heartbeat, NodeInventory, Topic};
use orion_store::Store;
use orion_types::Resource;
use serde::Serialize;
use std::{net::SocketAddr, sync::Arc};
use tracing::{info, warn};

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

#[derive(Clone)]
struct AppState {
    store: Arc<Store>,
    auth: AuthMode,
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
    let state = AppState { store: store.clone(), auth: auth.clone() };

    let nats = orion_auth::nats::connect_options(&auth)
        .name("orion-controller")
        .connect(&args.nats_url)
        .await
        .context("connecting to NATS")?;
    info!("connected to NATS");

    tokio::spawn({
        let nats = nats.clone();
        let store = store.clone();
        async move {
            if let Err(e) = subscribe_heartbeats(nats, store).await {
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

    // CORS: the orion-ui server runs on a different port (7879) than the API (7878),
    // so the browser treats it as cross-origin. Permissive is fine because the auth
    // layer (Authorization: Bearer) is what gates writes — CORS isn't the security boundary.
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
        .route("/v1/dispatch/:kind/:name", post(dispatch_stub))
        .layer(from_fn_with_state(auth, orion_auth::http::require_bearer))
        // /health is intentionally outside the auth layer — useful for liveness probes.
        .route("/health", get(health))
        .layer(cors)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    axum::serve(listener, router).await?;
    Ok(())
}

async fn subscribe_heartbeats(client: async_nats::Client, store: Arc<Store>) -> Result<()> {
    let mut sub = client.subscribe(Topic::Heartbeat.as_str().to_owned()).await?;
    info!("subscribed to {}", Topic::Heartbeat.as_str());
    while let Some(msg) = sub.next().await {
        match serde_json::from_slice::<Envelope<Heartbeat>>(&msg.payload) {
            Ok(env) => {
                let hb = env.payload;
                if let Err(e) = store.touch_node(&hb.node_id.0, &hb.agent_version).await {
                    warn!(error = ?e, "store touch_node failed");
                }
            }
            Err(e) => warn!(error = ?e, "malformed heartbeat envelope"),
        }
    }
    Ok(())
}

async fn subscribe_inventory(client: async_nats::Client, store: Arc<Store>) -> Result<()> {
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

// ----------------------------------------------------------- HTTP handlers

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

/// All resource kinds the API surface knows about — for UI tab generation.
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
        // Validate-only: no store mutation. UI uses this for the "Validate" button.
        return Ok(Json(ApplyOutcome {
            kind,
            name,
            generation: 0,
            dry_run: true,
        }));
    }

    let generation = state
        .store
        .upsert_resource(&r)
        .await
        .map_err(ApiError::store)?;
    Ok(Json(ApplyOutcome {
        kind,
        name,
        generation,
        dry_run: false,
    }))
}

/// Placeholder for the scheduler dispatch surface — returns 501 with a clear
/// message so the UI can show what's coming without lying about behaviour.
async fn dispatch_stub(
    Path((kind, name)): Path<(String, String)>,
) -> impl IntoResponse {
    (
        StatusCode::NOT_IMPLEMENTED,
        format!(
            "Dispatch for {kind}/{name} is Phase 5. The reconciler + scheduler \
             that turns 'desired' into 'running on agent X' isn't built yet.\n\n\
             Today, applying a resource stores it in SQLite. When Phase 5 ships, \
             this endpoint will publish orion.control.<node>.run to the chosen \
             node and you'll see it appear via orion.service.register."
        ),
    )
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

// ----------------------------------------------------------- error mapping

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
    fn store(e: orion_store::StoreError) -> Self {
        Self { status: StatusCode::INTERNAL_SERVER_ERROR, message: e.to_string() }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (self.status, self.message).into_response()
    }
}
