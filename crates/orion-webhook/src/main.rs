//! orion-webhook binary. Run as a Service; listens on $BIND, converts
//! POSTs to queue messages.
//!
//!     WEBHOOK_SECRET   optional HMAC-SHA256 secret. When set, requests
//!                      must include `X-Orion-Signature: sha256=<hex>`.
//!                      Accepts `X-Hub-Signature-256` (GitHub) and
//!                      `X-Signature-256` (generic) as aliases.
//!
//! Anything POSTed to `/hook/<route>` lands on the queue's subject with
//! `.route` appended (so consumers can filter via `--subject-from` if
//! they want).

use anyhow::{Context, Result};
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Router,
};
use chrono::Utc;
use clap::Parser;
use orion_webhook::{verify_hmac_sha256, WebhookEvent};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::signal;

#[derive(Parser, Debug)]
#[command(name = "orion-webhook")]
struct Args {
    /// Listen address.
    #[arg(long, env = "BIND", default_value = "0.0.0.0:8080")]
    bind: String,
}

#[derive(Clone)]
struct AppState {
    subject: String,
    secret: Option<String>,
    js: async_nats::jetstream::Context,
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
    let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let token = std::env::var("ORION_CLUSTER_TOKEN").ok();
    let subject = std::env::var("ORION_QUEUE_SUBJECT")
        .context("ORION_QUEUE_SUBJECT must be set")?;
    let stream = std::env::var("ORION_QUEUE_STREAM")
        .context("ORION_QUEUE_STREAM must be set")?;
    let secret = std::env::var("WEBHOOK_SECRET").ok();

    let nc = orion_bus::client::connect(&nats_url, token.as_deref()).await?;
    let js = async_nats::jetstream::new(nc);
    let cfg = async_nats::jetstream::stream::Config {
        name: stream,
        subjects: vec![subject.clone(), format!("{subject}.>")],
        ..Default::default()
    };
    let _ = orion_bus::client::ensure_stream(&js, cfg).await;

    let state = Arc::new(AppState { subject, secret, js });
    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/hook", post(receive))
        .route("/hook/", post(receive))
        .route("/hook/:route", post(receive_route))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(&args.bind).await?;
    tracing::info!(bind = %args.bind, "orion-webhook listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let _ = signal::ctrl_c().await;
}

async fn receive(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    receive_inner(state, None, headers, body).await
}

async fn receive_route(
    State(state): State<Arc<AppState>>,
    Path(route): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    receive_inner(state, Some(route), headers, body).await
}

async fn receive_inner(
    state: Arc<AppState>,
    route: Option<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    // Verify signature if a secret is configured.
    if let Some(secret) = &state.secret {
        let sig = headers
            .get("X-Orion-Signature")
            .or_else(|| headers.get("X-Hub-Signature-256"))
            .or_else(|| headers.get("X-Signature-256"))
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if !verify_hmac_sha256(secret, &body, sig) {
            tracing::warn!("rejected: invalid signature");
            return Err(StatusCode::UNAUTHORIZED);
        }
    }

    let subj = match &route {
        Some(r) => format!("{}.{r}", state.subject),
        None => state.subject.clone(),
    };

    let raw_body = String::from_utf8_lossy(&body).into_owned();
    let body_json: Value =
        serde_json::from_str(&raw_body).unwrap_or_else(|_| Value::String(raw_body.clone()));
    let mut headers_obj = serde_json::Map::new();
    for (k, v) in headers.iter() {
        if let Ok(s) = v.to_str() {
            headers_obj.insert(k.to_string(), json!(s));
        }
    }
    let event = WebhookEvent {
        at: Utc::now().to_rfc3339(),
        headers: Value::Object(headers_obj),
        body: body_json,
        raw_body,
        _subject: subj.clone(),
    };
    let payload = match serde_json::to_vec(&event) {
        Ok(v) => v,
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };
    if let Err(e) = state.js.publish(subj.clone(), payload.into()).await {
        tracing::error!(error = %e, "publish failed");
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }
    Ok(StatusCode::OK)
}
