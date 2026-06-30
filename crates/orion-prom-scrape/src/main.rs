//! orion-prom-scrape binary — combines two integrations:
//!
//! 1. Scraper mode (--mode scrape): periodically GET each URL in
//!    SCRAPE_TARGETS (comma-separated), parse the Prometheus text
//!    body, and publish each sample to a queue.
//!
//! 2. Alertmanager receiver mode (--mode alertmanager): listen on
//!    $BIND, accept POSTs from Prometheus Alertmanager, publish each
//!    alert to a queue (downstream consumers can dispatch Tasks etc).

use anyhow::{Context, Result};
use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use clap::Parser;
use orion_prom_scrape::{parse_scrape, AlertmanagerPayload, ScrapedSample};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(name = "orion-prom-scrape")]
struct Args {
    /// `scrape` or `alertmanager`.
    #[arg(long, default_value = "scrape")]
    mode: String,
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
    let subject = std::env::var("ORION_QUEUE_SUBJECT").context("ORION_QUEUE_SUBJECT")?;
    let stream = std::env::var("ORION_QUEUE_STREAM").context("ORION_QUEUE_STREAM")?;

    let nc = orion_bus::client::connect(&nats_url, token.as_deref()).await?;
    let js = async_nats::jetstream::new(nc);
    let cfg = async_nats::jetstream::stream::Config {
        name: stream,
        subjects: vec![subject.clone()],
        ..Default::default()
    };
    let _ = orion_bus::client::ensure_stream(&js, cfg).await;

    match args.mode.as_str() {
        "scrape" => run_scrape(js, subject).await,
        "alertmanager" => run_alertmanager(js, subject).await,
        other => anyhow::bail!("unknown mode: {other}"),
    }
}

async fn run_scrape(js: async_nats::jetstream::Context, subject: String) -> Result<()> {
    let targets: Vec<String> = std::env::var("SCRAPE_TARGETS")
        .context("SCRAPE_TARGETS")?
        .split(',')
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .collect();
    let interval = std::env::var("SCRAPE_INTERVAL_SECONDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(15u64);
    let http = reqwest::Client::builder().build()?;
    tracing::info!(?targets, interval, "orion-prom-scrape (scrape) started");
    let mut ticker = tokio::time::interval(Duration::from_secs(interval.max(1)));
    loop {
        ticker.tick().await;
        for url in &targets {
            let resp = match http.get(url).send().await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(url, error = %e, "scrape error");
                    continue;
                }
            };
            if !resp.status().is_success() {
                tracing::warn!(url, status = ?resp.status(), "non-2xx");
                continue;
            }
            let body = match resp.text().await {
                Ok(b) => b,
                Err(_) => continue,
            };
            let parsed = parse_scrape(&body);
            let now = Utc::now().to_rfc3339();
            for (name, labels, value) in parsed {
                let sample = ScrapedSample {
                    at: now.clone(),
                    source: url.clone(),
                    name,
                    labels,
                    value,
                };
                if let Ok(payload) = serde_json::to_vec(&sample) {
                    let _ = js.publish(subject.clone(), payload.into()).await?.await;
                }
            }
        }
    }
}

#[derive(Clone)]
struct AlertState {
    js: async_nats::jetstream::Context,
    subject: String,
}

async fn run_alertmanager(js: async_nats::jetstream::Context, subject: String) -> Result<()> {
    let bind = std::env::var("BIND").unwrap_or_else(|_| "0.0.0.0:9090".into());
    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route(
            "/alerts",
            post(
                |State(state): State<Arc<AlertState>>, Json(payload): Json<AlertmanagerPayload>| async move {
                    for alert in payload.alerts {
                        let body = json!({
                            "receiver": payload.receiver,
                            "status": payload.status,
                            "alert_status": alert.status,
                            "labels": alert.labels,
                            "annotations": alert.annotations,
                            "starts_at": alert.starts_at,
                            "at": Utc::now().to_rfc3339(),
                            "_subject": state.subject,
                        });
                        if let Ok(bytes) = serde_json::to_vec(&body) {
                            let _ = state
                                .js
                                .publish(state.subject.clone(), bytes.into())
                                .await;
                        }
                    }
                    StatusCode::OK
                },
            ),
        )
        .with_state(Arc::new(AlertState { js, subject }));
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(bind = %bind, "orion-prom-scrape (alertmanager) listening");
    axum::serve(listener, app).await?;
    Ok(())
}
