//! orion-sidecar binary. Polls the controller's log archive for another
//! Service's stdout/stderr and republishes each new line as a queue
//! message. Optionally filters by a regex (SIDECAR_FILTER_REGEX).

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use orion_sidecar::{compile_filter, line_matches, SidecarEvent};
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Deserialize)]
struct LogArchiveEntry {
    at: DateTime<Utc>,
    kind: String,
    name: String,
    node_id: String,
    stream: String,
    line: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let controller =
        std::env::var("ORION_CONTROLLER_URL").unwrap_or_else(|_| "http://127.0.0.1:7878".into());
    let token = std::env::var("ORION_CLUSTER_TOKEN").ok();
    let source = std::env::var("SIDECAR_SOURCE_SERVICE")
        .context("SIDECAR_SOURCE_SERVICE must be set")?;
    let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let subject = std::env::var("ORION_QUEUE_SUBJECT")
        .context("ORION_QUEUE_SUBJECT must be set")?;
    let stream = std::env::var("ORION_QUEUE_STREAM")
        .context("ORION_QUEUE_STREAM must be set")?;
    let interval = std::env::var("SIDECAR_INTERVAL_SECONDS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(5);
    let filter = match std::env::var("SIDECAR_FILTER_REGEX") {
        Ok(s) if !s.is_empty() => Some(compile_filter(&s)?),
        _ => None,
    };

    let nc = orion_bus::client::connect(&nats_url, token.as_deref()).await?;
    let js = async_nats::jetstream::new(nc);
    let cfg = async_nats::jetstream::stream::Config {
        name: stream,
        subjects: vec![subject.clone()],
        ..Default::default()
    };
    let _ = orion_bus::client::ensure_stream(&js, cfg).await;

    let http = reqwest::Client::builder().build()?;
    let mut since: Option<DateTime<Utc>> = None;

    tracing::info!(source = %source, subject = %subject, interval, "orion-sidecar started");
    let mut ticker = tokio::time::interval(Duration::from_secs(interval.max(1)));
    loop {
        ticker.tick().await;
        let mut req = http
            .get(format!(
                "{}/v1/logs-archive/Service/{}",
                controller.trim_end_matches('/'),
                source
            ))
            .query(&[("limit", "1000")]);
        if let Some(ts) = since {
            req = req.query(&[("since", ts.to_rfc3339())]);
        }
        if let Some(t) = &token {
            req = req.bearer_auth(t);
        }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "log fetch failed");
                continue;
            }
        };
        if !resp.status().is_success() {
            tracing::warn!(status = ?resp.status(), "log fetch non-2xx");
            continue;
        }
        let entries: Vec<LogArchiveEntry> = match resp.json().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "log decode failed");
                continue;
            }
        };
        // The archive returns newest-first; reverse so we publish in time order.
        for e in entries.into_iter().rev() {
            if !line_matches(filter.as_ref(), &e.line) {
                continue;
            }
            let event = SidecarEvent {
                source_service: e.name.clone(),
                stream: e.stream,
                at: e.at.to_rfc3339(),
                line: e.line,
                node_id: e.node_id,
                _subject: subject.clone(),
            };
            since = Some(e.at);
            let payload = serde_json::to_vec(&event)?;
            let _ = js.publish(subject.clone(), payload.into()).await?.await;
        }
        let _ = source; // silence unused warning when binary used purely as smoke test
        let _ = stream;
    }
}
