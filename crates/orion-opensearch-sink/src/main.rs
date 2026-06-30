//! orion-opensearch-sink binary. Polls the controller for new log
//! lines and ships them to OpenSearch via the bulk API.
//!
//! Env:
//!   OPENSEARCH_URL          base URL, e.g. https://search.local:9200
//!   OPENSEARCH_INDEX        index name (default: orion-logs)
//!   OPENSEARCH_USERNAME     optional basic auth
//!   OPENSEARCH_PASSWORD     optional basic auth
//!   ORION_CONTROLLER_URL    controller base URL
//!   LOG_SOURCE_KIND         "Service" (default) or "Task"
//!   LOG_SOURCE_NAMES        comma-separated workload names (required)
//!   SINK_INTERVAL_SECONDS   how often to poll (default 10)

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use orion_opensearch_sink::{build_bulk_body, LogDoc};
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
    let endpoint = std::env::var("OPENSEARCH_URL").context("OPENSEARCH_URL")?;
    let index = std::env::var("OPENSEARCH_INDEX").unwrap_or_else(|_| "orion-logs".into());
    let user = std::env::var("OPENSEARCH_USERNAME").ok();
    let pass = std::env::var("OPENSEARCH_PASSWORD").ok();
    let kind = std::env::var("LOG_SOURCE_KIND").unwrap_or_else(|_| "Service".into());
    let names: Vec<String> = std::env::var("LOG_SOURCE_NAMES")
        .context("LOG_SOURCE_NAMES (comma-separated workload names)")?
        .split(',')
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .collect();
    let interval = std::env::var("SINK_INTERVAL_SECONDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10u64);

    let http = reqwest::Client::builder()
        .danger_accept_invalid_certs(true) // common for self-hosted opensearch
        .build()?;
    let mut since: std::collections::HashMap<String, DateTime<Utc>> = Default::default();

    tracing::info!(endpoint, index, ?names, "orion-opensearch-sink started");
    let mut ticker = tokio::time::interval(Duration::from_secs(interval.max(1)));
    loop {
        ticker.tick().await;
        let mut batch: Vec<LogDoc> = Vec::new();
        for name in &names {
            let mut req = http
                .get(format!(
                    "{}/v1/logs-archive/{kind}/{name}",
                    controller.trim_end_matches('/')
                ))
                .query(&[("limit", "1000")]);
            if let Some(ts) = since.get(name) {
                req = req.query(&[("since", ts.to_rfc3339())]);
            }
            if let Some(t) = &token {
                req = req.bearer_auth(t);
            }
            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(workload = %name, error = %e, "fetch error");
                    continue;
                }
            };
            if !resp.status().is_success() {
                tracing::warn!(workload = %name, status = ?resp.status(), "non-2xx");
                continue;
            }
            let entries: Vec<LogArchiveEntry> = match resp.json().await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(error = %e, "decode error");
                    continue;
                }
            };
            for e in entries.into_iter().rev() {
                since
                    .entry(name.clone())
                    .and_modify(|t| if e.at > *t { *t = e.at })
                    .or_insert(e.at);
                batch.push(LogDoc {
                    at: e.at.to_rfc3339(),
                    kind: e.kind,
                    name: e.name,
                    node_id: e.node_id,
                    stream: e.stream,
                    line: e.line,
                });
            }
        }
        if batch.is_empty() {
            continue;
        }
        let body = build_bulk_body(&index, &batch);
        let mut req = http
            .post(format!("{}/_bulk", endpoint.trim_end_matches('/')))
            .header("content-type", "application/x-ndjson")
            .body(body);
        if let (Some(u), Some(p)) = (&user, &pass) {
            req = req.basic_auth(u, Some(p));
        }
        match req.send().await {
            Ok(r) if r.status().is_success() => {
                tracing::info!(count = batch.len(), "shipped batch");
            }
            Ok(r) => {
                tracing::warn!(status = ?r.status(), "opensearch returned non-2xx");
            }
            Err(e) => tracing::warn!(error = %e, "opensearch ship failed"),
        }
    }
}
