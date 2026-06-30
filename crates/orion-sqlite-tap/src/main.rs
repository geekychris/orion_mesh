//! orion-sqlite-tap binary. Polls a SQLite table's rowid and publishes
//! new rows to a queue.

use anyhow::{Context, Result};
use orion_sqlite_tap::{build_select, CdcEvent};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::Column;
use sqlx::Row;
use std::str::FromStr;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let db_url = std::env::var("SQLITE_URL").context("SQLITE_URL (e.g. sqlite:///tmp/app.db)")?;
    let table = std::env::var("SQLITE_TABLE").context("SQLITE_TABLE")?;
    let subject = std::env::var("ORION_QUEUE_SUBJECT").context("ORION_QUEUE_SUBJECT")?;
    let stream = std::env::var("ORION_QUEUE_STREAM").context("ORION_QUEUE_STREAM")?;
    let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let token = std::env::var("ORION_CLUSTER_TOKEN").ok();
    let interval = std::env::var("TAP_INTERVAL_SECONDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2u64);
    let start_from_zero = std::env::var("TAP_FROM_ZERO")
        .map(|v| matches!(v.as_str(), "1" | "true"))
        .unwrap_or(false);

    // CDC tap is strictly read-only. Open with mode=ro so we don't deadlock
    // with an upstream writer that holds the WAL lock.
    let opts = SqliteConnectOptions::from_str(&db_url)?
        .read_only(true)
        .create_if_missing(false);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(opts)
        .await?;

    let nc = orion_bus::client::connect(&nats_url, token.as_deref()).await?;
    let js = async_nats::jetstream::new(nc);
    let cfg = async_nats::jetstream::stream::Config {
        name: stream,
        subjects: vec![subject.clone()],
        ..Default::default()
    };
    let _ = orion_bus::client::ensure_stream(&js, cfg).await;

    let mut cursor: i64 = if start_from_zero {
        0
    } else {
        let sql = format!("SELECT COALESCE(MAX(rowid), 0) AS m FROM \"{table}\"");
        let row = sqlx::query(&sql).fetch_one(&pool).await?;
        row.try_get("m").unwrap_or(0)
    };

    tracing::info!(table, subject, interval, cursor, "orion-sqlite-tap started");
    let mut ticker = tokio::time::interval(Duration::from_secs(interval.max(1)));
    loop {
        ticker.tick().await;
        let sql = build_select(&table, cursor);
        let rows = match sqlx::query(&sql).fetch_all(&pool).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "select failed");
                continue;
            }
        };
        tracing::debug!(count = rows.len(), cursor, "polled");
        for r in rows {
            // We aliased rowid as _orion_rowid in build_select so sqlx can pull
            // it by name (raw "rowid" returns ColumnNotFound on the row decode).
            let rowid: i64 = match r.try_get("_orion_rowid") {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(error = %e, "rowid extract failed");
                    continue;
                }
            };
            let mut obj = serde_json::Map::new();
            for col in r.columns() {
                let name = col.name();
                if name == "_orion_rowid" {
                    continue;
                }
                let val = column_to_json(&r, name);
                obj.insert(name.to_owned(), val);
            }
            let ev = CdcEvent {
                at: chrono::Utc::now().to_rfc3339(),
                table: table.clone(),
                rowid,
                row: serde_json::Value::Object(obj),
                _subject: subject.clone(),
            };
            if let Ok(payload) = serde_json::to_vec(&ev) {
                let _ = js.publish(subject.clone(), payload.into()).await?.await;
                tracing::debug!(rowid, "published");
            }
            cursor = rowid;
        }
    }
}

fn column_to_json(row: &sqlx::sqlite::SqliteRow, name: &str) -> serde_json::Value {
    if let Ok(v) = row.try_get::<i64, _>(name) {
        return serde_json::Value::from(v);
    }
    if let Ok(v) = row.try_get::<f64, _>(name) {
        return serde_json::json!(v);
    }
    if let Ok(v) = row.try_get::<String, _>(name) {
        return serde_json::Value::String(v);
    }
    if let Ok(v) = row.try_get::<Option<String>, _>(name) {
        return match v {
            Some(s) => serde_json::Value::String(s),
            None => serde_json::Value::Null,
        };
    }
    serde_json::Value::Null
}
