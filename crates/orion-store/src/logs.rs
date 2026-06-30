//! Persistent log archive.
//!
//! The in-memory ring at the controller (`LogBuffer`) is the hot path for
//! `/v1/logs/<kind>/<name>` — fast, bounded, lost on restart. This module
//! adds an append-only SQLite mirror so logs survive a controller crash,
//! and so a `--since <ts>` query can look further back than 10k lines.

use crate::{Store, StoreError};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, serde::Serialize)]
pub struct LogArchiveEntry {
    pub at: DateTime<Utc>,
    pub kind: String,
    pub name: String,
    pub node_id: String,
    pub stream: String,
    pub line: String,
}

impl Store {
    pub async fn append_log(
        &self,
        kind: &str,
        name: &str,
        node_id: &str,
        stream: &str,
        line: &str,
    ) -> Result<(), StoreError> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO log_archive (at, kind, name, node_id, stream, line)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&now)
        .bind(kind)
        .bind(name)
        .bind(node_id)
        .bind(stream)
        .bind(line)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Read archived log entries for one workload after `since` (or all if None),
    /// most recent first, bounded by `limit`.
    pub async fn read_logs(
        &self,
        kind: &str,
        name: &str,
        since: Option<DateTime<Utc>>,
        limit: i64,
    ) -> Result<Vec<LogArchiveEntry>, StoreError> {
        let limit = limit.clamp(1, 100_000);
        let rows: Vec<(String, String, String, String, String, String)> = match since {
            Some(ts) => {
                sqlx::query_as(
                    "SELECT at, kind, name, node_id, stream, line
                     FROM log_archive
                     WHERE kind=? AND name=? AND at > ?
                     ORDER BY id DESC LIMIT ?",
                )
                .bind(kind)
                .bind(name)
                .bind(ts.to_rfc3339())
                .bind(limit)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as(
                    "SELECT at, kind, name, node_id, stream, line
                     FROM log_archive
                     WHERE kind=? AND name=?
                     ORDER BY id DESC LIMIT ?",
                )
                .bind(kind)
                .bind(name)
                .bind(limit)
                .fetch_all(&self.pool)
                .await?
            }
        };
        Ok(rows
            .into_iter()
            .map(|(at, kind, name, node_id, stream, line)| LogArchiveEntry {
                at: DateTime::parse_from_rfc3339(&at)
                    .map(|d| d.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now()),
                kind,
                name,
                node_id,
                stream,
                line,
            })
            .collect())
    }

    /// Drop log lines older than `retention_days` to keep the archive bounded.
    /// Returns the number of rows deleted.
    pub async fn purge_old_logs(&self, retention_days: u32) -> Result<u64, StoreError> {
        let cutoff = Utc::now() - chrono::Duration::days(retention_days as i64);
        let res = sqlx::query("DELETE FROM log_archive WHERE at < ?")
            .bind(cutoff.to_rfc3339())
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected())
    }
}
