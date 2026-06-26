//! SQLite-backed persistence for the controller.
//!
//! One table for declared resources (JSON body), one for observed-node cache.
//! Reconciler reads from both; scheduler reads from the resource table.

use chrono::Utc;
use orion_types::Resource;
use sqlx::{SqlitePool, sqlite::SqliteConnectOptions};
use std::str::FromStr;
use thiserror::Error;

pub mod node_cache;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("sqlx: {0}")]
    Sql(#[from] sqlx::Error),
    #[error("migrate: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("serialize: {0}")]
    Json(#[from] serde_json::Error),
    #[error("not found: {kind}/{name}")]
    NotFound { kind: String, name: String },
}

#[derive(Clone)]
pub struct Store {
    pool: SqlitePool,
}

impl Store {
    /// Open the SQLite database at `path` (creating it if missing) and apply migrations.
    ///
    /// After the migration runner, also runs a defensive `CREATE TABLE IF NOT EXISTS`
    /// pass for every table the application needs. This makes startup robust against
    /// the case where someone (or a stray `sqlite3 < script.sql`) dropped a table
    /// after V0002 was already marked applied; the migration system won't re-run
    /// V0002, but the runtime guard creates the missing table without failure.
    pub async fn open(path: &str) -> Result<Self, StoreError> {
        let opts = SqliteConnectOptions::from_str(path)?
            .create_if_missing(true)
            .foreign_keys(true);
        let pool = SqlitePool::connect_with(opts).await?;
        sqlx::migrate!("./src/migrations").run(&pool).await?;
        ensure_tables(&pool).await?;
        Ok(Self { pool })
    }

    /// Open an in-memory SQLite for tests.
    pub async fn in_memory() -> Result<Self, StoreError> {
        Self::open("sqlite::memory:").await
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    // ----------------------------------------------------- Resource CRUD

    /// Insert-or-replace a resource. Bumps generation if the body changed.
    pub async fn upsert_resource(&self, r: &Resource) -> Result<u64, StoreError> {
        let kind = r.kind_str().to_string();
        let namespace = r.metadata.namespace.clone().unwrap_or_else(|| "_".into());
        let name = r.metadata.name.0.clone();
        let body = serde_json::to_string(r)?;
        let now = Utc::now().to_rfc3339();

        let existing: Option<(String, i64)> = sqlx::query_as(
            "SELECT body, generation FROM resource WHERE kind = ? AND namespace = ? AND name = ?",
        )
        .bind(&kind)
        .bind(&namespace)
        .bind(&name)
        .fetch_optional(&self.pool)
        .await?;

        let next_gen: i64 = match existing {
            Some((prev_body, prev_gen)) if prev_body == body => prev_gen,
            Some((_, prev_gen)) => prev_gen + 1,
            None => 1,
        };

        sqlx::query(
            "INSERT INTO resource (kind, namespace, name, generation, body, updated_at)
             VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT(kind, namespace, name) DO UPDATE SET
                 generation = excluded.generation,
                 body = excluded.body,
                 updated_at = excluded.updated_at",
        )
        .bind(&kind)
        .bind(&namespace)
        .bind(&name)
        .bind(next_gen)
        .bind(&body)
        .bind(&now)
        .execute(&self.pool)
        .await?;

        Ok(next_gen as u64)
    }

    pub async fn get_resource(
        &self,
        kind: &str,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Resource>, StoreError> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT body FROM resource WHERE kind = ? AND namespace = ? AND name = ?",
        )
        .bind(kind)
        .bind(namespace)
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            Some((body,)) => Ok(Some(serde_json::from_str(&body)?)),
            None => Ok(None),
        }
    }

    pub async fn list_by_kind(&self, kind: &str) -> Result<Vec<Resource>, StoreError> {
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT body FROM resource WHERE kind = ? ORDER BY name")
                .bind(kind)
                .fetch_all(&self.pool)
                .await?;
        rows.into_iter()
            .map(|(body,)| serde_json::from_str(&body).map_err(StoreError::Json))
            .collect()
    }

    pub async fn delete_resource(
        &self,
        kind: &str,
        namespace: &str,
        name: &str,
    ) -> Result<bool, StoreError> {
        let res = sqlx::query(
            "DELETE FROM resource WHERE kind = ? AND namespace = ? AND name = ?",
        )
        .bind(kind)
        .bind(namespace)
        .bind(name)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }
}

pub use node_cache::ObservedNode;

/// Runtime guard — independent of the migration system. Idempotent CREATE for
/// every table this crate touches. Survives schema drops after migrations ran.
async fn ensure_tables(pool: &SqlitePool) -> Result<(), StoreError> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS resource (
            kind            TEXT    NOT NULL,
            namespace       TEXT    NOT NULL DEFAULT '_',
            name            TEXT    NOT NULL,
            generation      INTEGER NOT NULL DEFAULT 1,
            body            TEXT    NOT NULL,
            created_at      TEXT    NOT NULL DEFAULT (datetime('now', 'subsec')),
            updated_at      TEXT    NOT NULL DEFAULT (datetime('now', 'subsec')),
            PRIMARY KEY (kind, namespace, name)
        )",
    )
    .execute(pool)
    .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS resource_kind_idx ON resource(kind)")
        .execute(pool)
        .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS observed_node (
            node_id         TEXT PRIMARY KEY,
            agent_version   TEXT NOT NULL,
            inventory       TEXT,
            last_seen_at    TEXT NOT NULL
        )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests;
