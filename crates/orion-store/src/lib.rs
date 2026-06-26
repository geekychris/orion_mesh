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
    pub async fn open(path: &str) -> Result<Self, StoreError> {
        let opts = SqliteConnectOptions::from_str(path)?
            .create_if_missing(true)
            .foreign_keys(true);
        let pool = SqlitePool::connect_with(opts).await?;
        sqlx::migrate!("./src/migrations").run(&pool).await?;
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

#[cfg(test)]
mod tests;
