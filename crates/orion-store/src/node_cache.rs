use crate::{Store, StoreError};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct ObservedNode {
    pub node_id: String,
    pub agent_version: String,
    pub inventory_json: Option<String>,
    pub last_seen_at: DateTime<Utc>,
}

impl Store {
    pub async fn touch_node(
        &self,
        node_id: &str,
        agent_version: &str,
    ) -> Result<(), StoreError> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO observed_node (node_id, agent_version, last_seen_at)
             VALUES (?, ?, ?)
             ON CONFLICT(node_id) DO UPDATE SET
                 agent_version = excluded.agent_version,
                 last_seen_at = excluded.last_seen_at",
        )
        .bind(node_id)
        .bind(agent_version)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_node_inventory(
        &self,
        node_id: &str,
        agent_version: &str,
        inventory_json: &str,
    ) -> Result<(), StoreError> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO observed_node (node_id, agent_version, inventory, last_seen_at)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(node_id) DO UPDATE SET
                 agent_version = excluded.agent_version,
                 inventory = excluded.inventory,
                 last_seen_at = excluded.last_seen_at",
        )
        .bind(node_id)
        .bind(agent_version)
        .bind(inventory_json)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_nodes(&self) -> Result<Vec<ObservedNode>, StoreError> {
        let rows: Vec<(String, String, Option<String>, String)> = sqlx::query_as(
            "SELECT node_id, agent_version, inventory, last_seen_at
             FROM observed_node ORDER BY node_id",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(node_id, agent_version, inventory, last_seen_at)| ObservedNode {
                node_id,
                agent_version,
                inventory_json: inventory,
                last_seen_at: DateTime::parse_from_rfc3339(&last_seen_at)
                    .map(|d| d.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now()),
            })
            .collect())
    }
}
