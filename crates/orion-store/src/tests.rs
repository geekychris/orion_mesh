use super::*;
use orion_types::Resource;

async fn store() -> Store {
    Store::in_memory().await.expect("in-memory store")
}

fn svc(name: &str) -> Resource {
    let yaml = format!(
        r#"
kind: Service
metadata: {{ name: {name} }}
spec:
  runtime: {{ kind: native, exec: /bin/true }}
"#
    );
    Resource::from_yaml(&yaml).unwrap()
}

#[tokio::test]
async fn migrations_apply_and_schema_is_queryable() {
    let s = store().await;
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM resource")
        .fetch_one(s.pool())
        .await
        .unwrap();
    assert_eq!(count.0, 0);
}

#[tokio::test]
async fn upsert_then_get_round_trips_resource() {
    let s = store().await;
    let r = svc("amiga-search");
    let gen1 = s.upsert_resource(&r).await.unwrap();
    assert_eq!(gen1, 1);
    let fetched = s.get_resource("Service", "_", "amiga-search").await.unwrap().unwrap();
    assert_eq!(fetched.kind_str(), "Service");
    assert_eq!(fetched.name(), "amiga-search");
}

#[tokio::test]
async fn upsert_with_same_body_keeps_generation() {
    let s = store().await;
    let r = svc("svc1");
    let g1 = s.upsert_resource(&r).await.unwrap();
    let g2 = s.upsert_resource(&r).await.unwrap();
    assert_eq!(g1, g2, "identical body must not bump generation");
}

#[tokio::test]
async fn upsert_with_changed_body_bumps_generation() {
    let s = store().await;
    let r1 = Resource::from_yaml(
        r#"
kind: Service
metadata: { name: svc }
spec: { runtime: { kind: native, exec: /bin/true } }
"#,
    )
    .unwrap();
    let r2 = Resource::from_yaml(
        r#"
kind: Service
metadata: { name: svc }
spec: { runtime: { kind: native, exec: /bin/false } }
"#,
    )
    .unwrap();
    let g1 = s.upsert_resource(&r1).await.unwrap();
    let g2 = s.upsert_resource(&r2).await.unwrap();
    assert_eq!(g1, 1);
    assert_eq!(g2, 2);
}

#[tokio::test]
async fn list_by_kind_returns_only_matching() {
    let s = store().await;
    s.upsert_resource(&svc("a")).await.unwrap();
    s.upsert_resource(&svc("b")).await.unwrap();
    let other = Resource::from_yaml(
        r#"
kind: Volume
metadata: { name: scratch }
spec: { path: /mnt/scratch }
"#,
    )
    .unwrap();
    s.upsert_resource(&other).await.unwrap();
    let services = s.list_by_kind("Service").await.unwrap();
    assert_eq!(services.len(), 2);
    let volumes = s.list_by_kind("Volume").await.unwrap();
    assert_eq!(volumes.len(), 1);
}

#[tokio::test]
async fn delete_removes_and_reports_existence() {
    let s = store().await;
    s.upsert_resource(&svc("doomed")).await.unwrap();
    assert!(s.delete_resource("Service", "_", "doomed").await.unwrap());
    assert!(!s.delete_resource("Service", "_", "doomed").await.unwrap());
    assert!(s.get_resource("Service", "_", "doomed").await.unwrap().is_none());
}

#[tokio::test]
async fn node_touch_and_inventory_round_trip() {
    let s = store().await;
    s.touch_node("pi5", "0.1.0").await.unwrap();
    let nodes = s.list_nodes().await.unwrap();
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].node_id, "pi5");
    assert!(nodes[0].inventory_json.is_none());

    s.set_node_inventory("pi5", "0.1.0", r#"{"node_id":"pi5"}"#).await.unwrap();
    let nodes = s.list_nodes().await.unwrap();
    assert_eq!(nodes[0].inventory_json.as_deref(), Some(r#"{"node_id":"pi5"}"#));
}
