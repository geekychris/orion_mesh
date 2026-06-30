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

// ============================================================ log archive

#[tokio::test]
async fn log_archive_append_and_read_basic() {
    let s = store().await;
    for i in 0..5 {
        s.append_log("Service", "row-cruncher", "node-a", "stdout", &format!("line {i}"))
            .await
            .unwrap();
    }
    let entries = s.read_logs("Service", "row-cruncher", None, 100).await.unwrap();
    assert_eq!(entries.len(), 5);
    // Newest first.
    assert!(entries[0].line.contains("line 4"));
    assert!(entries[4].line.contains("line 0"));
    assert!(entries.iter().all(|e| e.stream == "stdout"));
    assert!(entries.iter().all(|e| e.node_id == "node-a"));
}

#[tokio::test]
async fn log_archive_kind_and_name_filter() {
    let s = store().await;
    s.append_log("Service", "a", "n", "stdout", "in-a").await.unwrap();
    s.append_log("Service", "b", "n", "stdout", "in-b").await.unwrap();
    s.append_log("Task", "a", "n", "stdout", "task-a").await.unwrap();

    let a_only = s.read_logs("Service", "a", None, 100).await.unwrap();
    assert_eq!(a_only.len(), 1);
    assert_eq!(a_only[0].line, "in-a");

    let task_a = s.read_logs("Task", "a", None, 100).await.unwrap();
    assert_eq!(task_a.len(), 1);
    assert_eq!(task_a[0].line, "task-a");
}

#[tokio::test]
async fn log_archive_since_returns_only_newer_entries() {
    let s = store().await;
    s.append_log("Service", "svc", "n", "stdout", "first").await.unwrap();
    // Give the clock a beat — RFC3339 has subsec precision but identical
    // timestamps could compare equal under a fast loop. 50ms is plenty.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let cutoff = chrono::Utc::now();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    s.append_log("Service", "svc", "n", "stdout", "second").await.unwrap();
    s.append_log("Service", "svc", "n", "stdout", "third").await.unwrap();
    let after = s.read_logs("Service", "svc", Some(cutoff), 100).await.unwrap();
    assert_eq!(after.len(), 2);
    let lines: Vec<_> = after.iter().map(|e| e.line.as_str()).collect();
    assert!(lines.contains(&"second"));
    assert!(lines.contains(&"third"));
    assert!(!lines.contains(&"first"));
}

#[tokio::test]
async fn log_archive_limit_bounds_returned_rows() {
    let s = store().await;
    for i in 0..20 {
        s.append_log("Service", "svc", "n", "stdout", &format!("l{i}")).await.unwrap();
    }
    let only_3 = s.read_logs("Service", "svc", None, 3).await.unwrap();
    assert_eq!(only_3.len(), 3);
    // Newest first → l19, l18, l17.
    assert_eq!(only_3[0].line, "l19");
    assert_eq!(only_3[2].line, "l17");
}

#[tokio::test]
async fn log_archive_purge_drops_old_rows() {
    let s = store().await;
    s.append_log("Service", "svc", "n", "stdout", "fresh").await.unwrap();
    // purge_old_logs(0) cuts at "right now" — anything strictly older goes.
    // We just inserted a row at ~now, so it should survive (at >= cutoff).
    // Inject a row with a far-past at directly via SQL to test the cutoff.
    sqlx::query(
        "INSERT INTO log_archive (at, kind, name, node_id, stream, line)
         VALUES ('2020-01-01T00:00:00Z', 'Service', 'svc', 'n', 'stdout', 'ancient')",
    )
    .execute(s.pool())
    .await
    .unwrap();
    let before = s.read_logs("Service", "svc", None, 100).await.unwrap();
    assert_eq!(before.len(), 2);
    let deleted = s.purge_old_logs(30).await.unwrap();
    assert_eq!(deleted, 1);
    let after = s.read_logs("Service", "svc", None, 100).await.unwrap();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].line, "fresh");
}
