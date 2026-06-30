//! End-to-end test: tempfile SQLite DB + NATS + the orion-sqlite-tap
//! binary. Inserts rows, subscribes to the queue, asserts the published
//! CDC events match.
//!
//! `#[ignore]` so default `cargo test` skips it; run via
//! `cargo test -p orion-sqlite-tap -- --ignored --nocapture`.

use std::process::{Child, Command, Stdio};
use std::str::FromStr;
use std::time::Duration;

const NATS_PORT: u16 = 14601;
const SUBJECT: &str = "orion.queue.sqlite-cdc-test";
const STREAM: &str = "ORION_QUEUE_SQLITE_CDC_TEST";

struct Stack {
    nats: Child,
    tap: Child,
    db_path: tempfile::NamedTempFile,
}

impl Drop for Stack {
    fn drop(&mut self) {
        let _ = self.tap.kill();
        let _ = self.nats.kill();
        let _ = self.tap.wait();
        let _ = self.nats.wait();
    }
}

fn start_nats(port: u16, store_dir: &std::path::Path) -> Child {
    Command::new("nats-server")
        .args([
            "-js",
            "--addr",
            "127.0.0.1",
            "-p",
            &port.to_string(),
            "-sd",
            &store_dir.to_string_lossy(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start nats-server (install via brew install nats-server)")
}

async fn wait_for_port(host: &str, port: u16, max_ms: u64) {
    let deadline = std::time::Instant::now() + Duration::from_millis(max_ms);
    while std::time::Instant::now() < deadline {
        if tokio::net::TcpStream::connect((host, port)).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("port {host}:{port} did not become ready in {max_ms}ms");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn sqlite_cdc_publishes_new_rows_to_queue() {
    // 1. start nats with an isolated JetStream store
    let nats_store = tempfile::tempdir().unwrap();
    let nats = start_nats(NATS_PORT, nats_store.path());
    wait_for_port("127.0.0.1", NATS_PORT, 5000).await;

    // 2. seed a temp DB with a table + 2 rows
    let db_file = tempfile::Builder::new()
        .suffix(".db")
        .tempfile()
        .expect("temp db");
    let db_path = db_file.path().to_path_buf();
    let db_url = format!("sqlite://{}", db_path.display());

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await
        .expect("connect");
    // Stay on default journal_mode (DELETE) so the tap pool sees data with no
    // WAL coordination.
    sqlx::query(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT, total_cents INTEGER)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO orders (customer, total_cents) VALUES ('alpha', 100), ('beta', 200)")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    drop(pool);

    // 3. start the tap binary with TAP_FROM_ZERO=1 so the seeded rows flow through too
    let bin = env!("CARGO_BIN_EXE_orion-sqlite-tap");
    let tap_log = format!("/tmp/orion-sqlite-tap-live-{}.log", std::process::id());
    let tap_stderr_file = std::fs::File::create(&tap_log).unwrap();
    let mut tap = Command::new(bin)
        .env("SQLITE_URL", &db_url)
        .env("SQLITE_TABLE", "orders")
        .env(
            "NATS_URL",
            format!("nats://127.0.0.1:{NATS_PORT}"),
        )
        .env("ORION_QUEUE_SUBJECT", SUBJECT)
        .env("ORION_QUEUE_STREAM", STREAM)
        .env("TAP_INTERVAL_SECONDS", "1")
        .env("TAP_FROM_ZERO", "1")
        .env("RUST_LOG", "info")
        .stdout(Stdio::null())
        .stderr(tap_stderr_file)
        .spawn()
        .expect("spawn orion-sqlite-tap");
    eprintln!("tap pid={:?} log={}", tap.id(), tap_log);
    std::thread::sleep(std::time::Duration::from_millis(500));
    if let Ok(Some(status)) = tap.try_wait() {
        let log = std::fs::read_to_string(&tap_log).unwrap_or_default();
        panic!("tap exited early ({status:?}); stderr:\n{log}");
    }

    let mut stack = Stack { nats, tap, db_path: db_file };

    // 4. subscribe to the queue ourselves; wait for the first 2 messages
    let nc = async_nats::connect(format!("nats://127.0.0.1:{NATS_PORT}"))
        .await
        .unwrap();
    let js = async_nats::jetstream::new(nc);
    // The tap creates the stream lazily; poll for it.
    for _ in 0..40 {
        if js.get_stream(STREAM).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let stream = js
        .get_stream(STREAM)
        .await
        .expect("stream created by tap within 4s");
    use async_nats::jetstream::consumer;
    let consumer = stream
        .get_or_create_consumer(
            "test-reader",
            consumer::pull::Config {
                durable_name: Some("test-reader".into()),
                filter_subject: SUBJECT.into(),
                ack_policy: consumer::AckPolicy::Explicit,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    use futures::StreamExt;
    let mut msgs = consumer.messages().await.unwrap();

    // Wait long enough for the tap to do at least one poll + publish.
    tokio::time::sleep(Duration::from_secs(3)).await;
    // Open a second pool to inject another row, demonstrating ongoing CDC.
    let pool2 = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::from_str(&db_url)
                .unwrap()
                .read_only(false),
        )
        .await
        .expect("second pool");

    // Insert the third row that should also flow through.
    sqlx::query("INSERT INTO orders (customer, total_cents) VALUES ('gamma', 300)")
        .execute(&pool2)
        .await
        .unwrap();
    pool2.close().await;

    // Collect 3 published events from the tap. Filter to only those whose
    // payload deserialises as a CdcEvent for this table.
    let mut received: Vec<serde_json::Value> = Vec::new();
    while received.len() < 3 {
        let next = tokio::time::timeout(Duration::from_secs(15), msgs.next()).await;
        let m = match next {
            Ok(Some(Ok(m))) => m,
            other => {
                let log = std::fs::read_to_string(&tap_log).unwrap_or_default();
                panic!("missing message: {other:?}\ngot so far: {received:?}\n---tap stderr---\n{log}");
            }
        };
        let v: serde_json::Value = serde_json::from_slice(&m.payload).unwrap();
        let _ = m.ack().await;
        if v.get("table").and_then(|t| t.as_str()) == Some("orders") {
            received.push(v);
        }
    }

    // 5. assertions
    eprintln!("=== received {} messages ===", received.len());
    for (i, r) in received.iter().enumerate() {
        eprintln!("  [{i}] rowid={} customer={}", r["rowid"], r["row"]["customer"]);
    }
    assert_eq!(received.len(), 3);
    for r in &received {
        assert_eq!(r["table"].as_str(), Some("orders"));
        assert!(r["rowid"].as_i64().is_some());
        assert!(r["row"]["customer"].is_string());
    }
    // Use a set since the tap may dedupe / order messages differently across
    // restarts of a long-running stream. The set membership is the contract.
    let customers: std::collections::HashSet<&str> = received
        .iter()
        .map(|r| r["row"]["customer"].as_str().unwrap())
        .collect();
    let expected: std::collections::HashSet<&str> =
        ["alpha", "beta", "gamma"].into_iter().collect();
    assert_eq!(customers, expected);
    drop(stack);
}
