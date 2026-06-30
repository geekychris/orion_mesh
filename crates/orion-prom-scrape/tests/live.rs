//! End-to-end tests for orion-prom-scrape: scrape mode + alertmanager mode.
//! Spawns a local nats-server (with isolated JetStream store) and uses an
//! in-process axum server to stand in for the workload's `/metrics`
//! endpoint and for the Alertmanager POSTer.
//!
//! `#[ignore]` so default `cargo test` skips. Run with
//! `cargo test -p orion-prom-scrape -- --ignored`.

use std::process::{Child, Command, Stdio};
use std::time::Duration;

const SCRAPE_NATS_PORT: u16 = 14611;
const SCRAPE_METRICS_PORT: u16 = 14612;
const SCRAPE_SUBJECT: &str = "orion.queue.prom-test";
const SCRAPE_STREAM: &str = "ORION_QUEUE_PROM_TEST";

const ALERT_NATS_PORT: u16 = 14613;
const ALERT_BIND_PORT: u16 = 14614;
const ALERT_SUBJECT: &str = "orion.queue.alert-test";
const ALERT_STREAM: &str = "ORION_QUEUE_ALERT_TEST";

struct Stack {
    nats: Child,
    binary: Child,
    _nats_store: tempfile::TempDir,
}

impl Drop for Stack {
    fn drop(&mut self) {
        let _ = self.binary.kill();
        let _ = self.nats.kill();
        let _ = self.binary.wait();
        let _ = self.nats.wait();
    }
}

fn start_nats(port: u16, store: &std::path::Path) -> Child {
    Command::new("nats-server")
        .args([
            "-js",
            "--addr",
            "127.0.0.1",
            "-p",
            &port.to_string(),
            "-sd",
            &store.to_string_lossy(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("nats-server")
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

async fn subscribe(
    nats_port: u16,
    stream: &str,
    subject: &str,
) -> async_nats::jetstream::consumer::PullConsumer {
    let nc = async_nats::connect(format!("nats://127.0.0.1:{nats_port}"))
        .await
        .unwrap();
    let js = async_nats::jetstream::new(nc);
    for _ in 0..40 {
        if js.get_stream(stream).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let s = js.get_stream(stream).await.expect("stream within 4s");
    use async_nats::jetstream::consumer;
    s.get_or_create_consumer(
        "test",
        consumer::pull::Config {
            durable_name: Some("test".into()),
            filter_subject: subject.into(),
            ack_policy: consumer::AckPolicy::Explicit,
            ..Default::default()
        },
    )
    .await
    .unwrap()
}

// ============================================================ scrape mode

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn scrape_publishes_parsed_samples_to_queue() {
    use axum::routing::get;
    use axum::Router;

    let nats_store = tempfile::tempdir().unwrap();
    let nats = start_nats(SCRAPE_NATS_PORT, nats_store.path());
    wait_for_port("127.0.0.1", SCRAPE_NATS_PORT, 5000).await;

    // Spin up a fake /metrics server.
    let metrics_body = "\
# HELP orion_test_counter Test counter
# TYPE orion_test_counter counter
orion_test_counter 17
# HELP orion_test_gauge Test gauge
# TYPE orion_test_gauge gauge
orion_test_gauge{quality=\"premium\"} 99.5
orion_test_gauge{quality=\"basic\"} 12
";
    let app = Router::new().route(
        "/metrics",
        get(move || async move { metrics_body.to_owned() }),
    );
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{SCRAPE_METRICS_PORT}"))
        .await
        .unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    wait_for_port("127.0.0.1", SCRAPE_METRICS_PORT, 2000).await;

    // Spawn the binary in scrape mode.
    let bin = env!("CARGO_BIN_EXE_orion-prom-scrape");
    let binary = Command::new(bin)
        .args(["--mode", "scrape"])
        .env(
            "SCRAPE_TARGETS",
            format!("http://127.0.0.1:{SCRAPE_METRICS_PORT}/metrics"),
        )
        .env("SCRAPE_INTERVAL_SECONDS", "1")
        .env(
            "NATS_URL",
            format!("nats://127.0.0.1:{SCRAPE_NATS_PORT}"),
        )
        .env("ORION_QUEUE_SUBJECT", SCRAPE_SUBJECT)
        .env("ORION_QUEUE_STREAM", SCRAPE_STREAM)
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn");
    let stack = Stack {
        nats,
        binary,
        _nats_store: nats_store,
    };

    // Subscribe and collect at least 3 samples.
    let consumer = subscribe(SCRAPE_NATS_PORT, SCRAPE_STREAM, SCRAPE_SUBJECT).await;
    use futures::StreamExt;
    let mut msgs = consumer.messages().await.unwrap();
    let mut received: Vec<serde_json::Value> = Vec::new();
    while received.len() < 3 {
        let next = tokio::time::timeout(Duration::from_secs(15), msgs.next()).await;
        match next {
            Ok(Some(Ok(m))) => {
                let v: serde_json::Value = serde_json::from_slice(&m.payload).unwrap();
                let _ = m.ack().await;
                received.push(v);
            }
            other => panic!("missing message: {other:?}; got {received:?}"),
        }
    }

    // Three expected samples: counter (no labels), two gauges with labels.
    let names: std::collections::HashSet<&str> = received
        .iter()
        .map(|r| r["name"].as_str().unwrap())
        .collect();
    assert!(names.contains("orion_test_counter"), "missing counter: {names:?}");
    assert!(names.contains("orion_test_gauge"), "missing gauge: {names:?}");
    // Source URL preserved.
    for r in &received {
        let src = r["source"].as_str().unwrap();
        assert!(src.contains(":14612"), "source missing port: {src}");
    }
    // Find a labeled sample and check the label survived.
    let labeled = received
        .iter()
        .find(|r| {
            r["name"] == "orion_test_gauge"
                && r["labels"]
                    .as_object()
                    .map(|o| o.contains_key("quality"))
                    .unwrap_or(false)
        })
        .expect("at least one labeled gauge sample");
    let q = labeled["labels"]["quality"].as_str().unwrap();
    assert!(matches!(q, "premium" | "basic"), "unexpected quality: {q}");

    drop(stack);
}

// ============================================================ alertmanager

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn alertmanager_publishes_each_alert_to_queue() {
    let nats_store = tempfile::tempdir().unwrap();
    let nats = start_nats(ALERT_NATS_PORT, nats_store.path());
    wait_for_port("127.0.0.1", ALERT_NATS_PORT, 5000).await;

    let bin = env!("CARGO_BIN_EXE_orion-prom-scrape");
    let binary = Command::new(bin)
        .args(["--mode", "alertmanager"])
        .env("BIND", format!("127.0.0.1:{ALERT_BIND_PORT}"))
        .env(
            "NATS_URL",
            format!("nats://127.0.0.1:{ALERT_NATS_PORT}"),
        )
        .env("ORION_QUEUE_SUBJECT", ALERT_SUBJECT)
        .env("ORION_QUEUE_STREAM", ALERT_STREAM)
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn");
    let stack = Stack {
        nats,
        binary,
        _nats_store: nats_store,
    };
    wait_for_port("127.0.0.1", ALERT_BIND_PORT, 5000).await;

    // POST a fabricated Alertmanager payload.
    let payload = serde_json::json!({
        "status": "firing",
        "receiver": "orion-test",
        "alerts": [
            {
                "status": "firing",
                "labels": { "alertname": "HighCPU", "instance": "i-1" },
                "annotations": { "summary": "CPU over 90%" },
                "startsAt": "2026-06-30T12:00:00Z"
            },
            {
                "status": "firing",
                "labels": { "alertname": "HighMem", "instance": "i-2" },
                "annotations": {},
                "startsAt": "2026-06-30T12:01:00Z"
            }
        ]
    });
    let http = reqwest::Client::new();
    let resp = http
        .post(format!("http://127.0.0.1:{ALERT_BIND_PORT}/alerts"))
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Subscribe and verify both alerts arrive.
    let consumer = subscribe(ALERT_NATS_PORT, ALERT_STREAM, ALERT_SUBJECT).await;
    use futures::StreamExt;
    let mut msgs = consumer.messages().await.unwrap();
    let mut received: Vec<serde_json::Value> = Vec::new();
    while received.len() < 2 {
        let next = tokio::time::timeout(Duration::from_secs(5), msgs.next()).await;
        match next {
            Ok(Some(Ok(m))) => {
                let v: serde_json::Value = serde_json::from_slice(&m.payload).unwrap();
                let _ = m.ack().await;
                received.push(v);
            }
            other => panic!("missing message: {other:?}; got {received:?}"),
        }
    }
    let alertnames: std::collections::HashSet<&str> = received
        .iter()
        .map(|r| r["labels"]["alertname"].as_str().unwrap())
        .collect();
    assert!(alertnames.contains("HighCPU"));
    assert!(alertnames.contains("HighMem"));
    for r in &received {
        assert_eq!(r["receiver"], "orion-test");
        assert_eq!(r["status"], "firing");
        assert_eq!(r["_subject"], ALERT_SUBJECT);
    }
    drop(stack);
}
