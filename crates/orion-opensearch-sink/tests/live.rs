//! End-to-end test for orion-opensearch-sink against a real OpenSearch
//! container. Stack:
//!   * docker container running opensearchproject/opensearch:2.x (single-node,
//!     security disabled for the test)
//!   * in-process axum server standing in for the controller's
//!     `/v1/logs-archive/<kind>/<name>` endpoint
//!   * the orion-opensearch-sink binary subscribes to the fake controller,
//!     POSTs batches to OpenSearch's `_bulk`
//!   * the test reads the doc back via `_search` and asserts
//!
//! `#[ignore]` + checks docker availability — skips with a printable message
//! when docker isn't reachable. Run via
//! `cargo test -p orion-opensearch-sink -- --ignored --nocapture`.

use std::process::{Child, Command, Stdio};
use std::time::Duration;

const OS_PORT: u16 = 14640;
const FAKE_CTRL_PORT: u16 = 14641;
const CONTAINER_NAME: &str = "orion-test-opensearch-sink";

struct Stack {
    sink: Child,
}

impl Drop for Stack {
    fn drop(&mut self) {
        let _ = self.sink.kill();
        let _ = self.sink.wait();
        let _ = Command::new("docker")
            .args(["stop", CONTAINER_NAME])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

fn docker_available() -> bool {
    Command::new("docker")
        .arg("version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

async fn wait_for_http(url: &str, max_ms: u64) {
    let deadline = std::time::Instant::now() + Duration::from_millis(max_ms);
    let http = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    while std::time::Instant::now() < deadline {
        if let Ok(r) = http.get(url).send().await {
            if r.status().is_success() {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("{url} did not become ready in {max_ms}ms");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn opensearch_sink_ships_log_lines_to_real_opensearch() {
    if !docker_available() {
        eprintln!("docker not reachable — skipping");
        return;
    }

    // Clean any prior container with the same name.
    let _ = Command::new("docker")
        .args(["rm", "-f", CONTAINER_NAME])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    // Start OpenSearch (security disabled for testing).
    let run = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--name",
            CONTAINER_NAME,
            "-p",
            &format!("{OS_PORT}:9200"),
            "-e",
            "discovery.type=single-node",
            "-e",
            "DISABLE_INSTALL_DEMO_CONFIG=true",
            "-e",
            "DISABLE_SECURITY_PLUGIN=true",
            "-e",
            "OPENSEARCH_INITIAL_ADMIN_PASSWORD=Passw0rd!unused",
            "opensearchproject/opensearch:2.13.0",
        ])
        .output()
        .expect("docker run");
    if !run.status.success() {
        panic!(
            "docker run failed: {}",
            String::from_utf8_lossy(&run.stderr)
        );
    }

    let endpoint = format!("http://127.0.0.1:{OS_PORT}");
    wait_for_http(&endpoint, 90_000).await; // OpenSearch first boot can take ~60s

    // Stand up a fake controller serving one log archive page.
    use axum::extract::Path;
    use axum::routing::get;
    use axum::Router;
    let archive = serde_json::json!([
        {
            "at": "2026-06-30T12:00:00Z",
            "kind": "Service",
            "name": "web",
            "node_id": "n1",
            "stream": "stdout",
            "line": "OPENSEARCH-TEST-MARKER alpha"
        },
        {
            "at": "2026-06-30T12:00:01Z",
            "kind": "Service",
            "name": "web",
            "node_id": "n1",
            "stream": "stdout",
            "line": "OPENSEARCH-TEST-MARKER beta"
        }
    ]);
    let archive_clone = archive.clone();
    let app = Router::new().route(
        "/v1/logs-archive/:kind/:name",
        get(move |_: Path<(String, String)>| {
            let body = archive_clone.clone();
            async move { axum::Json(body) }
        }),
    );
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{FAKE_CTRL_PORT}"))
        .await
        .unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    // Wait for the fake controller to accept.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        if tokio::net::TcpStream::connect(("127.0.0.1", FAKE_CTRL_PORT))
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Spawn the sink binary.
    let bin = env!("CARGO_BIN_EXE_orion-opensearch-sink");
    let sink = Command::new(bin)
        .env("OPENSEARCH_URL", &endpoint)
        .env("OPENSEARCH_INDEX", "orion-sink-test")
        .env("ORION_CONTROLLER_URL", format!("http://127.0.0.1:{FAKE_CTRL_PORT}"))
        .env("LOG_SOURCE_KIND", "Service")
        .env("LOG_SOURCE_NAMES", "web")
        .env("SINK_INTERVAL_SECONDS", "2")
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sink");
    let stack = Stack { sink };

    // Wait for the sink to ship at least one batch, then refresh + search.
    let http = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();
    let mut search_hits: u64 = 0;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        // Force refresh so freshly-indexed docs are visible.
        let _ = http
            .post(format!("{endpoint}/orion-sink-test/_refresh"))
            .send()
            .await;
        let resp = http
            .get(format!("{endpoint}/orion-sink-test/_search?q=line:OPENSEARCH-TEST-MARKER&size=10"))
            .send()
            .await;
        let body: serde_json::Value = match resp {
            Ok(r) => r.json().await.unwrap_or(serde_json::Value::Null),
            Err(_) => continue,
        };
        let total = body
            .pointer("/hits/total/value")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        if total >= 2 {
            search_hits = total;
            break;
        }
    }
    assert!(search_hits >= 2, "expected ≥2 hits, got {search_hits}");

    // Sample a hit and assert shape.
    let body: serde_json::Value = http
        .get(format!("{endpoint}/orion-sink-test/_search?size=1&q=line:OPENSEARCH-TEST-MARKER"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let first = &body["hits"]["hits"][0]["_source"];
    assert_eq!(first["kind"], "Service");
    assert_eq!(first["name"], "web");
    assert_eq!(first["stream"], "stdout");
    assert!(first["line"].as_str().unwrap().contains("OPENSEARCH-TEST-MARKER"));

    drop(stack);
}
