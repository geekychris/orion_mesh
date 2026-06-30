//! Live end-to-end roundtrip for orion-kafka-bridge against a real Kafka
//! container in KRaft single-node mode + a local nats-server with isolated
//! JetStream state. One test exercises both directions sequentially so we
//! pay the Kafka boot cost only once.
//!
//! `#[ignore]` and skips cleanly if docker isn't reachable. Run with
//! `cargo test -p orion-kafka-bridge -- --ignored --nocapture`.

use futures::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::Message;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

const KAFKA_PORT: u16 = 19092;
const NATS_PORT: u16 = 14620;
const CONTAINER_NAME: &str = "orion-test-kafka-bridge";

const OUT_TOPIC: &str = "orion-out-test";
const IN_TOPIC: &str = "orion-in-test";
const OUT_SUBJECT: &str = "orion.queue.kafka-out-test";
const IN_SUBJECT: &str = "orion.queue.kafka-in-test";
const OUT_STREAM: &str = "ORION_QUEUE_KAFKA_OUT_TEST";
const IN_STREAM: &str = "ORION_QUEUE_KAFKA_IN_TEST";

struct Stack {
    nats: Child,
    bridge_out: Option<Child>,
    bridge_in: Option<Child>,
    _nats_store: tempfile::TempDir,
}

impl Drop for Stack {
    fn drop(&mut self) {
        for c in [&mut self.bridge_out, &mut self.bridge_in] {
            if let Some(ch) = c.as_mut() {
                let _ = ch.kill();
                let _ = ch.wait();
            }
        }
        let _ = self.nats.kill();
        let _ = self.nats.wait();
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
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("port {host}:{port} did not become ready in {max_ms}ms");
}

fn brokers() -> String {
    format!("127.0.0.1:{KAFKA_PORT}")
}

async fn wait_for_kafka_ready(max_secs: u64) {
    let deadline = std::time::Instant::now() + Duration::from_secs(max_secs);
    while std::time::Instant::now() < deadline {
        // Try to fetch metadata as a readiness probe.
        let producer: rdkafka::producer::BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers())
            .set("socket.timeout.ms", "1000")
            .create()
            .expect("base producer config");
        let meta = producer.client().fetch_metadata(None, Duration::from_secs(2));
        if meta.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("kafka was not reachable within {max_secs}s");
}

async fn create_topic(topic: &str) {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .create()
        .expect("admin client");
    let nt = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
    let _ = admin
        .create_topics(&[nt], &AdminOptions::default().request_timeout(Some(Duration::from_secs(5))))
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn kafka_bridge_roundtrips_both_directions() {
    if !docker_available() {
        eprintln!("docker not reachable — skipping");
        return;
    }

    let _ = Command::new("docker")
        .args(["rm", "-f", CONTAINER_NAME])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    // Start Kafka in KRaft single-node mode.
    let run = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--name",
            CONTAINER_NAME,
            "-p",
            &format!("{KAFKA_PORT}:9092"),
            "-e",
            "KAFKA_NODE_ID=1",
            "-e",
            "KAFKA_PROCESS_ROLES=broker,controller",
            "-e",
            "KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093",
            "-e",
            &format!("KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:{KAFKA_PORT}"),
            "-e",
            "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
            "-e",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
            "-e",
            "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
            "-e",
            "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
            "-e",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
            "-e",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
            "-e",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
            "-e",
            "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
            "-e",
            "KAFKA_AUTO_CREATE_TOPICS_ENABLE=true",
            "apache/kafka:3.7.0",
        ])
        .output()
        .expect("docker run kafka");
    if !run.status.success() {
        panic!(
            "docker run failed: {}",
            String::from_utf8_lossy(&run.stderr)
        );
    }

    // Start nats with an isolated JetStream store dir.
    let nats_store = tempfile::tempdir().unwrap();
    let nats = start_nats(NATS_PORT, nats_store.path());
    wait_for_port("127.0.0.1", NATS_PORT, 5_000).await;

    let mut stack = Stack {
        nats,
        bridge_out: None,
        bridge_in: None,
        _nats_store: nats_store,
    };

    wait_for_port("127.0.0.1", KAFKA_PORT, 90_000).await;
    wait_for_kafka_ready(60).await;
    create_topic(OUT_TOPIC).await;
    create_topic(IN_TOPIC).await;

    // ============================================================ orion -> kafka
    let bin = env!("CARGO_BIN_EXE_orion-kafka-bridge");
    let bridge_out = Command::new(bin)
        .args(["--direction", "orion->kafka"])
        .env("KAFKA_BROKERS", brokers())
        .env("KAFKA_TOPIC", OUT_TOPIC)
        .env("ORION_QUEUE_SUBJECT", OUT_SUBJECT)
        .env("ORION_QUEUE_STREAM", OUT_STREAM)
        .env("NATS_URL", format!("nats://127.0.0.1:{NATS_PORT}"))
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn orion->kafka bridge");
    stack.bridge_out = Some(bridge_out);
    // give the bridge's NATS subscriber a moment to attach
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Publish one message on the OrionMesh side.
    let nc = async_nats::connect(format!("nats://127.0.0.1:{NATS_PORT}"))
        .await
        .unwrap();
    let body = serde_json::json!({ "marker": "ORION-TO-KAFKA", "n": 1 });
    nc.publish(OUT_SUBJECT.to_owned(), serde_json::to_vec(&body).unwrap().into())
        .await
        .unwrap();
    nc.flush().await.unwrap();

    // Consume from Kafka and look for our marker (allow a short retry window
    // — the bridge needs to forward + Kafka needs to flush).
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .set("group.id", "orion-test-out-consumer")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("consumer");
    consumer.subscribe(&[OUT_TOPIC]).expect("subscribe");
    let mut kafka_stream = consumer.stream();
    let mut found = false;
    let out_deadline = std::time::Instant::now() + Duration::from_secs(30);
    while std::time::Instant::now() < out_deadline {
        let next =
            tokio::time::timeout(Duration::from_secs(2), kafka_stream.next()).await;
        if let Ok(Some(Ok(m))) = next {
            let payload = m.payload().unwrap_or_default();
            let env: serde_json::Value =
                serde_json::from_slice(payload).expect("envelope is json");
            if env["orion_subject"] == OUT_SUBJECT
                && env["body"]["marker"] == "ORION-TO-KAFKA"
            {
                found = true;
                break;
            }
        }
    }
    assert!(found, "orion->kafka: marker never showed up in Kafka topic");
    drop(kafka_stream);
    drop(consumer);

    // ============================================================ kafka -> orion
    let bridge_in = Command::new(bin)
        .args(["--direction", "kafka->orion"])
        .env("KAFKA_BROKERS", brokers())
        .env("KAFKA_TOPIC", IN_TOPIC)
        .env("KAFKA_GROUP", "orion-test-in-group")
        .env("ORION_QUEUE_SUBJECT", IN_SUBJECT)
        .env("ORION_QUEUE_STREAM", IN_STREAM)
        .env("NATS_URL", format!("nats://127.0.0.1:{NATS_PORT}"))
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn kafka->orion bridge");
    stack.bridge_in = Some(bridge_in);
    // Bridge needs a moment to create the consumer + stream.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Produce on Kafka side.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer");
    let in_body = serde_json::json!({ "marker": "KAFKA-TO-ORION", "n": 7 });
    let payload = serde_json::to_vec(&in_body).unwrap();
    let rec = FutureRecord::<(), _>::to(IN_TOPIC).payload(&payload);
    producer
        .send(rec, Duration::from_secs(5))
        .await
        .expect("kafka send");

    // Subscribe to the Orion JS stream on the inbound subject and look for it.
    let js = async_nats::jetstream::new(nc.clone());
    // Make sure the stream exists (bridge created it but a fresh consumer
    // might race; retry up to a few times).
    let stream = {
        let mut s = None;
        for _ in 0..30 {
            if let Ok(handle) = js.get_stream(IN_STREAM).await {
                s = Some(handle);
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        s.expect("inbound stream created by bridge")
    };
    use async_nats::jetstream::consumer;
    let cons = stream
        .get_or_create_consumer(
            "test-in",
            consumer::pull::Config {
                durable_name: Some("test-in".into()),
                filter_subject: IN_SUBJECT.into(),
                ack_policy: consumer::AckPolicy::Explicit,
                ..Default::default()
            },
        )
        .await
        .expect("create test consumer");
    let mut msgs = cons.messages().await.unwrap();
    let mut got_inbound = false;
    let in_deadline = std::time::Instant::now() + Duration::from_secs(30);
    while std::time::Instant::now() < in_deadline {
        let next = tokio::time::timeout(Duration::from_secs(2), msgs.next()).await;
        if let Ok(Some(Ok(m))) = next {
            let v: serde_json::Value =
                serde_json::from_slice(&m.payload).expect("inbound is json");
            let _ = m.ack().await;
            if v["body"]["marker"] == "KAFKA-TO-ORION"
                && v["kafka_topic"] == IN_TOPIC
                && v["_subject"] == IN_SUBJECT
            {
                got_inbound = true;
                break;
            }
        }
    }
    assert!(got_inbound, "kafka->orion: marker never showed up on Orion subject");

    drop(stack);
}
