//! orion-kafka-bridge binary. Either Orion→Kafka or Kafka→Orion.
//!
//! Required env (both directions):
//!     KAFKA_BROKERS         "host:9092,host2:9092"
//!     KAFKA_TOPIC           Topic to produce to / consume from
//!     ORION_QUEUE_SUBJECT   The Orion queue's subject
//!     ORION_QUEUE_STREAM    The Orion queue's stream name
//!     NATS_URL              broker URL
//!
//! Kafka→Orion also needs:
//!     KAFKA_GROUP           consumer group
//!
//! `orion gen ...` doesn't currently scaffold this (Kafka cluster
//! config is too site-specific); craft the Service YAML by hand.

use anyhow::{Context, Result};
use clap::Parser;
use futures::StreamExt;
use orion_kafka_bridge::{make_inbound, make_outbound, Direction};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(name = "orion-kafka-bridge")]
struct Args {
    /// `orion->kafka` or `kafka->orion`.
    #[arg(long, default_value = "orion->kafka")]
    direction: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let direction = Direction::parse(&args.direction)?;
    let brokers = std::env::var("KAFKA_BROKERS").context("KAFKA_BROKERS")?;
    let topic = std::env::var("KAFKA_TOPIC").context("KAFKA_TOPIC")?;
    let subject = std::env::var("ORION_QUEUE_SUBJECT").context("ORION_QUEUE_SUBJECT")?;
    let stream = std::env::var("ORION_QUEUE_STREAM").context("ORION_QUEUE_STREAM")?;
    let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let token = std::env::var("ORION_CLUSTER_TOKEN").ok();

    let nc = orion_bus::client::connect(&nats_url, token.as_deref()).await?;
    let js = async_nats::jetstream::new(nc.clone());
    let cfg = async_nats::jetstream::stream::Config {
        name: stream,
        subjects: vec![subject.clone()],
        ..Default::default()
    };
    let _ = orion_bus::client::ensure_stream(&js, cfg).await;

    match direction {
        Direction::OrionToKafka => run_orion_to_kafka(&nc, &js, &brokers, &topic, &subject).await,
        Direction::KafkaToOrion => {
            let group = std::env::var("KAFKA_GROUP").context("KAFKA_GROUP")?;
            run_kafka_to_orion(&js, &brokers, &topic, &group, &subject).await
        }
    }
}

async fn run_orion_to_kafka(
    nc: &async_nats::Client,
    _js: &async_nats::jetstream::Context,
    brokers: &str,
    topic: &str,
    subject: &str,
) -> Result<()> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()?;
    tracing::info!(brokers, topic, subject, "orion->kafka started");
    let mut sub = nc.subscribe(subject.to_owned()).await?;
    while let Some(msg) = sub.next().await {
        let env = make_outbound(subject, &msg.payload, chrono::Utc::now());
        let body = serde_json::to_vec(&env)?;
        let record = FutureRecord::<(), _>::to(topic).payload(&body);
        if let Err((e, _)) = producer.send(record, Duration::from_secs(5)).await {
            tracing::warn!(error = %e, "kafka produce failed");
        }
    }
    Ok(())
}

async fn run_kafka_to_orion(
    js: &async_nats::jetstream::Context,
    brokers: &str,
    topic: &str,
    group: &str,
    subject: &str,
) -> Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .create()?;
    consumer.subscribe(&[topic])?;
    tracing::info!(brokers, topic, group, subject, "kafka->orion started");
    let mut stream = consumer.stream();
    while let Some(msg) = stream.next().await {
        let m = match msg {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "kafka recv error");
                continue;
            }
        };
        let payload = m.payload().unwrap_or_default();
        let ev = make_inbound(
            m.topic(),
            m.partition(),
            m.offset(),
            payload,
            subject,
            chrono::Utc::now(),
        );
        let body = serde_json::to_vec(&ev)?;
        let _ = js.publish(subject.to_owned(), body.into()).await?.await;
    }
    Ok(())
}
