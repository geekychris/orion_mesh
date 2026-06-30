//! Thin convenience layer over `async_nats` for connecting, ensuring
//! JetStream streams exist, and publishing JSON.
//!
//! Callers that want fine-grained control should keep using `async_nats`
//! directly. This module exists so that the CLI (`orion queue ...`) and any
//! future polyglot wrappers don't each re-discover the same boilerplate
//! (`get_or_create_stream`, subject-wildcard normalisation, token plumbing).

use async_nats::jetstream::{self, stream::Config as StreamConfig};
use orion_types::{QueueSpec, default_queue_stream, default_queue_subject};
use serde::Serialize;
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("nats connect: {0}")]
    Connect(#[from] async_nats::ConnectError),
    #[error("nats publish: {0}")]
    Publish(#[from] async_nats::PublishError),
    #[error("jetstream publish: {0}")]
    JsPublish(#[from] async_nats::jetstream::context::PublishError),
    #[error("jetstream publish ack: {0}")]
    JsPublishAck(String),
    #[error("jetstream create stream: {0}")]
    JsCreateStream(#[from] async_nats::jetstream::context::CreateStreamError),
    #[error("json encode: {0}")]
    Json(#[from] serde_json::Error),
}

/// Connect to NATS, optionally with the cluster shared token.
///
/// Equivalent to `async_nats::ConnectOptions::new().token(...).connect(url)`
/// but lets callers pass `None` for the token so the same call site works for
/// auth-disabled dev mode.
pub async fn connect(url: &str, token: Option<&str>) -> Result<async_nats::Client, ClientError> {
    let opts = match token {
        Some(t) if !t.is_empty() => async_nats::ConnectOptions::new().token(t.to_owned()),
        _ => async_nats::ConnectOptions::new(),
    };
    Ok(opts
        .connection_timeout(Duration::from_secs(5))
        .connect(url)
        .await?)
}

/// Build the JetStream subject + stream config for a `QueueSpec`.
///
/// `Topic` queues use the bare subject (`orion.queue.<name>`) so that all
/// messages land on the same shard. `Work` queues use the bare subject too —
/// the work-vs-broadcast distinction lives entirely on the *consumer* side.
pub fn queue_stream_config(name: &str, spec: &QueueSpec) -> (String, StreamConfig) {
    let subject = spec
        .subject
        .clone()
        .unwrap_or_else(|| default_queue_subject(name));
    let stream_name = spec
        .stream
        .clone()
        .unwrap_or_else(|| default_queue_stream(name));
    let cfg = StreamConfig {
        name: stream_name,
        subjects: vec![subject.clone()],
        max_age: spec
            .max_age_seconds
            .map(Duration::from_secs)
            .unwrap_or_default(),
        max_messages: spec.max_msgs.map(|n| n as i64).unwrap_or(-1),
        max_bytes: spec.max_bytes.map(|n| n as i64).unwrap_or(-1),
        num_replicas: spec.replicas.unwrap_or(1).max(1) as usize,
        ..Default::default()
    };
    (subject, cfg)
}

/// Idempotently create-or-update a JetStream stream.
pub async fn ensure_stream(
    js: &jetstream::Context,
    cfg: StreamConfig,
) -> Result<jetstream::stream::Stream, ClientError> {
    Ok(js.get_or_create_stream(cfg).await?)
}

/// Serialize `value` to JSON and publish to JetStream, returning the assigned
/// stream sequence number.
pub async fn publish_json<T: Serialize>(
    js: &jetstream::Context,
    subject: &str,
    value: &T,
) -> Result<u64, ClientError> {
    let bytes = serde_json::to_vec(value)?;
    let ack = js
        .publish(subject.to_owned(), bytes.into())
        .await
        .map_err(ClientError::JsPublish)?
        .await
        .map_err(|e| ClientError::JsPublishAck(e.to_string()))?;
    Ok(ack.sequence)
}

/// Publish a pre-formed JSON byte string (one ndjson line, already encoded).
pub async fn publish_bytes(
    js: &jetstream::Context,
    subject: &str,
    payload: Vec<u8>,
) -> Result<u64, ClientError> {
    let ack = js
        .publish(subject.to_owned(), payload.into())
        .await
        .map_err(ClientError::JsPublish)?
        .await
        .map_err(|e| ClientError::JsPublishAck(e.to_string()))?;
    Ok(ack.sequence)
}
