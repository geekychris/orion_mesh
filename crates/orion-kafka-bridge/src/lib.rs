//! Pure helpers for the OrionMesh ⇄ Kafka bridge.
//!
//! The binary in `src/main.rs` runs in one of two modes:
//!   * `orion->kafka` — subscribe to an OrionMesh queue, produce each
//!                       message to a Kafka topic.
//!   * `kafka->orion` — consume a Kafka topic, publish each record to
//!                       an OrionMesh queue.
//!
//! Helpers here cover envelope encoding, the small JSON shape we add
//! when bridging, and direction parsing.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    OrionToKafka,
    KafkaToOrion,
}

impl Direction {
    pub fn parse(s: &str) -> anyhow::Result<Direction> {
        match s {
            "orion->kafka" | "out" | "orion_to_kafka" => Ok(Direction::OrionToKafka),
            "kafka->orion" | "in" | "kafka_to_orion" => Ok(Direction::KafkaToOrion),
            other => anyhow::bail!("unknown direction: {other:?} (expected orion->kafka or kafka->orion)"),
        }
    }
}

/// Envelope written into Kafka when bridging outbound. Keeps the OrionMesh
/// subject and timestamp alongside the payload so downstream Kafka consumers
/// can tell where the message came from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OutboundEnvelope {
    pub orion_subject: String,
    pub at: String,
    pub body: serde_json::Value,
}

pub fn make_outbound(orion_subject: &str, body_bytes: &[u8], at: chrono::DateTime<chrono::Utc>) -> OutboundEnvelope {
    let body: serde_json::Value =
        serde_json::from_slice(body_bytes).unwrap_or_else(|_| {
            serde_json::Value::String(String::from_utf8_lossy(body_bytes).into_owned())
        });
    OutboundEnvelope {
        orion_subject: orion_subject.to_owned(),
        at: at.to_rfc3339(),
        body,
    }
}

/// What we publish onto an OrionMesh queue when bridging inbound (Kafka→Orion).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InboundEvent {
    pub kafka_topic: String,
    pub kafka_partition: i32,
    pub kafka_offset: i64,
    pub at: String,
    pub body: serde_json::Value,
    pub _subject: String,
}

pub fn make_inbound(
    topic: &str,
    partition: i32,
    offset: i64,
    body_bytes: &[u8],
    subject: &str,
    at: chrono::DateTime<chrono::Utc>,
) -> InboundEvent {
    let body: serde_json::Value =
        serde_json::from_slice(body_bytes).unwrap_or_else(|_| {
            serde_json::Value::String(String::from_utf8_lossy(body_bytes).into_owned())
        });
    InboundEvent {
        kafka_topic: topic.to_owned(),
        kafka_partition: partition,
        kafka_offset: offset,
        at: at.to_rfc3339(),
        body,
        _subject: subject.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    #[test]
    fn parse_direction_handles_canonical_and_aliases() {
        assert_eq!(Direction::parse("orion->kafka").unwrap(), Direction::OrionToKafka);
        assert_eq!(Direction::parse("out").unwrap(), Direction::OrionToKafka);
        assert_eq!(Direction::parse("orion_to_kafka").unwrap(), Direction::OrionToKafka);
        assert_eq!(Direction::parse("kafka->orion").unwrap(), Direction::KafkaToOrion);
        assert_eq!(Direction::parse("in").unwrap(), Direction::KafkaToOrion);
        assert_eq!(Direction::parse("kafka_to_orion").unwrap(), Direction::KafkaToOrion);
    }

    #[test]
    fn parse_direction_rejects_garbage() {
        assert!(Direction::parse("sideways").is_err());
    }

    #[test]
    fn make_outbound_decodes_json_body() {
        let at = chrono::Utc.with_ymd_and_hms(2026, 6, 30, 12, 0, 0).unwrap();
        let body = br#"{"n":1,"msg":"hello"}"#;
        let env = make_outbound("orion.queue.events", body, at);
        assert_eq!(env.orion_subject, "orion.queue.events");
        assert_eq!(env.at, "2026-06-30T12:00:00+00:00");
        assert_eq!(env.body, json!({"n": 1, "msg": "hello"}));
    }

    #[test]
    fn make_outbound_falls_back_to_string_for_non_json() {
        let at = chrono::Utc::now();
        let env = make_outbound("s", b"raw bytes", at);
        assert_eq!(env.body, json!("raw bytes"));
    }

    #[test]
    fn make_inbound_includes_kafka_coordinates_and_subject() {
        let at = chrono::Utc.with_ymd_and_hms(2026, 6, 30, 0, 0, 0).unwrap();
        let ev = make_inbound("logs", 3, 100, br#"{"hi":"there"}"#, "orion.queue.x", at);
        assert_eq!(ev.kafka_topic, "logs");
        assert_eq!(ev.kafka_partition, 3);
        assert_eq!(ev.kafka_offset, 100);
        assert_eq!(ev.body, json!({"hi": "there"}));
        assert_eq!(ev._subject, "orion.queue.x");
    }
}
