# 21 · Kafka bridge — OrionMesh queues ↔ Kafka topics

`orion-kafka-bridge` runs in one of two directions:

- **`orion->kafka`** — subscribes to an OrionMesh queue, produces each
  message to a Kafka topic.
- **`kafka->orion`** — consumes a Kafka topic, publishes each record
  to an OrionMesh queue.

It runs as a regular Service (`kind: native`). Configure via env vars
in the Service YAML.

## Required env

| Var | Both | orion→kafka | kafka→orion |
|---|---|---|---|
| `KAFKA_BROKERS` | ✓ | | |
| `KAFKA_TOPIC` | ✓ | | |
| `KAFKA_GROUP` | | | ✓ |
| `ORION_QUEUE_SUBJECT` | ✓ | | |
| `ORION_QUEUE_STREAM` | ✓ | | |
| `NATS_URL` | ✓ | | |

## Walkthrough (requires a Kafka cluster you can reach)

This example uses Confluent's `cp-kafka:7.5.0` container in a separate
`docker-compose.yml` (not committed — bring your own). See
[`docker-compose.example.yaml`](docker-compose.example.yaml) for a
template.

### Orion → Kafka

```yaml
# bridge-out.yaml
apiVersion: orionmesh.dev/v1
kind: Service
metadata: { name: bridge-out }
spec:
  replicas: 1
  restart_policy: on_failure
  runtime:
    kind: native
    exec: target/debug/orion-kafka-bridge
    args: ["--direction", "orion->kafka"]
    env:
      KAFKA_BROKERS: "localhost:9092"
      KAFKA_TOPIC: orion-events
      NATS_URL: "nats://127.0.0.1:4222"
      ORION_QUEUE_NAME: events
      ORION_QUEUE_SUBJECT: orion.queue.events
      ORION_QUEUE_STREAM: ORION_QUEUE_EVENTS
```

```bash
orion gen queue events --type work | orion apply -f -
orion apply -f bridge-out.yaml
orion dispatch Service bridge-out
ps -ef | orion json | orion queue pub events
# Verify on Kafka:
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orion-events --from-beginning
```

### Kafka → Orion

```yaml
# bridge-in.yaml
apiVersion: orionmesh.dev/v1
kind: Service
metadata: { name: bridge-in }
spec:
  replicas: 1
  restart_policy: on_failure
  runtime:
    kind: native
    exec: target/debug/orion-kafka-bridge
    args: ["--direction", "kafka->orion"]
    env:
      KAFKA_BROKERS: "localhost:9092"
      KAFKA_TOPIC: incoming-things
      KAFKA_GROUP: orion-bridge
      NATS_URL: "nats://127.0.0.1:4222"
      ORION_QUEUE_NAME: incoming
      ORION_QUEUE_SUBJECT: orion.queue.incoming
      ORION_QUEUE_STREAM: ORION_QUEUE_INCOMING
```

Each Kafka record arrives on the queue with `kafka_topic`,
`kafka_partition`, `kafka_offset`, and the parsed body.

## Envelope shapes

### Outbound (Kafka record body)

```json
{
  "orion_subject": "orion.queue.events",
  "at": "2026-06-30T12:00:00Z",
  "body": { "n": 42, "msg": "hello" }
}
```

### Inbound (Orion queue row)

```json
{
  "kafka_topic": "incoming-things",
  "kafka_partition": 0,
  "kafka_offset": 12345,
  "at": "2026-06-30T12:00:00Z",
  "body": { "from": "kafka" },
  "_subject": "orion.queue.incoming"
}
```

## Why use this

- Bridge OrionMesh's lightweight orchestration into an existing
  Kafka-based event backbone — `orion queue pub` flows out, downstream
  Kafka consumers pick up.
- Receive notifications from any Kafka producer (third-party services,
  CI, CDC streams) and let OrionMesh's workflow + Service stack react.
- Hybrid clusters where OrionMesh handles the small/native workloads
  and Kafka handles the high-throughput / long-retention stream.
