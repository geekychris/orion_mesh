# 23 · Prometheus integration

Two modes:

1. **Scrape mode** — `orion-prom-scrape --mode scrape` periodically
   GETs each URL in `SCRAPE_TARGETS`, parses the Prometheus text
   format, and publishes each sample as a JSON message to a queue.
2. **Alertmanager mode** — `orion-prom-scrape --mode alertmanager`
   listens on `$BIND` (default `0.0.0.0:9090`) for POSTs from
   Prometheus Alertmanager and publishes each alert to the queue.

The controller already exposes `/metrics` natively — this is for
scraping *other* workloads (or non-OrionMesh services) into OrionMesh
queues, and for *receiving* Alertmanager fires so OrionMesh workflows
can react.

## Scrape mode

```yaml
apiVersion: orionmesh.dev/v1
kind: Service
metadata: { name: prom-scraper }
spec:
  replicas: 1
  restart_policy: on_failure
  runtime:
    kind: native
    exec: target/debug/orion-prom-scrape
    args: ["--mode", "scrape"]
    env:
      SCRAPE_TARGETS: "http://127.0.0.1:7878/metrics,http://my-app:8080/metrics"
      SCRAPE_INTERVAL_SECONDS: "15"
      NATS_URL: "nats://127.0.0.1:4222"
      ORION_QUEUE_NAME: prom-samples
      ORION_QUEUE_SUBJECT: orion.queue.prom-samples
      ORION_QUEUE_STREAM: ORION_QUEUE_PROM_SAMPLES
      RUST_LOG: info
```

Each sample row looks like:

```json
{
  "at": "2026-06-30T12:00:00Z",
  "source": "http://127.0.0.1:7878/metrics",
  "name": "orion_agents_live",
  "labels": {"status": "healthy"},
  "value": 3.0
}
```

## Alertmanager mode

Configure Alertmanager to POST to the OrionMesh receiver:

```yaml
# alertmanager.yml
receivers:
  - name: orion
    webhook_configs:
      - url: http://orion-receiver.local:9090/alerts
route:
  receiver: orion
```

Then:

```yaml
apiVersion: orionmesh.dev/v1
kind: Service
metadata: { name: alertmanager-receiver }
spec:
  replicas: 1
  restart_policy: on_failure
  runtime:
    kind: native
    exec: target/debug/orion-prom-scrape
    args: ["--mode", "alertmanager"]
    env:
      BIND: "0.0.0.0:9090"
      NATS_URL: "nats://127.0.0.1:4222"
      ORION_QUEUE_NAME: alerts
      ORION_QUEUE_SUBJECT: orion.queue.alerts
      ORION_QUEUE_STREAM: ORION_QUEUE_ALERTS
  ports:
    - { name: http, port: 9090, protocol: tcp }
```

Each fired alert lands on the queue. Wire a processor that
`dispatch`es a remediation Task per alert, or just records the alert
to OpenSearch for later review.

## Why use this

- **Centralised metric collection** without running a full Prometheus
  server — for small clusters where the OrionMesh `/metrics` endpoint
  + a couple of scraped apps is enough.
- **Alert-to-Task** flow: Alertmanager fires → OrionMesh dispatches
  a remediation Task (restart service, run diagnostic, ping ops).
- **Alert audit trail**: every fired alert persisted on a queue,
  consumable by an OpenSearch sink for historical analysis.
