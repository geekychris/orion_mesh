# 24 · SQLite CDC tap

`orion-sqlite-tap` watches a SQLite table's `rowid` and publishes each
new row to an OrionMesh queue as a `CdcEvent`. The smallest possible
change-data-capture pattern — useful when an existing app already
writes to SQLite and you want those rows to flow into OrionMesh
workflows.

## Env

| Var | Required | Default |
|---|---|---|
| `SQLITE_URL` | ✓ | e.g. `sqlite:///tmp/app.db` |
| `SQLITE_TABLE` | ✓ | |
| `ORION_QUEUE_SUBJECT` | ✓ | |
| `ORION_QUEUE_STREAM` | ✓ | |
| `NATS_URL` | | `nats://127.0.0.1:4222` |
| `TAP_INTERVAL_SECONDS` | | `2` |
| `TAP_FROM_ZERO` | | `false` (start at current MAX(rowid)) |

## Service spec

```yaml
apiVersion: orionmesh.dev/v1
kind: Service
metadata: { name: orders-tap }
spec:
  replicas: 1
  restart_policy: on_failure
  runtime:
    kind: native
    exec: target/debug/orion-sqlite-tap
    args: []
    env:
      SQLITE_URL: "sqlite:///var/lib/myapp/orders.db"
      SQLITE_TABLE: orders
      NATS_URL: "nats://127.0.0.1:4222"
      ORION_QUEUE_NAME: orders-cdc
      ORION_QUEUE_SUBJECT: orion.queue.orders-cdc
      ORION_QUEUE_STREAM: ORION_QUEUE_ORDERS_CDC
      TAP_INTERVAL_SECONDS: "1"
      RUST_LOG: info
```

## Event shape

```json
{
  "at": "2026-06-30T12:00:00Z",
  "table": "orders",
  "rowid": 4242,
  "row": {
    "id": "ord-4242",
    "customer": "acme",
    "total_cents": 12345
  },
  "_subject": "orion.queue.orders-cdc"
}
```

## Demo (drop-in)

```bash
# Create a tiny db with a couple of rows
mkdir -p /tmp/cdc-demo
sqlite3 /tmp/cdc-demo/orders.db <<'EOF'
CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, customer TEXT, total_cents INTEGER);
INSERT INTO orders (customer, total_cents) VALUES ('alpha', 100), ('beta', 200);
EOF

# Apply queue + tap
orion gen queue orders-cdc --type work | orion apply -f -
orion apply -f orders-tap.yaml      # (use the spec above)
orion dispatch Service orders-tap

# Insert more rows from another shell:
sqlite3 /tmp/cdc-demo/orders.db "INSERT INTO orders (customer, total_cents) VALUES ('gamma', 300);"

# Consume:
orion queue sub orders-cdc --group reader --limit 1
```

## Caveats

- Only INSERTs are captured. UPDATEs and DELETEs aren't detectable
  via rowid alone — for those, build a trigger that writes to a CDC
  log table and tap *that*.
- Cursor is in-memory; if the tap restarts and `TAP_FROM_ZERO` is
  unset, it starts at the current `MAX(rowid)` and skips anything
  written in the gap. For durable cursors, persist on the tap side
  or use SQLite WAL with a higher TAP_INTERVAL_SECONDS.

## Why use this

- App is already writing to SQLite; don't want to change it.
- Need to fan out new rows to queues / workflows / OpenSearch /
  downstream services without coupling them to the schema.
- Simpler than setting up Debezium / a dedicated CDC server for the
  small-cluster case.
