# Named queues

OrionMesh's `Queue` resource is a thin declarative wrapper over a NATS
JetStream stream + a delivery-semantics decision. It exists so that:

- Queues show up as first-class resources (`orion get queues`, the UI's
  Resources tab, ships in YAML / git).
- Producers and consumers can refer to them by name and have the platform
  inject the right subject + stream + durable-name.
- Type drift (publisher thinks work, subscriber expects topic) is
  impossible: there's exactly one declared truth per name.

## The two types

| Type | Each message goes to | Underlying mechanic |
|---|---|---|
| **work** | exactly one consumer | All subscribers share a single JetStream **durable** — JetStream load-balances. |
| **topic** | every consumer | Every subscriber has its own durable — JetStream tracks a separate cursor per consumer, so each sees the full stream. |

Both backends use a JetStream stream so messages survive a subscriber restart
(at-least-once delivery). The difference is enforced on the *subscriber*
side: a work-mode subscriber must share its durable name with peers; a
topic-mode subscriber must not.

## Conventions

For a queue named `<name>`:

| Element | Default |
|---|---|
| subject | `orion.queue.<name>` |
| stream  | `ORION_QUEUE_<NAME_UPPER>` (uppercased, non-alnum → `_`) |

Both are overridable via `spec.subject` / `spec.stream`. The defaults are
deliberate: anywhere you find `orion.queue.*` on the broker, it's a managed
queue.

## YAML

```yaml
apiVersion: orionmesh.dev/v1
kind: Queue
metadata: { name: ps-rows }
spec:
  type: work             # or topic
  max_age_seconds: 3600  # JetStream drops anything older
  max_msgs: 100000       # …or anything past this count
  max_bytes: 1073741824  # …or this many total bytes
  replicas: 1            # JetStream stream replica factor
```

Or via the generator:

```bash
orion gen queue ps-rows --type work --max-age 1h
```

## CLI

| Command | What |
|---|---|
| `orion queue pub <name>` | reads ndjson stdin, publishes each line |
| `orion queue sub <name> [--group G]` | subscribes (work=shared durable, topic=private durable) |
| `orion queue ls` | declared queues + live message / consumer counts |
| `orion queue describe <name>` | spec + per-consumer state |
| `orion queue purge <name> [--yes]` | drop all messages |

## Subject routing

`orion queue pub <name> --subject-from <field>` appends `.${row[field]}` to
the publish subject. Consumers can filter via JetStream subject wildcards
(set `spec.subject` to `orion.queue.<name>.>`). Useful when one queue carries
mixed row types.

## Lifecycle

The Queue resource is **declared first**. `orion queue pub` against a missing
queue errors with a hint:

```
queue ps-rows not found. Run:
  orion gen queue ps-rows --type work | orion apply -f -
```

The JetStream stream itself is created lazily on the first `pub` or `sub`
using the spec — this avoids the controller needing a NATS connection at
apply time.

## When NOT to use a Queue

JetStream costs disk I/O and per-message acks. For ephemeral telemetry where
dropping is fine, use raw core NATS subjects (`orion-bus::client::connect` +
`async_nats::Client::publish`) — see [`./ipc.md`](./ipc.md) for the
lower-level patterns these queues are built on top of.

## Processors

A "processor" is a Service that consumes from a queue. The reference
templates live in [`../examples/10-queues/python/processor.py`](../examples/10-queues/python/processor.py)
and [`../examples/10-queues/java/`](../examples/10-queues/java/). They both
read their config from env (`ORION_QUEUE_NAME`, `ORION_QUEUE_SUBJECT`,
`ORION_QUEUE_STREAM`, `ORION_QUEUE_TYPE`, `ORION_QUEUE_GROUP`,
`ORION_REPLICA_INDEX`) and call a `handle(row)` function per message.

`orion gen processor <name> --queue <q> --lang python|java` scaffolds the
Service YAML wired up for either mode. See
[`./debugging-processors.md`](./debugging-processors.md) for how to attach
a debugger.
