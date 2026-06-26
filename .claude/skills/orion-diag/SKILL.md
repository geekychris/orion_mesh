---
name: orion-diag
description: Full diagnostic snapshot — controller / agents / instances / NATS JetStream / schedule observers / log buffer / process list. Use when the user says "what's the system doing?", "diagnose orion", "anything stuck?", "show me jetstream", "is the queue draining?", "any zombie processes?", "give me a debug dump", or similar.
---

# orion-diag

Hits the controller's diagnostics endpoints and prints a single compact dashboard. Useful for "what's actually going on" or as a first step before deeper drilling.

## How to use

```bash
python3 .claude/skills/orion-diag/scripts/diag.py
# Or scope to one section:
python3 .claude/skills/orion-diag/scripts/diag.py --section system
python3 .claude/skills/orion-diag/scripts/diag.py --section jetstream
python3 .claude/skills/orion-diag/scripts/diag.py --section instances
# JSON for piping:
python3 .claude/skills/orion-diag/scripts/diag.py --json | jq '.system.nats'
```

Endpoints hit:

| Endpoint | What it shows |
|---|---|
| `GET /v1/diag/system` | Controller info, agent count, all nodes, instance/schedule/log stats, NATS server info |
| `GET /v1/diag/jetstream` | NATS streams + consumers (pending, ack pending, delivered seq, ack floor → lag) |
| `GET /v1/instances` | Every tracked instance with replica_index + node + first/last-seen + line count |
| `GET /v1/logs/search?q=…` | Substring search across every workload's log ring (not called by default; use `--search QUERY`) |

## When to use this skill

- "Something's stuck" — diag shows whether the consumer lag is growing, an instance went stale, a node fell off, or a schedule hasn't fired.
- "How busy is the system?" — instance count + log line count + JetStream pending give you that.
- Sanity check after a config change: was the new agent picked up? Did NATS stay connected?

## When NOT to use this skill

- The user wants to see logs of a specific workload — `orion-logs` is more focused.
- They want to *change* state (start/stop) — `orion-run-task` / `orion-run-service` / `orion-manage`.
- They want the raw JSON — they can `curl $CTRL/v1/diag/system` directly.

## Reading the output

- **Agents** count is "nodes with last_seen_at < 30s". An agent that crashed shows as a stale node.
- **JetStream Consumer lag** = `delivered - ack_floor`. Non-zero with a steady stream of messages = the consumer is keeping up; growing fast = consumer is falling behind; capped + not draining = consumer is stuck (look at the workload's logs).
- **Instances total** counts replicas, not workloads — a Service with `replicas: 3` adds 3.
- **Log buffer** is in-memory; bounded ~500 lines per workload.

## Exit codes

- `0` — diagnostics retrieved
- `2` — controller unreachable
- Otherwise pass-through from HTTP
