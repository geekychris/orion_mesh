# Diagnostics and observability

Five HTTP endpoints, a UI tab, and a CLI skill that together give a single-pane view of what's running, what's queued, and where things are stuck.

For the wider API see [`docs/usage.md`](usage.md). For the runtime semantics see [`docs/architecture.md`](architecture.md) and [`docs/ipc.md`](ipc.md).

---

## TL;DR

| Surface | What it shows | Where |
|---|---|---|
| `GET /v1/diag/system` | Controller + agents + instance/schedule/log stats + NATS health | curl / UI Diag tab |
| `GET /v1/diag/jetstream` | NATS streams + consumers (lag, pending, delivered seq) | curl / UI Diag tab |
| `GET /v1/instances` | Every replica across every workload | curl / UI Diag tab |
| `GET /v1/logs/search?q=…` | Substring across every workload's log ring | curl / UI Diag tab |
| `POST /v1/control/{kind}/{name}/stop` | Stop ALL replicas of a workload | curl / per-workload detail panel |
| `POST /v1/control/{kind}/{name}/restart` | Stop + re-dispatch | curl / per-workload detail panel |
| **Diag** tab in the UI | All of the above, polled every 4-5s | `http://127.0.0.1:7879` → click **Diag** |
| **orion-diag** Claude skill | Driven from prompts like "what's running?" | `.claude/skills/orion-diag/` |

---

## `/v1/diag/system` — comprehensive overview

The first stop. One call returns enough to triage 90% of issues.

```bash
curl -s http://127.0.0.1:7878/v1/diag/system | jq .
```

Shape:

```json
{
  "controller": {
    "version": "0.1.0",
    "started_at": "2026-06-26T21:29:53Z",
    "uptime_seconds": 240,
    "nats_url": "nats://127.0.0.1:4222",
    "auth_disabled": true
  },
  "agents": 1,
  "nodes": [
    {
      "node_id": "demo-mac",
      "agent_version": "0.1.0",
      "last_seen_at": "2026-06-26T21:33:53Z",
      "seconds_since_seen": 0
    }
  ],
  "instances": {
    "total": 5,
    "by_workload": [
      { "kind": "Service", "name": "demo-pub", "instance_count": 1 },
      { "kind": "Service", "name": "demo-sub-workers", "instance_count": 3 }
    ]
  },
  "schedules": { "armed": 2, "fired_total": 47 },
  "logs": { "buffered_lines": 550, "workloads_with_logs": 4 },
  "nats": {
    "connected": true,
    "url": "nats://127.0.0.1:4222",
    "monitoring_url": "http://127.0.0.1:8222",
    "server_info": { /* /varz */ }
  }
}
```

### How to read it

| Field | Healthy | Worth investigating |
|---|---|---|
| `agents` | matches your fleet size | < fleet → some agent stopped reporting (look at `nodes[].seconds_since_seen`) |
| `nodes[].seconds_since_seen` | 0–10 | > 30 → that agent's heartbeat stalled or its process died |
| `nats.connected` | `true` | `false` → controller can't reach the NATS monitoring port — broker may be down, or `--nats-url`'s host doesn't have a 8222 monitor |
| `instances.total` | matches dispatches | larger than expected → leftover state from earlier sessions (Stop them via the UI or `POST /v1/control/.../stop`) |
| `schedules.fired_total` | grows over time | stuck at 0 with armed Schedules whose `next_fire_at` is in the past → controller tick loop crashed; look at controller stderr |
| `logs.buffered_lines` | grows, capped per-workload at ~500 | very large + steady → something is logging hot; use `/v1/logs/search` to find what |

NATS monitoring URL is derived by swapping the connect port (`4222`) for the conventional monitoring port (`8222`). If your broker doesn't expose `8222` you'll see `nats.connected: false` even when the control plane is healthy — that's a docs gap, not an outage.

---

## `/v1/diag/jetstream` — stream + consumer state

```bash
curl -s http://127.0.0.1:7878/v1/diag/jetstream | jq '.streams, .consumers'
```

Returns:

```json
{
  "streams": [
    {
      "name": "ORION_DEMO_JS",
      "subjects": ["orion.demo.js.>"],
      "messages": 45,
      "bytes": 4321,
      "first_seq": 1,
      "last_seq": 45,
      "consumer_count": 2
    }
  ],
  "consumers": [
    {
      "stream": "ORION_DEMO_JS",
      "name": "workers",
      "num_pending": 0,
      "num_ack_pending": 0,
      "delivered": 45,
      "last_ack_floor": 45
    }
  ]
}
```

### Reading consumer state

```
delivered - last_ack_floor = consumer lag
```

| Lag | Meaning |
|---|---|
| `0` | Consumer is keeping up — every delivered message has been acked |
| Small + steady | Normal under load; consumer is processing as messages arrive |
| Growing fast | Consumer is falling behind — scale out (add replicas with the same `--durable` name) or speed up the processing |
| Capped, not draining | Consumer is stuck — the workload is alive but not acking. Look at its logs (`/v1/logs/Service/<name>`) |
| Equal to `messages` | Consumer hasn't started yet (just connected) or got `--deliver_policy=all` and is catching up |

`num_pending` is the JetStream-side count of messages not yet delivered to *anyone* in the consumer's queue. `num_ack_pending` is what's been delivered but not acked.

### Common scenarios

- **"My consumer isn't getting messages."** Check `num_pending`. If it's > 0 but `delivered` isn't growing, your subscriber isn't pulling. If `num_pending` is 0 the stream might not be subscribed to the subjects you think — check `subjects`.
- **"My consumer is way behind."** Lag is growing. Either: more replicas with the same `--durable` name (load-balanced), or speed up per-message work.
- **"A consumer hangs around forever."** Ephemeral consumers stay around for `inactive_threshold`. Durable consumers stay around until you delete them.

---

## `/v1/instances` — every replica, flat

```bash
curl -s http://127.0.0.1:7878/v1/instances | jq .
```

```json
[
  {
    "instance_id": "27bea55b-7ff1-4bc9-ad44-40e537b81ddc",
    "kind": "Service",
    "name": "diag-chatty",
    "node": "demo-mac",
    "replica_index": 0,
    "dispatched_at": "2026-06-26T21:30:13Z",
    "first_seen_at": "2026-06-26T21:30:13Z",
    "last_seen_at":  "2026-06-26T21:30:18Z",
    "line_count": 6
  },
  ...
]
```

This is the same registry the per-workload `/v1/instances/{kind}/{name}` endpoint reads from. The list endpoint exists so you can see "what's actually running" without having to iterate through every workload.

### Spotting trouble

- **`last_seen_at` is old**: instance is dead but not cleaned up. Run `POST /v1/control/{kind}/{name}/stop` to remove from the registry (and kill the process if it's actually still there).
- **`replica_index` jumps**: e.g. r0 + r4 with no r1/r2/r3 → previous dispatches left stale state. The Stop button (or `/stop` endpoint) clears them; next dispatch numbers from 0 again.
- **`line_count` is 0** but `last_seen_at` is fresh: process is running but not printing. Common for `/bin/sleep` workloads.

---

## `/v1/logs/search?q=…` — find a line across every workload

```bash
curl -s "http://127.0.0.1:7878/v1/logs/search?q=ERROR&limit=50" | jq .
```

Query params:

| Param | Default | Meaning |
|---|---|---|
| `q` | required | substring (case-sensitive) |
| `kind` | any | scope to one resource kind (e.g. `Service`) |
| `name` | any | scope to one workload name |
| `limit` | 200 | max hits |

Returns hits sorted newest-first with full workload identity attached. The search runs over the in-memory ring buffer — bounded at ~500 lines per workload — so it can't surface old logs from before the controller restarted. For long-term retention, pipe `/v1/logs/...` into a file (see `docs/usage.md §5`).

### Patterns

```bash
# Find recent errors across the whole fleet
curl -s "$CTRL/v1/logs/search?q=ERROR&limit=20" | jq -r '.[] | "[\(.at)] \(.kind)/\(.name) \(.line)"'

# Find which replica handled tick=42 across a queue group
curl -s "$CTRL/v1/logs/search?q=tick%2042" | jq -r '.[] | "\(.kind)/\(.name) \(.line)"'

# Stuck-consumer-detection — find lines that say 'sent: tick N' without
# matching 'recv: tick N'
curl -s "$CTRL/v1/logs/search?q=sent:%20tick%2042" | jq .   # publisher saw it
curl -s "$CTRL/v1/logs/search?q=recv:%20tick%2042" | jq .   # any subscriber saw it?
```

---

## `POST /v1/control/{kind}/{name}/stop` — kill all replicas by name

Finds every tracked instance of the named workload in the controller's `InstanceRegistry` and publishes a `ControlStop` envelope to each instance's node on `orion.control.<node>.stop`. The agent's existing stop handler kills the matching child via `NativeAdapter.stop(instance_id)`.

```bash
curl -sS -X POST $CTRL/v1/control/Service/demo-sub-workers/stop
# → {"kind":"Service","name":"demo-sub-workers","stopped":3,"nodes":["demo-mac"]}
```

This drains the instances from the registry — they no longer appear in `/v1/instances` after the call returns. It does **not** delete the Resource; the Service YAML is still in SQLite, ready to be dispatched again.

If the workload isn't tracked (never dispatched, or all instances already gone) you get a `404`.

## `POST /v1/control/{kind}/{name}/restart`

Calls stop, waits 300 ms for the agent to reap children, then calls dispatch. Returns `{kind, name, stopped, redispatched: true, node, instance_id}`.

Useful when you've changed the YAML and want the new version running:

```bash
curl -X POST $CTRL/v1/resources/apply --data-binary @web.yaml
curl -X POST $CTRL/v1/control/Service/web/restart
```

---

## UI: the Diag tab

`http://127.0.0.1:7879` → **Diag**. Four cards, all polled every 4-5 seconds:

| Card | What it surfaces |
|---|---|
| **System overview** | The flat readable form of `/v1/diag/system` — uptime, NATS status, every node with `last_seen` deltas, instance counts per workload, schedule armed + total fires, log buffer size |
| **NATS JetStream** | Two tables: streams (name, subjects, messages, first/last seq, consumer count) and consumers (pending, ack pending, delivered seq, ack floor with lag highlighted) |
| **All instances** | Grouped by workload with a **Stop all** button per row. Shows replicas list (r0, r1, …), nodes, total line count |
| **Log search** | Text input + Search button — substring across every workload's log ring; results show `[ts] kind/name line` |

Per-workload detail panels (Service / Task tabs) also get new buttons:
- **Dispatch** — existing
- **Stop all** — calls `/v1/control/{kind}/{name}/stop`
- **Restart** — stop + re-dispatch
- **Edit (copy to Apply)** — existing
- **Delete** — existing

---

## CLI: the `orion-diag` skill

```bash
python3 .claude/skills/orion-diag/scripts/diag.py
```

Prints the same four sections as the Diag tab, in plain text. Options:

| Flag | Meaning |
|---|---|
| `--section system` | system overview only |
| `--section jetstream` | streams + consumers only |
| `--section instances` | flat instance list only |
| `--search QUERY` | log search |
| `--json` | raw JSON for piping into `jq` |

Reads `$ORION_CONTROLLER_URL` (default `http://127.0.0.1:7878`) and `$ORION_CLUSTER_TOKEN` (optional bearer).

Driven from Claude prompts like:
- "what's the system doing?"
- "anything stuck?"
- "show me jetstream"
- "is the queue draining?"

The skill's [SKILL.md](../.claude/skills/orion-diag/SKILL.md) lists every trigger phrase.

---

## Common troubleshooting recipes

### "The agent disappeared"

```bash
python3 .claude/skills/orion-diag/scripts/diag.py --section system | head -10
```

If `agents` is `0` and the node's `seconds_since_seen` is large: the agent crashed or its NATS connection dropped. Check the agent's stderr (`/tmp/agent.log` if you started it backgrounded).

### "Messages are being published but not received"

```bash
# 1. Are messages reaching the broker at all?
curl -s "$CTRL/v1/diag/jetstream" | jq '.streams[] | {name, messages, last_seq}'

# 2. Are consumers acking?
curl -s "$CTRL/v1/diag/jetstream" | jq '.consumers[] | {stream, name, delivered, last_ack_floor, lag: (.delivered - .last_ack_floor)}'

# 3. Are subscribers connecting?
curl -s "$CTRL/v1/diag/system" | jq '.instances.by_workload'
```

### "A workload is leaking replicas"

```bash
# How many replicas does the controller think exist?
curl -s "$CTRL/v1/instances" | jq 'group_by(.kind + "/" + .name) | map({wl: .[0].kind + "/" + .[0].name, count: length})'

# Stop them all by name
curl -X POST "$CTRL/v1/control/Service/<name>/stop"

# If processes are still running on the host (because ControlStop arrived but
# missed): kill by exec name
pkill -f '<exec>'
```

### "An old log message is haunting me"

The log buffer is in-memory and bounded; it survives the controller process but not a restart. To clear without restart, restart just the workload — its ring resets only when the controller restarts. If you really need it gone, restart the controller (`pkill -f orion-controller; cargo run -p orion-controller -- ...`).

---

## What's not here (yet)

- **Persistent log storage**: today's logs live in a ring buffer per workload, ~500 lines each. Persistent log streams (durable JetStream consumers, or a separate log store) are later-phase.
- **Process-level metrics (CPU%, RSS)**: agent doesn't yet sample child processes. The instance registry tracks `line_count` as a rough activity proxy.
- **Audit log**: dispatch/stop/restart events aren't persisted — they appear in the controller's tracing output but vanish on restart.
- **Distributed tracing**: spans across dispatch → control → log → consumer ack would help diagnose cross-component latency, but they're not wired today.

Each of these is a logical extension and would slot in next to the existing endpoints.
