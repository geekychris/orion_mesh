# Usage

How to actually use OrionMesh once it's installed. For setup see [installation.md](installation.md). For the system model see [architecture.md](architecture.md). For walking the example tree see [examples.md](examples.md).

> **What's live today.** The full resource model parses + persists; `Service` and `Task` with `runtime: native` **actually launch on an agent and stream stdout/stderr back through the bus**; `Schedule` resources fire on cron; two Services can talk to each other over the mesh's NATS broker (see [examples/09-ipc](../examples/09-ipc/)). The full reconciler + multi-node scheduler + non-native runtime adapters (Docker, Python, Java, …) land in Phase 4–5.

---

## 1. Surface map

```mermaid
flowchart LR
    you((you))
    cli["orion (CLI)"]
    http["HTTP /v1/*"]
    ui["orion-ui (web)"]
    api["controller HTTP API"]
    bus["NATS (under the hood)"]

    you --> cli
    you --> http
    you --> ui

    cli --> api
    http --> api
    ui --> api

    api --> bus
```

You interact with the controller, not NATS or agents directly. The CLI just wraps `curl`-style calls.

---

## 2. The CLI

The CLI binary is `orion` (from the `orion-cli` crate).

```bash
orion --help
```

Today's `orion` CLI commands:

| Command | What it does |
|---|---|
| `orion validate <file.yaml>` | Parse the file into a `Resource`, run semantic checks, print kind+name. No controller call. |
| `orion get nodes` | `GET /v1/nodes` |

Most operator workflows hit the controller HTTP API directly with `curl` — apply, dispatch, logs, delete, schedule observe — see §5. The CLI grows to match in upcoming phases:

| Command | Status |
|---|---|
| `orion apply -f <file>` | live as `POST /v1/resources/apply` |
| `orion delete <kind>/<name>` | live as `DELETE /v1/resources/{kind}/{name}` |
| `orion dispatch <kind>/<name>` | live as `POST /v1/dispatch/{kind}/{name}` |
| `orion logs <kind>/<name>` | live as `GET /v1/logs/{kind}/{name}` |
| `orion get services / tasks / capabilities` | live as `GET /v1/resources/{Kind}` |
| `orion find capability <cap> attr=value` | Phase 4 |

### Pointing the CLI at a remote controller

```bash
ORION_CONTROLLER_URL=https://controller.belmont.local:7878 orion get nodes
```

When auth is enforced, also pass the token. (CLI bearer handling is on the near-term punch list — for now, use `curl` with a bearer header.)

---

## 3. Authoring resources

Every desired-state document has the same four-block layout:

```yaml
apiVersion: orionmesh.dev/v1   # defaulted if omitted
kind: <Kind>
metadata:
  name: <dns-1123 name>
  namespace: <optional>
  labels: { ... }
  annotations: { ... }
  generation: <optional, set by controller>
spec:
  # per-kind, see sections below
status:
  # observed; written by the controller, not the operator
```

`orion validate` parses the file and runs `Resource::validate()`. The validator catches things serde can't — e.g. a Schedule with neither `task:` nor `task_template:`.

### 3.1 Valid kinds

```
Node Service Task Job Schedule Dataset Model Project
Secret Volume Network Runtime Capability Policy Integration
```

If you misspell the kind, the parse error lists every valid alternative. Same for `runtime.kind`.

### 3.2 Service

Long-running workload. Reconciler keeps `replicas` instances of it healthy.

```yaml
apiVersion: orionmesh.dev/v1
kind: Service
metadata:
  name: amiga-search
  labels: { site: belmont }
spec:
  runtime:
    kind: docker
    image: amiga-search:latest
  replicas: 1
  placement:
    arch: [arm64, x86_64]
    os: [linux]
  requires:
    search:
      dataset: amiga_schematics
  capabilities:
    - name: search
      attributes:
        dataset: amiga_schematics
        protocol: http
  ports:
    - { name: http, port: 8080 }
    - { name: metrics, port: 9090 }
  health:
    kind: http
    path: /healthz
    port: 8080
    interval_seconds: 10
    failure_threshold: 3
  restart_policy: on_failure
```

### 3.3 Task

One-shot workload with retry semantics.

```yaml
kind: Task
metadata: { name: train-once }
spec:
  runtime: { kind: python, module: train, venv: ./.venv }
  placement:
    gpu: { vendor: nvidia, min_vram_gb: 24 }
    acceleration: cuda
  prefer_data_locality: true
  timeout_seconds: 7200
  retry: { max_attempts: 3, backoff_seconds: 60 }
```

`prefer_data_locality: true` tells the scheduler to score nodes that already hold a referenced Dataset higher.

### 3.4 Schedule

Cron-fires a Task. Either reference a Task by name *or* inline a template — not both.

```yaml
kind: Schedule
metadata: { name: nightly-train }
spec:
  cron: "0 2 * * *"
  task: train-once       # mutually exclusive with task_template
# or:
# spec:
#   cron: "0 2 * * *"
#   task_template:
#     runtime: { kind: native, exec: /usr/local/bin/snapshot }
```

### 3.5 Dataset

Tells OrionMesh where data lives. Used for capability-aware scheduling and `find dataset`.

```yaml
kind: Dataset
metadata: { name: amiga-schematics }
spec:
  locations:
    - { node: pi5, path: /data/amiga, access: ro }
    - { node: mac-studio, path: /Volumes/data/amiga, access: rw }
  formats: [pdf, png]
  capabilities: [search]
  size_bytes: 12345678
```

`access` is one of `ro`, `rw`, `wo`.

### 3.6 Model

LLM / ONNX / MLX models with variant-level resource hints.

```yaml
kind: Model
metadata: { name: qwen-coder }
spec:
  model_id: qwen2.5-coder-32b
  variants:
    - { format: gguf, quant: q4_k_m, memory_gb: 22.0, context_window: 32768, preferred_runtime: "llama.cpp" }
    - { format: mlx,  quant: int8,   memory_gb: 36.0, context_window: 32768, preferred_runtime: mlx }
  served_by: [mac-studio]
```

The scheduler will pick a variant whose `memory_gb` fits the candidate node.

### 3.7 Node (declarative side)

You can declare a node ahead of time. The agent's observed inventory always wins for liveness, but the declared form is useful for asserting roles or labels.

```yaml
kind: Node
metadata: { name: gpu-rig }
spec:
  node_id: gpu-rig
  roles: [worker, llm]
  arch: x86_64
  os: linux
  gpus:
    - { vendor: nvidia, vram_gb: 24, name: "RTX 4090" }
  acceleration: cuda
  resources: { cpu_cores: 16, memory_gb: 64 }
  runtimes: [native, docker, python, llm]
  labels: { site: belmont, power: mains }
```

### 3.8 Runtime (peer catalog)

Registers an external runtime system (OrionMesh in another site, KQueue, …) so workloads can declare `runtime: { kind: peer, system: <name>, ref: <id> }`.

```yaml
kind: Runtime
metadata: { name: orionmesh-belmont }
spec:
  runtime_kind: orionmesh
  base_url: "http://controller.belmont.local:7878"
  admin_ui_url: "http://controller.belmont.local:7879"
  config:
    natsUrl: "nats://nats.belmont.local:4222"
```

### 3.9 Capability (declared schema)

Optional — declares the attribute shape of a capability so other consumers can validate selectors.

```yaml
kind: Capability
metadata: { name: search }
spec:
  capability: search
  description: "Full-text or vector lookup over a dataset"
  attribute_schema:
    type: object
    properties:
      dataset: { type: string }
      protocol: { type: string, enum: [http, grpc] }
```

### 3.10 Secret, Volume, Network

Smaller stubs:

```yaml
kind: Secret
metadata: { name: openai-api-key }
spec:
  vault_ref: "plaintext://openai-api-key"
---
kind: Volume
metadata: { name: scratch }
spec: { path: /mnt/scratch, mounted_on: [mac-studio], size_gb: 500 }
---
kind: Network
metadata: { name: belmont }
spec: { cidr: 10.10.0.0/16, sites: [belmont] }
```

---

## 4. Placement and capability selectors

This is where OrionMesh diverges from a generic orchestrator. You can constrain placement on hardware *and* on advertised capabilities.

### 4.1 Hard constraints

```yaml
placement:
  arch: [arm64, x86_64]            # ANY-of
  os: [linux]                       # ANY-of
  gpu: { vendor: nvidia, min_vram_gb: 24 }
  acceleration: cuda
  node_labels: { site: belmont }    # ALL-of (key must match value)
```

Empty placement = matches anything.

### 4.2 Soft preferences (scoring — wired in Phase 5)

```yaml
placement:
  arch: [arm64, x86_64]
  prefer:
    node_labels: { power: mains }   # bonus points for mains-powered
    data_locality: true             # bonus points for nodes that hold the dataset
```

### 4.3 Capability requirements

Reuses the same JSON-ish shape that services advertise. Each requirement is `capability → { attr: AttrMatch }`.

```yaml
requires:
  llm:
    model: qwen-coder
    gpu:
      min_vram_gb: { gte: 24 }      # Op form
  search:
    dataset: amiga_schematics       # Equals (bare value)
    format: [pdf, png]              # OneOf (array)
```

Three forms of attribute match — picked by JSON shape:

```mermaid
flowchart LR
    A["bare value<br/>(string/number/null/object)"] --> E["Equals"]
    B["JSON array<br/>[a, b, c]"] --> O["OneOf"]
    C["object with only<br/>{eq, ne, gt, gte, lt, lte}"] --> P["Op"]
```

`Op` lets you write `{ gte: 24 }`, `{ lt: 100, gt: 0 }`, etc. Everything else falls back to `Equals`.

---

## 5. HTTP API

Bearer auth required unless `ORION_AUTH_DISABLED=1` was set on the controller. `/health` is intentionally outside the auth layer.

| Method + path | What it does |
|---|---|
| `GET /health` | Liveness probe (no auth) |
| `GET /v1/nodes` | Observed node list — heartbeat + inventory |
| `GET /v1/kinds` | All resource kinds (drives the UI's tab generator) |
| `GET /v1/resources/<Kind>` | List all resources of that kind |
| `GET /v1/resources/<Kind>/<name>` | Fetch one resource (404 if missing) |
| `POST /v1/resources/apply[?dry_run=1]` | Upsert from YAML body. `?dry_run=1` parses + validates without storing. Accepts `1`/`true`/`yes`/`on`/bare. |
| `DELETE /v1/resources/<Kind>/<name>` | Remove from store |
| `POST /v1/dispatch/<Kind>/<name>` | Publish `orion.control.{node}.run` to a live node for the workload. Returns `{kind, name, node, instance_id}`. |
| `GET /v1/logs/<Kind>/<name>[?since=N]` | Stream-friendly ring-buffer tail of the workload's stdout/stderr. Pass back the previous response's `total` as `since` for incremental polling. |
| `GET /v1/schedules/observed` | Cron observer state per Schedule: `armed_at`, `last_fired_at`, `next_fire_at`, `fire_count`, `last_error`. |

Phase 4 adds `POST /v1/find` for capability lookup; Phase 5 will replace the heuristic "first observed node" inside `/v1/dispatch` with the real scheduler.

### 5.1 Examples

```bash
TOKEN=$(cat ~/.config/orion/cluster.token)
CTRL=http://controller.local:7878
H=(-H "Authorization: Bearer $TOKEN")    # bash array — expands in curl

# Health (no auth)
curl $CTRL/health                                # → ok

# Nodes
curl "${H[@]}" $CTRL/v1/nodes | jq .

# Apply a service
curl "${H[@]}" -X POST --data-binary @amiga-search.yaml $CTRL/v1/resources/apply
# → {"kind":"Service","name":"amiga-search","generation":1,"dry_run":false}

# Re-apply the same body — generation stays at 1 (idempotent)
curl "${H[@]}" -X POST --data-binary @amiga-search.yaml $CTRL/v1/resources/apply
# → {"kind":"Service","name":"amiga-search","generation":1,"dry_run":false}

# Validate without storing
curl "${H[@]}" -X POST 'amiga-search.yaml' $CTRL/v1/resources/apply?dry_run=1
# → {"kind":"Service","name":"amiga-search","generation":0,"dry_run":true}

# Change a field, re-apply — generation goes to 2
sed -i '' 's/replicas: 1/replicas: 2/' amiga-search.yaml
curl "${H[@]}" -X POST --data-binary @amiga-search.yaml $CTRL/v1/resources/apply
# → {"kind":"Service","name":"amiga-search","generation":2,"dry_run":false}

# List
curl "${H[@]}" $CTRL/v1/resources/Service | jq .

# Fetch one
curl "${H[@]}" $CTRL/v1/resources/Service/amiga-search | jq .

# Dispatch — publishes ControlRun to orion.control.{node}.run
curl "${H[@]}" -X POST $CTRL/v1/dispatch/Service/amiga-search
# → {"kind":"Service","name":"amiga-search","node":"demo-mac","instance_id":"…"}

# Tail logs — pass `since=0` first; then echo back `total`
curl "${H[@]}" "$CTRL/v1/logs/Service/amiga-search?since=0" | jq .
# → { kind, name, total, entries: [ { at, node_id, stream, line }, … ] }

# Delete
curl "${H[@]}" -X DELETE $CTRL/v1/resources/Service/amiga-search

# Schedule observer
curl "${H[@]}" $CTRL/v1/schedules/observed | jq .
```

### 5.2 What `apply` + `dispatch` actually do

```mermaid
sequenceDiagram
    autonumber
    participant U as You
    participant C as orion-controller
    participant S as Store (SQLite)
    participant N as NATS
    participant A as orion-agent

    U->>C: POST /v1/resources/apply (YAML)
    C->>C: parse + Resource::validate()
    C->>S: upsert_resource
    S-->>C: generation
    C-->>U: {kind, name, generation}

    U->>C: POST /v1/dispatch/Service/<name>
    C->>S: get_resource(kind, name)
    S-->>C: Resource { runtime, … }
    C->>C: pick a node (most-recent live)
    C->>N: publish Envelope<ControlRun><br/>on orion.control.{node}.run
    N->>A: deliver ControlRun
    A->>A: instances.record(id → kind, name)
    A->>A: NativeAdapter.launch (pipes stdout/stderr)
    par stdout / stderr forwarding
        A->>N: publish Envelope<LogLine><br/>on orion.logs.{node}
        N->>C: deliver LogLine
        C->>C: LogBuffer.push(kind, name, entry)
    end
    C-->>U: {kind, name, node, instance_id}

    U->>C: GET /v1/logs/Service/<name>?since=N
    C-->>U: { total, entries: [...] }
```

Phase 5 replaces "pick a node (most-recent live)" with the real scheduler — filter by `placement`, score by `prefer:`, dispatch to the winner.

---

## 6. Resource lifecycle expectations

```mermaid
stateDiagram-v2
    [*] --> Declared : POST /v1/resources/apply
    Declared --> Pending : reconciler picks it up (Phase 5)
    Pending --> Running : agent reports ServiceRegister
    Running --> Pending : config changed (generation +1)
    Running --> Failed : health probe fails N times
    Failed --> Running : restart_policy + reconciler
    Running --> [*] : DELETE
    Failed --> [*] : DELETE
```

`status.phase` is wired but only `Declared` is automatically reachable today. Dispatched workloads do run; the per-resource `status.phase` getting updated by a reconciler is Phase 5.

---

## 7. Worked recipes (end-to-end, runnable)

The runnable demos assume a local stack (`docker run … nats:2.10 -js`, `orion-controller` on `:7878`, `orion-agent`, `orion-ui` on `:7879`, `ORION_AUTH_DISABLED=1` on every process). See [installation.md §6](installation.md#6-local-dev--fastest-path).

```bash
CTRL=http://127.0.0.1:7878
```

### 7.1 Recipe: a Service that prints, dispatched, logs in real time

```bash
# 1. Apply
curl -X POST $CTRL/v1/resources/apply --data-binary @- <<'EOF'
kind: Service
metadata: { name: chatty }
spec:
  runtime:
    kind: native
    exec: /bin/sh
    args: ["-c", "for i in 1 2 3 4 5; do echo line-$i; sleep 1; done; echo done"]
EOF
# → {"kind":"Service","name":"chatty","generation":1,"dry_run":false}

# 2. Dispatch
curl -X POST $CTRL/v1/dispatch/Service/chatty
# → {"kind":"Service","name":"chatty","node":"demo-mac","instance_id":"…"}

# 3. Tail the logs (poll every second for ~7s)
for _ in 1 2 3 4 5 6 7; do
  curl -s "$CTRL/v1/logs/Service/chatty" \
    | python3 -c "import sys,json;d=json.load(sys.stdin);print('total='+str(d['total']))"
  sleep 1
done

# 4. Final read
curl -s $CTRL/v1/logs/Service/chatty | python3 -m json.tool
```

Sample output of the last command:

```json
{
  "kind": "Service", "name": "chatty", "total": 6,
  "entries": [
    { "at": "2026-06-26T06:40:20Z", "node_id": "demo-mac", "stream": "stdout", "line": "line-1" },
    { "at": "2026-06-26T06:40:21Z", "node_id": "demo-mac", "stream": "stdout", "line": "line-2" },
    …
    { "at": "2026-06-26T06:40:25Z", "node_id": "demo-mac", "stream": "stdout", "line": "done"   }
  ]
}
```

### 7.2 Recipe: a Schedule that fires every minute

```bash
# 1. Apply a Task
curl -X POST $CTRL/v1/resources/apply --data-binary @- <<'EOF'
kind: Task
metadata: { name: cron-job }
spec:
  runtime:
    kind: native
    exec: /bin/sh
    args: ["-c", "echo fired-at-$(date +%H:%M:%S)"]
EOF

# 2. Apply a Schedule pointing at it (5-field POSIX cron, every minute)
curl -X POST $CTRL/v1/resources/apply --data-binary @- <<'EOF'
kind: Schedule
metadata: { name: every-min }
spec:
  cron: "* * * * *"
  task: cron-job
EOF

# 3. Watch the cron observer
watch -n 5 'curl -s $CTRL/v1/schedules/observed | python3 -m json.tool'
```

Within the next minute mark, `fire_count` will tick 0→1 and the Task will produce a `fired-at-HH:MM:SS` log line.

```bash
curl -s $CTRL/v1/logs/Task/cron-job | python3 -m json.tool
```

### 7.3 Recipe: two Services talking over the mesh's NATS broker

(`orion-demo-pub`/`orion-demo-sub` come from `cargo build --release -p orion-demo-bins`.)

```bash
# 1. Apply the subscriber + publisher
curl -X POST --data-binary @examples/09-ipc/demo-sub.yaml $CTRL/v1/resources/apply
curl -X POST --data-binary @examples/09-ipc/demo-pub.yaml $CTRL/v1/resources/apply

# 2. Dispatch sub FIRST so it's listening, then pub
curl -X POST $CTRL/v1/dispatch/Service/demo-sub
sleep 1
curl -X POST $CTRL/v1/dispatch/Service/demo-pub

# 3. Watch both — same timestamps end-to-end
sleep 5
echo "=== publisher stdout ==="
curl -s $CTRL/v1/logs/Service/demo-pub | python3 -c "import sys,json;d=json.load(sys.stdin);[print(e['line']) for e in d['entries'][-6:]]"
echo "=== subscriber stdout ==="
curl -s $CTRL/v1/logs/Service/demo-sub | python3 -c "import sys,json;d=json.load(sys.stdin);[print(e['line']) for e in d['entries'][-6:]]"
```

```
=== publisher stdout ===
[demo-pub:P] sent: tick 1 from P at 06:51:51.060
[demo-pub:P] sent: tick 2 from P at 06:51:52.061
…
=== subscriber stdout ===
[demo-sub:S] recv: tick 1 from P at 06:51:51.060 (subject=orion.demo.ipc)
[demo-sub:S] recv: tick 2 from P at 06:51:52.061 (subject=orion.demo.ipc)
…
```

The mesh's own NATS broker is the IPC. Same millisecond timestamps on send + receive.

### 7.4 Recipe: validate-only without storing

```bash
# Good YAML — returns dry_run:true, generation:0
curl -X POST $CTRL/v1/resources/apply?dry_run=1 \
  --data-binary @examples/08-canonical/amiga-search.yaml

# Bad YAML — semantic error, no store mutation
curl -X POST $CTRL/v1/resources/apply?dry_run=1 \
  --data-binary @examples/bad/schedule-both.yaml
# → validate: schedule must set exactly one of `task` or `taskTemplate`
```

`?dry_run=1`, `?dry_run=true`, `?dry_run=yes`, `?dry_run=on`, and the bare `?dry_run` (no value) are all accepted.

---

## 8. Working with peers

### 8.1 Register OrionMesh in Dev Portal

OrionMesh writes itself into the Dev Portal peer-runtime catalog so assets there can declare `this runs on OrionMesh`.

```bash
TOKEN=...
curl -X POST http://devportal.local:8081/api/peer-runtimes \
     -H "Content-Type: application/json" \
     -d '{
       "name": "orionmesh-belmont",
       "kind": "orionmesh",
       "baseUrl": "http://controller.belmont.local:7878",
       "adminUiUrl": "http://controller.belmont.local:7879",
       "config": { "natsUrl": "nats://nats.belmont.local:4222" }
     }'
```

Once registered, Dev Portal asset pages will deep-link / iframe the matching OrionMesh admin view.

### 8.2 Delegate a workload to KQueue

`runtime: peer` hands off to a peer system registered in Dev Portal.

```yaml
kind: Service
metadata: { name: ingest-worker }
spec:
  runtime:
    kind: peer
    system: kqueue-default
    ref: my-queue
```

OrionMesh's scheduler treats this like any other runtime; KQueue runs it.

---

## 9. Common pitfalls

| Symptom | Likely cause |
|---|---|
| `unknown variant 'X'` from validate | Typo in `kind:` or `runtime.kind:` — the error lists valid options |
| `schedule must set exactly one of task or taskTemplate` | Schedule has both set or neither — pick one |
| `Capability` matching unexpectedly | Selector is using `OneOf` when you meant `Equals` — `[a]` is `OneOf([a])`, not `Equals(a)` |
| Apply returns the same generation | Body is byte-identical to the stored body — change something to bump |
| Service not running yet | Scheduler dispatch is Phase 5; declared resources don't launch until then |
| 401 on `/v1/nodes` | Wrong bearer token or no header — `/health` does work unauthenticated |

For deeper triage see [installation.md §11](installation.md#11-troubleshooting).
