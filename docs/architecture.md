# Architecture

This document describes OrionMesh's runtime topology, the resource model, the NATS contract, persistence, and the lifecycle of common operations. Diagrams are mermaid — they render natively on GitHub.

For the *why* behind each choice see [design.md](design.md). For how to actually run it see [installation.md](installation.md) and [usage.md](usage.md).

---

## 1. Bird's-eye view

OrionMesh is a controller-plus-agents mesh communicating over a NATS bus. Persistence is local to the controller (SQLite). Peer systems — Dev Portal, KQueue — are reachable over HTTP and treated as optional catalogs, never hard dependencies.

```mermaid
flowchart LR
    subgraph User["User surfaces"]
        CLI["orion CLI"]
        UI["orion-ui (web)"]
        MCP["orion-mcp (stub)"]
    end

    subgraph Controller["Controller node"]
        CTRL["orion-controller"]
        STORE[("SQLite<br/>orion-store")]
        SCHED["orion-scheduler<br/>(filter / score)"]
        CTRL --- STORE
        CTRL --- SCHED
    end

    NATS{{"NATS broker<br/>core + JetStream"}}

    subgraph Pi["Pi node"]
        AGT_P["orion-agent"]
        ADP_P["RuntimeRegistry<br/>(native)"]
        AGT_P --- ADP_P
    end

    subgraph Mac["Mac Studio node"]
        AGT_M["orion-agent"]
        ADP_M["RuntimeRegistry<br/>(native + docker + llm)"]
        AGT_M --- ADP_M
    end

    subgraph GPU["GPU rig node"]
        AGT_G["orion-agent"]
        ADP_G["RuntimeRegistry<br/>(native + docker + python)"]
        AGT_G --- ADP_G
    end

    CLI -- "HTTP /v1/*" --> CTRL
    UI  -- "HTTP /v1/*" --> CTRL
    MCP -- "HTTP /v1/*" --> CTRL

    CTRL <-- "publish + subscribe" --> NATS
    NATS <-- "publish + subscribe" --> AGT_P
    NATS <-- "publish + subscribe" --> AGT_M
    NATS <-- "publish + subscribe" --> AGT_G

    subgraph Peers["Peer systems (optional)"]
        DEVP[/"Dev Portal<br/>Java/Spring"/]
        KQ[/"KQueue<br/>Go"/]
    end

    CTRL -. "HTTP /api/peer-runtimes" .-> DEVP
    DEVP -. "deep-link / iframe" .-> UI
    CTRL -. "delegate workload" .-> KQ
```

Boundaries to respect:

| Pair | Transport | Authority |
|---|---|---|
| User → Controller | HTTP (bearer) | Controller owns desired state |
| Controller → Agent | NATS `orion.control.{node}.*` | Controller owns scheduling decisions |
| Agent → Controller | NATS data plane | Agent owns observed-node truth |
| Controller → Dev Portal | HTTP | Dev Portal owns asset metadata; OrionMesh writes the runtime catalog entry |
| Controller → KQueue | HTTP via Dev Portal catalog | KQueue runs its own workloads |

---

## 2. Crate map

```mermaid
flowchart TB
    types["orion-types<br/>resource model"]
    bus["orion-bus<br/>NATS contract"]
    auth["orion-auth<br/>token + middleware"]
    rt["orion-runtime<br/>RuntimeAdapter +<br/>SecretResolver"]
    store["orion-store<br/>SQLite via sqlx"]
    sched["orion-scheduler<br/>placement filter"]
    dp["orion-devportal<br/>HTTP client"]
    mcp["orion-mcp<br/>(stub)"]

    agent["orion-agent<br/>binary"]
    controller["orion-controller<br/>binary"]
    cli["orion-cli<br/>orion binary"]
    ui["orion-ui<br/>binary"]

    bus --> types
    rt --> types
    store --> types
    sched --> types
    sched --> bus
    dp --> types

    agent --> types
    agent --> bus
    agent --> auth
    agent --> rt

    controller --> types
    controller --> bus
    controller --> auth
    controller --> store
    controller --> sched
    controller --> dp

    cli --> types
    cli --> bus
```

A new feature usually touches at most three crates: a type in `orion-types`, a message in `orion-bus`, and the consumer (`orion-agent` or `orion-controller`). If a change cuts across more, it's a sign the layering needs revisiting.

---

## 3. Resource model

Every desired-state document on the wire has the same four-block layout. Internally, the variant is a tagged enum (`kind:`) and apiVersion + metadata sit alongside it via `serde(flatten)`.

```mermaid
classDiagram
    class Resource {
        +String api_version
        +Metadata metadata
        +ResourceBody body
        +from_yaml() Resource
        +to_yaml() String
        +validate() Result
    }

    class Metadata {
        +ResourceName name
        +Option~String~ namespace
        +Map labels
        +Map annotations
        +Option~u64~ generation
    }

    class ResourceBody {
        <<tagged enum>>
        Node
        Service
        Task
        Job
        Schedule
        Dataset
        Model
        Project
        Secret
        Volume
        Network
        Runtime
        Capability
        Policy
        Integration
    }

    class Status {
        +Phase phase
        +Option~u64~ observed_generation
        +Vec~Condition~ conditions
        +Option~NodeId~ node
        +Option~String~ message
    }

    class ServiceSpec {
        +Option~Runtime~ runtime
        +Option~u32~ replicas
        +Placement placement
        +CapabilitySelector requires
        +Vec~Capability~ capabilities
        +Vec~PortSpec~ ports
        +Option~HealthCheck~ health
        +RestartPolicy restart_policy
    }

    class Placement {
        +Vec~Arch~ arch
        +Vec~OperatingSystem~ os
        +Option~GpuRequirement~ gpu
        +Option~Acceleration~ acceleration
        +Map node_labels
        +PlacementPreferences prefer
    }

    class Capability {
        +String name
        +serde_json::Value attributes
    }

    Resource *-- Metadata
    Resource *-- ResourceBody
    ResourceBody ..> Status : optional per variant
    ResourceBody ..> ServiceSpec : variant Service
    ServiceSpec *-- Placement
    ServiceSpec *-- Capability
```

The full set of `*Spec` types lives in `crates/orion-types/src/specs.rs`. Each kind has a roundtrip test against canonical YAML.

### Capability and selector

Capabilities are how services advertise *what they can do* — not just by name. The selector reuses the same shape; matching is structural on the JSON.

```mermaid
flowchart LR
    subgraph Advertise["Service advertises"]
        A1["name: search<br/>attributes:<br/>  dataset: amiga_schematics<br/>  protocol: http"]
    end
    subgraph Require["Workload requires"]
        R1["requires:<br/>  search:<br/>    dataset: amiga_schematics<br/>    format: [pdf, png]"]
    end
    subgraph Match["AttrMatch dispatch"]
        M1["bare value → Equals"]
        M2["JSON array → OneOf"]
        M3["{eq, ne, gt, gte, lt, lte} → Op"]
    end
    R1 --> M1
    R1 --> M2
    R1 --> M3
    M1 -. resolves against .-> A1
    M2 -. resolves against .-> A1
```

`AttrMatch` has a custom `Deserialize` impl because `serde(untagged)` mis-routes arrays to `Equals(Value)` (Value accepts any JSON type).

---

## 4. NATS topic map

Hybrid namespace: wide control plane (per-node subjects, NATS subject-side filtering), consolidated data plane (one stream per concern with the discriminator in the payload).

```mermaid
flowchart LR
    subgraph CorePub["Core NATS (ephemeral)"]
        HB["orion.heartbeat<br/>slim, every ~5s"]
        INV["orion.node.inventory<br/>full snapshot on change"]
        CAP["orion.capabilities<br/>per-service"]
        HEALTH["orion.service.health<br/>periodic probe result"]
        LOG["orion.logs.{node}<br/>line stream"]
        MET["orion.metrics.{node}<br/>sample batches"]
    end

    subgraph JS["JetStream (durable)"]
        SREG["orion.service.register"]
        SUN["orion.service.unregister"]
        TEV["orion.task.events<br/>TaskOutcome in payload"]
        CRUN["orion.control.{node}.run"]
        CSTOP["orion.control.{node}.stop"]
        CRES["orion.control.{node}.restart"]
        CDRAIN["orion.control.{node}.drain"]
    end

    Agents((Agents))
    Ctrl((Controller))

    Agents -- publish --> HB
    Agents -- publish --> INV
    Agents -- publish --> CAP
    Agents -- publish --> HEALTH
    Agents -- publish --> LOG
    Agents -- publish --> MET
    Agents -- publish --> SREG
    Agents -- publish --> SUN
    Agents -- publish --> TEV

    Ctrl -- subscribe --> HB
    Ctrl -- subscribe --> INV
    Ctrl -- subscribe --> CAP
    Ctrl -- subscribe --> HEALTH
    Ctrl -- subscribe --> SREG
    Ctrl -- subscribe --> SUN
    Ctrl -- subscribe --> TEV

    Ctrl -- publish --> CRUN
    Ctrl -- publish --> CSTOP
    Ctrl -- publish --> CRES
    Ctrl -- publish --> CDRAIN

    Agents -- subscribe to own node --> CRUN
    Agents -- subscribe to own node --> CSTOP
    Agents -- subscribe to own node --> CRES
    Agents -- subscribe to own node --> CDRAIN
```

`Topic::requires_jetstream()` is the source of truth for which topics get durability. Adding a topic: extend `Topic`, define a payload in `crates/orion-bus/src/messages/`, decide the durability tier, write a roundtrip test in `crates/orion-bus/src/tests.rs`.

---

## 5. Storage schema

SQLite via `sqlx`. Migrations live under `crates/orion-store/src/migrations/`. Body is the JSON serialization of `Resource`; generation bumps only when the body actually changes.

```mermaid
erDiagram
    resource {
        TEXT kind PK
        TEXT namespace PK
        TEXT name PK
        INTEGER generation
        TEXT body "JSON Resource"
        TEXT created_at
        TEXT updated_at
    }
    observed_node {
        TEXT node_id PK
        TEXT agent_version
        TEXT inventory "JSON NodeInventory"
        TEXT last_seen_at
    }
```

Phase-1 schema is intentionally thin. Future migrations add Job history, observed-service state, scheduler decisions, audit events.

---

## 6. Lifecycles

### 6.1 Agent boot → heartbeat → controller persistence

```mermaid
sequenceDiagram
    autonumber
    participant A as orion-agent
    participant N as NATS
    participant C as orion-controller
    participant S as Store (SQLite)

    A->>A: load AuthMode<br/>(env / file / disabled)
    A->>N: connect (token or open)
    N-->>A: connected
    A->>A: build RuntimeRegistry<br/>(native + future adapters)
    A->>A: sysinfo snapshot<br/>(cpu, mem, gpus, arch, os)
    A->>N: publish orion.node.inventory<br/>(Envelope<NodeInventory>)
    N->>C: deliver inventory
    C->>S: set_node_inventory(node_id, json)

    loop every heartbeat_interval (5s default)
        A->>N: publish orion.heartbeat<br/>(slim Heartbeat)
        N->>C: deliver heartbeat
        C->>S: touch_node(node_id, version)
    end

    A->>N: subscribe orion.control.{node}.run/stop/restart/drain
```

### 6.2 `orion apply -f svc.yaml` (current MVP)

```mermaid
sequenceDiagram
    autonumber
    participant U as User
    participant CLI as orion CLI
    participant C as orion-controller
    participant S as Store (SQLite)

    U->>CLI: orion validate svc.yaml
    CLI->>CLI: Resource::from_yaml + validate()
    CLI-->>U: ok: kind=Service name=amiga-search

    U->>C: POST /v1/resources/apply<br/>Authorization: Bearer <token><br/>body: svc.yaml
    C->>C: parse + validate
    C->>S: upsert_resource(r)
    S-->>C: generation (1 if new, +1 if body changed, unchanged if identical)
    C-->>U: {kind, name, generation}
```

### 6.3 Direct dispatch + stdout/stderr capture (Phase A — live)

`POST /v1/dispatch/{kind}/{name}` picks a node (currently "most-recent live"; full scheduler is Phase 5), publishes `ControlRun`, the agent launches via the runtime adapter, and stdout/stderr stream back to a ring buffer the UI tails.

```mermaid
sequenceDiagram
    autonumber
    participant U as User
    participant C as orion-controller
    participant S as Store (SQLite)
    participant N as NATS
    participant A as orion-agent
    participant W as Workload (forked process)

    U->>C: POST /v1/dispatch/Service/<name>
    C->>S: get_resource(kind, name)
    S-->>C: Resource { runtime, … }
    C->>C: pick a node (most-recent live)
    C->>N: publish Envelope<ControlRun><br/>on orion.control.{node}.run
    N->>A: deliver ControlRun
    A->>A: instances.record(id → (kind, name))
    A->>W: NativeAdapter.launch — spawn /bin/sh -c …<br/>stdout/stderr piped
    par stdout / stderr forwarder
        W-->>A: (line, OutStream::Stdout)
        A->>N: publish Envelope<LogLine><br/>on orion.logs.{node}
        N->>C: deliver LogLine
        C->>C: LogBuffer.push((kind, name), entry)
    end
    C-->>U: {kind, name, node, instance_id}

    U->>C: GET /v1/logs/{kind}/{name}?since=N
    C-->>U: { total, entries[…] }
```

Phase 5 replaces "pick a node (most-recent live)" with the real scheduler — `filter_nodes_by_placement` + scoring + capability index.

### 6.4 Scheduler tick — Schedules firing on cron (Phase B — live)

```mermaid
sequenceDiagram
    autonumber
    participant TICK as scheduler_tick_loop<br/>(every 5s)
    participant S as Store
    participant SR as ScheduleRegistry<br/>(in-memory)
    participant DISP as dispatch_workload
    participant N as NATS

    TICK->>S: list_by_kind("Schedule")
    S-->>TICK: schedules
    loop each Schedule
        TICK->>TICK: cron.parse (5-field auto-promoted to 6)
        TICK->>SR: read armed_at + last_fired_at
        TICK->>TICK: next = cron.after(after).next()
        alt next ≤ now
            TICK->>S: resolve task (lookup or inline template)
            S-->>TICK: TaskSpec.runtime
            TICK->>DISP: dispatch_workload(Task, name, runtime)
            DISP->>N: publish ControlRun (same path as §6.3)
            DISP-->>TICK: (node, instance_id)
            TICK->>SR: last_fired_at = now<br/>fire_count += 1<br/>next_fire_at = cron.after(now).next()
        else not yet
            TICK->>SR: next_fire_at = next
        end
    end
```

Observable via `GET /v1/schedules/observed` — returns `armed_at`, `last_fired_at`, `last_instance_id`, `next_fire_at`, `fire_count`, `last_error` per Schedule.

### 6.5 IPC over NATS — two Services talking (Phase C — live)

```mermaid
sequenceDiagram
    autonumber
    participant U as User
    participant C as orion-controller
    participant N as NATS broker
    participant Asub as orion-demo-sub<br/>(Service: demo-sub)
    participant Apub as orion-demo-pub<br/>(Service: demo-pub)

    U->>C: apply + dispatch demo-sub
    C->>Asub: ControlRun via §6.3
    Asub->>N: subscribe "orion.demo.ipc"

    U->>C: apply + dispatch demo-pub
    C->>Apub: ControlRun via §6.3

    loop every 1s
        Apub->>N: publish "tick N from P at HH:MM:SS.mmm"<br/>on orion.demo.ipc
        N->>Asub: deliver
        Asub->>Asub: stdout: "recv: tick N …"
        par log forwarder
            Asub->>N: Envelope<LogLine> on orion.logs.{node}
            N->>C: deliver
        end
        Apub->>Apub: stdout: "sent: tick N …"
        par log forwarder
            Apub->>N: Envelope<LogLine> on orion.logs.{node}
            N->>C: deliver
        end
    end

    U->>C: GET /v1/logs/Service/demo-pub
    U->>C: GET /v1/logs/Service/demo-sub
    Note over U,C: Same timestamps in both:<br/>the mesh's own broker carried the messages.
```

The IPC subject is **distinct from the control plane subjects** — workloads share NATS with the mesh but the namespaces don't overlap.

### 6.6 Peer integration — Dev Portal registration

```mermaid
sequenceDiagram
    autonumber
    participant Op as Operator
    participant CLI as orion CLI
    participant C as orion-controller
    participant DP as Dev Portal
    participant UI as Dev Portal UI

    Op->>CLI: orion apply -f orionmesh-runtime.yaml<br/>(kind: Runtime)
    CLI->>C: POST /v1/resources/apply
    C->>C: store Resource locally
    C->>DP: POST /api/peer-runtimes<br/>(name, kind=orionmesh, baseUrl, adminUiUrl)
    DP-->>C: 200 OK
    Note over DP,UI: Dev Portal asset page can now deep-link / iframe<br/>OrionMesh admin for assets tagged with this runtime
```

Both sides continue to work when the other is unreachable. Stub mode in `orion-devportal` (no base URL) makes every call return `NotConfigured`.

---

## 7. Auth model

Single shared cluster token. NATS uses it as the connection token; HTTP uses it as a bearer. `ORION_AUTH_DISABLED=1` shorts both for dev.

```mermaid
flowchart TD
    Start([Component startup])
    Start --> Disabled{ORION_AUTH_DISABLED set?}
    Disabled -- yes --> ModeDisabled[AuthMode::Disabled<br/>WARN logged]
    Disabled -- no --> EnvToken{ORION_CLUSTER_TOKEN set?}
    EnvToken -- yes --> ModeEnforce1[AuthMode::Enforce]
    EnvToken -- no --> FileToken{Token file exists?<br/>$ORION_TOKEN_FILE or<br/>~/.config/orion/cluster.token}
    FileToken -- yes, non-empty --> ModeEnforce2[AuthMode::Enforce]
    FileToken -- no --> Err[Error: MissingToken]

    ModeDisabled --> Use
    ModeEnforce1 --> Use
    ModeEnforce2 --> Use

    Use[Use everywhere]
    Use --> NATSc[NATS: ConnectOptions::token]
    Use --> HTTPm[axum middleware:<br/>require_bearer]
    Use --> ApiCall[outbound API calls<br/>add Authorization: Bearer]
```

`/health` on the controller is intentionally *outside* the auth layer so liveness probes don't need the token.

---

## 8. What's not in this document

- **Reconciler internals** — Phase 5. Today dispatch is operator-initiated (via `POST /v1/dispatch/...` or the scheduler tick); the closed loop where the controller compares desired vs observed and decides to dispatch on its own is the next layer.
- **Full scheduler** — Phase 5. Today's pick-a-node logic is "most-recent live"; the filter+score+place pipeline ships with the reconciler.
- **Find API** — Phase 4. The capability index exists in [`crates/orion-types/src/capability.rs`](../crates/orion-types/src/capability.rs); the `POST /v1/find` endpoint lights it up.
- **Federation across sites** — out of MVP scope (mentioned in plan future ideas).
- **MCP server tools** — `orion-mcp` is a stub; the tool surface lands in Phase 7.

See [design.md](design.md) for the reasoning behind these omissions and the broader trade-offs.
