# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository status

Phase 1 substrate is live: 12-crate Cargo workspace, capability-aware resource model, hybrid NATS bus, SQLite-backed controller, shared-token auth (with dev disable), end-to-end heartbeat + inventory + apply verified against `nats:2.10` in docker. The companion Dev Portal extension (peer runtime catalog + MCP tools) ships on a feature branch in `~/code/claude_world/dev_portal`.

## Decisions locked in

Decisions are durable — when adding code, conform to these rather than re-litigating. Conflicts with `OrionMesh_Complete_Plan.md` are flagged below.

### Language and layout

1. **All Rust, edition 2024** — `orion-agent`, `orion-controller`, `orion-cli`, `orion-ui` plus the libraries below. Single Cargo workspace. `tokio` + `async-nats` + `axum` + `serde_yml` + `clap` + `sqlx` (SQLite) + `redb` available but not currently used.
2. **Crate split**: `orion-types` (resource model), `orion-bus` (NATS contract), `orion-auth` (token + middleware), `orion-runtime` (RuntimeAdapter + SecretResolver), `orion-store` (SQLite via sqlx), `orion-scheduler` (placement filter, scoring later), `orion-devportal` (Dev Portal HTTP client), `orion-mcp` (stub for the upcoming OrionMesh MCP server), `orion-agent`, `orion-controller`, `orion-cli`, `orion-ui`. Matches plan section 20.

### Wire format and persistence

3. **`apiVersion: orionmesh.dev/v1`** is mandatory on the wire (defaults in via `#[serde(default)]` for backwards compat). Every `Resource` has the K8s-style four-block layout: `apiVersion / kind / metadata / spec / status`.
4. **Inline `Option<Status>`** on every `ResourceBody` variant (phase + conditions + observedGeneration + node + message). Reconciler tracks generation drift via `metadata.generation` vs `status.observed_generation`.
5. **Capability shape**: `{ name: String, attributes: serde_json::Value }` — nested JSON, freeform. `CapabilitySelector` uses `AttrMatch` with a **custom Deserialize impl** (don't switch to `serde(untagged)` — it mis-routes arrays to `Equals(Value)`). Accepted forms:
   - bare value → `AttrMatch::Equals`
   - JSON array → `AttrMatch::OneOf`
   - object whose keys are all in `{eq, ne, gt, gte, lt, lte}` → `AttrMatch::Op`
6. **GPU split**: `NodeGpu { vendor, vram_gb, name }` describes what a node has; `GpuRequirement { vendor: Option, min_vram_gb: Option }` describes what a workload needs. Don't conflate.
7. **Persistence**: SQLite via `sqlx` in `orion-store`. Migrations under `crates/orion-store/src/migrations/`. Body is JSON-encoded `Resource`. `upsert_resource` bumps generation only when the body actually changes.

### Bus

8. **Hybrid NATS namespace**: wide control plane + consolidated data plane.
   - Data plane (consolidated): `orion.heartbeat` (slim), `orion.node.inventory` (full snapshot), `orion.capabilities`, `orion.service.register/unregister`, `orion.service.health`, `orion.task.events` (one stream with `TaskOutcome` variants), `orion.logs.{node_id}`, `orion.metrics.{node_id}`.
   - Control plane (per-node): `orion.control.{node_id}.{run,stop,restart,drain}`.
   - `Topic::requires_jetstream()` marks `service.register/unregister`, `task.events`, and every `control.*` subject as durable. Heartbeat / inventory / capabilities / health / logs / metrics stay on core NATS.
9. **`PROTOCOL_VERSION`** in the envelope. Bump only on incompatible wire-shape changes — adding a new topic or message type doesn't qualify.

### Auth and secrets

10. **Shared cluster token** at `~/.config/orion/cluster.token` (or `ORION_CLUSTER_TOKEN` env, or `$ORION_TOKEN_FILE` override). NATS uses it as the connection token; controller HTTP requires `Authorization: Bearer <token>`. `/health` is intentionally outside the auth layer for liveness probes.
11. **`ORION_AUTH_DISABLED=1`** turns off both NATS auth and HTTP middleware. For dev only — `orion-auth` logs a single WARN on startup. Never run this way in production.
12. **`SecretResolver` trait** in `orion-runtime`. MVP impl is `PlaintextResolver` reading `~/.config/orion/secrets/<basename>` for `plaintext://<basename>` URIs. Path traversal blocked. SecureVault/age/1Password are future impls — drop them in without touching consumers.

### Resource kinds

13. **15 kinds in `ResourceBody`**: Node, Service, Task, Job, Schedule, Dataset, Model, Project, Secret, Volume, Network, Runtime (peer catalog), Capability (declared schema), Policy (stub), Integration (stub). Adding a kind: declare a `*Spec` in `specs.rs`, add the variant to `ResourceBody`, extend `kind_str()`, write a roundtrip test.
14. **Service spec hardening**: `HealthCheck` (Http/Tcp/Exec), `RestartPolicy::{Always, OnFailure, Never}`, named `PortSpec { name, port, protocol }`. Default `RestartPolicy::Always`.
15. **Dataset = `Vec<DatasetLocation { node, path, access }>`**; **Model = `Vec<ModelVariant { format, quant, memory_gb, context_window }>`**. Capability-aware scheduling reads these.
16. **Schedule** supports both `task: <ResourceName>` and inline `task_template: TaskSpec`. Exactly one must be set — enforced by `Resource::validate()`, not serde.

### Peer integration

17. **Dev Portal is a peer** (CLAUDE.md decision 4 from prior round). `orion-devportal` is the Rust client; defaults to **stub mode** when no base URL is set so OrionMesh stays standalone. Real client speaks the camelCase JSON the Java side ships (`baseUrl`, `adminUiUrl`).
18. **KQueue is also a peer**. No code from KQueue is ported in; it registers itself in Dev Portal as a `kqueue-worker` runtime and OrionMesh deploys workloads to it via the catalog.

## What's wired up vs. stubbed

| Surface | State |
|---|---|
| Resource model + serde + validate | ✅ 24 tests (`orion-types`) |
| NATS topic enum + 13 message types + envelope | ✅ 15 tests (`orion-bus`) |
| Cluster token + axum middleware + NATS auth | ✅ 12 tests (`orion-auth`) — covers disabled mode, enforce mode, env precedence, middleware allow/deny |
| `RuntimeAdapter` trait + `NativeAdapter` + `SecretResolver` + `PlaintextResolver` | ✅ 7 tests (`orion-runtime`) |
| `Store` (SQLite via sqlx, migrations, resource CRUD, node cache) | ✅ 7 tests (`orion-store`) |
| `filter_nodes_by_placement` (arch/os/gpu/labels) | ✅ 4 tests (`orion-scheduler`) — scoring + capability index pending |
| Dev Portal HTTP client + wiremock tests | ✅ 4 tests (`orion-devportal`) |
| Agent: connect, publish inventory + heartbeats, subscribe to control plane, launch via `NativeAdapter` | ✅ end-to-end verified against `nats:2.10` |
| Controller: subscribe heartbeats + inventory, persist to SQLite, serve `/v1/nodes` + `/v1/resources/{kind}` + `POST /v1/resources/apply` | ✅ end-to-end verified |
| Scheduler dispatch loop (controller → `control.run`) | ❌ Phase 5 |
| Reconciler (compare desired vs observed → produce control events) | ❌ Phase 5 |
| Find API (`POST /v1/find` with capability selector) | ❌ Phase 4 |
| Docker / Python / Java / Node / Spark / LLM / HA / Wasm runtime adapters | ❌ Phase 5+ |
| Log / metric forwarders on the agent | ❌ Phase 2/3 |
| Service health probe loop (publish to `orion.service.health`) | ❌ Phase 5 |
| MCP server (`orion-mcp`) | 🟡 crate exists, only declares planned tool names |
| UI views per resource kind | ❌ later phase |

## Local dev — how to actually run it

```bash
# 1. NATS broker in docker (JetStream enabled)
docker run -d --rm --name orion-nats -p 4222:4222 nats:2.10 -js

# 2. Build the workspace
cargo build --workspace

# 3. Start controller (in-memory SQLite, auth off)
ORION_AUTH_DISABLED=1 ORION_STORE_PATH=sqlite::memory: \
  target/debug/orion-controller --bind 127.0.0.1:7878 &

# 4. Start an agent
ORION_AUTH_DISABLED=1 \
  target/debug/orion-agent --node-id local-dev --heartbeat-interval 2 &

# 5. Talk to the controller
curl http://127.0.0.1:7878/v1/nodes
curl -X POST --data-binary @resource.yaml http://127.0.0.1:7878/v1/resources/apply
curl http://127.0.0.1:7878/v1/resources/Service
```

For auth-enforced runs: `ORION_CLUSTER_TOKEN=s3cr3t` on every component, and HTTP clients pass `Authorization: Bearer s3cr3t`.

## What OrionMesh is

A "Kubernetes-lite" orchestration platform for a heterogeneous personal cluster of mixed-architecture machines (Linux x86_64/ARM64, Raspberry Pi, Intel + Apple Silicon Macs). The defining tagline:

> Run the right workload, on the right machine, with the right data, using the right runtime.

Differentiators:
- **Capability-aware discovery** — services advertise *what they can do* (datasets/models/hardware), not just a name.
- **Mixed runtimes** — native, Docker, Python venv, Java, Node, Spark, LLM, Home Assistant, future WASM, plus `Peer` for hand-off to another OrionMesh / KQueue / etc.
- **Mixed architectures** — placement keys: `arch`, `os`, `gpu (vendor + min_vram_gb)`, `acceleration`, `node_labels`, plus a `prefer:` block for soft scoring.

## Roadmap

- **Phase 1** — Agent + Discovery + Heartbeats + CLI ✅ substrate done
- **Phase 2** — Native task execution + Logs/Metrics forwarders
- **Phase 3** — Service registry + Capability lookup (Find API)
- **Phase 4** — Docker runtime adapter
- **Phase 5** — Desired-state reconciliation + scheduler dispatch
- **Phase 6** — GitHub portfolio integration (Dev Portal peer link production-ready)
- **Phase 7** — Home Assistant + Telegram + MCP (`orion-mcp` fleshes out)

When the user asks for a feature, check which phase it belongs to and flag if they're skipping ahead — earlier phases provide the substrate.

## Working with the plan documents

`OrionMesh_Complete_Plan.md` (2084 lines) is the authoritative design source. `OrionMesh_Architecture_Plan.md` (279 lines) is the original sketch; the complete plan supersedes it. **The "Decisions locked in" section above supersedes both where they conflict.** When the plan and the codebase disagree, ask the user to choose rather than silently diverging.
