# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository status

Pre-implementation. The only files present are `OrionMesh_Architecture_Plan.md` (the design source of truth) and this CLAUDE.md. No code, no build system, no git history yet. The implementation decisions in the next section are locked in — use them when scaffolding rather than re-asking.

## Decisions locked in (2026-06-25)

These were agreed with the user after surveying the surrounding portfolio (~65 repos at github.com/geekychris). Treat them as constraints, not preferences:

1. **OrionMesh itself is all Rust** — agent, controller, CLI, UI server. Single Cargo workspace, shared crates for resource types and NATS contracts. Suggested stack: `tokio` + `async-nats` + `axum` + `serde_yaml` + `clap`; persistence open (sqlx or `redb` — `redb` matches gravel and avoids a Postgres dep on small nodes).
2. **Polyglot at the edges, not in OrionMesh's core**. Java/Spring remains the language of Dev Portal and most existing services; OrionMesh integrates with them over NATS + HTTP + MCP, not native interop.
3. **NATS is the queue/bus**. JetStream for durable consumer groups (heartbeat/task queues); core NATS for ephemeral pub/sub. Picked for ~10M msg/s throughput plus official clients in every language the portfolio uses.
4. **Dev Portal is a peer, not a parent**. Bidirectional integration with mutual independence:
   - Dev Portal gets extended (new `runtime` records, new `mcp__devportal__list_runtimes` / `register_runtime` tools) so OrionMesh can register itself as a runtime and so assets can pin "this runs on OrionMesh".
   - OrionMesh treats Dev Portal as **one** catalog it can read/write — present-but-not-required. Falls back to local YAML for desired state when Dev Portal is unreachable.
   - UI: Dev Portal asset pages deep-link (and optionally iframe-embed) the OrionMesh admin view for that asset; OrionMesh admin links back to the Dev Portal asset card.
   - Both projects ship working without the other.
5. **KQueue is also a peer** (same pattern as Dev Portal). KQueue stays a Go product; it registers itself in Dev Portal as a `kqueue-worker` runtime; OrionMesh can place workloads on KQueue via the catalog. No code port from KQueue into OrionMesh in the initial scope.

## What OrionMesh is

A "Kubernetes-lite" orchestration platform for a heterogeneous personal cluster of mixed-architecture machines (Linux x86_64/ARM64, Raspberry Pi, Intel + Apple Silicon Macs). The defining tagline:

> Run the right workload, on the right machine, with the right data, using the right runtime.

Differentiators from generic orchestrators:

- **Capability-aware discovery**: services advertise *what they can do* (datasets they hold, models they serve, hardware they expose), not just a name. Scheduling and lookup both key off capabilities.
- **Mixed runtimes as a first-class concern**: native binary, Docker, Python venv, Java, Node, Spark, LLM runtime, Home Assistant, future WASM. The agent abstracts these behind one runtime interface.
- **Mixed architectures**: placement constraints include `arch`, `os`, `gpu`, `acceleration` (e.g. `metal`). Don't assume Linux/x86.

## Architecture

Four layers, communicating over a **NATS messaging fabric**:

1. **UI / CLI** — user entry point. Rust (CLI via `clap`; UI server via `axum`).
2. **Controller** — Rust. Holds desired state and runs the reconciliation loop; contains the Scheduler and Discovery services.
3. **NATS** — the only inter-component transport. Topics from the plan: `heartbeat`, `capabilities`, `service.register`, `service.unregister`, `task.submit`, `task.events`, `logs`, `metrics`. New cross-component communication should land on a NATS topic, not a direct HTTP call between OrionMesh components. (HTTP is fine for the peer boundaries — Dev Portal, KQueue.)
4. **Agent** — Rust, single static binary cross-compiled for `aarch64-unknown-linux-gnu` (Pi), `aarch64-apple-darwin` (Apple Silicon), `x86_64-unknown-linux-gnu`, `x86_64-apple-darwin`. Handles heartbeats, inventory, runtime management, service registration, log streaming, metrics, health checks.

**Resource model** (everything is a resource, K8s-style): `Node`, `Service`, `Task`, `Schedule`, `Dataset`, `Model`, `Project`, `Secret`, `Volume`, `Network`. Desired-state YAML uses a `kind:` discriminator — see the `amiga-search` example in the plan for the canonical shape (`runtime`, `replicas`, `placement`, `requires`).

## Peer integration boundaries

When working on code that crosses into a peer system, respect these contracts:

- **Dev Portal** (Java/Spring, Postgres, at `~/code/claude_world/dev_portal`). Integration is over HTTP + the `mcp__devportal__*` MCP tools. Do **not** reach into Dev Portal's Postgres directly. When OrionMesh needs an asset's metadata, query Dev Portal; when Dev Portal needs OrionMesh runtime state, it calls OrionMesh's HTTP API. Schema for `devportal.yaml` lives at `~/code/claude_world/dev_portal/schema/devportal-asset.schema.json`.
- **KQueue** (Go, NATS JetStream, at `~/code/claude_world/scalable_ks_queue_process`). Integration is the existing sidecar contract (`POST /process`, NATS subject conventions, DLQ semantics). OrionMesh treats KQueue as a black-box runtime; don't port its internals.
- **code_graph_search** (Java + MCP, at `~/code/claude_world/code_graph_search`). When the user asks "do I have something that does X?", call its MCP surface rather than re-implementing portfolio search.

## Claude skills to develop alongside OrionMesh

Four families were approved. Build them as the workflows show up — don't author skill files speculatively.

1. **OrionMesh-specific**: `add-service`, `define-capability`, `define-placement`, `validate-resource`. Author/validate OrionMesh resource YAML; standalone (no Dev Portal required).
2. **Peer-integration**: `register-orion-runtime-in-devportal`, `link-asset-to-orion-service`, `peer-health-check`. Where the peer pattern actually shows up in workflows.
3. **Back-catalog bridges**: `onboard-existing-repo`, `devportal-asset-to-mesh`, `lift-systemd-to-orion`. Translate the existing ~65-repo portfolio into OrionMesh services. Highest leverage for getting the back-catalog on the mesh.
4. **Portfolio-wide patterns**: `scaffold-spring-react-asset`, `add-mcp-surface`, `add-telegram-bridge`, `pi-systemd-service`. The repeated shapes the survey found across the portfolio. Independent of the mesh project; pay off everywhere.

## Roadmap-driven scope

Phases from the plan:

- **Phase 1** — Agent + Discovery + Heartbeats + CLI
- **Phase 2** — Native task execution + Logs
- **Phase 3** — Service registry + Capability lookup
- **Phase 4** — Docker runtime
- **Phase 5** — Desired state reconciliation
- **Phase 6** — GitHub portfolio integration (Dev Portal peer link)
- **Phase 7** — Home Assistant + Telegram + MCP

When the user asks for a feature, check which phase it belongs to and flag if they're skipping ahead — earlier phases provide the substrate.

## Working with the architecture plan

`OrionMesh_Architecture_Plan.md` is the design document. The "Decisions locked in" section above supersedes it where they conflict. If the user's request conflicts with either, surface the conflict rather than silently diverging — and offer to update both documents if the new direction is intentional.
