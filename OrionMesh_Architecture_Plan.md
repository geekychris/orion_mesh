# OrionMesh Architecture Plan

## Vision

**OrionMesh** is a capability-aware orchestration platform for a
heterogeneous personal cluster.

> Run the right workload, on the right machine, with the right data,
> using the right runtime.

## Goals

-   Kubernetes-lite
-   Declarative desired state
-   Mixed runtimes (native, Docker, Spark, Python, Java, Node)
-   Mixed architectures (Linux x86_64, Linux ARM64, Raspberry Pi, macOS
    Intel, macOS Apple Silicon)
-   Capability-aware service discovery
-   Dataset and model discovery
-   GitHub portfolio integration
-   Home Assistant, Telegram and MCP integrations

------------------------------------------------------------------------

# High-level Architecture

``` text
                      +----------------------+
                      |   Orion UI / CLI     |
                      +----------+-----------+
                                 |
                                 v
                    +--------------------------+
                    |    Orion Controller      |
                    | Desired State / Schedule |
                    +------------+-------------+
                                 |
                 +---------------+----------------+
                 |                                |
                 v                                v
          +-------------+                 +--------------+
          | Discovery   |                 | Scheduler    |
          +------+------+                 +------+-------+
                 |                                |
                 +-------------+------------------+
                               |
                     NATS Messaging Fabric
                               |
      +-----------+------------+------------+-----------+
      |           |                         |           |
      v           v                         v           v
+-----------+ +-----------+          +-----------+ +-----------+
| Mac ARM   | | Linux GPU |          | Raspberry | | Intel Mac |
| Agent      | | Agent     |          | Pi Agent  | | Agent     |
+-----------+ +-----------+          +-----------+ +-----------+
```

# Core Components

## Orion Agent

Runs on every node.

Responsibilities:

-   Heartbeats
-   Node inventory
-   Runtime management
-   Service registration
-   Log streaming
-   Metrics
-   Health checks

## Discovery

Services advertise capabilities rather than just names.

Example:

``` yaml
service: search
capabilities:
  datasets:
    - amiga_schematics
    - personal_docs
```

LLM:

``` yaml
service: llm
capabilities:
  models:
    - qwen2.5-coder
    - llama3.1
```

## Desired State

Example:

``` yaml
kind: Service
name: amiga-search
runtime: docker
replicas: 1
placement:
  arch: [arm64,x86_64]
  os: linux
requires:
  dataset: amiga_schematics
```

Controller continually reconciles reality with desired state.

------------------------------------------------------------------------

# Runtime Layer

Supported runtimes:

-   Native executable
-   Docker
-   Python virtualenv
-   Java
-   Node
-   Spark
-   LLM runtime
-   Home Assistant
-   WASM (future)

------------------------------------------------------------------------

# Scheduler

Placement constraints:

``` yaml
placement:
  os: linux
  arch: x86_64
  gpu: nvidia
```

or

``` yaml
placement:
  os: macos
  arch: arm64
  acceleration: metal
```

------------------------------------------------------------------------

# Resources

Everything becomes a resource.

``` text
Node
Service
Task
Schedule
Dataset
Model
Project
Secret
Volume
Network
```

------------------------------------------------------------------------

# GitHub Portfolio

Dev Portal is source of truth.

``` text
GitHub
    |
    v
Dev Portal
    |
    +------ Repository metadata
    +------ Build system
    +------ Docker support
    +------ Services
    +------ Tasks
    +------ Libraries
    |
    v
 OrionMesh
```

Claude can query:

"Do I already have something that does Lucene inspection?"

------------------------------------------------------------------------

# Messaging

Use NATS.

Topics:

-   heartbeat
-   capabilities
-   service.register
-   service.unregister
-   task.submit
-   task.events
-   logs
-   metrics

------------------------------------------------------------------------

# Integrations

-   Home Assistant
-   Telegram
-   MCP
-   GitHub
-   Docker
-   Prometheus
-   Ollama
-   OpenAI-compatible APIs

------------------------------------------------------------------------

# Roadmap

## Phase 1

-   Agent
-   Discovery
-   Heartbeats
-   CLI

## Phase 2

-   Native task execution
-   Logs

## Phase 3

-   Service registry
-   Capability lookup

## Phase 4

-   Docker runtime

## Phase 5

-   Desired state reconciliation

## Phase 6

-   GitHub portfolio integration

## Phase 7

-   Home Assistant
-   Telegram
-   MCP

------------------------------------------------------------------------

# Future Ideas

-   Autonomous cluster healing
-   Automatic repo-to-service deployment
-   Dataset-aware scheduling
-   Model-aware scheduling
-   Cost-aware scheduling
-   Multi-site federation
-   Edge/cloud bursting
