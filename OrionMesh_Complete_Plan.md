# OrionMesh Complete Architecture and Implementation Plan

**Project:** OrionMesh  
**Purpose:** Capability-aware orchestration for a heterogeneous personal compute mesh  
**Audience:** AI coding agents, future maintainers, and human implementers  
**Status:** Full implementation plan, expanded from original conversation  
**Owner:** Chris Collins  

---

## 0. Original User Intent and Requirements

This section preserves the original asks and comments that led to OrionMesh. These are intentionally written as product requirements so an implementation agent understands the thinking behind the architecture.

### 0.1 New project direction

The project began from the desire to find a meaningful infrastructure/software-infrastructure project suitable for vibe coding.

Original direction:

> Looking for new projects to do with vibe coding.

The desired area became:

> Infrastructure projects, software infrastructure projects.

Initial idea accepted:

> A distributed task orchestrator that can scale across multiple machines and manage/monitor jobs like data pipelines, model training, indexing, and services.

### 0.2 Messaging and mesh foundation

The first foundational requirement was a lightweight messaging layer:

> Start with some kind of lightweight messaging system that could be used for messaging across processes on the same machine, and potentially like a mesh mechanism that would support auto-discovery of other nodes in my network.

This implies:

- local IPC/process messaging
- LAN messaging
- node discovery
- peer membership
- lightweight enough for small machines
- extensible enough for multi-node orchestration

### 0.3 Service self-discovery

The next requirement was discovery of services running on nodes:

> Perhaps some kind of self-discovery of what services are on those machines.

This implies each Orion Agent should discover and/or advertise:

- local services
- ports
- protocols
- health endpoints
- runtime type
- workload identity
- capability metadata

### 0.4 Task management

A key desired feature:

> I like the idea of task management.

This implies OrionMesh must support:

- one-off tasks
- scheduled tasks
- long-running jobs
- task status
- logs
- retries
- placement
- task history
- task cancellation

### 0.5 Fine-grained capability discovery

The discovery system should not only find a generic service type. It must support fine-grained service capabilities.

Original example:

> If you had a service that was search, it might be that you'd want to differentiate an instance of search for different datasets.

Implication:

A search service is not only:

```yaml
service: search
```

It is:

```yaml
service: search
capabilities:
  datasets:
    - amiga_schematics
    - personal_docs
    - github_repos
```

### 0.6 Dataset-aware discovery

Original requirement:

> We might have common infrastructure for searching, but we might have different data segments and want to discover not only a service, but the specific data that it can serve.

This becomes a first-class concept:

- Dataset
- Dataset location
- Dataset owner
- Dataset index
- Dataset serving service
- Dataset freshness
- Dataset access policy
- Dataset locality for scheduling

### 0.7 Model-aware discovery

Original requirement:

> If there's a LLM runtime, we might want to know which LLMs are served from it and be able to discover where a particular LLM model is running.

This becomes another first-class concept:

- Model
- Runtime
- Quantization
- Context length
- Acceleration type
- Node location
- API protocol
- memory requirements
- loaded/unloaded state

### 0.8 Runtime abstraction

Original question:

> What do you think would be appropriate for runtime? Should there be a way to run things natively, and then also perhaps run things from Docker containers, so there would be some kind of instance type runtime?

This implies a runtime abstraction supporting:

- native process execution
- Docker containers
- Docker Compose
- Python virtual environments
- Java/JAR execution
- Node/TypeScript
- Spark jobs
- LLM runtimes
- Home Assistant add-ons/bridges
- future WASM
- possibly Kubernetes adapter later

### 0.9 Workload lifetime distinction

Original question:

> Wouldn't we also want to consider things like whether we invoke something that's a one-off task versus services that continue running?

This implies the resource model must distinguish:

- Service: long-running, health-checked workload
- Task: one-off execution
- Job: tracked execution with result/history
- Schedule: recurring task
- Daemon: node-local always-on helper
- Workflow: multi-step orchestration

### 0.10 Extension surfaces

Original requirement:

> It seems also like you might want ways to access this cluster of things through extensions like, let's say, a Telegram agent.

This implies:

- API-first controller
- CLI
- Telegram bot
- MCP server
- Web UI
- Home Assistant bridge
- possible Slack/Discord later

### 0.11 Tasks based on user interests

Original examples of tasks that would fit:

- distributed search indexing
- batch processing for 3D model slicing
- training or running small ML models
- gathering sensor data from home projects
- checking water levels
- Starlink/site monitoring
- home automation actions
- retro-computing services
- code/repo analysis
- document indexing

### 0.12 Home Assistant integration

Original requirement:

> It seems like also there should be an integration with things like Home Assistant.

This implies:

- Home Assistant event ingestion
- Home Assistant service calls
- entity discovery
- automation triggers
- task launch from HA events
- HA dashboard cards
- OrionMesh status exposed to Home Assistant

### 0.13 Name

The project name chosen:

> OrionMesh

Rationale:

- Orion evokes constellation/navigation.
- Mesh describes the distributed ad-hoc network.
- It feels less generic than “TaskMesh.”

### 0.14 Kubernetes-lite idea

Original observation:

> This almost kind of looks like a mesh ad hoc kind of Kubernetes-lite mechanism.

This is central.

OrionMesh should borrow ideas from Kubernetes:

- resources
- desired state
- reconciliation
- controllers
- scheduling
- health checks
- labels/selectors
- service discovery

But it should not attempt to be Kubernetes.

It should be:

- lighter
- more personal
- more heterogeneous
- more capability-aware
- easier for AI agents to understand
- better suited for home-lab and workstation environments

### 0.15 Desired-state concept

Original requirement:

> With Kubernetes' concept of resources, it's not just about tasks, right? It's where persistence is and the idea that there's a specific stable state and achieving, analyzing and constantly trying to get to some kind of stable state. It probably would be useful to have some kind of concept of things that should be running and how they should be configured.

This becomes the desired-state/reconciliation engine.

OrionMesh must support:

- declarative specs
- current state observation
- diffing desired vs actual
- reconcile loops
- restart policies
- health checks
- dependency handling
- configuration drift detection
- resource status

### 0.16 GitHub portfolio awareness

Original requirement:

> I have a lot of projects in GitHub. There's something like 200 repos. It would be useful if there was a skill for Claude that took account of all that stuff when I'm working on new projects to suggest potentially using some of those tools.

This implies:

- GitHub portfolio index
- repo capability extraction
- language/build/runtime detection
- relationship to Dev Portal
- Claude skill/MCP integration
- “reuse my existing tools” feature

### 0.17 GitHub projects as OrionMesh services

Original requirement:

> From this mesh perspective, it might be useful if it knows of that portfolio for potentially running them as services within this Orion mesh.

This implies:

- repo-to-service manifests
- build/deploy/run metadata
- project capability metadata
- runtime compatibility
- artifact building
- service deployment from GitHub repo
- task deployment from GitHub repo

### 0.18 Dev Portal integration

Original requirement:

> I have a dev portal, a type project that also kind of would know of my Git projects, so it seems like that would be part of the portfolio.

This implies Dev Portal should be a source of truth for:

- repositories
- projects
- dependencies
- capabilities
- runtime metadata
- deployment manifests
- service inventory
- peer runtimes
- OrionMesh integration

### 0.19 Architecture support requirement

Original explicit requirement:

> It needs to support not only Docker but a mix of architectures: Linux ARM, DGX Spark, Raspberry Pi, Linux x86, macOS ARM, macOS Intel.

This becomes a hard requirement:

- Docker is not enough.
- Native execution is required.
- Architecture-aware scheduling is required.
- OS-aware scheduling is required.
- Runtime compatibility is required.
- Cross-build metadata is required.
- Mac must be first-class, not an afterthought.
- Raspberry Pi must be first-class.
- GPU/Linux nodes must be first-class.

---

## 1. Executive Summary

OrionMesh is a lightweight, capability-aware orchestration platform for a heterogeneous collection of personal, home-lab, edge, and workstation machines.

It is inspired by Kubernetes, Nomad, Consul, NATS, systemd, Home Assistant, and developer portals, but it is intentionally different.

Kubernetes schedules containers.  
Nomad schedules jobs.  
Consul discovers services.  
Home Assistant automates devices.  
Developer portals catalog projects.  

**OrionMesh schedules capabilities.**

A user or AI agent should be able to ask:

- “Who can search the Amiga schematics dataset?”
- “Where is `qwen2.5-coder` running?”
- “Which node has CUDA and 48 GB of VRAM?”
- “Run this indexing job near the data.”
- “Deploy this GitHub repo as a service.”
- “Start the Home Assistant bridge on the Raspberry Pi.”
- “Find an existing tool in my GitHub portfolio that can inspect Lucene indexes.”

---

## 2. Key Design Principles

### 2.1 Capability-first

The primary abstraction is not a pod, machine, or container.

The primary abstraction is:

```text
Capability
```

Examples:

```text
search(dataset=amiga_schematics)
llm(model=qwen2.5-coder)
build(language=rust)
slice(format=stl)
index(type=lucene)
homeassistant(entity=water_tank)
```

### 2.2 Heterogeneous by default

Every workload must declare what platforms it can run on.

```yaml
platforms:
  - os: linux
    arch: x86_64
  - os: linux
    arch: arm64
  - os: macos
    arch: arm64
  - os: macos
    arch: x86_64
```

### 2.3 Runtime-pluggable

Workloads should not assume Docker.

Supported runtimes:

- native
- docker
- docker-compose
- python
- java
- node
- spark
- llm
- homeassistant
- wasm
- peer/dev-portal

### 2.4 Desired state with reconciliation

OrionMesh should continuously compare desired state with actual state and perform actions to converge the system.

### 2.5 Agent-friendly

The system should be easy for AI agents to inspect and manipulate:

- simple YAML resources
- readable CLI output
- JSON APIs
- MCP integration
- explicit schemas
- validation tools
- clear error messages
- dry-run mode

---

## 3. Target Node Types

| Node Type | OS | Arch | Capabilities |
|---|---|---:|---|
| Mac Studio | macOS | arm64 | Metal LLMs, dev tools, native builds |
| MacBook | macOS | arm64 | development, UI, testing |
| Old Intel Mac | macOS/Linux | x86_64 | legacy services |
| Linux server | Linux | x86_64 | Docker, storage, APIs |
| DGX / GPU box | Linux | x86_64 | CUDA, ML, model serving |
| DGX Spark / ARM server | Linux | arm64 | ARM workloads, AI edge |
| Raspberry Pi | Linux | arm64 | sensors, HA bridges, lightweight services |
| NAS | Linux | x86_64/arm64 | datasets, backups, persistent volumes |

---

## 4. High-Level Architecture

```text
                          +----------------------+
                          |      User / AI       |
                          | CLI / Web / Claude   |
                          +----------+-----------+
                                     |
                                     v
                          +----------------------+
                          |   Orion Controller   |
                          +----------+-----------+
                                     |
             +-----------------------+-----------------------+
             |                       |                       |
             v                       v                       v
      +-------------+         +-------------+         +-------------+
      | Resource    |         | Scheduler   |         | Capability  |
      | Store       |         |             |         | Index       |
      +-------------+         +-------------+         +-------------+
             |                       |                       |
             +-----------------------+-----------------------+
                                     |
                                     v
                          +----------------------+
                          |      NATS Bus        |
                          +----------+-----------+
                                     |
       +-----------------------------+-----------------------------+
       |                             |                             |
       v                             v                             v
+--------------+              +--------------+              +--------------+
| Orion Agent  |              | Orion Agent  |              | Orion Agent  |
| macOS ARM    |              | Linux GPU    |              | Raspberry Pi |
+--------------+              +--------------+              +--------------+
```

---

## 5. Component Architecture

```text
+--------------------------------------------------------------------------------+
|                              Orion Controller                                  |
+--------------------------------------------------------------------------------+
| API Server | Resource Store | Reconciler | Scheduler | Capability Index | Auth |
+--------------------------------------------------------------------------------+
          |                 |                  |                  |
          v                 v                  v                  v
+--------------------------------------------------------------------------------+
|                                  NATS Bus                                      |
+--------------------------------------------------------------------------------+
          |                 |                  |                  |
          v                 v                  v                  v
+----------------+ +----------------+ +----------------+ +----------------+
| Agent: macOS   | | Agent: Linux   | | Agent: Pi      | | Agent: GPU     |
+----------------+ +----------------+ +----------------+ +----------------+
| Inventory      | | Inventory      | | Inventory      | | Inventory      |
| Native Runtime | | Docker Runtime | | Native Runtime | | CUDA Runtime   |
| Docker Runtime | | Native Runtime | | HA Bridge      | | LLM Runtime    |
| Metal LLM      | | Service Probe  | | Sensor Tasks   | | Spark Runtime  |
+----------------+ +----------------+ +----------------+ +----------------+
```

---

## 6. Core Components

### 6.1 Orion Controller

Responsibilities:

- Accept resource definitions.
- Validate resources.
- Store desired state.
- Track observed state.
- Run reconciliation loops.
- Schedule workloads.
- Maintain capability index.
- Expose HTTP API.
- Serve CLI, UI, MCP, Telegram integrations.
- Integrate with Dev Portal.
- Integrate with Home Assistant.
- Dispatch work to agents over NATS.

Suggested implementation:

```text
crates/orion-controller
```

Suggested Rust libraries:

- axum
- tokio
- serde
- sqlx
- async-nats
- tracing
- utoipa/openapi optional

### 6.2 Orion Agent

Runs on every node.

Responsibilities:

- Advertise node inventory.
- Advertise runtimes.
- Advertise datasets.
- Advertise models.
- Advertise capabilities.
- Run tasks.
- Start/stop services.
- Monitor processes.
- Stream logs.
- Report health.
- Report metrics.
- Discover local services.

Suggested implementation:

```text
crates/orion-agent
```

Agent should work on:

- Linux x86_64
- Linux arm64
- macOS arm64
- macOS x86_64
- Raspberry Pi OS
- Ubuntu
- Debian
- Fedora optional

### 6.3 Orion CLI

Main commands:

```bash
orion nodes
orion node describe <node>
orion capabilities
orion find capability <capability>
orion find dataset <dataset>
orion find model <model>
orion apply -f <file.yaml>
orion delete -f <file.yaml>
orion get services
orion get tasks
orion logs service/<name>
orion run -f task.yaml
orion validate -f resource.yaml
orion portfolio scan
orion portfolio suggest "<need>"
```

### 6.4 NATS Bus

Subjects:

```text
orion.node.heartbeat
orion.node.inventory
orion.node.capabilities

orion.service.register
orion.service.unregister
orion.service.health

orion.task.submit
orion.task.assigned
orion.task.started
orion.task.output
orion.task.completed
orion.task.failed

orion.logs.<node>.<workload>
orion.metrics.<node>

orion.control.<node>.run
orion.control.<node>.stop
orion.control.<node>.restart
```

### 6.5 Resource Store

Start with SQLite.

Tables:

```text
resources
observed_nodes
observed_services
observed_capabilities
tasks
task_events
service_events
heartbeats
```

---

## 7. Resource Model

Resources:

```text
Node
Service
Task
Job
Schedule
Dataset
Model
Project
Capability
Runtime
Secret
Volume
Network
Policy
Integration
```

### 7.1 Common Metadata

```yaml
apiVersion: orionmesh.dev/v1
kind: Service
metadata:
  name: amiga-search
  namespace: default
  labels:
    domain: retro
    project: amiga
  annotations:
    owner: chris
spec: {}
status: {}
```

### 7.2 Node Resource

```yaml
apiVersion: orionmesh.dev/v1
kind: Node
metadata:
  name: mac-studio
spec:
  os: macos
  arch: arm64
  roles:
    - workstation
    - llm
  labels:
    site: belmont
    room: office
  resources:
    cpu_cores: 12
    memory_gb: 64
  runtimes:
    - native
    - docker
    - llm
  accelerators:
    - type: metal
```

### 7.3 Service Resource

```yaml
apiVersion: orionmesh.dev/v1
kind: Service
metadata:
  name: amiga-search
spec:
  replicas: 1
  runtime:
    type: docker
    image: geekychris/amiga-search:latest
    ports:
      - name: http
        containerPort: 8080
  capabilities:
    - name: search
      attributes:
        datasets:
          - amiga_schematics
        protocols:
          - http
  placement:
    os:
      - linux
    arch:
      - arm64
      - x86_64
    requires:
      datasets:
        - amiga_schematics
  health:
    http:
      path: /health
      port: 8080
  restartPolicy: always
```

### 7.4 Task Resource

```yaml
apiVersion: orionmesh.dev/v1
kind: Task
metadata:
  name: reindex-amiga-schematics
spec:
  runtime:
    type: docker
    image: geekychris/amiga-indexer:latest
    command:
      - /app/index
      - --dataset
      - amiga_schematics
  placement:
    preferDataLocality: true
    requires:
      datasets:
        - amiga_schematics
  retry:
    maxAttempts: 3
```

### 7.5 Schedule Resource

```yaml
apiVersion: orionmesh.dev/v1
kind: Schedule
metadata:
  name: nightly-github-scan
spec:
  cron: "0 2 * * *"
  taskTemplate:
    runtime:
      type: native
      command:
        - orion-devportal-sync
        - scan
```

### 7.6 Dataset Resource

```yaml
apiVersion: orionmesh.dev/v1
kind: Dataset
metadata:
  name: amiga_schematics
spec:
  description: Amiga schematic PDFs and extracted search index
  locations:
    - node: nas01
      path: /data/amiga/schematics
      access: readwrite
    - node: pi5
      path: /mnt/amiga/schematics
      access: readonly
  formats:
    - pdf
    - text
    - lucene
  capabilities:
    - searchable
```

### 7.7 Model Resource

```yaml
apiVersion: orionmesh.dev/v1
kind: Model
metadata:
  name: qwen2.5-coder-14b
spec:
  family: qwen
  variants:
    - name: qwen2.5-coder:14b
      format: gguf
      quantization: q4_k_m
      memory_gb: 12
      context_tokens: 32768
  preferredRuntime:
    - ollama
    - llama.cpp
  placement:
    acceleration:
      - metal
      - cuda
```

### 7.8 Project Resource

```yaml
apiVersion: orionmesh.dev/v1
kind: Project
metadata:
  name: lucene-viewer
spec:
  repo: github.com/geekychris/lucene-viewer
  language:
    - java
    - typescript
  capabilities:
    - lucene-index-inspection
    - search-debugging
  build:
    type: gradle
  runtimes:
    - native
    - docker
  services:
    - name: lucene-viewer-ui
      port: 3000
```

---

## 8. Runtime Model

### 8.1 Runtime trait

Conceptual Rust trait:

```rust
#[async_trait]
pub trait RuntimeAdapter {
    async fn can_run(&self, workload: &WorkloadSpec) -> Result<RuntimeFit>;
    async fn start_service(&self, service: &ServiceSpec) -> Result<RuntimeHandle>;
    async fn run_task(&self, task: &TaskSpec) -> Result<TaskHandle>;
    async fn stop(&self, handle: &RuntimeHandle) -> Result<()>;
    async fn status(&self, handle: &RuntimeHandle) -> Result<RuntimeStatus>;
    async fn logs(&self, handle: &RuntimeHandle) -> Result<LogStream>;
}
```

### 8.2 Runtime specs

Native:

```yaml
runtime:
  type: native
  command:
    - ./bin/my-service
  env:
    PORT: "8080"
```

Docker:

```yaml
runtime:
  type: docker
  image: repo/service:latest
  ports:
    - name: http
      containerPort: 8080
```

Python:

```yaml
runtime:
  type: python
  python: "3.11"
  requirements: requirements.txt
  command:
    - python
    - worker.py
```

Java:

```yaml
runtime:
  type: java
  jar: target/service.jar
  args:
    - --server.port=8080
```

Node:

```yaml
runtime:
  type: node
  packageManager: pnpm
  command:
    - pnpm
    - start
```

Spark:

```yaml
runtime:
  type: spark
  mainClass: com.example.ReindexJob
  jar: jobs/reindex.jar
```

LLM:

```yaml
runtime:
  type: llm
  engine: ollama
  model: qwen2.5-coder:14b
```

Home Assistant:

```yaml
runtime:
  type: homeassistant
  action: light.turn_on
  target:
    entity_id: light.workshop
```

---

## 9. Scheduling

### 9.1 Scheduler inputs

- workload requirements
- node inventory
- runtime availability
- architecture
- OS
- memory
- CPU
- GPU
- datasets
- models
- labels
- site
- health
- current load
- affinity/anti-affinity
- user preference

### 9.2 Placement example

```yaml
placement:
  os:
    - linux
  arch:
    - x86_64
  requires:
    gpu:
      vendor: nvidia
      min_vram_gb: 24
    datasets:
      - training_docs
  prefer:
    site: belmont
```

### 9.3 Scheduling algorithm v1

1. Filter nodes by online status.
2. Filter by OS.
3. Filter by architecture.
4. Filter by required runtime.
5. Filter by resources.
6. Filter by required datasets/models/capabilities.
7. Score by locality.
8. Score by free resources.
9. Score by preference.
10. Pick best node.
11. Assign task/service.

---

## 10. Desired State Reconciliation

### 10.1 Reconciliation loop

```text
loop:
    desired = load_desired_resources()
    observed = load_observed_state()
    diff = compare(desired, observed)

    for action in diff.actions:
        dispatch(action)

    sleep(reconcile_interval)
```

### 10.2 Example actions

```text
StartService
StopService
RestartService
MoveService
RunTask
CancelTask
UpdateConfig
MarkUnhealthy
Reschedule
```

### 10.3 Status model

```yaml
status:
  phase: Running
  node: pi5
  observedGeneration: 3
  conditions:
    - type: Available
      status: "True"
    - type: Healthy
      status: "True"
```

---

## 11. Capability Discovery

### 11.1 Capability schema

```yaml
capability:
  name: search
  attributes:
    dataset: amiga_schematics
    protocol: http
    endpoint: http://pi5.local:8080
```

### 11.2 Examples

Search:

```yaml
name: search
attributes:
  datasets:
    - amiga_schematics
  index_type: lucene
  supports:
    - keyword
    - vector
```

LLM:

```yaml
name: llm
attributes:
  models:
    - qwen2.5-coder:14b
  engine: ollama
  acceleration: metal
```

Build:

```yaml
name: build
attributes:
  languages:
    - rust
    - java
  tools:
    - cargo
    - gradle
```

3D printing:

```yaml
name: slicer
attributes:
  formats:
    - stl
    - 3mf
  engine: prusaslicer
```

Home automation:

```yaml
name: homeassistant
attributes:
  site: belmont
  entities:
    - sensor.water_tank_level
    - switch.workshop_fan
```

---

## 12. Dev Portal and GitHub Portfolio Integration

### 12.1 Purpose

The Dev Portal should become the source of truth for Chris's GitHub portfolio.

It should provide OrionMesh with:

- repository list
- languages
- build systems
- runtime metadata
- Dockerfile detection
- dependency graph
- service definitions
- task definitions
- capabilities
- deployment hints
- project maturity
- useful code reuse suggestions

### 12.2 Repo scanning

For each repo, detect:

```text
README
Dockerfile
docker-compose.yml
package.json
Cargo.toml
pom.xml
build.gradle
requirements.txt
pyproject.toml
Makefile
.github/workflows
src/
docs/
```

### 12.3 Project manifest generation

Generate:

```yaml
apiVersion: orionmesh.dev/v1
kind: Project
metadata:
  name: repo-name
spec:
  repo: github.com/geekychris/repo-name
  capabilities: []
  runtimes: []
  services: []
  tasks: []
```

### 12.4 Claude skill

Claude skill should support:

- search user's repos
- suggest reuse
- generate Orion manifests
- deploy repo into OrionMesh
- explain dependencies
- identify service/task candidates

Example prompt:

```text
Use my GitHub portfolio and OrionMesh registry to determine whether I already have code that can solve this.
```

### 12.5 Dev Portal peer runtime

Dev Portal can expose peer runtimes as Orion resources.

```yaml
kind: Runtime
metadata:
  name: devportal-peer
spec:
  type: peer
  source: dev_portal
```

---

## 13. Home Assistant Integration

### 13.1 Inbound events

Home Assistant can trigger OrionMesh tasks.

Examples:

- water tank low
- workshop temperature high
- Starlink offline
- garage open too long
- 3D printer finished
- motion detected near equipment

### 13.2 Outbound actions

OrionMesh can call Home Assistant services.

Examples:

- turn on workshop fan
- send phone notification
- switch relay
- update dashboard sensor
- trigger automation

### 13.3 Orion as HA entities

Expose:

```text
sensor.orion_nodes_online
sensor.orion_tasks_running
sensor.orion_services_unhealthy
binary_sensor.orion_controller_online
```

---

## 14. Telegram Integration

Commands:

```text
/orion nodes
/orion services
/orion tasks
/orion find model llama
/orion find dataset amiga
/orion restart service amiga-search
/orion run backup
```

Safety:

- require allowlist of Telegram user IDs
- read-only mode by default
- confirmation for destructive actions

---

## 15. MCP Integration

OrionMesh should expose an MCP server so Claude can:

- list nodes
- inspect capabilities
- query services
- validate resources
- apply resources
- run tasks
- read logs
- search portfolio
- suggest deployments

Tools:

```text
orion_list_nodes
orion_find_capability
orion_find_model
orion_find_dataset
orion_validate_resource
orion_apply_resource
orion_run_task
orion_get_logs
orion_search_portfolio
```

---

## 16. API Design

### 16.1 HTTP endpoints

```text
GET    /v1/nodes
GET    /v1/nodes/{name}
GET    /v1/services
GET    /v1/services/{name}
GET    /v1/tasks
POST   /v1/tasks
GET    /v1/capabilities
GET    /v1/find
POST   /v1/resources/apply
POST   /v1/resources/validate
DELETE /v1/resources/{kind}/{name}
GET    /v1/logs/{kind}/{name}
```

### 16.2 Find API

Request:

```json
{
  "capability": "search",
  "attributes": {
    "dataset": "amiga_schematics"
  }
}
```

Response:

```json
{
  "matches": [
    {
      "service": "amiga-search",
      "node": "pi5",
      "url": "http://pi5.local:8080",
      "score": 0.97
    }
  ]
}
```

---

## 17. Security

### 17.1 Security goals

- prevent random LAN devices from joining
- protect secrets
- authenticate agents
- authorize control actions
- support read-only integrations
- keep initial setup simple

### 17.2 Initial model

- shared cluster token
- node enrollment command
- NATS credentials
- controller API token
- local config file permissions

### 17.3 Later model

- mTLS
- per-node identity
- role-based access
- short-lived tokens
- signed resource manifests

---

## 18. Observability

### 18.1 Logs

Support:

```bash
orion logs service/amiga-search
orion logs task/reindex-amiga
orion logs node/pi5
```

### 18.2 Metrics

Metrics:

- node online/offline
- CPU
- memory
- disk
- task count
- service health
- runtime failures
- scheduling failures
- NATS latency

### 18.3 Events

Store events:

```text
TaskScheduled
TaskStarted
TaskCompleted
TaskFailed
ServiceStarted
ServiceUnhealthy
NodeOffline
CapabilityRegistered
```

---

## 19. Storage

### 19.1 SQLite-first schema

```sql
resources(
  id text primary key,
  kind text not null,
  namespace text not null,
  name text not null,
  generation integer not null,
  spec_json text not null,
  status_json text,
  created_at text not null,
  updated_at text not null
);

nodes(
  name text primary key,
  os text,
  arch text,
  labels_json text,
  resources_json text,
  last_seen_at text
);

capabilities(
  id text primary key,
  node text,
  service text,
  name text,
  attributes_json text,
  updated_at text
);

tasks(
  id text primary key,
  name text,
  phase text,
  node text,
  spec_json text,
  result_json text,
  created_at text,
  updated_at text
);

events(
  id text primary key,
  resource_id text,
  type text,
  message text,
  created_at text
);
```

---

## 20. Repository Structure

Suggested Rust workspace:

```text
orion_mesh/
  Cargo.toml
  README.md
  CLAUDE.md

  crates/
    orion-types/
    orion-controller/
    orion-agent/
    orion-cli/
    orion-nats/
    orion-store/
    orion-scheduler/
    orion-runtime/
    orion-devportal/
    orion-ha/
    orion-mcp/

  docs/
    architecture/
    resources/
    api/
    examples/

  examples/
    service-amiga-search.yaml
    task-reindex.yaml
    model-qwen.yaml
    dataset-amiga.yaml

  .claude/
    skills/
      validate-resource/
      deploy-service/
      inspect-capability/
```

---

## 21. Implementation Roadmap

### Phase 1: Foundation

Deliverables:

- Rust workspace
- `orion-types`
- common resource model
- `orion-cli validate`
- basic controller API
- NATS local docker-compose
- basic agent heartbeat

Acceptance:

```bash
orion validate examples/service.yaml
orion nodes
```

### Phase 2: Node inventory

Deliverables:

- OS/arch detection
- CPU/memory detection
- Docker detection
- runtime detection
- heartbeat updates
- controller `/v1/nodes`

Acceptance:

```bash
orion nodes
orion node describe mac-studio
```

### Phase 3: Native task execution

Deliverables:

- task resource
- scheduler v1
- agent task runner
- task logs
- task status

Acceptance:

```bash
orion run -f examples/task-echo.yaml
orion logs task/echo
```

### Phase 4: Service registry and capability index

Deliverables:

- service registration
- capability schema
- find API
- CLI find commands

Acceptance:

```bash
orion find capability search dataset=amiga_schematics
```

### Phase 5: Docker runtime

Deliverables:

- Docker start/stop
- container logs
- health checks
- port metadata
- restart policy

Acceptance:

```bash
orion apply -f examples/service-docker.yaml
orion get services
```

### Phase 6: Desired-state reconciliation

Deliverables:

- resource store
- observed state
- reconcile loop
- restart unhealthy services
- status conditions

Acceptance:

```bash
orion apply -f examples/service.yaml
docker stop service-container
# Orion restarts it
```

### Phase 7: Dataset and model resources

Deliverables:

- Dataset resource
- Model resource
- dataset-aware discovery
- model-aware discovery
- placement constraints

Acceptance:

```bash
orion find dataset amiga_schematics
orion find model qwen2.5-coder
```

### Phase 8: Dev Portal integration

Deliverables:

- repo scanner
- project resource generation
- Dev Portal API integration
- project capabilities
- Claude skill integration

Acceptance:

```bash
orion portfolio scan
orion portfolio suggest "I need a Lucene index viewer"
```

### Phase 9: Home Assistant and Telegram

Deliverables:

- HA event listener
- HA service caller
- Telegram bot
- safe read-only commands

Acceptance:

```text
Telegram: /orion nodes
HA: sensor.orion_nodes_online
```

### Phase 10: MCP and AI-native workflows

Deliverables:

- MCP server
- Claude skill docs
- AI-safe tools
- resource validation
- apply with dry-run
- explain scheduling decision

Acceptance:

```text
Claude can ask OrionMesh:
- what nodes exist
- where a model is running
- whether a resource is valid
- deploy a service from a repo
```

---

## 22. Example End-to-End Flow

### User asks

```text
Run an Amiga schematic search service.
```

### Claude/Dev Portal discovers

```text
Repo: amiga-schematic-search
Dataset: amiga_schematics
Runtime: docker
Platforms: linux/arm64, linux/x86_64
```

### OrionMesh finds

```text
Dataset located on nas01 and mounted on pi5.
pi5 supports docker and arm64.
```

### Orion applies

```yaml
kind: Service
metadata:
  name: amiga-search
spec:
  runtime:
    type: docker
    image: geekychris/amiga-search:arm64
  placement:
    requires:
      datasets:
        - amiga_schematics
```

### Controller schedules

```text
Assigned to pi5.
```

### Agent starts

```text
Docker container running.
Health check passed.
Capability registered:
search(dataset=amiga_schematics)
```

### User queries

```bash
orion find capability search dataset=amiga_schematics
```

Result:

```text
amiga-search on pi5: http://pi5.local:8080
```

---

## 23. Non-Goals

OrionMesh should not initially try to be:

- a full Kubernetes replacement
- a public cloud orchestrator
- a multi-tenant enterprise platform
- a service mesh with transparent sidecars
- a distributed filesystem
- a secrets manager as complex as Vault
- a full CI/CD platform
- a complete Home Assistant replacement

---

## 24. Design Differentiators

OrionMesh is different because it combines:

- personal cluster management
- capability-first discovery
- dataset-aware scheduling
- model-aware scheduling
- GitHub portfolio awareness
- AI-agent-friendly APIs
- heterogeneous OS/architecture support
- native plus Docker runtimes
- Home Assistant and maker-space integration

The defining idea:

> OrionMesh does not merely know what is running. It knows what your personal compute constellation can do.

---

## 25. Immediate Next Agent Tasks

An implementation agent should start here:

1. Review this document.
2. Ensure the Rust workspace builds.
3. Implement/verify `orion-types`.
4. Implement resource validation.
5. Implement agent heartbeat.
6. Implement controller `/v1/nodes`.
7. Implement CLI `orion nodes`.
8. Implement capability advertisement.
9. Implement CLI `orion find capability`.
10. Implement native task runner.
11. Implement Docker runtime.
12. Implement desired-state reconcile loop.

---

## 26. Build Acceptance Checklist

### MVP must support

- [ ] agent starts on macOS ARM
- [ ] agent starts on Linux x86_64
- [ ] agent starts on Raspberry Pi ARM64
- [ ] controller starts with SQLite
- [ ] NATS connection works
- [ ] nodes heartbeat
- [ ] CLI lists nodes
- [ ] CLI validates YAML
- [ ] native task runs
- [ ] Docker service runs
- [ ] service registers capability
- [ ] capability lookup works
- [ ] desired state restarts stopped service

### Next milestone must support

- [ ] dataset resource
- [ ] model resource
- [ ] placement by dataset
- [ ] placement by model
- [ ] Dev Portal project import
- [ ] GitHub repo capability scanning
- [ ] Home Assistant read integration
- [ ] Telegram read-only bot
- [ ] MCP read-only tools

---

## 27. Glossary

**Capability**  
Something a node, service, project, model, or runtime can do.

**Dataset**  
A named body of data that may exist on one or more nodes.

**Model**  
An LLM or ML model that may be served by a runtime.

**Runtime**  
A mechanism for running workloads, such as native, Docker, Python, Java, Spark, or LLM.

**Service**  
A long-running workload.

**Task**  
A one-off workload.

**Schedule**  
A recurring task.

**Desired State**  
The declared state OrionMesh should maintain.

**Observed State**  
The actual state reported by agents.

**Reconciliation**  
The process of converging observed state toward desired state.

**Dev Portal**  
Chris's project/repository catalog, used as a source of truth for repo capabilities.

---

# Appendix A: Example Resources

## A.1 Echo Task

```yaml
apiVersion: orionmesh.dev/v1
kind: Task
metadata:
  name: echo-test
spec:
  runtime:
    type: native
    command:
      - echo
      - hello from orion
```

## A.2 Raspberry Pi Home Assistant Bridge

```yaml
apiVersion: orionmesh.dev/v1
kind: Service
metadata:
  name: ha-bridge
spec:
  runtime:
    type: native
    command:
      - orion-ha-bridge
  placement:
    labels:
      site: belmont
      device_class: raspberry-pi
  capabilities:
    - name: homeassistant
      attributes:
        site: belmont
```

## A.3 LLM Runtime

```yaml
apiVersion: orionmesh.dev/v1
kind: Service
metadata:
  name: macstudio-ollama
spec:
  runtime:
    type: llm
    engine: ollama
  placement:
    os:
      - macos
    arch:
      - arm64
  capabilities:
    - name: llm
      attributes:
        models:
          - llama3.1:8b
          - qwen2.5-coder:14b
        acceleration: metal
```

## A.4 CUDA Model Server

```yaml
apiVersion: orionmesh.dev/v1
kind: Service
metadata:
  name: dgx-vllm
spec:
  runtime:
    type: docker
    image: vllm/vllm-openai:latest
  placement:
    requires:
      gpu:
        vendor: nvidia
        min_vram_gb: 48
  capabilities:
    - name: llm
      attributes:
        engine: vllm
        acceleration: cuda
        models:
          - qwen2.5-coder:32b
```

## A.5 Dev Portal Project

```yaml
apiVersion: orionmesh.dev/v1
kind: Project
metadata:
  name: boingtrace
spec:
  repo: github.com/geekychris/boingtrace
  capabilities:
    - amiga-schematic-search
    - retro-computing-knowledge
  runtimes:
    - docker
    - native
```

---

# Appendix B: Suggested First GitHub Issues

1. Define `orion-types` resource structs.
2. Add YAML validation command.
3. Add NATS docker-compose.
4. Implement agent heartbeat publisher.
5. Implement controller heartbeat subscriber.
6. Add `/v1/nodes`.
7. Add `orion nodes`.
8. Add capability advertisement message.
9. Add capability index table.
10. Add `orion find capability`.
11. Add native runtime adapter.
12. Add task execution.
13. Add task logs.
14. Add Docker runtime adapter.
15. Add service resource apply.
16. Add desired-state reconciler.
17. Add Dataset resource.
18. Add Model resource.
19. Add Dev Portal repo scanner.
20. Add MCP server.
