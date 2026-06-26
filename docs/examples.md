# Examples

The `examples/` tree is the fastest way to learn OrionMesh by reading. Each directory groups YAMLs by aspect — placement, capabilities, datasets, IPC. This doc walks each group, explains what it demonstrates, when to reach for it as a template, and how to drive it from the command line.

> **Assumed setup.** A controller on `http://127.0.0.1:7878`, an agent reporting as `demo-mac` (or any node id), and `ORION_AUTH_DISABLED=1` set on every process — see [installation.md §6](installation.md#6-local-dev--fastest-path). For auth-enforced runs, prepend every `curl` with `-H "Authorization: Bearer $TOKEN"`.

```bash
CTRL=http://127.0.0.1:7878
```

The companion shell helper that runs every example serially lives at [`examples/walkthrough.sh`](../examples/walkthrough.sh) — apply a curated set, list each kind back, exit. The rest of this doc is the manual breakdown.

---

## Tree map

| Dir | Kinds covered | What it teaches | Phase needed |
|---|---|---|---|
| [`01-services/`](#01-services) | Service | native + Docker, named ports, HealthCheck variants, restart policies | Phase A for dispatch + logs |
| [`02-tasks/`](#02-tasks) | Task | Python (GPU + retry + locality), Java (x86 placement), native one-shots | Phase A |
| [`03-schedules/`](#03-schedules) | Schedule + Task | `task:` reference vs inline `task_template:` | Phase B for actual firing |
| [`04-capabilities/`](#04-capabilities) | Service + Task + Capability | advertise + 3 selector forms + declared schema | parse-only today |
| [`05-placement/`](#05-placement) | Service + Task | arch, GPU vendor/min_vram, site labels, `prefer:` block | parse-only today |
| [`06-data/`](#06-data) | Dataset + Model + Volume + Secret | locations + access modes, model variants, secret resolver URIs | parse-only today |
| [`07-peers/`](#07-peers) | Runtime + Service + Task | catalog entries for OrionMesh / KQueue / Dev Portal; `runtime: peer` | parse-only today |
| [`08-canonical/`](#08-canonical) | Service | The plan's `amiga-search` example, plus a fleshed-out version | Phase A for dispatch |
| [`09-ipc/`](#09-ipc) | Service | Two `orion-demo-pub` / `orion-demo-sub` Services that talk over the mesh's NATS | Phase A + C for the full demo |
| [`bad/`](#bad) | (deliberately invalid) | What `Resource::validate()` catches | none — `orion validate` only |

"Parse-only today" = applies cleanly, round-trips through SQLite, surfaces in `GET /v1/resources/{Kind}`. The runtime path for Dataset, Model, Capability, etc. lands in later phases (their semantics feed the scheduler and Find API; storing them now lets you author content the moment those land).

---

## How to run an example

The four moves you'll repeat:

```bash
# 1. Validate locally (no controller call)
./target/debug/orion validate examples/01-services/native-sleeper.yaml

# 2. Apply (store in SQLite)
curl -X POST --data-binary @examples/01-services/native-sleeper.yaml \
     $CTRL/v1/resources/apply
# → {"kind":"Service","name":"sleeper","generation":1,"dry_run":false}

# 3. Dispatch (publish ControlRun → agent launches via the runtime adapter)
curl -X POST $CTRL/v1/dispatch/Service/sleeper
# → {"kind":"Service","name":"sleeper","node":"demo-mac","instance_id":"…"}

# 4. Stream logs (poll-friendly — pass back the previous `total` as `since`)
curl $CTRL/v1/logs/Service/sleeper
```

Dispatch + logs only make sense for `kind: Service` and `kind: Task` with a runtime an adapter actually supports. Today that's `runtime.kind: native`; Docker/Python/Java land in Phase 5.

To delete a resource (also stops the workload from being re-dispatched):

```bash
curl -X DELETE $CTRL/v1/resources/Service/sleeper
```

---

## 01-services

[`examples/01-services/`](../examples/01-services/)

What a long-running workload looks like. Four files exercise different runtime + health combinations.

| File | What's special |
|---|---|
| [`native-sleeper.yaml`](../examples/01-services/native-sleeper.yaml) | Minimum viable Service — native `/bin/sleep 3600`. Dispatchable today; runs immediately. |
| [`docker-nginx.yaml`](../examples/01-services/docker-nginx.yaml) | Docker runtime, 2 replicas, named ports (`http`, `metrics`), HTTP health check, `on_failure` restart. **Storable; not dispatchable until the Docker adapter ships in Phase 5.** |
| [`docker-redis.yaml`](../examples/01-services/docker-redis.yaml) | Docker runtime, env vars, TCP health check (port 6379). Same Phase-5 caveat. |
| [`native-with-exec-health.yaml`](../examples/01-services/native-with-exec-health.yaml) | Native + a custom `exec:` health command. Dispatches today; health probe loop is Phase 5. |

**Use as a template when**: you want a workload to stay up — a daemon, an HTTP server, a queue worker. Pick `native-sleeper.yaml` if you have a binary on disk; `docker-nginx.yaml` once the Docker adapter is real.

**Dispatch + watch (works today)**:

```bash
curl -X POST --data-binary @examples/01-services/native-sleeper.yaml $CTRL/v1/resources/apply
curl -X POST $CTRL/v1/dispatch/Service/sleeper
# Sleep services don't print, but the agent records the launch — see:
curl $CTRL/v1/logs/Service/sleeper
```

A more demonstrative dispatch — replace the YAML inline with something that prints:

```bash
curl -X POST $CTRL/v1/resources/apply --data-binary @- <<'EOF'
kind: Service
metadata: { name: chatty }
spec:
  runtime:
    kind: native
    exec: /bin/sh
    args: ["-c", "for i in 1 2 3 4 5; do echo line-$i; sleep 1; done; echo done"]
EOF
curl -X POST $CTRL/v1/dispatch/Service/chatty
sleep 6
curl $CTRL/v1/logs/Service/chatty
```

You'll see 5 stdout `line-$i` rows and a `done` row, each with millisecond timestamps.

---

## 02-tasks

[`examples/02-tasks/`](../examples/02-tasks/)

Tasks are one-shot — run to completion (success or failure), don't restart unless `retry:` is set.

| File | What's special |
|---|---|
| [`python-train.yaml`](../examples/02-tasks/python-train.yaml) | Python runtime, GPU placement (`vendor: nvidia, min_vram_gb: 24`), CUDA accel, `prefer_data_locality: true`, retry + timeout. Best showcase of Task-side knobs. |
| [`java-batch.yaml`](../examples/02-tasks/java-batch.yaml) | Java runtime, x86_64 placement, node label filter. |
| [`native-snapshot.yaml`](../examples/02-tasks/native-snapshot.yaml) | Native `pg_dump`, intended as a Schedule target. |

**Use as a template when**: the work has a beginning and an end — train a model, run a backup, build a dataset. `Service` is for "keep this alive". `Task` is for "do this once, tell me when it's done".

**Dispatch + watch (works today for native runtime)**:

```bash
# Apply native-snapshot.yaml (would normally be fired by a Schedule)
curl -X POST --data-binary @examples/02-tasks/native-snapshot.yaml $CTRL/v1/resources/apply
# But pg_dump probably isn't installed — substitute a printf-based one:
curl -X POST $CTRL/v1/resources/apply --data-binary @- <<'EOF'
kind: Task
metadata: { name: snapshot-demo }
spec:
  runtime:
    kind: native
    exec: /bin/sh
    args: ["-c", "echo snapshot-start; sleep 2; echo wrote-12345-rows; echo snapshot-done"]
EOF
curl -X POST $CTRL/v1/dispatch/Task/snapshot-demo
sleep 4
curl $CTRL/v1/logs/Task/snapshot-demo
```

---

## 03-schedules

[`examples/03-schedules/`](../examples/03-schedules/)

A Schedule fires a Task on cron. The validator (`Resource::validate()`) enforces exactly one of `task:` (reference) or `task_template:` (inline) — see [`bad/schedule-both.yaml`](#bad) and [`bad/schedule-neither.yaml`](#bad) for the failure modes.

| File | Form |
|---|---|
| [`reference.yaml`](../examples/03-schedules/reference.yaml) | `task: postgres-snapshot` — points at an existing Task resource |
| [`inline-template.yaml`](../examples/03-schedules/inline-template.yaml) | `task_template: { runtime: ..., placement: ... }` — fully inline |
| [`hourly-health-check.yaml`](../examples/03-schedules/hourly-health-check.yaml) | Inline template, `0 * * * *` (every hour on the minute) |

**Use as a template when**: a workload needs to run periodically. Reference form is good when the same Task is fired by multiple Schedules; inline is good for one-off cadence-only-here jobs.

**Fire it (Phase B works today)**:

```bash
# Apply a Task + a Schedule pointing at it
curl -X POST $CTRL/v1/resources/apply --data-binary @- <<'EOF'
kind: Task
metadata: { name: cron-job }
spec:
  runtime:
    kind: native
    exec: /bin/sh
    args: ["-c", "echo fired-at-$(date +%H:%M:%S)"]
EOF
curl -X POST $CTRL/v1/resources/apply --data-binary @- <<'EOF'
kind: Schedule
metadata: { name: every-min }
spec: { cron: "* * * * *", task: cron-job }
EOF

# Watch the observed state — fire_count goes 0→1 at the next minute mark
watch -n 5 'curl -s $CTRL/v1/schedules/observed | python3 -m json.tool'
```

The cron is 5-field POSIX (minute hour day month weekday). 6-field with seconds also works.

---

## 04-capabilities

[`examples/04-capabilities/`](../examples/04-capabilities/)

The OrionMesh differentiator. Workloads constrain placement on advertised `capabilities:`, not just names. Three forms of attribute check; the deserializer dispatches based on JSON shape.

| File | What's special |
|---|---|
| [`advertise-search.yaml`](../examples/04-capabilities/advertise-search.yaml) | A Service advertising `search` + `web` capabilities with nested attributes |
| [`require-equals.yaml`](../examples/04-capabilities/require-equals.yaml) | Selector form: bare value → `Equals` |
| [`require-oneof.yaml`](../examples/04-capabilities/require-oneof.yaml) | Selector form: JSON array → `OneOf` |
| [`require-op.yaml`](../examples/04-capabilities/require-op.yaml) | Selector form: `{gte: 24}` → `Op` |
| [`declared-schema.yaml`](../examples/04-capabilities/declared-schema.yaml) | A `kind: Capability` resource declaring the attribute schema |

**Use as a template when**: you want the scheduler (Phase 4 Find API) to match workloads to capable nodes/services on something richer than a name — "an LLM with ≥ 24 GB VRAM", "a search service over the amiga dataset that returns PDFs".

**Try the matcher live**: open the **Demo** tab → **Capability matcher** card. Or apply the YAMLs and watch them in the catalog:

```bash
curl -X POST --data-binary @examples/04-capabilities/advertise-search.yaml $CTRL/v1/resources/apply
curl -X POST --data-binary @examples/04-capabilities/declared-schema.yaml   $CTRL/v1/resources/apply
curl $CTRL/v1/resources/Service | python3 -m json.tool
curl $CTRL/v1/resources/Capability | python3 -m json.tool
```

---

## 05-placement

[`examples/05-placement/`](../examples/05-placement/)

Hard constraints (`arch`, `os`, `gpu`, `acceleration`, `node_labels`) filter candidate nodes. Soft preferences (`prefer:`) score the survivors.

| File | What's special |
|---|---|
| [`arch-only.yaml`](../examples/05-placement/arch-only.yaml) | `arch: [arm64]` — Pi-only |
| [`gpu-required.yaml`](../examples/05-placement/gpu-required.yaml) | `gpu: { vendor: nvidia, min_vram_gb: 24 }, acceleration: cuda` |
| [`site-label.yaml`](../examples/05-placement/site-label.yaml) | `node_labels: { site: belmont }` |
| [`prefer-soft.yaml`](../examples/05-placement/prefer-soft.yaml) | `prefer: { node_labels: { power: mains }, data_locality: true }` |
| [`combined.yaml`](../examples/05-placement/combined.yaml) | All four together with inline `capabilities:`, ports, and health |

**Use as a template when**: a workload only makes sense on specific hardware — needs a GPU, must be at a specific site, prefers mains power, etc.

**Simulate placement live**: Demo tab → **Placement simulator** card. Define a placement; see which nodes (real or simulated) survive the filter.

---

## 06-data

[`examples/06-data/`](../examples/06-data/)

Resources that *describe data and secrets the workloads will consume* — not workloads themselves.

| File | Kind | What's special |
|---|---|---|
| [`dataset-multi-location.yaml`](../examples/06-data/dataset-multi-location.yaml) | Dataset | Three locations across three nodes, mixed `ro`/`rw` access, multiple formats |
| [`dataset-readonly.yaml`](../examples/06-data/dataset-readonly.yaml) | Dataset | Single-location, single-access example |
| [`model-variants.yaml`](../examples/06-data/model-variants.yaml) | Model | Three variants (gguf q4_k_m / gguf q8_0 / mlx int8), `memory_gb` + `context_window` per variant |
| [`model-served-by.yaml`](../examples/06-data/model-served-by.yaml) | Model | Single Pi-friendly variant |
| [`volume.yaml`](../examples/06-data/volume.yaml) | Volume | Shared scratch volume mounted on three nodes |
| [`secret-plaintext.yaml`](../examples/06-data/secret-plaintext.yaml) | Secret | `vault_ref: "plaintext://openai-api-key"` — resolved at workload startup by the agent's `PlaintextResolver` |

**Use as a template when**: you have a corpus, a model, a shared filesystem, or a credential that workloads need to reference. Dataset locality drives Phase-5 scheduling (`prefer_data_locality: true` on a Task scores nodes that hold the referenced dataset higher).

```bash
curl -X POST --data-binary @examples/06-data/dataset-multi-location.yaml $CTRL/v1/resources/apply
curl -X POST --data-binary @examples/06-data/model-variants.yaml         $CTRL/v1/resources/apply
curl $CTRL/v1/resources/Dataset | python3 -m json.tool
curl $CTRL/v1/resources/Model   | python3 -m json.tool
```

---

## 07-peers

[`examples/07-peers/`](../examples/07-peers/)

OrionMesh treats Dev Portal, KQueue, and other OrionMesh instances as peers, not parents. Each is a `kind: Runtime` resource; workloads can hand off via `runtime: { kind: peer, system: <name>, ref: <id> }`.

| File | What it registers |
|---|---|
| [`orionmesh-belmont.yaml`](../examples/07-peers/orionmesh-belmont.yaml) | A peer OrionMesh controller at another site |
| [`kqueue-default.yaml`](../examples/07-peers/kqueue-default.yaml) | A KQueue instance + its JetStream prefix |
| [`devportal-local.yaml`](../examples/07-peers/devportal-local.yaml) | The local Dev Portal as a peer |
| [`service-via-kqueue.yaml`](../examples/07-peers/service-via-kqueue.yaml) | A Service whose `runtime: peer` delegates to `kqueue-default` |
| [`task-via-peer-mesh.yaml`](../examples/07-peers/task-via-peer-mesh.yaml) | A Task delegated to `orionmesh-belmont` |

**Use as a template when**: you want to run something on a system you've registered as a peer rather than on the local mesh — KQueue's autoscaling Go workers, another OrionMesh at a different site, a Dev Portal-managed local runtime.

```bash
for f in examples/07-peers/*.yaml; do
  curl -X POST --data-binary @"$f" $CTRL/v1/resources/apply
done
curl $CTRL/v1/resources/Runtime | python3 -m json.tool
```

The peer-runtime catalog also surfaces in Dev Portal once the `peerruntime` extension lands there — see the open PR at `geekychris/devhubpro#1`.

---

## 08-canonical

[`examples/08-canonical/`](../examples/08-canonical/)

The plan's own example, plus a fleshed-out version. Useful as a reference for the documented YAML shape; `amiga-search.yaml` is also what the `service_amiga_search_roundtrip` test in `crates/orion-types/src/tests.rs` parses, so editing it changes the test.

| File | Source |
|---|---|
| [`amiga-search.yaml`](../examples/08-canonical/amiga-search.yaml) | Verbatim from `OrionMesh_Architecture_Plan.md` |
| [`amiga-search-full.yaml`](../examples/08-canonical/amiga-search-full.yaml) | The same Service with health, capabilities, ports, env, labels |

```bash
curl -X POST --data-binary @examples/08-canonical/amiga-search-full.yaml $CTRL/v1/resources/apply
curl $CTRL/v1/resources/Service/amiga-search | python3 -m json.tool
```

---

## 09-ipc

[`examples/09-ipc/`](../examples/09-ipc/)

Two trivial Services demonstrating **service-to-service IPC over the mesh's own NATS broker**. Real bidirectional messaging — not stub data.

| File | What it runs |
|---|---|
| [`demo-pub.yaml`](../examples/09-ipc/demo-pub.yaml) | `target/release/orion-demo-pub` — publishes `tick N from P at HH:MM:SS.mmm` to `orion.demo.ipc` every second |
| [`demo-sub.yaml`](../examples/09-ipc/demo-sub.yaml) | `target/release/orion-demo-sub` — subscribes to `orion.demo.ipc`, prints each message |

Both binaries are built by the workspace:

```bash
cargo build --release -p orion-demo-bins
# produces target/release/orion-demo-pub and target/release/orion-demo-sub
```

**Use as a template when**: you want two of your workloads to talk to each other without you having to deploy a separate message bus — OrionMesh already runs one for the control plane, and your workloads can ride it.

**Drive it from the command line**:

```bash
# Apply both Services
curl -X POST --data-binary @examples/09-ipc/demo-sub.yaml $CTRL/v1/resources/apply
curl -X POST --data-binary @examples/09-ipc/demo-pub.yaml $CTRL/v1/resources/apply

# Dispatch the subscriber FIRST so it's listening when the publisher starts
curl -X POST $CTRL/v1/dispatch/Service/demo-sub
sleep 1
curl -X POST $CTRL/v1/dispatch/Service/demo-pub

# Watch the messages flowing — same timestamps end-to-end
sleep 5
echo "=== publisher stdout ==="
curl -s $CTRL/v1/logs/Service/demo-pub | python3 -c "import sys,json;d=json.load(sys.stdin);[print(e['line']) for e in d['entries'][-6:]]"
echo "=== subscriber stdout ==="
curl -s $CTRL/v1/logs/Service/demo-sub | python3 -c "import sys,json;d=json.load(sys.stdin);[print(e['line']) for e in d['entries'][-6:]]"
```

Or — open the **Demo** tab in the UI and click the **IPC demo** card's *Apply + Dispatch both* button. Side-by-side log tails show the same NATS-mediated messages from both perspectives.

**Stopping cleanly**: today, `DELETE /v1/resources/Service/{name}` removes the resource but the running process keeps going until the agent ends. ControlStop-by-name is on the Phase 5 list. For now: `pkill -f orion-demo-` does the job.

---

## bad

[`examples/bad/`](../examples/bad/)

Deliberately broken YAMLs. Run them through `orion validate` to see exactly what `Resource::validate()` + serde catch, including the suggestion text on enum mistypes.

| File | What goes wrong |
|---|---|
| [`schedule-both.yaml`](../examples/bad/schedule-both.yaml) | Schedule sets both `task:` and `task_template:` |
| [`schedule-neither.yaml`](../examples/bad/schedule-neither.yaml) | Schedule sets neither |
| [`unknown-kind.yaml`](../examples/bad/unknown-kind.yaml) | `kind: Slartibartfast` |
| [`unknown-runtime.yaml`](../examples/bad/unknown-runtime.yaml) | `runtime.kind: zigzag` |
| [`bad-restart-policy.yaml`](../examples/bad/bad-restart-policy.yaml) | `restart_policy: maybe` |

**Sample outputs** (these are exactly what you get):

```bash
$ ./target/debug/orion validate examples/bad/schedule-both.yaml
Error: validating resource
Caused by: schedule must set exactly one of `task` or `taskTemplate`

$ ./target/debug/orion validate examples/bad/unknown-runtime.yaml
Error: parsing resource yaml
Caused by:
    0: invalid resource yaml: unknown variant `zigzag`, expected one of
       `native`, `docker`, `python`, `java`, `node`, `spark`, `llm`,
       `homeassistant`, `wasm`, `peer`
```

**Use as**: CI / pre-commit pattern. `orion validate file.yaml` exits non-zero on parse or semantic failure with a structured cause chain; pipe it into a hook to refuse bad YAML before commit.

---

## Bulk: walkthrough.sh

[`examples/walkthrough.sh`](../examples/walkthrough.sh)

Runs the four moves above against every YAML in the tree:

1. `orion validate` on every non-`bad/` file (33 today). Asserts all pass.
2. `orion validate` on every `bad/` file. Asserts all fail.
3. Applies a curated subset via HTTP (`amiga-search`, `chatty`, `web-frontend`, etc.).
4. Reads each kind back from `/v1/resources/{Kind}`.

```bash
./examples/walkthrough.sh
```

If `ORION_CLUSTER_TOKEN` is set, the script automatically adds an `Authorization: Bearer` header.

---

## Index by aspect

When you're looking for one specific thing, here's the cross-reference:

| Aspect | File(s) |
|---|---|
| Native runtime | `01-services/native-sleeper.yaml`, `02-tasks/native-snapshot.yaml`, `08-canonical/amiga-search.yaml` |
| Docker runtime | `01-services/docker-{nginx,redis}.yaml` |
| Python runtime | `02-tasks/python-train.yaml` |
| HTTP health check | `01-services/docker-nginx.yaml`, `05-placement/combined.yaml` |
| TCP health check | `01-services/docker-redis.yaml` |
| Exec health check | `01-services/native-with-exec-health.yaml` |
| Named ports | `01-services/docker-nginx.yaml`, `08-canonical/amiga-search-full.yaml` |
| Restart policy | `01-services/docker-{nginx,redis}.yaml`, `01-services/native-with-exec-health.yaml` |
| Retry / timeout | `02-tasks/python-train.yaml`, `02-tasks/java-batch.yaml`, `02-tasks/native-snapshot.yaml` |
| GPU requirement | `02-tasks/python-train.yaml`, `05-placement/gpu-required.yaml`, `05-placement/combined.yaml` |
| Node label filter | `02-tasks/java-batch.yaml`, `05-placement/site-label.yaml`, `05-placement/combined.yaml` |
| `prefer:` block | `05-placement/prefer-soft.yaml`, `05-placement/combined.yaml`, `08-canonical/amiga-search-full.yaml` |
| `prefer_data_locality` | `02-tasks/python-train.yaml`, `05-placement/prefer-soft.yaml` |
| Capabilities (advertise) | `04-capabilities/advertise-search.yaml`, `05-placement/combined.yaml`, `08-canonical/amiga-search-full.yaml` |
| Capabilities (require) | `04-capabilities/require-{equals,oneof,op}.yaml` |
| Capability schema | `04-capabilities/declared-schema.yaml` |
| Inline schedule | `03-schedules/inline-template.yaml`, `03-schedules/hourly-health-check.yaml` |
| Schedule-by-reference | `03-schedules/reference.yaml` |
| Multi-location dataset | `06-data/dataset-multi-location.yaml` |
| Model variants | `06-data/model-variants.yaml`, `06-data/model-served-by.yaml` |
| Secret reference | `06-data/secret-plaintext.yaml` |
| Peer registration | `07-peers/{orionmesh-belmont,kqueue-default,devportal-local}.yaml` |
| Peer delegation | `07-peers/{service-via-kqueue,task-via-peer-mesh}.yaml` |
| NATS IPC | `09-ipc/{demo-pub,demo-sub}.yaml` |
