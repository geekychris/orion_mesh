# 06 — Datasets, Models, Volumes, Secrets

These resources describe **data and credentials that workloads consume**, not workloads themselves. The scheduler uses Datasets to drive data-locality scoring; Models drive runtime + memory matching; Volumes describe shared storage; Secrets are resolver URIs that the agent dereferences at workload start.

> **Runnable.** `scripts/run-md.py examples/06-data/README.md` walks the recipes end-to-end. Tags: `{name=X}`, `{skip}`, `{allow_fail}`, `{teardown}`.

## Datasets

### Concept

```mermaid
flowchart LR
    Dataset[Dataset resource<br/>locations + formats + capabilities]
    Workload[Task / Service<br/>requires: { dataset: name } +<br/>prefer_data_locality: true]
    Scheduler[Phase-5 scheduler]
    Nodes[Nodes that hold the dataset]
    Workload --> Scheduler
    Dataset --> Scheduler
    Scheduler -. scores higher .-> Nodes
```

### Dataset spec

```yaml
apiVersion: orionmesh.dev/v1
kind: Dataset
metadata: { name: amiga-schematics }
spec:
  description: "Repair-shop scans of Amiga 500 / 1200 / CD32 schematics"

  locations:
    - node: pi5                # NodeId — matches the agent's --node-id
      path: /data/amiga
      access: ro               # ro (read-only) | rw (read-write) | wo (write-only)
    - node: mac-studio
      path: /Volumes/data/amiga
      access: rw

  formats: [pdf, png, gerber]  # free-form list — used by Find API
  capabilities: [search, view] # capability NAMES this dataset enables
  size_bytes: 12345678901
```

| `access` | Meaning |
|---|---|
| `ro` | Read-only — safe to mount on many workloads simultaneously |
| `rw` | Read-write — exclusive in practice; one writer at a time |
| `wo` | Write-only — used for sinks/dumps |

### Files

| File | Demonstrates |
|---|---|
| [`dataset-multi-location.yaml`](dataset-multi-location.yaml) | Same dataset replicated across 3 nodes with mixed access modes |
| [`dataset-readonly.yaml`](dataset-readonly.yaml) | Single-location, read-only |

## Models

```yaml
apiVersion: orionmesh.dev/v1
kind: Model
metadata: { name: qwen-coder }
spec:
  model_id: qwen2.5-coder-32b
  description: "Qwen 2.5 Coder, 32B, three quantizations"

  variants:
    - format: gguf                # gguf | safetensors | onnx | mlx | torch
      quant:  q4_k_m              # q4_k_m | q8_0 | int8 | fp16 | …
      memory_gb: 22.0             # approx peak memory to serve this variant
      context_window: 32768
      preferred_runtime: "llama.cpp"
    - format: mlx
      quant:  int8
      memory_gb: 36.0
      context_window: 32768
      preferred_runtime: mlx

  served_by: [mac-studio, gpu-rig]  # nodes that have this model on disk
```

The scheduler picks a variant whose `memory_gb` fits on the chosen node. `preferred_runtime` is a hint to the agent (`llama.cpp` vs `mlx` vs `vllm` etc.).

| File | Demonstrates |
|---|---|
| [`model-variants.yaml`](model-variants.yaml) | Three variants (gguf q4, gguf q8, mlx int8) |
| [`model-served-by.yaml`](model-served-by.yaml) | Single-variant Pi-friendly model |

## Volumes

```yaml
apiVersion: orionmesh.dev/v1
kind: Volume
metadata: { name: shared-scratch }
spec:
  path: /mnt/scratch                   # absolute path on the node
  mounted_on: [pi5, mac-studio, gpu-rig]
  size_gb: 500
```

## Secrets

A Secret is a **reference**, not a value. The agent resolves the URI at workload startup via the `SecretResolver` trait in `orion-runtime`.

```yaml
apiVersion: orionmesh.dev/v1
kind: Secret
metadata: { name: openai-api-key }
spec:
  vault_ref: "plaintext://openai-api-key"
```

Supported schemes:

| URI | Resolver | Notes |
|---|---|---|
| `plaintext://<name>` | `PlaintextResolver` | Reads `~/.config/orion/secrets/<name>` (override via `$ORION_SECRETS_DIR`). Path traversal blocked. **Clear-text on disk — MVP only.** |
| `vaultrix://<key>` | not yet implemented | SecureVault when Phase 5 lands |
| `op://...` | not yet implemented | 1Password CLI shim |
| `age://...` | not yet implemented | age-encrypted local file |

Trait at `crates/orion-runtime/src/secrets.rs` — adding a resolver is additive.

## Recipe

```bash {name=build}
cargo build -p orion-cli
cargo build --release -p orion-controller -p orion-agent
```

```bash {name=validate-all}
for f in examples/06-data/*.yaml; do
  ./target/debug/orion validate "$f"
done
```

```bash {name=apply-all}
CTRL=${ORION_CONTROLLER_URL:-http://127.0.0.1:7878}
for f in examples/06-data/*.yaml; do
  curl -sS -X POST --data-binary @"$f" $CTRL/v1/resources/apply ; echo
done
```

Pretty-print the catalog:

```bash {name=show-catalog}
CTRL=${ORION_CONTROLLER_URL:-http://127.0.0.1:7878}
echo "=== Datasets ==="
curl -s $CTRL/v1/resources/Dataset | python3 -c "
import sys, json
for r in json.load(sys.stdin):
    s = r['spec']
    print(f\"  {r['metadata']['name']}\")
    print(f\"    formats: {s.get('formats') or []}\")
    print(f\"    locations:\")
    for loc in s.get('locations') or []:
        print(f\"      {loc['node']}  {loc['path']}  ({loc.get('access','ro')})\")"

echo
echo "=== Models ==="
curl -s $CTRL/v1/resources/Model | python3 -c "
import sys, json
for r in json.load(sys.stdin):
    s = r['spec']
    print(f\"  {r['metadata']['name']}  ({s.get('model_id')})\")
    for v in s.get('variants') or []:
        print(f\"    {v.get('format'):8} {v.get('quant'):8} {v.get('memory_gb','?'):>6} GB  ctx={v.get('context_window')}\")"

echo
echo "=== Volumes ==="
curl -s $CTRL/v1/resources/Volume | python3 -c "
import sys, json
for r in json.load(sys.stdin):
    s = r['spec']
    print(f\"  {r['metadata']['name']}  {s.get('path')}  on={s.get('mounted_on')}  size={s.get('size_gb','?')}GB\")"

echo
echo "=== Secrets ==="
curl -s $CTRL/v1/resources/Secret | python3 -c "
import sys, json
for r in json.load(sys.stdin):
    print(f\"  {r['metadata']['name']}  {r['spec']['vault_ref']}\")"
```

Set up a Plaintext secret you can actually resolve:

```bash {name=plaintext-secret}
mkdir -p ~/.config/orion/secrets
echo "hunter2-demo" > ~/.config/orion/secrets/openai-api-key
chmod 600 ~/.config/orion/secrets/openai-api-key
ls -la ~/.config/orion/secrets/openai-api-key
# (The agent's PlaintextResolver reads this when a workload references
# Secret/openai-api-key. Phase 5 wires env vars through the resolver.)
```

Simulate data-locality scoring against your live fleet:

```bash {name=simulate-locality}
python3 <<'PY'
import json
import urllib.request as u
CTRL = "http://127.0.0.1:7878"

with u.urlopen(f"{CTRL}/v1/resources/Dataset") as r:
    datasets = {d["metadata"]["name"]: d["spec"] for d in json.load(r)}
with u.urlopen(f"{CTRL}/v1/nodes") as r:
    live = [n["node_id"] for n in json.load(r)]

NEEDS = "amiga-schematics"
ds = datasets.get(NEEDS)
if not ds:
    print(f"Dataset {NEEDS!r} not registered; apply it first")
else:
    held_by = {loc["node"]: loc for loc in ds.get("locations", [])}
    print(f"=== nodes scored for a workload that requires {NEEDS} ===")
    for n in live:
        if n in held_by:
            loc = held_by[n]
            print(f"  {n:14} HOLDS at {loc['path']} ({loc.get('access','ro')}) — +score")
        else:
            print(f"  {n:14} doesn't hold {NEEDS} — would need network read")
    cold = sorted(set(held_by) - set(live))
    print(f"\nholds {NEEDS} but currently offline: {cold or '(none)'}")
PY
```

## Tear down

```bash {teardown}
CTRL=${ORION_CONTROLLER_URL:-http://127.0.0.1:7878}
for k in Dataset Model Volume Secret; do
  for n in $(curl -s $CTRL/v1/resources/$k | python3 -c "import sys,json;[print(r['metadata']['name']) for r in json.load(sys.stdin)]" 2>/dev/null); do
    curl -sS -X DELETE $CTRL/v1/resources/$k/$n > /dev/null 2>&1 || true
  done
done
echo "data examples torn down"
```

## See also

- [`crates/orion-runtime/src/secrets.rs`](../../crates/orion-runtime/src/secrets.rs) — `SecretResolver` trait + `PlaintextResolver`
- [`examples/02-tasks/python-train.yaml`](../02-tasks/python-train.yaml) — uses `prefer_data_locality: true` against a Dataset
- [`docs/usage.md §3`](../../docs/usage.md#3-authoring-resources)
