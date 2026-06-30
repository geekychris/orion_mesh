# Runtime model

OrionMesh is **native-first**. The agent launches workloads as ordinary
OS processes (`tokio::process::Command`) — no containers, no chroots, no
hypervisors. This is intentional and is the project's core differentiator
vs container-orchestrated platforms.

## What this means in practice

- **Your Python processor runs `python /path/to/processor.py` in the host
  shell**, with the host's venv on PATH. No Dockerfile, no image build.
- **Your Java service runs `java -jar processor.jar`** against the host
  JDK. Heap, native libs, perf flags — all set via plain JVM args.
- **`orion-controller`, `orion-agent`, and `orion-ui` are native
  binaries** installed to `~/.orion/bin/`. The entire control plane is
  three small Rust processes.
- **The only Docker thing in the dev stack is NATS itself**, and only
  because there's no Rust `nats-server` crate. `orion up` defaults to
  trying `nats-server` on PATH first and only falls back to Docker if
  it's not installed. Force one or the other with `--nats native` /
  `--nats docker`.

## Why native-first?

The pitch in the README is: *the right workload, on the right machine,
with the right data, using the right runtime*. Heterogeneous personal
clusters (Apple Silicon Macs, Raspberry Pis, Linux x86) often have
hand-tuned local toolchains — your Mac has CUDA-equivalent MPS, your
Pi has gpio access, your Linux box has a particular CUDA version
pinned. Forcing every workload through a container layer loses those
local affinities, multiplies dev friction, and burns disk for image
caches. The Docker adapter exists in the spec for cases where it
*actually* helps (sandboxing untrusted code, distributing a closed-source
service), not as the default execution model.

## What's actually implemented

| Runtime variant | Adapter |
|---|---|
| `kind: native`        | ✅ [`crates/orion-runtime/src/native.rs`](../crates/orion-runtime/src/native.rs) |
| `kind: docker`        | ❌ Phase 5+ |
| `kind: python`        | ❌ Phase 5+ (use `kind: native exec: python args: […]` for now) |
| `kind: java`          | ❌ Phase 5+ (use `kind: native exec: java args: ['-jar', …]`) |
| `kind: node`          | ❌ |
| `kind: spark`         | ❌ |
| `kind: llm`           | ❌ |
| `kind: homeassistant` | ❌ |
| `kind: wasm`          | ❌ |
| `kind: peer`          | ❌ |

If you apply a Service with an unimplemented `kind` today, the agent
fails with a pointer to this page.

## Examples

| File | Runtime | Status |
|---|---|---|
| [`examples/01-services/native-sleeper.yaml`](../examples/01-services/native-sleeper.yaml) | `native` | Runs |
| [`examples/01-services/native-with-exec-health.yaml`](../examples/01-services/native-with-exec-health.yaml) | `native` | Runs |
| [`examples/01-services/docker-nginx.yaml`](../examples/01-services/docker-nginx.yaml) | `docker` | Spec-only — illustrates the YAML shape; agent will refuse to launch it |
| [`examples/01-services/docker-redis.yaml`](../examples/01-services/docker-redis.yaml) | `docker` | Spec-only — same |
| [`examples/02-tasks/python-train.yaml`](../examples/02-tasks/python-train.yaml) | `python` | Spec-only — same |
| [`examples/02-tasks/java-batch.yaml`](../examples/02-tasks/java-batch.yaml) | `java` | Spec-only — same |
| [`examples/02-tasks/native-snapshot.yaml`](../examples/02-tasks/native-snapshot.yaml) | `native` | Runs |
| [`examples/09-ipc/polyglot/yaml/*.yaml`](../examples/09-ipc/polyglot/) | `native` (launching `python` / `java`) | Runs |
| [`examples/10-queues/service-yamls/*.yaml`](../examples/10-queues/service-yamls/) | `native` (launching `python` / `java`) | Runs |

**The non-native YAMLs are kept as documentation of the spec shape**, so
you can see what a `kind: docker` resource looks like in YAML and so the
roundtrip tests can verify serialisation. They are not runnable in this
phase — that's what the adapter status table above shows.

## How to run Python / Java today (without Docker)

You wrap the interpreter / JVM as a `kind: native` workload. The
processor scaffolders already do this:

```yaml
runtime:
  kind: native           # the OrionMesh runtime
  exec: python           # the binary the OS will fork
  args: ['/path/to/processor.py']
  env:
    NATS_URL: nats://127.0.0.1:4222
    ORION_QUEUE_NAME: ps-rows
```

Or for Java:

```yaml
runtime:
  kind: native
  exec: java
  args: ['-jar', '/path/to/processor.jar']
```

For Rust workloads launched by `cargo build --release`, the binary path
goes straight into `exec`. There's no penalty for any of these vs Docker
— it's just process spawning.

## When to use Docker (in the future)

Once the Docker adapter lands, the right cases are:

- **Untrusted code**: a third-party processor whose source you don't
  control.
- **Closed-source binaries** that ship as images.
- **Hard-to-install runtimes**: a brittle Conda env you don't want to
  reproduce across nodes.
- **Reproducibility**: scientific workloads that need a pinned image
  hash.

Everything else stays native.
