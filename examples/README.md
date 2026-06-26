# OrionMesh examples

Focused YAMLs grouped by aspect. Each subdirectory has a short README explaining what it demonstrates. For an annotated walkthrough with per-example CLI recipes, see [`docs/examples.md`](../docs/examples.md).

> **What runs today.** Every YAML parses via `orion validate` and applies via `POST /v1/resources/apply`. Service / Task with `runtime: native` actually **launch on an agent** through `POST /v1/dispatch/{kind}/{name}` and their stdout/stderr stream into `GET /v1/logs/{kind}/{name}`. Schedules **actually fire on cron**. The IPC demo in `09-ipc/` shows two Services talking over the mesh's NATS broker. Docker / Python / Java adapters + the full filter-and-score scheduler land in Phase 5.

## Layout

```
examples/
├── 01-services/      # Service kind: native + docker, health, restart policy, named ports
├── 02-tasks/         # Task kind: retry + timeout, dataset locality
├── 03-schedules/     # Schedule: task reference + inline template
├── 04-capabilities/  # Capability advertise + 3 selector forms + declared schema
├── 05-placement/     # arch, GPU requirement, site labels, prefer block
├── 06-data/          # Dataset locations, Model variants, Volumes, Secrets
├── 07-peers/         # Runtime catalog entries + delegating to a peer
├── 08-canonical/     # The plan's amiga-search example
├── 09-ipc/           # Two Services talking over NATS (Phase C demo)
├── bad/              # YAMLs that intentionally fail validation
└── walkthrough.sh    # Apply a curated set against a local controller and read it back
```

## Run the walkthrough

```bash
# 1. Start the stack (see docs/installation.md §6 if it's not already up)
docker run -d --rm --name orion-nats -p 4222:4222 nats:2.10 -js

ORION_AUTH_DISABLED=1 ORION_STORE_PATH=sqlite::memory: \
  cargo run -p orion-controller -- --bind 127.0.0.1:7878 &

ORION_AUTH_DISABLED=1 \
  cargo run -p orion-agent -- --node-id demo &

ORION_AUTH_DISABLED=1 \
  cargo run -p orion-ui -- --bind 127.0.0.1:7879 &

# 2. Walk through the examples
./examples/walkthrough.sh
```

Open the UI at <http://127.0.0.1:7879> while it runs — the node table refreshes every 3 seconds.

## Validate without applying

`orion validate` only does local parse + semantic checks. Useful in CI or pre-commit hooks.

```bash
cargo build -p orion-cli
./target/debug/orion validate examples/01-services/native-sleeper.yaml
# → ok: kind=Service name=sleeper
```

The validator catches what serde can't — try one of the `bad/*.yaml` files:

```bash
./target/debug/orion validate examples/bad/schedule-both.yaml
# → Error: validating resource
#   Caused by: schedule must set exactly one of `task` or `taskTemplate`
```

## Apply + Dispatch + watch logs

The four moves that work end-to-end today:

```bash
CTRL=http://127.0.0.1:7878
F=examples/01-services/native-sleeper.yaml

orion validate $F                                        # parse-only
curl -X POST --data-binary @$F $CTRL/v1/resources/apply  # store
curl -X POST $CTRL/v1/dispatch/Service/sleeper           # publish ControlRun
curl $CTRL/v1/logs/Service/sleeper                        # tail
```

For the chatty / cron / IPC recipes (which actually print stuff in their logs), see [`docs/examples.md`](../docs/examples.md) — every example has a copy-pasteable command-line walkthrough there.

## Auth on / auth off

The examples don't change between modes — only your `curl` invocation does. When the controller runs without `ORION_AUTH_DISABLED=1`:

```bash
TOKEN=$(cat ~/.config/orion/cluster.token)
curl -H "Authorization: Bearer $TOKEN" \
     -X POST --data-binary @examples/01-services/native-sleeper.yaml \
     http://127.0.0.1:7878/v1/resources/apply
```

See [`docs/usage.md`](../docs/usage.md) for the full HTTP API.
