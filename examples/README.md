# OrionMesh examples

Focused YAMLs grouped by aspect. Each subdirectory has a short README explaining what it demonstrates.

> **Phase 1 reality check** — today every example **parses cleanly via `orion validate`** and **applies through `POST /v1/resources/apply` into the SQLite store**. They round-trip out through `GET /v1/resources/<Kind>`. The scheduler dispatch + non-native runtime adapters (Phase 5) are what makes them *run* on a node; not shipped yet. The walkthrough below exercises everything that's wired today.

## Layout

```
examples/
├── 01-services/      # Service kind: native + docker, health, restart policy, named ports
├── 02-tasks/         # Task kind: retry + timeout, dataset locality
├── 03-schedules/     # Schedule: task reference + inline template
├── 04-capabilities/  # Capability advertise + 3 selector forms + declared schema
├── 05-placement/     # arch, GPU requirement, site labels, prefer block
├── 06-data/          # Dataset locations, Model variants
├── 07-peers/         # Runtime catalog entries + delegating to a peer
├── 08-canonical/     # The plan's amiga-search example
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

## Auth on / auth off

The examples don't change between modes — only your `curl` invocation does. When the controller runs without `ORION_AUTH_DISABLED=1`:

```bash
TOKEN=$(cat ~/.config/orion/cluster.token)
curl -H "Authorization: Bearer $TOKEN" \
     -X POST --data-binary @examples/01-services/native-sleeper.yaml \
     http://127.0.0.1:7878/v1/resources/apply
```

See `docs/usage.md` for the full HTTP API.
