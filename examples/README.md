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

## Executing READMEs inline (`run-md.py`)

Every example README's bash blocks can be **executed in order** by [`scripts/run-md.py`](../scripts/run-md.py):

```bash {skip}
# Print all blocks without running
scripts/run-md.py examples/09-ipc/README.md --list

# Dry-run the composite script
scripts/run-md.py examples/09-ipc/README.md --dry-run

# Run end-to-end (each block shares one bash session, so env carries over)
scripts/run-md.py examples/09-ipc/README.md

# Run a single named block
scripts/run-md.py examples/09-ipc/README.md --only run

# Step through interactively, pausing before each block
scripts/run-md.py examples/09-ipc/README.md --interactive
```

Block info-strings opt into behaviour:

| Tag | Meaning |
|---|---|
| (no tag) | Runs in order |
| `{name=apply}` | Names the block for `--only` / `--list` |
| `{skip}` | Never runs — for display-only blocks (e.g. expected output) |
| `{allow_fail}` | Don't fail-fast if this block exits non-zero (used for `bad/`) |
| `{teardown}` | Runs at the end, after main blocks finish |
| `{dry}` | Like `{skip}`, but shown in `--dry-run` |

There's also a Claude skill (`run-readme`) that wraps this — say "run examples/09-ipc/README.md" and Claude calls the script.

## Run the walkthrough (legacy bash script)

```bash {skip}
# 1. Start NATS — either docker or `brew install nats-server`
# Option A: docker
docker run -d --rm --name orion-nats -p 4222:4222 nats:2.10 -js
# Option B: native
nats-server -js -m 8222 &

ORION_AUTH_DISABLED=1 ORION_STORE_PATH=sqlite::memory: \
  cargo run -p orion-controller -- --bind 127.0.0.1:7878 &
ORION_AUTH_DISABLED=1 \
  cargo run -p orion-agent -- --node-id demo &
ORION_AUTH_DISABLED=1 \
  cargo run -p orion-ui -- --bind 127.0.0.1:7879 &

# 2. Walk through every example
./examples/walkthrough.sh
```

Open the UI at <http://127.0.0.1:7879> while it runs.

## Validate without applying

`orion validate` only does local parse + semantic checks. Useful in CI or pre-commit hooks.

```bash {name=build-cli}
cargo build -p orion-cli
```

```bash {name=validate-good}
./target/debug/orion validate examples/01-services/native-sleeper.yaml
# → ok: kind=Service name=sleeper
```

The validator catches what serde can't — try one of the `bad/*.yaml` files:

```bash {name=validate-bad, allow_fail}
./target/debug/orion validate examples/bad/schedule-both.yaml
# Expected:
#   Error: validating resource
#   Caused by: schedule must set exactly one of `task` or `taskTemplate`
```

## Apply + Dispatch + watch logs

The four moves that work end-to-end today:

```bash {name=apply-dispatch-tail}
CTRL=${ORION_CONTROLLER_URL:-http://127.0.0.1:7878}
F=examples/01-services/native-sleeper.yaml

./target/debug/orion validate $F                                       # parse-only
curl -sS -X POST --data-binary @$F $CTRL/v1/resources/apply ; echo     # store
curl -sS -X POST $CTRL/v1/dispatch/Service/sleeper ; echo              # publish ControlRun
sleep 1
curl -s $CTRL/v1/logs/Service/sleeper | python3 -m json.tool | head -8 # tail
```

For the chatty / cron / IPC recipes (which actually print stuff in their logs), see [`docs/examples.md`](../docs/examples.md) — every example has a copy-pasteable command-line walkthrough there.

## Tear down

```bash {teardown}
CTRL=${ORION_CONTROLLER_URL:-http://127.0.0.1:7878}
curl -sS -X DELETE $CTRL/v1/resources/Service/sleeper > /dev/null 2>&1 || true
pkill -f 'native-sleeper' 2>/dev/null || true
echo "tutorial Service torn down"
```

## Auth on / auth off

The examples don't change between modes — only your `curl` invocation does. When the controller runs without `ORION_AUTH_DISABLED=1`:

```bash {skip}
TOKEN=$(cat ~/.config/orion/cluster.token)
curl -H "Authorization: Bearer $TOKEN" \
     -X POST --data-binary @examples/01-services/native-sleeper.yaml \
     http://127.0.0.1:7878/v1/resources/apply
```

See [`docs/usage.md`](../docs/usage.md) for the full HTTP API.
