# Skills

A bundle of Claude Code [skills](https://github.com/anthropics/claude-code) that let you (and Claude on your behalf) drive OrionMesh from natural-language prompts. Each skill is one `SKILL.md` + a self-contained Python script using only Python's stdlib — no `pip install`, no extra dependencies, no hidden state.

If you're new to skills: a "skill" is a small markdown file in `.claude/skills/<name>/` that tells Claude *when to use it* (via the `description:` field) and *how to use it* (the body). Claude reads them automatically when working in a directory that contains them, and triggers the matching one when your prompt mentions the trigger phrases.

For the wider docs see [usage.md](usage.md), [examples.md](examples.md), and [architecture.md](architecture.md).

---

## What ships

Eight skills live under [`.claude/skills/`](../.claude/skills/) in this repo:

| Skill | What it does | Most-natural prompt |
|---|---|---|
| [`orion-status`](#orion-status) | Compact cluster dashboard: nodes, resource counts, schedule observations | "what's running?", "show cluster status" |
| [`orion-run-task`](#orion-run-task) | Apply a Task YAML, dispatch, watch until idle | "run this task", "execute `train.yaml`", "kick off the rollup" |
| [`orion-run-service`](#orion-run-service) | Apply a Service, dispatch, watch the first N seconds | "start this service", "bring up nginx" |
| [`orion-schedule`](#orion-schedule) | Create / list Schedule resources with cron | "schedule this every hour", "fire X at 2 AM" |
| [`orion-logs`](#orion-logs) | Tail or follow logs of a running workload | "logs for X", "tail -f Y" |
| [`orion-manage`](#orion-manage) | List, get, delete resources | "show services", "delete the demo stuff" |
| [`orion-ipc-demo`](#orion-ipc-demo) | End-to-end NATS pub/sub demo with side-by-side logs | "demo IPC", "prove services can talk" |
| [`validate-resource`](#validate-resource) | Run `orion validate` on a YAML to surface parse + semantic errors | "validate this", "lint X.yaml" |

Every Python script:

- Uses Python's stdlib only (works on Python ≥ 3.6).
- Reads `$ORION_CONTROLLER_URL` (defaults to `http://127.0.0.1:7878`).
- Sends `Authorization: Bearer $ORION_CLUSTER_TOKEN` if the env var is set, otherwise unauthed (fine for `ORION_AUTH_DISABLED=1` dev mode).
- Returns useful exit codes: `0` success, `1` argument problem, `2` controller unreachable, `3+` operation-specific.
- Has `--help` and a `--json` (or equivalent) mode for piping into `jq`.

---

## Install

### Per-repo (built-in here)

Nothing to do — they're checked into `.claude/skills/` in this repo. Open Claude Code with the repo as your working directory and the skills become available automatically.

### Globally — use from anywhere

Symlink them into your user-level skills directory:

```bash
mkdir -p ~/.claude/skills
for s in .claude/skills/orion-*; do
  ln -snf "$(pwd)/$s" "$HOME/.claude/skills/$(basename "$s")"
done
```

Symlinks (rather than copies) mean updates in this repo propagate without a re-install.

Or copy them if you want a frozen snapshot:

```bash
cp -R .claude/skills/orion-* ~/.claude/skills/
```

### Verify install

```bash
ls ~/.claude/skills/        # should list orion-status, orion-run-task, …
```

Open a new Claude Code session in any directory. The skills now appear in the available-skills list and you can drive them by typing prompts like "run this task" or invoking them explicitly: `/orion-status`.

---

## Required env

```bash
# In dev (auth off on the controller):
export ORION_CONTROLLER_URL=http://127.0.0.1:7878

# In prod (auth on):
export ORION_CONTROLLER_URL=https://controller.example.com:7878
export ORION_CLUSTER_TOKEN=$(cat ~/.config/orion/cluster.token)
```

Both are read by every Python script. Setting them in your shell rc gets you out of the constant exporting.

---

## Skill catalogue

### orion-status

Compact at-a-glance dashboard. Two read-only calls (`/v1/nodes` + `/v1/schedules/observed`) plus a count per resource kind.

```bash
python3 .claude/skills/orion-status/scripts/status.py
```

```
Controller http://127.0.0.1:7878

Nodes (1)
  demo-mac                 v0.1.0    arm64/macos · 16 cores · 128.0 GB · [native]
    last seen 2026-06-26T15:22:23.561490+00:00

Resources
  Schedule       1
  Service        2
  Task           1

Schedules (1)
  every-min                fired 487×  last=2026-06-26T15:22:04Z  next=2026-06-26T15:23:00Z
```

Pipeable JSON: `python3 .../status.py --json | jq '.nodes'`.

---

### orion-run-task

Apply + Dispatch + tail-until-quiet for a Task.

```bash
# From a file
python3 .claude/skills/orion-run-task/scripts/run_task.py path/to/task.yaml

# From stdin
cat task.yaml | python3 .claude/skills/orion-run-task/scripts/run_task.py -

# On-the-fly one-liner — generate the Task from a shell command
python3 .claude/skills/orion-run-task/scripts/run_task.py - \
  --exec "for i in 1 2 3 4 5; do echo line-\$i; sleep 1; done"
```

Sample output:

```
applied   Task/adhoc-1782487343 generation=1
dispatched on node=demo-mac instance=cea9dd03…
[15:22:23] line-1
[15:22:24] line-2
[15:22:25] line-3
[15:22:26] line-4
[15:22:27] line-5
done — 5 line(s), last at 15:22:27
```

Tails until the log goes idle for 3 seconds or `--timeout` (default 90) hits. `--quiet` strips the framing for piping.

---

### orion-run-service

Same flow, but the workload is expected to *keep running* — the script only watches for `--watch` seconds (default 10), then returns.

```bash
python3 .claude/skills/orion-run-service/scripts/run_service.py examples/01-services/native-sleeper.yaml

# On-the-fly
python3 .claude/skills/orion-run-service/scripts/run_service.py - \
  --exec "while true; do echo alive; sleep 2; done" --watch 6
```

The output ends with the curl commands to keep watching and to delete the service:

```
done watching — Service is still running.
  follow the rest with:
    curl http://127.0.0.1:7878/v1/logs/Service/adhoc-svc-1782487376
  stop it:
    curl -X DELETE http://127.0.0.1:7878/v1/resources/Service/adhoc-svc-1782487376
```

---

### orion-schedule

Two modes:

```bash
# Mode 1 — build a Schedule from cron + Task ref
python3 .claude/skills/orion-schedule/scripts/schedule.py \
  --cron "0 2 * * *" --task nightly-rollup --name backup-nightly

# Mode 2 — apply a Schedule YAML you authored
python3 .claude/skills/orion-schedule/scripts/schedule.py path/to/schedule.yaml
```

List existing schedules with their observed state:

```bash
python3 .claude/skills/orion-schedule/scripts/schedule.py --list
```

```
SCHEDULE             CRON               TASK                  FIRED  LAST                       NEXT
every-min            * * * * *          cron-job                487  2026-06-26T15:22:04Z       2026-06-26T15:23:00Z
skill-test-schedule  */5 * * * *        cron-job                  0  never                      2026-06-26T15:25:00Z
```

The cron is 5-field POSIX (`min hour day month weekday`) — the controller auto-promotes to the 6-field form internally.

---

### orion-logs

```bash
# Last 50 lines (default)
python3 .claude/skills/orion-logs/scripts/logs.py Service/demo-pub

# Live follow until Ctrl-C
python3 .claude/skills/orion-logs/scripts/logs.py -f Task/cron-job

# Last 200 lines, JSON (for piping)
python3 .claude/skills/orion-logs/scripts/logs.py --tail 200 --json Service/web-frontend | jq '.entries'
```

ANSI colour on for TTY output (stderr lines highlighted), off when piped. Override either direction with `--no-color`.

---

### orion-manage

Read-and-delete operations:

```bash
# List one kind
python3 .claude/skills/orion-manage/scripts/manage.py list Service

# List every populated kind at once
python3 .claude/skills/orion-manage/scripts/manage.py list --all

# Full JSON of one resource
python3 .claude/skills/orion-manage/scripts/manage.py get Service/amiga-search

# Delete one
python3 .claude/skills/orion-manage/scripts/manage.py delete Service/demo-pub          # prompts y/N
python3 .claude/skills/orion-manage/scripts/manage.py delete Service/demo-pub --yes    # skip prompt

# Bulk delete by name prefix — good for cleaning up demo resources
python3 .claude/skills/orion-manage/scripts/manage.py delete --prefix demo- Service --yes
```

Returns `3` on a missing target, `130` if you say no at the prompt, `2` if the controller's down.

---

### orion-ipc-demo

End-to-end NATS pub/sub demo. Useful both as a one-stop wow-demo and as a sanity check after touching the dispatch / log forwarder / bus code.

```bash
# Build the demo binaries (one-time)
cargo build --release -p orion-demo-bins

# Run the demo
python3 .claude/skills/orion-ipc-demo/scripts/ipc_demo.py
```

The script:

1. applies `examples/09-ipc/demo-sub.yaml`
2. applies `examples/09-ipc/demo-pub.yaml`
3. dispatches the subscriber first (so it's listening when the publisher fires)
4. dispatches the publisher
5. polls both `/v1/logs/Service/{demo-pub,demo-sub}` for `--duration` seconds (default 8) and prints two side-by-side log windows

```
sub dispatched on demo-mac; pub dispatched on demo-mac
polling for 8s …

=== publisher stdout                          | === subscriber stdout
--------------------------------------------- + ---------------------------------------------
15:22:01 [demo-pub:P] sent: tick 1 from P     | 15:22:01 [demo-sub:S] connecting to nats://1
15:22:02 [demo-pub:P] sent: tick 2 from P     | 15:22:01 [demo-sub:S] subscribed
15:22:03 [demo-pub:P] sent: tick 3 from P     | 15:22:01 [demo-sub:S] recv: tick 1 from P at
…
done — pub emitted 8 lines, sub received 11 lines.
Services left running. Tear down with:
  curl -X DELETE $CTRL/v1/resources/Service/demo-pub
  curl -X DELETE $CTRL/v1/resources/Service/demo-sub
  pkill -f 'orion-demo-'
```

Pass `--cleanup` to DELETE both resources at the end (running processes outlive the resource until the agent restarts — see `docs/usage.md §7.3`).

---

### validate-resource

Original skill that ships with the repo. Runs `orion validate <file.yaml>` and surfaces the parse or semantic error verbatim.

```bash
./target/debug/orion validate examples/08-canonical/amiga-search.yaml
# → ok: kind=Service name=amiga-search

./target/debug/orion validate examples/bad/schedule-both.yaml
# → Error: validating resource
#   Caused by: schedule must set exactly one of `task` or `taskTemplate`
```

The full SKILL.md lives at [.claude/skills/validate-resource/SKILL.md](../.claude/skills/validate-resource/SKILL.md).

---

## Worked recipes

### Run a one-off backup at 2 AM nightly

```bash
# 1. Define the Task
python3 .claude/skills/orion-run-task/scripts/run_task.py - --exec "pg_dump -Fc orion > /var/backups/orion-\$(date +%F).dump" --no-dispatch
# applied Task/adhoc-1782487343 generation=1

# 2. Schedule it
python3 .claude/skills/orion-schedule/scripts/schedule.py \
  --cron "0 2 * * *" --task adhoc-1782487343 --name nightly-backup

# 3. Verify
python3 .claude/skills/orion-schedule/scripts/schedule.py --list
```

When the controller hits 02:00, the Task fires; check `python3 …/orion-logs/scripts/logs.py Task/adhoc-…` the next morning.

### Smoke-test the cluster after a code change

```bash
# Start fresh
python3 .claude/skills/orion-manage/scripts/manage.py delete --prefix demo- Service --yes
python3 .claude/skills/orion-manage/scripts/manage.py delete --prefix demo- Task    --yes

# IPC demo (also exercises dispatch + log forwarder + NATS bus)
python3 .claude/skills/orion-ipc-demo/scripts/ipc_demo.py --duration 6 --cleanup

# A one-off Task
python3 .claude/skills/orion-run-task/scripts/run_task.py - --exec "echo green-light"

# Status
python3 .claude/skills/orion-status/scripts/status.py
```

If all four steps return `0` and the IPC demo reports ≥ 2 messages on both sides, the dispatch path is healthy.

### Bring up a stack of services

```bash
for f in examples/01-services/*.yaml; do
  python3 .claude/skills/orion-run-service/scripts/run_service.py "$f" --watch 3 || true
done
python3 .claude/skills/orion-status/scripts/status.py
```

`|| true` keeps the loop going if any one Service has an unsupported runtime (Docker today, etc.).

### Dump the controller's view

```bash
python3 .claude/skills/orion-status/scripts/status.py --json \
  | jq '{nodes: .nodes | length, services: .counts.Service, tasks: .counts.Task, fired: [.schedules[].fire_count] | add}'
```

---

## Letting Claude drive

Once installed, Claude reads the `description:` lines and triggers on natural prompts:

| You say | Claude tends to use |
|---|---|
| "what's running?" / "show me orion status" | `orion-status` |
| "run this task" / "execute X.yaml" / "kick off the rollup" | `orion-run-task` |
| "start this service" / "bring up the queue worker" | `orion-run-service` |
| "schedule X every hour" / "fire Y at 2 AM" | `orion-schedule` |
| "tail Y" / "show me Z's logs" / "follow demo-pub" | `orion-logs` |
| "list services" / "delete the demo stuff" / "what schedules?" | `orion-manage` |
| "demo IPC" / "prove services can talk" / "end-to-end demo" | `orion-ipc-demo` |
| "validate X.yaml" / "lint this resource" | `validate-resource` |

You can also invoke explicitly with `/skill-name` if you want to skip the matching.

---

## Authoring more skills

Pattern that every skill in this bundle follows — copy as a template:

```
.claude/skills/orion-NEWNAME/
├── SKILL.md
└── scripts/
    └── newname.py
```

`SKILL.md` frontmatter:

```yaml
---
name: orion-newname
description: >
  One-sentence summary that includes trigger phrases the user might say.
  Use when the user says "X", "Y", "Z", or similar.
---
```

Followed by sections: `## How to use`, `## When to use`, `## When NOT to use`, `## Exit codes`.

For the Python script, the boilerplate is exactly what the bundled scripts use — copy `status.py` or `logs.py` as a starting point. The req() helper is ~25 lines and handles env, bearer auth, and JSON decoding.

Test by:

1. Running the script standalone with `--help`.
2. Opening a fresh Claude Code session in the repo; the skill should appear in `Available skills:`.
3. Asking Claude something that matches your `description:` trigger phrases.
