# 03 — Schedules

A **Schedule** fires a Task on a cron expression. The controller has a tick loop running every 5 seconds; when a Schedule's next fire time has passed, the controller dispatches the referenced (or inline) Task.

> **Runnable.** `scripts/run-md.py examples/03-schedules/README.md` walks every recipe in this README end-to-end (with a `{teardown}` step at the end). See [`../docs/runner.md`](../docs/runner.md) for the tag conventions (`{name=X}`, `{skip}`, `{allow_fail}`, `{teardown}`) and the drive flags (`--list`, `--only X`, `--dry-run`, `--interactive`).

## Concept

```mermaid
sequenceDiagram
    autonumber
    participant TICK as controller scheduler_tick_loop (every 5s)
    participant S as Store
    participant SR as ScheduleRegistry (in-mem)
    participant DISP as dispatch_workload
    participant A as Agent

    TICK->>S: list_by_kind("Schedule")
    S-->>TICK: schedules
    loop each Schedule
        TICK->>TICK: cron.parse (5-field → 6-field internally)
        TICK->>SR: read last_fired_at + armed_at
        TICK->>TICK: next = cron.after(after).next()
        alt next <= now
            TICK->>S: resolve task (lookup name or use task_template)
            S-->>TICK: TaskSpec.runtime
            TICK->>DISP: dispatch_workload(Task, name, runtime)
            DISP->>A: ControlRun via orion.control.{node}.run
            TICK->>SR: last_fired_at=now; fire_count++; next_fire_at = cron.after(now).next()
        else not yet
            TICK->>SR: next_fire_at = next
        end
    end
```

The controller exposes the per-Schedule observed state at `GET /v1/schedules/observed`:

```json
{
  "every-min": {
    "armed_at":        "2026-06-26T15:48:00Z",
    "last_fired_at":   "2026-06-26T15:49:00Z",
    "last_instance_id":"e1cdbed2-…",
    "next_fire_at":    "2026-06-26T15:50:00Z",
    "last_error":      null,
    "fire_count":      1
  }
}
```

## Schedule spec — every field

```yaml
apiVersion: orionmesh.dev/v1
kind: Schedule
metadata: { name: backup-nightly }
spec:
  cron: "0 2 * * *"             # 5-field POSIX cron (auto-promoted to 6-field
                                # internally); seconds are always 0 unless you
                                # write a 6-field expression yourself.

  # EXACTLY ONE of `task:` or `task_template:`. Resource::validate() enforces
  # this and rejects on apply if both or neither is set.

  task: nightly-rollup          # name reference: looks up a Task resource

  # OR

  task_template:                # inline TaskSpec — same shape as the Task spec
    runtime:
      kind: native
      exec: /usr/local/bin/snapshot
    placement:
      arch: [x86_64]
      os: [linux]
    timeout_seconds: 600
    retry: { max_attempts: 2 }
```

## Cron syntax

5-field POSIX: `minute hour day-of-month month day-of-week`. The runner promotes to NATS's cron parser's 6-field form by prepending a `0` (seconds). 6-field input is accepted as-is.

| Cron | Fires |
|---|---|
| `* * * * *` | every minute |
| `*/5 * * * *` | every 5 minutes |
| `0 * * * *` | top of every hour |
| `*/30 9-17 * * 1-5` | every 30 min, 9-5, weekdays |
| `0 2 * * *` | 02:00 daily |
| `0 2 * * 0` | 02:00 every Sunday |
| `0 0 1 * *` | midnight on the 1st of every month |
| `0 0 1 1 *` | midnight Jan 1 (annually) |

Edge: the controller tick is 5s, so the minimum useful cadence is "every minute". Sub-minute crons get coarsened to the nearest 5s tick.

## The three files

| File | What's distinctive |
|---|---|
| [`reference.yaml`](reference.yaml) | `task: <name>` form — references an existing Task |
| [`inline-template.yaml`](inline-template.yaml) | `task_template: { ... }` form — full Task inlined |
| [`hourly-health-check.yaml`](hourly-health-check.yaml) | Simple hourly inline-template |

### Reference form

```yaml
kind: Schedule
metadata: { name: nightly-postgres-snapshot }
spec:
  cron: "0 2 * * *"
  task: postgres-snapshot       # references examples/02-tasks/native-snapshot.yaml
```

Good when the same Task is fired from multiple Schedules, or when you want to dispatch the Task ad-hoc as well as on cron.

### Inline form

```yaml
kind: Schedule
metadata: { name: weekly-rollup }
spec:
  cron: "0 3 * * 0"
  task_template:
    runtime: { kind: java, jar: /opt/orion-rollup/weekly-runner.jar }
    placement: { arch: [x86_64], os: [linux] }
    timeout_seconds: 14400
    retry: { max_attempts: 2, backoff_seconds: 600 }
```

Good when this cadence is the only consumer of this Task spec — keeps related config in one place.

## Recipe — schedule fires on cron

```bash {name=build}
cargo build -p orion-cli
cargo build --release -p orion-controller -p orion-agent
```

Apply a Task + a Schedule that points at it; wait for the next minute mark; verify the Task ran and the observer state shows `fire_count: 1`:

```bash {name=run-cron-job}
CTRL=${ORION_CONTROLLER_URL:-http://127.0.0.1:7878}

# A small native Task that prints when fired
curl -sS -X POST $CTRL/v1/resources/apply --data-binary @- <<'YAML' ; echo
apiVersion: orionmesh.dev/v1
kind: Task
metadata: { name: cron-job }
spec:
  runtime:
    kind: native
    exec: /bin/sh
    args: ["-c", "echo fired-at-$(date +%H:%M:%S)"]
YAML

# A Schedule that fires every minute
curl -sS -X POST $CTRL/v1/resources/apply --data-binary @- <<'YAML' ; echo
apiVersion: orionmesh.dev/v1
kind: Schedule
metadata: { name: every-min }
spec:
  cron: "* * * * *"
  task: cron-job
YAML

# Wait up to 70s for the next minute mark; poll fire_count
for i in 1 2 3 4 5 6 7; do
  sleep 10
  obs=$(curl -s $CTRL/v1/schedules/observed)
  fired=$(echo "$obs" | python3 -c "import sys,json;d=json.load(sys.stdin);print((d.get('every-min') or {}).get('fire_count', 0))")
  echo "  +${i}0s fire_count=$fired"
  if [ "$fired" -gt 0 ]; then break; fi
done

echo "=== final observed state ==="
curl -s $CTRL/v1/schedules/observed | python3 -m json.tool

echo "=== task logs (fired-at-HH:MM:SS) ==="
curl -s $CTRL/v1/logs/Task/cron-job | python3 -c "
import sys, json
d = json.load(sys.stdin)
for e in d['entries']:
    print(f'  [{e[\"at\"][11:19]}] {e[\"line\"]}')"
```

Inline-template variant (no separate Task resource needed):

```bash {name=run-inline}
CTRL=${ORION_CONTROLLER_URL:-http://127.0.0.1:7878}
curl -sS -X POST $CTRL/v1/resources/apply --data-binary @- <<'YAML' ; echo
apiVersion: orionmesh.dev/v1
kind: Schedule
metadata: { name: inline-sched }
spec:
  cron: "* * * * *"
  task_template:
    runtime:
      kind: native
      exec: /bin/sh
      args: ["-c", "echo inline-fired"]
    timeout_seconds: 30
YAML
echo "(inline-sched will fire on the next minute mark)"
```

Validation: both-or-neither is caught at apply time.

```bash {name=schedule-validate-bad, allow_fail}
./target/debug/orion validate examples/bad/schedule-both.yaml
# expected: schedule must set exactly one of `task` or `taskTemplate`
./target/debug/orion validate examples/bad/schedule-neither.yaml
```

## Tear down

```bash {teardown}
CTRL=${ORION_CONTROLLER_URL:-http://127.0.0.1:7878}
for n in every-min inline-sched nightly-postgres-snapshot weekly-rollup hourly-fleet-ping; do
  curl -sS -X DELETE $CTRL/v1/resources/Schedule/$n > /dev/null 2>&1 || true
done
for n in cron-job postgres-snapshot; do
  curl -sS -X DELETE $CTRL/v1/resources/Task/$n > /dev/null 2>&1 || true
done
echo "schedule examples torn down"
```

## See also

- [`docs/architecture.md §6.4`](../../docs/architecture.md#64-scheduler-tick--schedules-firing-on-cron-phase-b--live) — the tick-loop sequence diagram
- [`examples/02-tasks/`](../02-tasks/) — Tasks that Schedules reference
- `.claude/skills/orion-schedule/` — the `orion-schedule` skill for creating Schedules with one command
