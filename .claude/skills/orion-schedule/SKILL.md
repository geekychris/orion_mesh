---
name: orion-schedule
description: Create or list OrionMesh Schedule resources — cron-fires a Task. Two modes: build a Schedule from a cron + existing Task reference, or apply a Schedule YAML directly. Use when the user says "schedule this", "run X every hour", "fire `nightly-rollup` at 2 AM", "set up a cron for Y", or "list schedules".
---

# orion-schedule

OrionMesh's `Schedule` kind fires a referenced `Task` (or inline `task_template`) on a cron. The controller ticks every 5 seconds; a Schedule with `cron: "* * * * *"` fires at the next minute mark.

## How to use

### Mode 1 — build a Schedule from a cron + Task name

```bash
python3 .claude/skills/orion-schedule/scripts/schedule.py \
  --cron "0 2 * * *" \
  --task nightly-rollup \
  --name backup-nightly
```

Generates a minimal Schedule YAML, validates it, applies it. Returns the generated name (auto-derived if not given) and the next computed fire time from the controller.

### Mode 2 — apply a Schedule YAML directly

```bash
python3 .claude/skills/orion-schedule/scripts/schedule.py path/to/schedule.yaml
# or stdin
cat schedule.yaml | python3 .claude/skills/orion-schedule/scripts/schedule.py -
```

### Mode 3 — list current schedules with observed state

```bash
python3 .claude/skills/orion-schedule/scripts/schedule.py --list
```

Output:

```
SCHEDULE             CRON              TASK             FIRED   LAST                  NEXT
backup-nightly       0 2 * * *         nightly-rollup   12      2026-06-26T02:00:00Z  2026-06-27T02:00:00Z
hourly-rollup        0 * * * *         rollup-task      288     2026-06-26T13:00:00Z  2026-06-26T14:00:00Z
```

## Cron syntax

5-field POSIX: `minute hour day month weekday`. The controller auto-prepends `0` to make it 6-field for the underlying parser, but the user-facing form is always 5-field.

| Cron | Fires |
|---|---|
| `* * * * *` | every minute |
| `*/5 * * * *` | every 5 minutes |
| `0 * * * *` | top of every hour |
| `0 2 * * *` | 02:00 every day |
| `0 2 * * 0` | 02:00 every Sunday |
| `0 0 1 * *` | midnight on the 1st of every month |

## When NOT to use this skill

- The user wants to *fire something now* — use `orion-run-task` (one-shot).
- The user wants to *change* a Schedule — re-apply the YAML (apply is idempotent on name; generation bumps on body change).
- The user wants to *delete* a Schedule — `orion-manage` or `curl -X DELETE`.

## Exit codes

- `0` — success
- `1` — bad arguments or YAML
- `2` — controller unreachable
- `3` — apply failed (schedule must set exactly one of `task:` or `task_template:`)
