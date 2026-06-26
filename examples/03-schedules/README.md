# Schedules

A `Schedule` fires a Task on a cron expression. Demonstrates both shapes the validator accepts.

| File | Demonstrates |
|---|---|
| [reference.yaml](reference.yaml) | `task: <name>` — points at an existing Task resource |
| [inline-template.yaml](inline-template.yaml) | `task_template: { ... }` — fully inline definition |
| [hourly-health-check.yaml](hourly-health-check.yaml) | Simple inline task on `0 * * * *` |

The validator (`Resource::validate()`) catches the case where both are set, or neither — see [`bad/`](../bad/) for the failing examples.
