---
name: orion-run-task
description: Apply a Task YAML, dispatch it to an agent, watch its output until it exits or a timeout hits. Use when the user says "run this task", "execute this task.yaml", "kick off the rollup", "fire `train.yaml`", or hands over a Task resource and wants to see it finish.
---

# orion-run-task

Apply + Dispatch a Task, then tail its stdout/stderr until it stops emitting or hits a timeout. Honest about scope: only works for `runtime: native` today (the Docker/Python/Java adapters land in Phase 5).

## How to use

```bash
python3 .claude/skills/orion-run-task/scripts/run_task.py <file-or-stdin> [--timeout SECONDS] [--quiet]
```

- File mode: `run_task.py path/to/task.yaml`
- Stdin mode: `cat task.yaml | run_task.py -` — useful when generating YAML inline

Reads `$ORION_CONTROLLER_URL` (default `http://127.0.0.1:7878`) and `$ORION_CLUSTER_TOKEN` (sent as bearer if set).

Output by default looks like:

```
applied   Task/nightly-rollup generation=1
dispatched on node=demo-mac instance=ee291a7b…
[06:42:01] line-1
[06:42:02] line-2
…
done — exited cleanly (5 lines, last at 06:42:05)
```

Options:

- `--timeout 60` — give up tailing after 60 seconds (default: 90)
- `--quiet` — only print the lines, no `applied`/`dispatched`/`done` framing
- `--no-dispatch` — apply only; don't dispatch (useful for staged workflows)
- `--exec STRING` — generate the YAML on the fly: a one-off Task with `runtime: native, exec: /bin/sh -c '<STRING>'`. Mutually exclusive with the YAML argument.

## When to use this skill

- The user has a finished Task YAML and wants it run + observed.
- They want to fire a one-off shell command on a node ("run `tar -czf …` on the cluster" → use `--exec`).
- They want to retry a failed Schedule fire manually.

## When NOT to use this skill

- The workload is long-running and you want to *leave* it running — that's `orion-run-service`.
- The user just wants to apply without dispatch — say so and use `curl -X POST /v1/resources/apply` directly (or `--no-dispatch`).
- The task uses a non-native runtime (`docker`, `python`, etc.) — the apply will succeed but dispatch will fail until Phase 5. Flag this.

## Exit codes

- `0` — Task ran (logs may show non-zero exit *of the workload itself* — see the last line)
- `1` — argument or YAML problem
- `2` — controller unreachable
- `3` — validation or apply failed (semantic error from `Resource::validate()`)
- `4` — dispatch failed (no live agent, runtime mismatch, etc.)
