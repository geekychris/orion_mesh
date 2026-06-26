---
name: orion-status
description: Quick at-a-glance OrionMesh cluster status — live nodes (with inventory), resource counts per kind, recent Schedule fires. Use when the user says "what's running?", "show cluster status", "any nodes alive?", "how's orion doing?", "what tasks are queued?", or similar.
---

# orion-status

Print a compact dashboard of the OrionMesh controller's current state.

## How to use

Run the bundled Python script. It targets `$ORION_CONTROLLER_URL` (default `http://127.0.0.1:7878`); if `$ORION_CLUSTER_TOKEN` is set it sends `Authorization: Bearer <token>` automatically.

```bash
python3 .claude/skills/orion-status/scripts/status.py
# Or with explicit overrides:
ORION_CONTROLLER_URL=http://controller.belmont.local:7878 \
ORION_CLUSTER_TOKEN=$(cat ~/.config/orion/cluster.token) \
  python3 .claude/skills/orion-status/scripts/status.py
```

Options:

- `--json` — emit raw JSON instead of formatted text (useful for piping into `jq`)
- `--kinds Service,Task,…` — restrict the resource-counts row to these kinds (default: every populated kind)

## What to report

Output is already formatted; pass it through verbatim and add a one-sentence summary at the top if the user asked an open-ended question ("looks healthy", "1 node down", etc.).

If the script exits non-zero, the most common cause is the controller being unreachable — tell the user how to start it (see `docs/installation.md §6`) rather than re-trying blindly.

## When NOT to use this skill

- The user wants logs of a specific workload — use `orion-logs` instead.
- The user wants to apply or dispatch something — use `orion-run-task` / `orion-run-service`.
- The user wants the full JSON of one resource — `curl $CTRL/v1/resources/<Kind>/<name>` is shorter than a skill.
