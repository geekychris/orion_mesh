---
name: orion-manage
description: List, inspect, and delete OrionMesh resources. Use when the user says "list services", "show all tasks", "delete `demo-pub`", "get the YAML for `amiga-search`", "what schedules are there?", or wants a CRUD operation that doesn't involve dispatch.
---

# orion-manage

Read-and-delete operations on the OrionMesh resource store. For apply + dispatch use `orion-run-task` / `orion-run-service`.

## How to use

```bash
# List all resources of a kind
python3 .claude/skills/orion-manage/scripts/manage.py list Service

# List every populated kind
python3 .claude/skills/orion-manage/scripts/manage.py list --all

# Fetch one resource (full YAML)
python3 .claude/skills/orion-manage/scripts/manage.py get Service/amiga-search

# Delete one resource (with confirm prompt)
python3 .claude/skills/orion-manage/scripts/manage.py delete Service/demo-pub
# Skip the prompt:
python3 .claude/skills/orion-manage/scripts/manage.py delete Service/demo-pub --yes

# Bulk delete by prefix (good for demo cleanup)
python3 .claude/skills/orion-manage/scripts/manage.py delete --prefix demo- Service
```

Defaults: `$ORION_CONTROLLER_URL` (default `http://127.0.0.1:7878`), `$ORION_CLUSTER_TOKEN` (optional bearer).

## When to use this skill

- "Show me the services / tasks / schedules"
- "Delete the demo stuff"
- "Give me the YAML for X"

## When NOT to use this skill

- The user wants to apply a YAML — `orion-run-task` / `orion-run-service`.
- The user wants logs — `orion-logs`.
- The user wants to know if the system is healthy — `orion-status`.

## Exit codes

- `0` — success
- `1` — bad arguments
- `2` — controller unreachable
- `3` — resource not found (`get` / `delete` of a missing one)
- `130` — user said no at the confirm prompt
