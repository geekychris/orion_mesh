---
name: orion-run-service
description: Apply a Service YAML, dispatch it, show the first few seconds of its output, then return so the Service keeps running in the background. Use when the user says "start this service", "deploy `web.yaml`", "run nginx", "bring up my queue worker", or hands over a Service resource to launch.
---

# orion-run-service

A Service is a *long-running* workload — unlike a Task, you launch it and walk away. This skill applies the YAML, dispatches it to a live agent, prints the first 10 seconds of stdout/stderr to confirm it's actually doing something, then exits leaving the Service running.

## How to use

```bash
python3 .claude/skills/orion-run-service/scripts/run_service.py <file-or-stdin> [--watch SECONDS]
```

Reads `$ORION_CONTROLLER_URL` (default `http://127.0.0.1:7878`) and `$ORION_CLUSTER_TOKEN` (optional).

Sample interaction:

```
applied   Service/web-frontend generation=2
dispatched on node=demo-mac instance=ee291a7b…
watching for 10 seconds…
[06:42:01] starting nginx
[06:42:01] listening on :80
[06:42:01] worker process started
done watching — Service is still running.
  follow the rest with:
    curl $CTRL/v1/logs/Service/web-frontend
  stop it:
    curl -X DELETE $CTRL/v1/resources/Service/web-frontend
```

Options:

- `--watch 10` — how long to watch for output before returning (default: 10s)
- `--quiet` — only output the lines, no framing
- `--no-dispatch` — apply only
- `--exec STRING` — generate a Service on the fly from a shell command; mutually exclusive with `path`

## When to use this skill

- The user wants something *up and serving* — an HTTP service, a queue consumer, a daemon.
- They want a quick confirmation that the service is *actually* doing what it should before they walk away.

## When NOT to use this skill

- The workload is a one-shot — that's `orion-run-task`.
- The user wants to *stop* a running service — `curl -X DELETE` on the resource (removes from store) plus `pkill -f <process>` on the agent's machine (until ControlStop-by-name lands).

## Phase note

Today only `runtime: native` actually launches. Docker / Python / Java adapters land in Phase 5; until then `apply` works for those but `dispatch` fails with `no adapter for kind 'docker'`. The skill surfaces the error verbatim.

## Exit codes

Same as `orion-run-task`: `0` success / `1` arg / `2` controller / `3` apply / `4` dispatch.
