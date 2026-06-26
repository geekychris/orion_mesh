---
name: orion-logs
description: Tail logs of a running OrionMesh Service or Task. One-shot (last N lines) or live follow (`-f`). Use when the user says "logs for X", "tail Y", "what is the rollup printing?", "follow demo-pub", or similar.
---

# orion-logs

A thin `tail -f` over `GET /v1/logs/{kind}/{name}` with pretty-printed timestamps and stream colouring.

## How to use

```bash
# Last 50 lines
python3 .claude/skills/orion-logs/scripts/logs.py Service/demo-pub

# Live follow until Ctrl-C
python3 .claude/skills/orion-logs/scripts/logs.py -f Task/cron-job

# Last 200 lines, JSON
python3 .claude/skills/orion-logs/scripts/logs.py --tail 200 --json Service/web-frontend
```

Path arg form: `<Kind>/<Name>`. Kinds with logs today: `Service` and `Task`. Other kinds: nothing is captured.

Options:

- `-f`, `--follow` — keep polling every 1s until interrupted
- `--tail N` — number of recent lines to show on first read (default: 50)
- `--since SEQ` — start from a specific log sequence number (advanced; usually you want `--tail`)
- `--json` — emit JSON instead of human-formatted
- `--no-color` — disable ANSI colouring (useful when piping)

## When to use this skill

- The user just dispatched a workload and wants to see what it's doing.
- They're debugging an unhealthy Service.
- They want to live-stream output to a file: `python3 logs.py -f Service/demo-pub > /tmp/demo-pub.log &`

## When NOT to use this skill

- The user wants the raw API response — `curl $CTRL/v1/logs/<Kind>/<Name>` is shorter.
- The user wants logs from *before* the controller started — those weren't captured; the in-memory ring loses old data on controller restart.

## Exit codes

- `0` — success
- `2` — controller unreachable or workload has no logs (returns empty JSON)
- `130` — interrupted (`Ctrl-C` in follow mode)
