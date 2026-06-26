---
name: orion-ipc-demo
description: One-button end-to-end IPC demo — applies the `demo-pub` + `demo-sub` Services, dispatches both, and prints their side-by-side log streams to prove NATS-mediated service-to-service messaging works. Use when the user says "show me the IPC demo", "prove services can talk", "run the pub/sub demo", or "demo OrionMesh end-to-end".
---

# orion-ipc-demo

Spins up two demo Services that send and receive on the same NATS broker the mesh uses. The whole sequence — apply → dispatch (sub first so it's listening) → poll both log streams → tear down — is one Python invocation.

## How to use

```bash
# Build the demo binaries (one-time)
cargo build --release -p orion-demo-bins

# Run the demo
python3 .claude/skills/orion-ipc-demo/scripts/ipc_demo.py
```

What it does:

1. Applies `examples/09-ipc/demo-sub.yaml`
2. Applies `examples/09-ipc/demo-pub.yaml`
3. Dispatches the subscriber FIRST (so it's listening when the publisher fires)
4. Dispatches the publisher
5. Polls both `/v1/logs/Service/{demo-pub,demo-sub}` for `--duration` seconds (default 8) and prints two side-by-side log windows
6. Asks whether to leave the Services running. With `--cleanup`, deletes both at the end.

Sample output:

```
=== publisher stdout              | === subscriber stdout
06:51:51 sent: tick 1 from P     | 06:51:51 recv: tick 1 from P
06:51:52 sent: tick 2 from P     | 06:51:52 recv: tick 2 from P
…
done — 5 ticks sent, 5 received.
```

Options:

- `--duration 8` — seconds to watch (default: 8)
- `--cleanup` — DELETE both Services at the end (running processes outlive the resource until the agent restarts)
- `--subject orion.demo.ipc` — override the NATS subject (default matches the canonical demo YAMLs)
- `--examples-dir <path>` — point at a checkout of the YAMLs if they're not at `examples/09-ipc/` relative to CWD

## Prerequisites

- A NATS broker (typically `docker run -d --rm --name orion-nats -p 4222:4222 nats:2.10 -js`)
- Running controller, agent, and the `orion-demo-pub` + `orion-demo-sub` binaries on the agent's machine
- A reachable controller (`$ORION_CONTROLLER_URL`)

If any prereq is missing the skill prints the exact command to fix it and exits non-zero.

## When to use this skill

- Showing the mesh to someone new — this is the most demonstrative single thing it can do.
- Sanity-checking that dispatch + log forwarder + NATS bus are all working after a code change.

## When NOT to use this skill

- The user wants a custom pub/sub workflow — write your own YAMLs and use `orion-run-service`.
- The user is debugging the dispatch path — `orion-status` + `orion-logs` are smaller, more targeted.

## Exit codes

- `0` — full sequence completed (messages flowed end-to-end)
- `1` — prerequisite missing (binary not found, YAML missing)
- `2` — controller unreachable
- `3` — no messages flowed within `--duration` (broker probably down, or the agent's machine doesn't have the binaries)
