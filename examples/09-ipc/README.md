# IPC over NATS

Two trivial Services that talk to each other via the same NATS broker the mesh uses.

| File | Demonstrates |
|---|---|
| [demo-pub.yaml](demo-pub.yaml) | A Service that runs `orion-demo-pub` — publishes "tick N at HH:MM:SS" on `orion.demo.ipc` every second |
| [demo-sub.yaml](demo-sub.yaml) | A Service that runs `orion-demo-sub` — subscribes to `orion.demo.ipc` and prints each message |
| [fanout-3-replicas.yaml](fanout-3-replicas.yaml) | 3 replicas, NO queue group — every replica gets every message |
| [queue-group-3-workers.yaml](queue-group-3-workers.yaml) | 3 replicas, queue group `ipc-workers` — load-balanced |
| [jetstream/](jetstream/) | Persistent + at-least-once + replayable subdir |
| [polyglot/](polyglot/) | Python + Java + Rust pub/sub, all on the same wire |

> **Runnable.** `scripts/run-md.py examples/09-ipc/README.md` walks every recipe in this README end-to-end (with a `{teardown}` step at the end). See [`../docs/runner.md`](../docs/runner.md) for the tag conventions (`{name=X}`, `{skip}`, `{allow_fail}`, `{teardown}`) and the drive flags (`--list`, `--only X`, `--dry-run`, `--interactive`).

## Build the demo binaries (one-time)

```bash {name=build}
cargo build --release -p orion-demo-bins
# produces target/release/orion-demo-pub and target/release/orion-demo-sub
```

## Run the demo

Apply, dispatch, watch.

```bash {name=run}
CTRL=${ORION_CONTROLLER_URL:-http://127.0.0.1:7878}

# Apply the two Services
curl -sS -X POST --data-binary @examples/09-ipc/demo-sub.yaml $CTRL/v1/resources/apply ; echo
curl -sS -X POST --data-binary @examples/09-ipc/demo-pub.yaml $CTRL/v1/resources/apply ; echo

# Dispatch the subscriber first so it's listening
curl -sS -X POST $CTRL/v1/dispatch/Service/demo-sub ; echo
sleep 1
curl -sS -X POST $CTRL/v1/dispatch/Service/demo-pub ; echo

# Let messages flow
sleep 5

# Side-by-side preview
echo "=== publisher (last 3 lines) ==="
curl -s $CTRL/v1/logs/Service/demo-pub | python3 -c "import sys,json;d=json.load(sys.stdin);[print(' ',e['line']) for e in d['entries'][-3:]]"
echo "=== subscriber (last 3 lines) ==="
curl -s $CTRL/v1/logs/Service/demo-sub | python3 -c "import sys,json;d=json.load(sys.stdin);[print(' ',e['line']) for e in d['entries'][-3:]]"
```

The subscriber's log should show `recv: tick N from demo at HH:MM:SS` for each message the publisher sent — proving real bidirectional NATS-mediated IPC, not stub data.

The UI's "IPC demo" card on the Demo tab automates all of this with a single button.

## Tear it down

```bash {teardown}
CTRL=${ORION_CONTROLLER_URL:-http://127.0.0.1:7878}
curl -sS -X DELETE $CTRL/v1/resources/Service/demo-pub > /dev/null
curl -sS -X DELETE $CTRL/v1/resources/Service/demo-sub > /dev/null
pkill -f 'target/release/orion-demo-' 2>/dev/null || true
echo "ipc demo torn down"
```

## See also

- [fanout-3-replicas.yaml](fanout-3-replicas.yaml) — every replica gets every message
- [queue-group-3-workers.yaml](queue-group-3-workers.yaml) — load-balanced worker pool
- [jetstream/](jetstream/) — durable + ack'd + replayable
- [polyglot/](polyglot/) — Python + Java + Rust pub/sub
- [../../docs/ipc.md](../../docs/ipc.md) — the conceptual doc covering all four NATS patterns
