# IPC over NATS

Two trivial Services that talk to each other via the same NATS broker the mesh uses.

| File | Demonstrates |
|---|---|
| [demo-pub.yaml](demo-pub.yaml) | A Service that runs `orion-demo-pub` — publishes "tick N at HH:MM:SS" on `orion.demo` every second |
| [demo-sub.yaml](demo-sub.yaml) | A Service that runs `orion-demo-sub` — subscribes to `orion.demo` and prints each message |

Both binaries ship with the workspace:

```bash
cargo build --release -p orion-demo-bins
# produces target/release/orion-demo-pub and target/release/orion-demo-sub
```

## Run the demo

Either apply + Dispatch both from the UI, or via CLI:

```bash
# Apply the two Services
curl -X POST --data-binary @examples/09-ipc/demo-sub.yaml \
     http://127.0.0.1:7878/v1/resources/apply
curl -X POST --data-binary @examples/09-ipc/demo-pub.yaml \
     http://127.0.0.1:7878/v1/resources/apply

# Dispatch the subscriber first
curl -X POST http://127.0.0.1:7878/v1/dispatch/Service/demo-sub
# Then the publisher
curl -X POST http://127.0.0.1:7878/v1/dispatch/Service/demo-pub

# Watch the messages flowing
curl http://127.0.0.1:7878/v1/logs/Service/demo-pub
curl http://127.0.0.1:7878/v1/logs/Service/demo-sub
```

The subscriber's log should show `recv: tick N from demo at HH:MM:SS` for each message the publisher sent — proving real bidirectional NATS-mediated IPC, not stub data.

The UI's "IPC demo" card on the Demo tab automates all of this with a single button.
