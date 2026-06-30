# `orion-mesh` — Python client

Sync client for the OrionMesh controller REST + JetStream queues. Drop
this into any Python app to talk to a running cluster without spawning
shell commands.

## Install

From a checkout:

```bash
pip install -e clients/python
```

From PyPI (after publish):

```bash
pip install orion-mesh
```

Dependencies: `requests`, `nats-py`, `pyyaml`.

## 60-second example

```python
from orion_mesh import Client

c = Client()                                      # picks up ORION_*, NATS_URL

# Declare a Queue
c.apply("""
apiVersion: orionmesh.dev/v1
kind: Queue
metadata: { name: events }
spec: { type: work }
""")

# Publish 5 messages
q = c.queue("events")
for i in range(5):
    q.pub({"n": i})

# Consume the next 5
for row in q.sub(group="reader", limit=5):
    print(row)

c.close()
```

That entire flow is also in [`examples/quickstart.py`](examples/quickstart.py) —
runnable as-is against a local `orion up` stack.

## What's covered

| Surface | Method |
|---|---|
| Liveness | `c.health()` |
| Get/list resources | `c.get(kind, name)`, `c.list(kind)` |
| Apply (yaml string or dict) | `c.apply(body)` |
| Delete | `c.delete(kind, name)` |
| Dispatch a Service/Task | `c.dispatch(kind, name)` |
| Logs | `c.logs(kind, name, since=N)` |
| Find by capability | `c.find({"llm": {"min_vram_gb": {"gte": 24}}})` |
| Doctor / diag | `c.doctor()` |
| Queue publish | `c.queue(name).pub(value)` |
| Queue publish batch | `c.queue(name).pub_many([...])` |
| Queue subscribe | `for row in c.queue(name).sub(group=..., limit=N): ...` |
| Queue consume forever | `c.queue(name).consume(handler)` |

## Configuration

Environment variables (matching the Rust CLI):

| Var | Default |
|---|---|
| `ORION_CONTROLLER_URL` | `http://127.0.0.1:7878` |
| `NATS_URL` | `nats://127.0.0.1:4222` |
| `ORION_CLUSTER_TOKEN` | (unset → auth-disabled) |

All three can be overridden as `Client(controller=..., nats_url=..., token=...)`.

## Async semantics

The public surface is **blocking**. NATS internally is async — the
client runs a dedicated event loop on a daemon thread and `await`s on
your behalf. From your code's perspective:

```python
seq = q.pub({"hi": "there"})        # blocks until the JetStream ack lands
for row in q.sub(limit=10): ...     # blocks until each row arrives
```

If you're in an async app, instantiate `Client` once and call its
methods from your sync entry points (FastAPI handlers, etc.). A
native-async client is on the roadmap but not in 0.1.

## Tests

Unit tests use `responses` to mock the controller. Integration tests
need a live stack and are skipped by default.

```bash
pip install -e 'clients/python[test]'
pytest clients/python                              # unit tests
pytest clients/python --run-integration            # against a live stack
```

## What this client is, and isn't

It **is** the official way to drive OrionMesh from Python. It maps
1:1 to the REST surface plus the JetStream conventions, with one extra
class (`Queue`) that handles the queue lifecycle so you don't have to
think about subjects / streams / durables.

It **isn't** a port of the controller, an orchestration framework, or
a workflow engine — those live in the controller. The client is a thin
wire.

## Where to go next

- [`orion-mesh/docs/queues.md`](../../docs/queues.md) — the Queue model
- [`orion-mesh/docs/runtime.md`](../../docs/runtime.md) — native vs Docker
- [`examples/14-python-client/`](../../examples/14-python-client/) — end-to-end
  walkthrough using this client to declare + dispatch a processor written
  *in* Python (no Rust CLI required)
