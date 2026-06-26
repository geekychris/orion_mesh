# Polyglot IPC demo

The same NATS subject (`orion.demo.ipc`) carried by the mesh's own broker, with publishers and subscribers written in **Rust, Python, and Java**. Each language reads exactly the same wire — bytes on a NATS subject — and self-identifies via `ORION_REPLICA_INDEX` (set by the OrionMesh agent for replicas).

```
polyglot/
├── rust/      Same binaries as ../../crates/orion-demo-bins. Build with
│              `cargo build --release -p orion-demo-bins`.
├── python/    pub.py + sub.py + requirements.txt + setup.sh
└── java/      Pub.java + Sub.java + pom.xml + setup.sh
                  Produces fat jars: target/orion-demo-pub.jar
                                     target/orion-demo-sub.jar
```

All three implementations honour the same flags:

| Flag | What it does |
|---|---|
| `--nats-url` | NATS broker URL (also `$NATS_URL`). Default `nats://127.0.0.1:4222`. |
| `--subject` | NATS subject. Default `orion.demo.ipc`. |
| `--queue-group` *(sub only)* | Join a queue group — load-balanced. Omit for fan-out. |
| `--label` | Logical name in stdout. Defaults to `r$ORION_REPLICA_INDEX` if the agent set that env, else the language tag (`py` / `java`). |
| `--interval` *(pub only)* | Seconds between messages. Default `1.0`. |

> **Runnable.** `scripts/run-md.py examples/09-ipc/polyglot/README.md`
> runs the `setup` + `interop` recipes end-to-end and tears down. The
> Terminal-1/Terminal-2 block below is `{skip}` because it expects two
> separate shells.

## Setup

```bash {name=setup}
# Rust (one-time)
cargo build --release -p orion-demo-bins

# Python
bash examples/09-ipc/polyglot/python/setup.sh
# → creates .venv, installs nats-py

# Java
bash examples/09-ipc/polyglot/java/setup.sh
# → mvn package, produces target/orion-demo-{pub,sub}.jar
```

## Standalone smoke test (no OrionMesh required)

Each pair talks to the NATS broker directly. With NATS running on `:4222`:

```bash {skip}
# Terminal 1 — Python subscriber
examples/09-ipc/polyglot/python/.venv/bin/python3 examples/09-ipc/polyglot/python/sub.py --label py

# Terminal 2 — Java publisher
java -jar examples/09-ipc/polyglot/java/target/orion-demo-pub.jar --label java --interval 1.0

# You should see in Terminal 1:
#   [py-sub:py] recv: tick 1 from java at 15:42:01.123 (subject=orion.demo.ipc)
#   [py-sub:py] recv: tick 2 from java at 15:42:02.124 ...
```

Mix and match: Rust pub → Java + Python subs, Python pub → 5 Rust + 2 Java + 3 Python queue-grouped subs, etc. Same wire, same subject, same semantics.

## Through OrionMesh — Service YAMLs

The companion YAMLs in this directory wrap each language as an OrionMesh `Service`:

- [`yaml/python-pub.yaml`](yaml/python-pub.yaml)
- [`yaml/python-sub-qg.yaml`](yaml/python-sub-qg.yaml) — 2 replicas, queue group
- [`yaml/java-pub.yaml`](yaml/java-pub.yaml)
- [`yaml/java-sub-qg.yaml`](yaml/java-sub-qg.yaml) — 2 replicas, queue group
- [`yaml/interop-mixed-sub.yaml`](yaml/interop-mixed-sub.yaml) — comments only; demonstrates how a single subject is shared

Apply and dispatch any combination:

```bash {name=interop}
CTRL=${ORION_CONTROLLER_URL:-http://127.0.0.1:7878}

# One Python publisher + one Java subscriber + three Rust subscribers, all queue-grouped
curl -sS -X POST --data-binary @examples/09-ipc/polyglot/yaml/python-pub.yaml      $CTRL/v1/resources/apply ; echo
curl -sS -X POST --data-binary @examples/09-ipc/polyglot/yaml/java-sub-qg.yaml     $CTRL/v1/resources/apply ; echo
curl -sS -X POST --data-binary @examples/09-ipc/queue-group-3-workers.yaml         $CTRL/v1/resources/apply ; echo
curl -sS -X POST $CTRL/v1/dispatch/Service/java-sub-qg     ; echo
curl -sS -X POST $CTRL/v1/dispatch/Service/demo-sub-workers ; echo
sleep 1
curl -sS -X POST $CTRL/v1/dispatch/Service/python-pub      ; echo

sleep 6
echo "=== publisher (python) — last 3 ==="
curl -s $CTRL/v1/logs/Service/python-pub      | python3 -c "import sys,json;d=json.load(sys.stdin);[print(' ',e['line']) for e in d['entries'][-3:]]"
echo "=== java subscribers — last 3 ==="
curl -s $CTRL/v1/logs/Service/java-sub-qg     | python3 -c "import sys,json;d=json.load(sys.stdin);[print(' ',e['line']) for e in d['entries'][-3:]]"
echo "=== rust subscribers — last 3 ==="
curl -s $CTRL/v1/logs/Service/demo-sub-workers | python3 -c "import sys,json;d=json.load(sys.stdin);[print(' ',e['line']) for e in d['entries'][-3:]]"
```

All four subscriber processes (2 Java + 3 Rust = 5 total in the same queue group `ipc-workers`) share the load — each message goes to exactly one of them.

## Tear down

```bash {teardown}
CTRL=${ORION_CONTROLLER_URL:-http://127.0.0.1:7878}
for s in python-pub java-sub-qg demo-sub-workers; do
  curl -sS -X DELETE $CTRL/v1/resources/Service/$s > /dev/null
done
pkill -f 'orion-demo-' 2>/dev/null || true
pkill -f 'js_(pub|sub)\.py' 2>/dev/null || true
pkill -f 'orion-demo-(js-)?(pub|sub)\.jar' 2>/dev/null || true
echo "polyglot demo torn down"
```

## Why this is a big deal

A workload written in Rust can send a structured event; a workload written in Python can pick it up and ML-process it; a workload in Java can take the result and write it to a database — all over the same broker OrionMesh already runs. No new infrastructure, no shared library, no API spec to keep in sync — just bytes on a subject. The mesh's control plane and the workloads' data plane share NATS, but the subject namespaces don't overlap (`orion.*` is the mesh; everything else is yours).

For the semantics — fan-out vs queue group vs JetStream — see [docs/ipc.md](../../../docs/ipc.md).
