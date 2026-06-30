# 10 · Named queues + CLI pipelines + polyglot processors

End-to-end demo of OrionMesh's named-queue substrate.

You will:

1. **Parse `ps -ef` output** into ndjson with `orion json`.
2. **Push it** into a named queue with `orion queue pub`.
3. **Process it** with a 3-replica Python service in **work** (one-per-row) mode.
4. **Broadcast the same stream** to multiple watchers in **topic** (everyone-sees-everything) mode — in two languages.
5. **Attach a debugger** to a Python processor.
6. **Tear it all down** in a single block.

> **Runnable.** `scripts/run-md.py examples/10-queues/README.md` walks every recipe in this document end-to-end. See [`../../docs/runner.md`](../../docs/runner.md) for tag conventions (`{name=X}`, `{skip}`, `{allow_fail}`, `{teardown}`).

## 0 · Prerequisites

Run the standard local stack from [`../../CLAUDE.md`](../../CLAUDE.md#local-dev--how-to-actually-run-it):

```bash {name=prereq}
# NATS broker with JetStream
docker run -d --rm --name orion-nats -p 4222:4222 nats:2.10 -js 2>/dev/null || true
# Controller + agent in dev mode
pkill -f orion-controller 2>/dev/null || true
pkill -f orion-agent 2>/dev/null || true
cargo build --workspace --quiet
ORION_AUTH_DISABLED=1 ORION_STORE_PATH=sqlite::memory: \
    target/debug/orion-controller --bind 127.0.0.1:7878 >/tmp/orion-ctrl.log 2>&1 &
sleep 1
ORION_AUTH_DISABLED=1 \
    target/debug/orion-agent --node-id local-dev --heartbeat-interval 2 >/tmp/orion-agent.log 2>&1 &
sleep 1
# Python venv for the processor template
bash examples/10-queues/python/setup.sh
# Java jar (skipped if Maven isn't installed; the work-mode and topic
# Python recipes still work without it)
bash examples/10-queues/java/setup.sh 2>/dev/null || \
    echo "(Maven not installed — Java recipes will fall back to Python equivalents)"
```

Quick health check:

```bash {name=health}
target/debug/orion get nodes
target/debug/orion diag jetstream
```

## 1 · Pipe `ps -ef` into a work queue

A **work** queue load-balances rows across N consumers — each row is processed by exactly one of them.

```bash {name=work-create}
# Declare the queue
target/debug/orion gen queue ps-rows --type work --max-age 1h | target/debug/orion apply -f -
target/debug/orion queue ls

# Pipe ps into ndjson, then into the queue
ps -ef | target/debug/orion json --headers uid,pid,ppid,c,stime,tty,time,cmd | \
    target/debug/orion queue pub ps-rows

# Confirm messages landed
target/debug/orion queue describe ps-rows
```

## 2 · Run 3 processors that share the work

Each row goes to exactly **one** of the three Python replicas — the JetStream durable consumer named `crunchers` is shared.

```bash {name=work-process}
target/debug/orion gen processor row-cruncher \
    --queue ps-rows --lang python --replicas 3 --group crunchers \
    --env PATH="$PWD/examples/10-queues/python/.venv/bin:$PATH" \
    --env PYTHONUNBUFFERED=1 | \
  target/debug/orion apply -f -

target/debug/orion dispatch Service row-cruncher

# Give them a few seconds to drain
sleep 5
target/debug/orion logs Service row-cruncher | head -20

# Distribution across the 3 replicas (each msg consumed once)
target/debug/orion logs Service row-cruncher | \
    awk '/processed/ { match($0,/r[0-9]/); print substr($0,RSTART,RLENGTH) }' | \
    sort | uniq -c
```

## 3 · Broadcast the same data to topic-mode watchers

A **topic** queue gives every subscriber its own JetStream cursor — every watcher sees every message independently.

```bash {name=topic-create}
target/debug/orion gen queue ps-broadcast --type topic --max-age 1h | target/debug/orion apply -f -

# Two Python watchers (each gets every row)
target/debug/orion gen processor watcher-python \
    --queue ps-broadcast --lang python --replicas 2 \
    --env PATH="$PWD/examples/10-queues/python/.venv/bin:$PATH" \
    --env PYTHONUNBUFFERED=1 | \
  target/debug/orion apply -f -
target/debug/orion dispatch Service watcher-python

# Pump rows in
ps -ef | head -20 | target/debug/orion json --headers uid,pid,ppid,c,stime,tty,time,cmd | \
    target/debug/orion queue pub ps-broadcast

sleep 3
# Both replicas should report ~the same row count
target/debug/orion logs Service watcher-python | \
    awk '/processed/ { match($0,/r[0-9]/); print substr($0,RSTART,RLENGTH) }' | \
    sort | uniq -c
```

## 4 · Add a Java watcher to the same broadcast

```bash {name=topic-java}
if [ -f examples/10-queues/java/target/orion-queue-processor.jar ]; then
    target/debug/orion gen processor watcher-java \
        --queue ps-broadcast --lang java --replicas 1 | \
      target/debug/orion apply -f -
    target/debug/orion dispatch Service watcher-java
    sleep 5
    target/debug/orion logs Service watcher-java | head -5
else
    echo "(skip: Java jar not built — see examples/10-queues/java/setup.sh)"
fi
```

## 5 · Debug a processor with a breakpoint

This brings up a Python processor that **suspends until a debugger attaches** on port 5678. With suspend mode you can hit a breakpoint on the very first row.

```bash {name=debug skip}
# (Marked {skip} for the runner — runs interactively only.)
target/debug/orion gen processor row-cruncher-debug \
    --queue ps-rows --lang python --debug --debug-suspend \
    --env PATH="$PWD/examples/10-queues/python/.venv/bin:$PATH" \
    --env PYTHONUNBUFFERED=1 | \
  target/debug/orion apply -f -

target/debug/orion dispatch Service row-cruncher-debug
target/debug/orion logs Service row-cruncher-debug
# Wait for: "debugpy listening on 0.0.0.0:5678 — Waiting for client to attach..."

# Attach from VS Code:
#   Run > Add Configuration > Python: Remote Attach
#   Host: localhost, Port: 5678
# Set a breakpoint in examples/10-queues/python/processor.py handle()

ps -ef | head -5 | target/debug/orion json --headers uid,pid,ppid,c,stime,tty,time,cmd | \
    target/debug/orion queue pub ps-rows
# Each row hits handle() — step through.
```

See [`../../docs/debugging-processors.md`](../../docs/debugging-processors.md) for the same recipe with Java + IntelliJ.

## 6 · Introspect

Every REST endpoint and UI surface has a CLI verb:

```bash {name=introspect}
target/debug/orion queue ls
target/debug/orion queue describe ps-rows
target/debug/orion instances
target/debug/orion get services
target/debug/orion describe service row-cruncher
target/debug/orion diag system
target/debug/orion diag jetstream
target/debug/orion schedule observed
```

## 7 · Teardown

```bash {teardown}
target/debug/orion delete service row-cruncher 2>/dev/null || true
target/debug/orion delete service row-cruncher-debug 2>/dev/null || true
target/debug/orion delete service watcher-python 2>/dev/null || true
target/debug/orion delete service watcher-java 2>/dev/null || true
target/debug/orion delete queue ps-rows 2>/dev/null || true
target/debug/orion delete queue ps-broadcast 2>/dev/null || true
pkill -f orion-controller 2>/dev/null || true
pkill -f orion-agent 2>/dev/null || true
docker stop orion-nats 2>/dev/null || true
echo "torn down"
```

## What you just saw

| Concept | Maps to |
|---|---|
| `orion json` | column-header autodetect parser (works on `ps`, `df`, `ls -la`, TSV / CSV via `--delim`, or per-line regex via `--regex`) |
| `orion queue pub <name>` | ndjson stdin → JetStream subject `orion.queue.<name>`, stream `ORION_QUEUE_<NAME>` |
| Queue (kind) | Declared resource; type=`work` load-balances, type=`topic` broadcasts. Live consumer counts surface in `orion queue ls` |
| `orion gen processor` | Builder that emits a Service YAML pointing at the polyglot processor template, env-wired to the queue |
| `--debug`, `--debug-suspend` | Wrap the process in debugpy (Python) or JDWP (Java); attach from VS Code / IntelliJ |
| `examples/10-queues/python/processor.py` | Reference processor — edit `handle(row)` to put your logic in |
| `examples/10-queues/java/.../Processor.java` | Same shape in Java |

Compare with [`../09-ipc/README.md`](../09-ipc/README.md), which covers the lower-level core-NATS / JetStream raw IPC patterns these queues are built on.
