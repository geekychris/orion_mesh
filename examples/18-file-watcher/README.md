# 18 · File watcher — tail any file into a queue

`orion-file-watcher` runs as an OrionMesh Service, tails one or more
files, and publishes each new line as a JSON message to a named queue.
The companion processor consumes those messages from any language.

## What you'll build

```mermaid
flowchart LR
    Log[/var/log/myapp.log] --> Watcher["orion-file-watcher<br/>Service"]
    Watcher --> Queue[(orion.queue.log-events)]
    Queue --> Consumer["row processor<br/>(any language)"]
```

## 0 · Prereqs

```bash {name=prereq}
docker ps --format '{{.Names}}' | grep -q orion-nats || \
    docker run -d --rm --name orion-nats -p 4222:4222 nats:2.10 -js
pkill -f orion-controller 2>/dev/null || true
pkill -f orion-agent 2>/dev/null || true
sleep 1
cargo build --workspace --quiet
ORION_AUTH_DISABLED=1 ORION_STORE_PATH=sqlite::memory: \
    target/debug/orion-controller --bind 127.0.0.1:7878 >/tmp/orion-ctrl.log 2>&1 &
sleep 1
ORION_AUTH_DISABLED=1 \
    target/debug/orion-agent --node-id local-dev --heartbeat-interval 2 >/tmp/orion-agent.log 2>&1 &
sleep 2
mkdir -p /tmp/orion-watch-demo
: > /tmp/orion-watch-demo/app.log
```

## 1 · Declare the queue + watcher Service

```bash {name=setup}
ORION=target/debug/orion
$ORION gen queue log-events --type work | $ORION apply -f -
$ORION apply -f examples/18-file-watcher/watcher-service.yaml
$ORION dispatch Service log-watcher
```

## 2 · Write to the file and watch it propagate

```bash {name=trigger}
for i in 1 2 3 4 5; do
    echo "line-$i: timestamp=$(date +%s)" >> /tmp/orion-watch-demo/app.log
    sleep 0.5
done
sleep 2
target/debug/orion logs Service log-watcher | head -10
```

The lines should appear on the queue. Consume them to verify:

```bash {name=consume}
target/debug/orion queue sub log-events --group consumer --limit 5 2>&1 | head -15
```

Each subscriber row contains `{path, line, at, _subject}` — the
producer side is identical regardless of what's writing to the file
(application, syslog, cron, etc.).

## 3 · Teardown

```bash {teardown}
target/debug/orion delete service log-watcher 2>/dev/null || true
target/debug/orion delete queue log-events 2>/dev/null || true
pkill -f orion-controller 2>/dev/null || true
pkill -f orion-agent 2>/dev/null || true
docker stop orion-nats 2>/dev/null || true
rm -rf /tmp/orion-watch-demo
echo "torn down"
```

## How it works

The watcher remembers each file's offset and polls the file's size
every `--interval-ms` (default 500). On size growth, it reads the new
bytes, splits into lines, and publishes each one. On size shrinkage
(rotation/truncation), it rewinds to 0 and starts over.

Cursor state is in-memory — if the watcher restarts, it picks up at the
file's current end (unless `--from-start` is passed). For
crash-tolerant offset tracking, run with `--from-start` and let your
consumer dedupe on `at`.
