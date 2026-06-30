# 16 · Go client — drive OrionMesh from Go

Same flow as `examples/14-python-client/` and `15-java-client/`,
implemented in Go. The consumer binary is launched by the agent as a
`kind: native` Service.

## 0 · Prereq stack

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
target/debug/orion doctor
```

## 1 · Build the producer + consumer binaries

```bash {name=build}
cd examples/16-go-client && go build -o producer ./cmd/producer && go build -o consumer ./cmd/consumer && cd -
ls examples/16-go-client/producer examples/16-go-client/consumer
```

## 2 · Publish 20 rows from Go

```bash {name=publish}
examples/16-go-client/producer
```

## 3 · Dispatch consumer as an OrionMesh Service

```bash {name=consume}
target/debug/orion apply -f examples/16-go-client/consumer-service.yaml
target/debug/orion dispatch Service go-consumer
sleep 5
target/debug/orion logs Service go-consumer | head -10
```

## 4 · Teardown

```bash {teardown}
target/debug/orion delete service go-consumer 2>/dev/null || true
target/debug/orion delete queue events 2>/dev/null || true
pkill -f orion-controller 2>/dev/null || true
pkill -f orion-agent 2>/dev/null || true
docker stop orion-nats 2>/dev/null || true
echo "torn down"
```

## See also

- [`clients/go/README.md`](../../clients/go/README.md) — API reference
