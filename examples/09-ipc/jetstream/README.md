# JetStream demos

JetStream is NATS's persistent + at-least-once layer. Unlike core NATS (which drops a message if nobody's listening), a JetStream message stays on the broker until it's acked by every durable consumer that cares about it.

| File | What it demonstrates |
|---|---|
| [`js-pub.yaml`](js-pub.yaml) | Single Rust publisher; auto-creates stream `ORION_DEMO_JS` |
| [`js-sub-workers.yaml`](js-sub-workers.yaml) | **3 replicas** sharing one durable consumer `workers` — load-balanced |
| [`js-sub-replay-on-restart.yaml`](js-sub-replay-on-restart.yaml) | 1 replica with its own durable `single-replay` — kill it, let pub keep going, restart it, it picks up where it left off |

Polyglot JetStream wrappers live under [`../polyglot/python/js_{pub,sub}.py`](../polyglot/python/) and [`../polyglot/java/src/main/java/io/orionmesh/demo/Js{Pub,Sub}.java`](../polyglot/java/). All three share stream + subject conventions so they interoperate.

## Run the durability demo

```bash
CTRL=http://127.0.0.1:7878

# 1. Apply publisher + a single replay-style subscriber
curl -X POST --data-binary @examples/09-ipc/jetstream/js-pub.yaml             $CTRL/v1/resources/apply
curl -X POST --data-binary @examples/09-ipc/jetstream/js-sub-replay-on-restart.yaml $CTRL/v1/resources/apply

# 2. Dispatch the publisher — let it accumulate ~10 messages
curl -X POST $CTRL/v1/dispatch/Service/js-pub
sleep 12

# 3. Dispatch the subscriber — it'll catch up from seq 1
curl -X POST $CTRL/v1/dispatch/Service/js-sub-replay
sleep 3
curl $CTRL/v1/logs/Service/js-sub-replay | jq -r '.entries[].line' | grep recv | head -10
# →  [demo-sub:r0] recv (seq=1):  tick 1 from r0 at ...
# →  [demo-sub:r0] recv (seq=2):  tick 2 from r0 at ...
# →  ...
# →  [demo-sub:r0] recv (seq=10): tick 10 from r0 at ...

# 4. Stop and restart the subscriber after the publisher has sent more
pkill -f 'orion-demo-sub.*single-replay'
sleep 5    # publisher keeps going, accumulating seq 11..15
curl -X POST $CTRL/v1/dispatch/Service/js-sub-replay
sleep 2
curl $CTRL/v1/logs/Service/js-sub-replay | jq -r '.entries[-5:][].line'
# → [demo-sub:r0] recv (seq=11): tick 11 from r0 at ...
# → [demo-sub:r0] recv (seq=12): ...
```

JetStream remembered the last acked seq (10) and replayed only the unacked ones (11+).

## Run the load-balanced demo

```bash
curl -X POST --data-binary @examples/09-ipc/jetstream/js-sub-workers.yaml $CTRL/v1/resources/apply
curl -X POST $CTRL/v1/dispatch/Service/js-sub-workers   # 3 replicas, all in durable "workers"
sleep 1
curl -X POST $CTRL/v1/dispatch/Service/js-pub
sleep 10
curl $CTRL/v1/logs/Service/js-sub-workers | jq -r '.entries[].line' | grep recv \
  | sed -E 's/.*\[demo-sub:(r[0-9])\].*/\1/' | sort | uniq -c
```

Distribution across r0/r1/r2 will vary — NATS picks whichever consumer is ready. With more publish volume the distribution evens out.

## When NOT to use JetStream

JetStream costs disk I/O and per-message acks. For short-lived telemetry where dropping is fine, use core NATS (`subscribe` / `queue_subscribe`) — it's cheaper. The decision tree in [docs/ipc.md §3](../../../docs/ipc.md#3-choosing-a-pattern) covers when each pattern fits.
