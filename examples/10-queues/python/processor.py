#!/usr/bin/env python3
"""OrionMesh queue processor — Python reference.

Reads its configuration from environment variables injected by the Service
spec that `orion gen processor` produces:

    ORION_QUEUE_NAME    queue name (display only)
    ORION_QUEUE_SUBJECT subject to filter on
    ORION_QUEUE_STREAM  JetStream stream name (must already exist; the agent
                        ensures it via `orion queue pub` or `orion apply`)
    ORION_QUEUE_TYPE    "work" or "topic"
    ORION_QUEUE_GROUP   durable consumer name (shared for work, unique for topic)
    ORION_REPLICA_INDEX agent-injected replica id (0..replicas-1)
    NATS_URL            broker URL (default nats://127.0.0.1:4222)

Replace `handle(row)` with your own logic. Each row is a Python dict — the
parsed ndjson message.

Debug attach (when started via `orion gen processor --debug ...`):
    1. `orion logs Service <name>` — wait for "debugpy listening on 0.0.0.0:5678"
    2. In VS Code: Run > Add Configuration > Python: Remote Attach
                   host: localhost, port: 5678
    3. Set a breakpoint in handle() — message arrival hits it.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys

try:
    import nats
    from nats.js.api import ConsumerConfig, AckPolicy, StreamConfig
    from nats.js.errors import NotFoundError
except ImportError:
    sys.stderr.write("nats-py not installed. Run: pip install -r requirements.txt\n")
    sys.exit(1)


def handle(row: dict) -> None:
    """User-editable per-row handler. Replace with your own logic.

    Set a breakpoint here when attaching with a debugger.
    """
    print(
        f"[{LABEL}] processed {row.get('_subject', '?')}: "
        f"{json.dumps(row, sort_keys=True)[:200]}",
        flush=True,
    )


NATS_URL = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")
QUEUE_NAME = os.environ.get("ORION_QUEUE_NAME", "unnamed")
SUBJECT = os.environ.get("ORION_QUEUE_SUBJECT", f"orion.queue.{QUEUE_NAME}")
STREAM = os.environ.get("ORION_QUEUE_STREAM", f"ORION_QUEUE_{QUEUE_NAME.upper().replace('-', '_')}")
QTYPE = os.environ.get("ORION_QUEUE_TYPE", "work")
BASE_GROUP = os.environ.get("ORION_QUEUE_GROUP", f"{QUEUE_NAME}-workers")
REPLICA = os.environ.get("ORION_REPLICA_INDEX", "0")
# For topic queues every replica needs its own durable so JetStream tracks an
# independent cursor — that's what makes broadcast work. For work queues every
# replica shares the same durable so JetStream load-balances messages.
GROUP = BASE_GROUP if QTYPE == "work" else f"{BASE_GROUP}-r{REPLICA}"
LABEL = f"{QUEUE_NAME}#r{REPLICA}"


async def main() -> None:
    print(
        f"[{LABEL}] starting — type={QTYPE} subject={SUBJECT} stream={STREAM} group={GROUP}",
        flush=True,
    )
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()

    # Ensure the stream exists — `orion queue pub` does the same. Idempotent.
    try:
        await js.stream_info(STREAM)
    except NotFoundError:
        await js.add_stream(StreamConfig(name=STREAM, subjects=[SUBJECT]))
        print(f"[{LABEL}] created stream {STREAM}", flush=True)

    consumer = await js.pull_subscribe(
        subject=SUBJECT,
        durable=GROUP,
        stream=STREAM,
        config=ConsumerConfig(ack_policy=AckPolicy.EXPLICIT),
    )
    print(f"[{LABEL}] bound to durable={GROUP}", flush=True)

    while True:
        try:
            msgs = await consumer.fetch(batch=1, timeout=10)
        except asyncio.TimeoutError:
            continue
        for m in msgs:
            try:
                payload = m.data.decode("utf-8", errors="replace")
                try:
                    row = json.loads(payload)
                except json.JSONDecodeError:
                    row = {"_raw": payload}
                row.setdefault("_subject", m.subject)
                handle(row)
                await m.ack()
            except Exception as e:  # noqa: BLE001
                # nak and let JetStream redeliver; another worker can try.
                print(f"[{LABEL}] handler error: {e!r} — naking", file=sys.stderr, flush=True)
                try:
                    await m.nak()
                except Exception:  # noqa: BLE001
                    pass


if __name__ == "__main__":
    asyncio.run(main())
