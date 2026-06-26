#!/usr/bin/env python3
"""Python JetStream publisher — interoperates with the Rust + Java JS demos.

Auto-creates a JetStream stream covering --subject.> (idempotent).
Publishes each message and prints the JS sequence number.

Setup:
  bash setup.sh   # in this directory; creates .venv, installs nats-py
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timezone

try:
    import nats
    from nats.js.api import StreamConfig
except ImportError:
    sys.stderr.write("nats-py not installed. Run: pip install nats-py\n")
    sys.exit(1)


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--nats-url", default=os.environ.get("NATS_URL", "nats://127.0.0.1:4222"))
    p.add_argument("--subject", default="orion.demo.js")
    p.add_argument("--stream", default="ORION_DEMO_JS")
    p.add_argument("--interval", type=float, default=1.0)
    p.add_argument("--label", default=None)
    args = p.parse_args()

    label = args.label or (
        f"r{os.environ.get('ORION_REPLICA_INDEX')}"
        if os.environ.get("ORION_REPLICA_INDEX")
        else "py-js"
    )

    print(f"[py-pub:{label}] connecting to {args.nats_url} -> {args.subject} (JetStream)", flush=True)
    nc = await nats.connect(args.nats_url)
    js = nc.jetstream()
    print(f"[py-pub:{label}] connected", flush=True)

    subj_wildcard = args.subject if ("*" in args.subject or ">" in args.subject) else f"{args.subject}.>"
    try:
        await js.add_stream(StreamConfig(name=args.stream, subjects=[subj_wildcard]))
    except Exception:
        # already exists with same config — fine
        pass
    print(f"[py-pub:{label}] stream {args.stream} ready (subjects: {subj_wildcard})", flush=True)

    i = 0
    publish_subj = f"{args.subject}.tick"
    try:
        while True:
            i += 1
            ts = datetime.now(timezone.utc).strftime("%H:%M:%S.") + f"{datetime.now().microsecond // 1000:03d}"
            line = f"tick {i} from {label} at {ts}"
            ack = await js.publish(publish_subj, line.encode())
            print(f"[py-pub:{label}] sent (js seq={ack.seq}): {line}", flush=True)
            await asyncio.sleep(args.interval)
    finally:
        await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
