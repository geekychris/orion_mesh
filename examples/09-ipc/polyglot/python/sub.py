#!/usr/bin/env python3
"""Python NATS subscriber — interoperates with the Rust + Java demos.

Two modes:
  default                  fan-out (every subscriber gets every message)
  --queue-group <name>     load-balanced (one subscriber in the group gets each)

Setup:
  python3 -m venv .venv
  . .venv/bin/activate
  pip install nats-py
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

try:
    import nats
except ImportError:
    sys.stderr.write("nats-py not installed. Run: pip install nats-py\n")
    sys.exit(1)


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--nats-url", default=os.environ.get("NATS_URL", "nats://127.0.0.1:4222"))
    p.add_argument("--subject", default="orion.demo.ipc")
    p.add_argument("--queue-group", default=None)
    p.add_argument("--label", default=None)
    args = p.parse_args()

    label = args.label or (
        f"r{os.environ.get('ORION_REPLICA_INDEX')}"
        if os.environ.get("ORION_REPLICA_INDEX")
        else "py"
    )
    mode = f"queue-group '{args.queue_group}'" if args.queue_group else "fan-out (no queue group)"
    print(f"[py-sub:{label}] connecting to {args.nats_url} -> {args.subject} ({mode})", flush=True)
    nc = await nats.connect(args.nats_url)
    print(f"[py-sub:{label}] connected", flush=True)

    async def handler(msg):
        body = msg.data.decode("utf-8", errors="replace")
        print(f"[py-sub:{label}] recv: {body} (subject={msg.subject})", flush=True)

    if args.queue_group:
        await nc.subscribe(args.subject, queue=args.queue_group, cb=handler)
    else:
        await nc.subscribe(args.subject, cb=handler)
    print(f"[py-sub:{label}] subscribed", flush=True)

    # Stay alive until killed
    await asyncio.Event().wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
