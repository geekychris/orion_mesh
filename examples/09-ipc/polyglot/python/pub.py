#!/usr/bin/env python3
"""Python NATS publisher — interoperates with the Rust + Java demos.

Publishes "tick N from <label> at HH:MM:SS.mmm" to a NATS subject every
--interval seconds. Reads ORION_REPLICA_INDEX (set by the OrionMesh agent
when launched as one of N replicas) to self-identify.

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
from datetime import datetime, timezone

try:
    import nats
except ImportError:
    sys.stderr.write("nats-py not installed. Run: pip install nats-py\n")
    sys.exit(1)


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--nats-url", default=os.environ.get("NATS_URL", "nats://127.0.0.1:4222"))
    p.add_argument("--subject", default="orion.demo.ipc")
    p.add_argument("--interval", type=float, default=1.0)
    p.add_argument("--label", default=None)
    args = p.parse_args()

    label = args.label or f"r{os.environ.get('ORION_REPLICA_INDEX')}" if os.environ.get("ORION_REPLICA_INDEX") else (args.label or "py")
    print(f"[py-pub:{label}] connecting to {args.nats_url} -> {args.subject}", flush=True)
    nc = await nats.connect(args.nats_url)
    print(f"[py-pub:{label}] connected", flush=True)
    i = 0
    try:
        while True:
            i += 1
            ts = datetime.now(timezone.utc).strftime("%H:%M:%S.") + f"{datetime.now().microsecond // 1000:03d}"
            line = f"tick {i} from {label} at {ts}"
            await nc.publish(args.subject, line.encode())
            print(f"[py-pub:{label}] sent: {line}", flush=True)
            await asyncio.sleep(args.interval)
    finally:
        await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
