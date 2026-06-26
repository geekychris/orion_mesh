#!/usr/bin/env python3
"""Python JetStream subscriber — interoperates with the Rust + Java JS demos.

Auto-creates the stream + a durable pull consumer; replays from last ack on
restart. Multiple subs sharing the same --durable name share the load.

Setup:
  bash setup.sh   # creates .venv, installs nats-py
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

try:
    import nats
    from nats.js.api import StreamConfig, ConsumerConfig
except ImportError:
    sys.stderr.write("nats-py not installed. Run: pip install nats-py\n")
    sys.exit(1)


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--nats-url", default=os.environ.get("NATS_URL", "nats://127.0.0.1:4222"))
    p.add_argument("--subject", default="orion.demo.js")
    p.add_argument("--stream", default="ORION_DEMO_JS")
    p.add_argument("--durable", default="workers")
    p.add_argument("--label", default=None)
    args = p.parse_args()

    label = args.label or (
        f"r{os.environ.get('ORION_REPLICA_INDEX')}"
        if os.environ.get("ORION_REPLICA_INDEX")
        else "py-js"
    )

    print(
        f"[py-sub:{label}] connecting to {args.nats_url} -> {args.subject} "
        f"(JetStream stream={args.stream} durable={args.durable})",
        flush=True,
    )
    nc = await nats.connect(args.nats_url)
    js = nc.jetstream()
    print(f"[py-sub:{label}] connected", flush=True)

    subj_wildcard = args.subject if ("*" in args.subject or ">" in args.subject) else f"{args.subject}.>"
    try:
        await js.add_stream(StreamConfig(name=args.stream, subjects=[subj_wildcard]))
    except Exception:
        pass
    print(f"[py-sub:{label}] stream {args.stream} ready", flush=True)

    # Pull subscription using a durable consumer name.
    psub = await js.pull_subscribe(
        f"{args.subject}.>",
        durable=args.durable,
        stream=args.stream,
    )
    print(f"[py-sub:{label}] consumer '{args.durable}' bound", flush=True)

    while True:
        try:
            msgs = await psub.fetch(batch=10, timeout=5)
        except Exception:
            # timeout — keep polling
            continue
        for m in msgs:
            body = m.data.decode("utf-8", errors="replace")
            seq = m.metadata.sequence.stream if m.metadata else 0
            print(f"[py-sub:{label}] recv (seq={seq}): {body} (subject={m.subject})", flush=True)
            await m.ack()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
