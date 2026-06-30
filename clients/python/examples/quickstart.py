"""End-to-end quickstart using the OrionMesh Python client.

Run against a live local stack:

    cd ~/code/orion_mesh
    orion up &
    pip install -e clients/python
    python clients/python/examples/quickstart.py

What it does:
    1. Declare a Queue resource
    2. Declare a Service that runs this same file with --consume to drain
    3. Publish 5 messages
    4. Subscribe locally and print them
    5. Clean up
"""
from __future__ import annotations

import argparse
import sys
import time

from orion_mesh import Client


QUEUE = "py-quickstart"
SERVICE = "py-quickstart-consumer"


def main(consume_mode: bool) -> None:
    c = Client()
    if not c.health():
        sys.exit("controller not reachable — start `orion up &` first")

    if consume_mode:
        # Used by the spawned-by-controller Service that processes messages.
        q = c.queue(QUEUE)
        for row in q.sub(group="py-quickstart-workers"):
            print(f"[consumer] processed {row}", flush=True)
        return

    print(f"declaring Queue/{QUEUE} ...")
    c.apply(
        f"""
apiVersion: orionmesh.dev/v1
kind: Queue
metadata: {{ name: {QUEUE} }}
spec:
  type: work
  max_age_seconds: 600
"""
    )

    q = c.queue(QUEUE)
    print(f"publishing 5 messages to {q.subject} ...")
    for i in range(5):
        seq = q.pub({"n": i, "msg": f"hello-{i}"})
        print(f"  seq={seq}")

    print("subscribing locally for those 5 rows ...")
    for row in q.sub(group="py-local-reader", limit=5):
        print(f"  got: {row}")

    print("cleaning up ...")
    c.delete("Queue", QUEUE)
    c.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--consume", action="store_true")
    args = parser.parse_args()
    main(args.consume)
