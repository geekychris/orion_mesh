"""Consumer Service for the 14-python-client walkthrough.

Run via:
  target/debug/orion apply -f consumer-service.yaml
  target/debug/orion dispatch Service py-consumer
"""
from __future__ import annotations

import os
from collections import Counter

from orion_mesh import Client


def main():
    queue = os.environ.get("ORION_QUEUE_NAME", "events")
    group = os.environ.get("ORION_QUEUE_GROUP", "py-consumer-workers")

    c = Client()
    q = c.queue(queue)

    counts: Counter = Counter()
    for row in q.sub(group=group):
        msg = row.get("msg", "?")
        basename = msg.split("-", 1)[0] if isinstance(msg, str) else "?"
        counts[basename] += 1
        print(f"got: {row}  count={dict(counts.most_common(3))}", flush=True)


if __name__ == "__main__":
    main()
