"""Pure-Python producer for the 14-python-client walkthrough."""
from __future__ import annotations

import argparse

from orion_mesh import Client


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--queue", default="events")
    p.add_argument("--count", type=int, default=20)
    p.add_argument("--prefix", default="hello")
    args = p.parse_args()

    c = Client()
    c.apply(f"""
apiVersion: orionmesh.dev/v1
kind: Queue
metadata: {{ name: {args.queue} }}
spec: {{ type: work, max_age_seconds: 3600 }}
""")
    q = c.queue(args.queue)
    n = q.pub_many({"i": i, "msg": f"{args.prefix}-{i}"} for i in range(args.count))
    print(f"published {n} messages to {q.subject}")
    print(f"queue describe: {c.get('Queue', args.queue).spec}")
    c.close()


if __name__ == "__main__":
    main()
