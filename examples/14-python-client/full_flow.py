"""Single-script end-to-end demo: bootstrap a Service, dispatch, tail.

Useful for CI/deployment tools that want to provision an OrionMesh
resource from pure Python without shelling out.
"""
from __future__ import annotations

import time

from orion_mesh import Client


SERVICE_YAML = """
apiVersion: orionmesh.dev/v1
kind: Service
metadata: { name: py-bootstrap }
spec:
  replicas: 1
  restart_policy: on_failure
  runtime:
    kind: native
    exec: /bin/sh
    args: ["-c", "for i in 1 2 3 4 5; do echo bootstrap-line-$i; sleep 0.5; done"]
"""


def main():
    c = Client()
    if not c.health():
        raise SystemExit("controller unreachable")

    print("apply ...")
    c.apply(SERVICE_YAML)

    print("dispatch ...")
    out = c.dispatch("Service", "py-bootstrap")
    print(f"  node={out.get('node')} instance={out.get('instance_id', '')[:8]}")

    # Poll logs until we see "bootstrap-line-5" or 10s elapse.
    print("tailing ...")
    deadline = time.time() + 10
    seen = set()
    while time.time() < deadline:
        resp = c.logs("Service", "py-bootstrap")
        for e in resp.get("entries", []):
            line = e["line"]
            if line not in seen:
                seen.add(line)
                print(f"  [log] {line}")
        if any("line-5" in l for l in seen):
            break
        time.sleep(0.5)

    print("cleanup ...")
    c.delete("Service", "py-bootstrap")
    c.close()
    print("done")


if __name__ == "__main__":
    main()
