"""End-to-end tests that need a live OrionMesh stack.

Skipped by default; pass `--run-integration` to opt in. The runner is
expected to have:
  - controller at $ORION_CONTROLLER_URL (or http://127.0.0.1:7878)
  - nats-server at $NATS_URL (or nats://127.0.0.1:4222)
  - ORION_AUTH_DISABLED=1 (or a token in $ORION_CLUSTER_TOKEN)
"""
from __future__ import annotations

import time

import pytest

from orion_mesh import Client


pytestmark = pytest.mark.integration


def _wait_for_controller(c, attempts=20):
    for _ in range(attempts):
        if c.health():
            return
        time.sleep(0.3)
    raise RuntimeError("controller not reachable")


def test_full_roundtrip_apply_dispatch_logs():
    with Client() as c:
        _wait_for_controller(c)
        # Apply a tiny Service.
        c.apply(
            """
apiVersion: orionmesh.dev/v1
kind: Service
metadata: { name: py-test-svc }
spec:
  runtime: { kind: native, exec: /bin/sh, args: ["-c", "echo from-python-test; sleep 1"] }
"""
        )
        out = c.dispatch("Service", "py-test-svc")
        assert "instance_id" in out
        time.sleep(2)
        logs = c.logs("Service", "py-test-svc")
        lines = [e["line"] for e in logs.get("entries", [])]
        assert any("from-python-test" in l for l in lines)
        assert c.delete("Service", "py-test-svc") is True


def test_queue_pub_sub_round_trips_a_message():
    with Client() as c:
        _wait_for_controller(c)
        c.apply("apiVersion: orionmesh.dev/v1\nkind: Queue\nmetadata: { name: py-roundtrip }\nspec: { type: work }\n")
        q = c.queue("py-roundtrip")
        seq = q.pub({"hello": "world", "n": 42})
        assert isinstance(seq, int)
        seq2 = q.pub({"n": 43})
        assert seq2 > seq
        rows = list(q.sub(group="py-test", limit=2))
        assert len(rows) == 2
        ns = sorted(r.get("n") for r in rows)
        assert ns == [42, 43]
        c.delete("Queue", "py-roundtrip")
