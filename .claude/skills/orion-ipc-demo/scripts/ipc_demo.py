#!/usr/bin/env python3
"""orion-ipc-demo — end-to-end NATS-IPC demo.

Applies + dispatches the demo-pub / demo-sub Services and prints their
side-by-side log streams for `--duration` seconds.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

CTRL = os.environ.get("ORION_CONTROLLER_URL", "http://127.0.0.1:7878")
TOKEN = os.environ.get("ORION_CLUSTER_TOKEN")


def req(method, path, body=None, content_type="application/json"):
    url = CTRL + path
    data = None
    headers = {}
    if body is not None:
        if isinstance(body, (dict, list)):
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        else:
            data = body.encode() if isinstance(body, str) else body
            headers["Content-Type"] = content_type
    if TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"
    r = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(r, timeout=10) as resp:
            raw = resp.read().decode()
            return resp.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def apply_and_dispatch(yaml_path: Path, kind: str, name: str):
    s, body = req("POST", "/v1/resources/apply", open(yaml_path).read(), "application/yaml")
    if s >= 400:
        raise RuntimeError(f"apply {yaml_path}: {body}")
    s, body = req("POST", f"/v1/dispatch/{kind}/{name}")
    if s >= 400:
        raise RuntimeError(f"dispatch {kind}/{name}: {body}")
    return body


def main() -> int:
    p = argparse.ArgumentParser(description="OrionMesh IPC demo")
    p.add_argument("--duration", type=int, default=8)
    p.add_argument("--cleanup", action="store_true")
    p.add_argument("--examples-dir", default="examples/09-ipc")
    args = p.parse_args()

    ex = Path(args.examples_dir)
    pub_yaml = ex / "demo-pub.yaml"
    sub_yaml = ex / "demo-sub.yaml"
    if not pub_yaml.exists() or not sub_yaml.exists():
        sys.stderr.write(f"YAMLs not found in {ex}/\n")
        sys.stderr.write("hint: run from the orion_mesh repo root, or pass --examples-dir\n")
        return 1

    # Controller reachable?
    try:
        urllib.request.urlopen(CTRL + "/health", timeout=3).read()
    except Exception as e:
        sys.stderr.write(f"controller unreachable at {CTRL}: {e}\n")
        return 2

    # Apply + dispatch — sub FIRST so it's listening.
    try:
        sub_info = apply_and_dispatch(sub_yaml, "Service", "demo-sub")
        time.sleep(0.6)
        pub_info = apply_and_dispatch(pub_yaml, "Service", "demo-pub")
    except RuntimeError as e:
        sys.stderr.write(f"apply/dispatch failed: {e}\n")
        sys.stderr.write("hint: did you `cargo build --release -p orion-demo-bins`?\n")
        return 1

    print(f"sub dispatched on {sub_info.get('node')}; pub dispatched on {pub_info.get('node')}")
    print(f"polling for {args.duration}s …")
    print()

    # Pretty side-by-side header
    print(f"{'=== publisher stdout':45} | === subscriber stdout")
    print(f"{'-'*45} + {'-'*45}")

    pub_since = 0
    sub_since = 0
    last_seen_any = time.time()
    started = time.time()

    while time.time() - started < args.duration:
        s_pub, pub_body = req("GET", f"/v1/logs/Service/demo-pub?since={pub_since}")
        s_sub, sub_body = req("GET", f"/v1/logs/Service/demo-sub?since={sub_since}")
        if s_pub >= 400 or s_sub >= 400:
            sys.stderr.write(f"log fetch failed\n")
            return 2
        pub_new = pub_body.get("entries", [])
        sub_new = sub_body.get("entries", [])
        pub_since = pub_body.get("total", pub_since)
        sub_since = sub_body.get("total", sub_since)
        # Interleave: drain whichever has lines
        n = max(len(pub_new), len(sub_new))
        for i in range(n):
            left = ""
            right = ""
            if i < len(pub_new):
                e = pub_new[i]
                left = f"{e['at'][11:19]} {e['line'][:38]}"
            if i < len(sub_new):
                e = sub_new[i]
                right = f"{e['at'][11:19]} {e['line'][:38]}"
            if left or right:
                last_seen_any = time.time()
            print(f"{left:45} | {right}")
        time.sleep(0.5)

    pub_total = pub_since
    sub_total = sub_since

    print()
    print(f"done — pub emitted {pub_total} lines, sub received {sub_total} lines.")

    if pub_total < 2 or sub_total < 2:
        sys.stderr.write("no messages flowed — is NATS running? "
                         "are the demo-pub/demo-sub binaries built and on PATH or at target/release/?\n")
        return 3

    if args.cleanup:
        for k, n in (("Service", "demo-pub"), ("Service", "demo-sub")):
            req("DELETE", f"/v1/resources/{k}/{n}")
            print(f"deleted {k}/{n} (running processes outlive the resource until the agent restarts)")
    else:
        print("Services left running. Tear down with:")
        print("  curl -X DELETE $CTRL/v1/resources/Service/demo-pub")
        print("  curl -X DELETE $CTRL/v1/resources/Service/demo-sub")
        print("  pkill -f 'orion-demo-' ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
