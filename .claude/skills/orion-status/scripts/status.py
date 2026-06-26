#!/usr/bin/env python3
"""orion-status — at-a-glance OrionMesh cluster summary.

Prints live nodes (with inventory), resource counts per kind, and Schedule
observation state. Uses only Python's stdlib so it works on any Python ≥ 3.6.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

CTRL = os.environ.get("ORION_CONTROLLER_URL", "http://127.0.0.1:7878")
TOKEN = os.environ.get("ORION_CLUSTER_TOKEN")


def req(method: str, path: str):
    url = CTRL + path
    headers = {}
    if TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"
    request = urllib.request.Request(url, method=method, headers=headers)
    with urllib.request.urlopen(request, timeout=10) as r:
        return json.loads(r.read().decode())


def fmt_bytes(b):
    if not b:
        return "–"
    units = ["B", "KB", "MB", "GB", "TB"]
    n, i = float(b), 0
    while n >= 1024 and i < len(units) - 1:
        n /= 1024
        i += 1
    return f"{n:.1f} {units[i]}"


def main() -> int:
    p = argparse.ArgumentParser(description="OrionMesh cluster status")
    p.add_argument("--json", action="store_true", help="raw JSON output")
    p.add_argument("--kinds", default="", help="comma-separated kinds to count (default: all populated)")
    args = p.parse_args()

    try:
        nodes = req("GET", "/v1/nodes")
        kinds_resp = req("GET", "/v1/kinds")
        schedules = req("GET", "/v1/schedules/observed")
    except urllib.error.URLError as e:
        sys.stderr.write(f"controller unreachable at {CTRL}: {e}\n")
        sys.stderr.write("hint: start it with `cargo run -p orion-controller` "
                         "(see docs/installation.md §6)\n")
        return 2

    kinds_to_check = (args.kinds.split(",") if args.kinds else kinds_resp.get("kinds", []))
    counts = {}
    for kind in kinds_to_check:
        try:
            rows = req("GET", f"/v1/resources/{kind}")
            if rows:
                counts[kind] = len(rows)
        except urllib.error.URLError:
            pass

    if args.json:
        print(json.dumps({"nodes": nodes, "counts": counts, "schedules": schedules},
                         indent=2, sort_keys=True))
        return 0

    print(f"Controller {CTRL}")
    print()
    print(f"Nodes ({len(nodes)})")
    if not nodes:
        print("  (no agents reporting)")
    for n in nodes:
        inv = n.get("inventory") or {}
        bits = []
        if inv.get("arch"):
            bits.append(f"{inv['arch']}/{inv.get('os','?')}")
        if inv.get("cpu_cores"):
            bits.append(f"{inv['cpu_cores']} cores")
        if inv.get("mem_total_bytes"):
            bits.append(fmt_bytes(inv["mem_total_bytes"]))
        if inv.get("runtimes"):
            bits.append("[" + ",".join(inv["runtimes"]) + "]")
        meta = " · ".join(bits) if bits else "(no inventory yet)"
        print(f"  {n['node_id']:24} v{n['agent_version']:8} {meta}")
        print(f"    last seen {n['last_seen_at']}")
    print()

    print("Resources")
    if not counts:
        print("  (none applied yet)")
    for kind, n in sorted(counts.items()):
        print(f"  {kind:14} {n}")
    print()

    print(f"Schedules ({len(schedules)})")
    if not schedules:
        print("  (no Schedule resources, or controller hasn't ticked them yet)")
    for name, obs in schedules.items():
        fired = obs.get("fire_count", 0)
        last = obs.get("last_fired_at") or "never"
        nxt = obs.get("next_fire_at") or "?"
        err = obs.get("last_error")
        line = f"  {name:24} fired {fired}×  last={last}  next={nxt}"
        if err:
            line += f"  ERROR={err}"
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
