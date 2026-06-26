#!/usr/bin/env python3
"""orion-diag — comprehensive diagnostic snapshot of the OrionMesh stack."""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

CTRL = os.environ.get("ORION_CONTROLLER_URL", "http://127.0.0.1:7878")
TOKEN = os.environ.get("ORION_CLUSTER_TOKEN")


def req(path: str):
    url = CTRL + path
    headers = {}
    if TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"
    r = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(r, timeout=10) as resp:
        return json.loads(resp.read().decode())


def print_system(d):
    ctrl = d.get("controller", {})
    up_min = (ctrl.get("uptime_seconds", 0) or 0) // 60
    print("=== controller ===")
    print(f"  version          v{ctrl.get('version')}")
    print(f"  uptime           {up_min}m  (started {ctrl.get('started_at')})")
    print(f"  auth             {'disabled (dev)' if ctrl.get('auth_disabled') else 'enforced'}")
    print(f"  nats             {ctrl.get('nats_url')}")
    nats = d.get("nats", {})
    nats_ok = "✓ connected" if nats.get("connected") else "✗ disconnected"
    print(f"  nats status      {nats_ok}")
    if nats.get("monitoring_url"):
        print(f"  nats monitoring  {nats['monitoring_url']}/jsz")

    nodes = d.get("nodes", [])
    print(f"\n=== nodes ({len(nodes)}, {d.get('agents', 0)} live) ===")
    for n in nodes:
        secs = n.get("seconds_since_seen", 0)
        marker = "✓" if secs < 30 else "(stale)"
        print(f"  {n['node_id']:14}  v{n['agent_version']:8}  {marker}  last seen {secs}s ago")

    inst = d.get("instances", {})
    print(f"\n=== instances ({inst.get('total', 0)} total) ===")
    for w in inst.get("by_workload", []) or []:
        print(f"  {w['kind']}/{w['name']:24}  ×{w['instance_count']}")
    if not inst.get("by_workload"):
        print("  (none — dispatch a Service or Task)")

    sched = d.get("schedules", {})
    print(f"\n=== schedules ===")
    print(f"  armed            {sched.get('armed', 0)}")
    print(f"  fired (total)    {sched.get('fired_total', 0)}")

    logs = d.get("logs", {})
    print(f"\n=== log buffer ===")
    print(f"  lines            {logs.get('buffered_lines', 0)}")
    print(f"  workloads        {logs.get('workloads_with_logs', 0)} with captured logs")


def print_jetstream(d):
    if not d.get("streams"):
        print("=== JetStream ===")
        print(f"  (no streams; monitoring={d.get('monitoring_url')})")
        return
    print(f"=== JetStream streams ({len(d['streams'])}) ===")
    for s in d["streams"]:
        subs = ",".join(s.get("subjects", []))
        print(f"  {s['name']:22}  msgs={s['messages']}  first={s['first_seq']}  last={s['last_seq']}  cons={s['consumer_count']}  subjects={subs}")
    print(f"\n=== JetStream consumers ({len(d.get('consumers', []))}) ===")
    for c in d.get("consumers", []):
        lag = c["delivered"] - c["last_ack_floor"]
        lag_str = f" lag={lag}" if lag > 0 else ""
        print(f"  {c['stream']}/{c['name']:18}  pending={c['num_pending']}  ack_pending={c['num_ack_pending']}  delivered_seq={c['delivered']}{lag_str}")


def print_instances(rows):
    print(f"=== instances ({len(rows)}) ===")
    if not rows:
        print("  (none tracked)")
        return
    print(f"  {'kind/name':30}  {'r':>3}  {'instance':10}  {'node':14}  lines  first→last")
    for r in rows:
        iid = r["instance_id"][:8]
        node = r.get("node") or "—"
        first = (r.get("first_seen_at") or "")[11:19] or "—"
        last  = (r.get("last_seen_at")  or "")[11:19] or "—"
        print(f"  {r['kind']+'/'+r['name']:30}  r{r['replica_index']:<2}  {iid:10}  {node:14}  {r['line_count']:>5}  {first}→{last}")


def main() -> int:
    p = argparse.ArgumentParser(description="OrionMesh diagnostics")
    p.add_argument(
        "--section",
        choices=["all", "system", "jetstream", "instances"],
        default="all",
    )
    p.add_argument("--search", help="substring search across log buffer")
    p.add_argument("--json", action="store_true")
    args = p.parse_args()

    if args.search:
        try:
            hits = req(f"/v1/logs/search?q={args.search}&limit=100")
        except urllib.error.URLError as e:
            sys.stderr.write(f"controller unreachable at {CTRL}: {e}\n")
            return 2
        if args.json:
            print(json.dumps(hits, indent=2))
            return 0
        print(f"=== log search '{args.search}' — {len(hits)} matches ===")
        for h in hits:
            print(f"  [{h['at'][11:19]}] {h['kind']}/{h['name']:24} {h['stream']:6} {h['line']}")
        return 0

    try:
        out = {}
        if args.section in ("all", "system"):
            out["system"] = req("/v1/diag/system")
        if args.section in ("all", "jetstream"):
            out["jetstream"] = req("/v1/diag/jetstream")
        if args.section in ("all", "instances"):
            out["instances"] = req("/v1/instances")
    except urllib.error.URLError as e:
        sys.stderr.write(f"controller unreachable at {CTRL}: {e}\n")
        return 2

    if args.json:
        print(json.dumps(out, indent=2))
        return 0

    if "system" in out:
        print_system(out["system"])
        print()
    if "jetstream" in out:
        print_jetstream(out["jetstream"])
        print()
    if "instances" in out:
        print_instances(out["instances"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
