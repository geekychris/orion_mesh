#!/usr/bin/env python3
"""orion-schedule — create / apply / list OrionMesh Schedule resources."""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

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
            ct = resp.headers.get("content-type", "")
            raw = resp.read().decode()
            return resp.status, json.loads(raw) if "application/json" in ct else raw
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def list_schedules() -> int:
    try:
        status, obs = req("GET", "/v1/schedules/observed")
        status_r, rows = req("GET", "/v1/resources/Schedule")
    except urllib.error.URLError as e:
        sys.stderr.write(f"controller unreachable at {CTRL}: {e}\n")
        return 2
    if status >= 400:
        sys.stderr.write(f"observe failed [{status}]: {obs}\n")
        return 2
    if not rows:
        print("(no Schedule resources)")
        return 0
    print(f"{'SCHEDULE':20} {'CRON':18} {'TASK':20} {'FIRED':>6}  {'LAST':24} {'NEXT'}")
    for r in rows:
        name = r["metadata"]["name"]
        spec = r.get("spec", {})
        cron = spec.get("cron", "?")
        task = spec.get("task") or "(inline)"
        o = obs.get(name, {})
        fired = o.get("fire_count", 0)
        last = o.get("last_fired_at") or "never"
        nxt = o.get("next_fire_at") or "?"
        print(f"{name:20} {cron:18} {task:20} {fired:>6}  {last:24} {nxt}")
    return 0


def build_yaml(cron: str, task: str, name: str) -> str:
    return (
        "apiVersion: orionmesh.dev/v1\n"
        "kind: Schedule\n"
        f"metadata: {{ name: {name} }}\n"
        f"spec: {{ cron: {json.dumps(cron)}, task: {task} }}\n"
    )


def extract_name(yaml_text: str):
    in_meta = False
    for raw in yaml_text.splitlines():
        line = raw.split("#", 1)[0].rstrip()
        s = line.strip()
        if s.startswith("metadata:"):
            in_meta = True
            if "{" in line:
                inner = line.split("{", 1)[1]
                for kv in inner.split(","):
                    k, _, v = kv.partition(":")
                    if k.strip() == "name":
                        return v.strip(" }").strip('"\'')
            continue
        if in_meta and s.startswith("name:"):
            return s.split(":", 1)[1].strip().strip('"\'')
        if in_meta and line and not line.startswith(" ") and not line.startswith("\t"):
            in_meta = False
    return None


def main() -> int:
    p = argparse.ArgumentParser(description="OrionMesh Schedule helper")
    p.add_argument("path", nargs="?", help="Schedule YAML path, or '-' for stdin")
    p.add_argument("--list", action="store_true", help="list existing Schedule resources + observed state")
    p.add_argument("--cron", help="cron expression (5-field POSIX)")
    p.add_argument("--task", help="name of the Task to fire")
    p.add_argument("--name", help="Schedule resource name (defaults to '<task>-cron')")
    args = p.parse_args()

    if args.list:
        return list_schedules()

    if args.cron or args.task:
        if not (args.cron and args.task):
            sys.stderr.write("`--cron` and `--task` must be given together\n")
            return 1
        name = args.name or (args.task + "-cron")
        yaml_text = build_yaml(args.cron, args.task, name)
    elif args.path:
        yaml_text = sys.stdin.read() if args.path == "-" else open(args.path).read()
    else:
        p.print_help(sys.stderr)
        return 1

    name = extract_name(yaml_text) or args.name or "<unknown>"

    try:
        status, body = req("POST", "/v1/resources/apply", yaml_text, "application/yaml")
    except urllib.error.URLError as e:
        sys.stderr.write(f"controller unreachable at {CTRL}: {e}\n")
        return 2
    if status >= 400:
        sys.stderr.write(f"apply failed [{status}]: {body}\n")
        return 3
    print(f"applied   Schedule/{name} generation={body.get('generation')}")
    print(f"cron will be ticked next at the controller's next 5s heartbeat;")
    print(f"observe with:")
    print(f"  curl {CTRL}/v1/schedules/observed | python3 -m json.tool")
    return 0


if __name__ == "__main__":
    sys.exit(main())
