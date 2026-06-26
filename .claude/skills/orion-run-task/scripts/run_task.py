#!/usr/bin/env python3
"""orion-run-task — apply a Task YAML, dispatch it, watch logs until quiet.

Reads $ORION_CONTROLLER_URL (default http://127.0.0.1:7878) and
$ORION_CLUSTER_TOKEN (optional, sent as Bearer).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

CTRL = os.environ.get("ORION_CONTROLLER_URL", "http://127.0.0.1:7878")
TOKEN = os.environ.get("ORION_CLUSTER_TOKEN")


def req(method: str, path: str, body=None, content_type="application/json"):
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
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def extract_name(yaml_text: str) -> str | None:
    # Quick + crude — no full YAML parse. Looks for `name:` under metadata.
    in_meta = False
    for raw in yaml_text.splitlines():
        line = raw.split("#", 1)[0].rstrip()
        stripped = line.strip()
        if stripped.startswith("metadata:"):
            in_meta = True
            # inline form: metadata: { name: foo }
            if "{" in line:
                inner = line.split("{", 1)[1]
                for kv in inner.split(","):
                    k, _, v = kv.partition(":")
                    if k.strip() == "name":
                        return v.strip(" }").strip('"\'')
            continue
        if in_meta and stripped.startswith("name:"):
            return stripped.split(":", 1)[1].strip().strip('"\'')
        if in_meta and line and not line.startswith(" ") and not line.startswith("\t"):
            in_meta = False
    return None


def main() -> int:
    p = argparse.ArgumentParser(description="Apply + dispatch + tail a Task")
    p.add_argument("path", help="Task YAML path, or '-' to read stdin")
    p.add_argument("--timeout", type=int, default=90, help="seconds to tail logs (default 90)")
    p.add_argument("--quiet", action="store_true")
    p.add_argument("--no-dispatch", action="store_true")
    p.add_argument("--exec", dest="exec_cmd", help="generate the Task on the fly (incompatible with `path`)")
    args = p.parse_args()

    if args.exec_cmd:
        if args.path != "-":
            sys.stderr.write("`--exec` is mutually exclusive with a path argument; pass `-` as path\n")
            return 1
        # Build a one-off Task with a stable demo name.
        name = "adhoc-" + str(int(time.time()))
        yaml_text = (
            "apiVersion: orionmesh.dev/v1\n"
            "kind: Task\n"
            f"metadata: {{ name: {name} }}\n"
            "spec:\n"
            "  runtime:\n"
            "    kind: native\n"
            "    exec: /bin/sh\n"
            f"    args: [\"-c\", {json.dumps(args.exec_cmd)}]\n"
        )
    else:
        yaml_text = sys.stdin.read() if args.path == "-" else open(args.path).read()

    name = extract_name(yaml_text)
    if not name:
        sys.stderr.write("could not find metadata.name in the YAML\n")
        return 1

    # 1. apply
    status, body = req("POST", "/v1/resources/apply", yaml_text, "application/yaml")
    if status >= 400:
        sys.stderr.write(f"apply failed [{status}]: {body}\n")
        return 3
    if not args.quiet:
        print(f"applied   Task/{name} generation={body.get('generation')}")
    if args.no_dispatch:
        return 0

    # 2. dispatch
    status, body = req("POST", f"/v1/dispatch/Task/{name}")
    if status >= 400:
        sys.stderr.write(f"dispatch failed [{status}]: {body}\n")
        return 4
    if not args.quiet:
        node = body.get("node", "?")
        iid = body.get("instance_id", "?")
        short = (iid[:8] + "…") if len(iid) > 8 else iid
        print(f"dispatched on node={node} instance={short}")

    # 3. tail logs until idle for ~3s or timeout hits
    started = time.time()
    seen = 0
    quiet_since = None
    while time.time() - started < args.timeout:
        try:
            status, body = req("GET", f"/v1/logs/Task/{name}?since={seen}")
            if status >= 400:
                sys.stderr.write(f"logs fetch failed: {status} {body}\n")
                break
            for e in body.get("entries", []):
                ts = e["at"][11:19]
                pref = f"[{ts}] " if not args.quiet else ""
                stream = e["stream"]
                line = e["line"]
                if stream == "stderr":
                    sys.stderr.write(f"{pref}{line}\n")
                else:
                    sys.stdout.write(f"{pref}{line}\n")
            new_total = body.get("total", seen)
            if new_total > seen:
                seen = new_total
                quiet_since = None
            else:
                quiet_since = quiet_since or time.time()
                if time.time() - quiet_since > 3.0 and seen > 0:
                    break
        except urllib.error.URLError as e:
            sys.stderr.write(f"controller unreachable: {e}\n")
            return 2
        time.sleep(0.5)

    if not args.quiet:
        last = (body.get("entries") or [{}])[-1].get("at", "")[11:19] if seen else "—"
        print(f"done — {seen} line(s), last at {last}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
