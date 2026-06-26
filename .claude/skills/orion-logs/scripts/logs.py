#!/usr/bin/env python3
"""orion-logs — tail or follow logs of an OrionMesh Service or Task."""
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


def req(method, path):
    url = CTRL + path
    headers = {}
    if TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"
    r = urllib.request.Request(url, method=method, headers=headers)
    with urllib.request.urlopen(r, timeout=10) as resp:
        return resp.status, json.loads(resp.read().decode())


def fmt_line(e, use_color: bool) -> str:
    ts = e["at"][11:19]
    stream = e["stream"]
    line = e["line"]
    if not use_color:
        return f"[{ts}] {stream:6} {line}"
    if stream == "stderr":
        return f"\033[90m[{ts}]\033[0m \033[31m{stream:6}\033[0m {line}"
    return f"\033[90m[{ts}]\033[0m \033[36m{stream:6}\033[0m {line}"


def main() -> int:
    p = argparse.ArgumentParser(description="Tail/follow logs for an OrionMesh workload")
    p.add_argument("target", help="<Kind>/<Name>, e.g. Service/demo-pub")
    p.add_argument("-f", "--follow", action="store_true", help="poll forever (until Ctrl-C)")
    p.add_argument("--tail", type=int, default=50, help="number of recent lines on first read (default 50)")
    p.add_argument("--since", type=int, default=None, help="start from a specific sequence")
    p.add_argument("--json", dest="json_out", action="store_true")
    p.add_argument("--no-color", action="store_true")
    args = p.parse_args()

    if "/" not in args.target:
        sys.stderr.write("target must be <Kind>/<Name>\n")
        return 1
    kind, name = args.target.split("/", 1)

    use_color = (not args.no_color) and sys.stdout.isatty()
    since = 0
    try:
        status, body = req("GET", f"/v1/logs/{kind}/{name}?since=0")
    except urllib.error.URLError as e:
        sys.stderr.write(f"controller unreachable at {CTRL}: {e}\n")
        return 2

    entries = body.get("entries", [])
    if args.since is not None:
        # start emitting from this seq; trim earlier
        entries = entries[args.since:]
        since = args.since + len(entries)
    else:
        # show the last `--tail` entries
        if len(entries) > args.tail:
            entries = entries[-args.tail:]
        since = body.get("total", 0)

    if args.json_out:
        print(json.dumps(body))
    else:
        for e in entries:
            print(fmt_line(e, use_color))

    if not args.follow:
        return 0

    try:
        while True:
            time.sleep(1.0)
            try:
                status, body = req("GET", f"/v1/logs/{kind}/{name}?since={since}")
            except urllib.error.URLError as e:
                sys.stderr.write(f"controller unreachable: {e}\n")
                return 2
            for e in body.get("entries", []):
                if args.json_out:
                    print(json.dumps(e))
                else:
                    print(fmt_line(e, use_color))
            since = body.get("total", since)
    except KeyboardInterrupt:
        return 130

    return 0


if __name__ == "__main__":
    sys.exit(main())
