#!/usr/bin/env python3
"""orion-manage — list/get/delete OrionMesh resources."""
from __future__ import annotations

import argparse
import json
import os
import sys
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
    try:
        with urllib.request.urlopen(r, timeout=10) as resp:
            raw = resp.read().decode()
            return resp.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def cmd_list(args) -> int:
    if args.all_kinds:
        s, kinds = req("GET", "/v1/kinds")
        if s >= 400:
            sys.stderr.write(f"failed [{s}]: {kinds}\n")
            return 2
        for k in kinds.get("kinds", []):
            s, rows = req("GET", f"/v1/resources/{k}")
            if s >= 400 or not rows:
                continue
            print(f"== {k} ({len(rows)}) ==")
            for r in rows:
                _print_row(k, r)
            print()
        return 0

    if not args.kind:
        sys.stderr.write("provide a kind, or pass `--all`\n")
        return 1
    s, rows = req("GET", f"/v1/resources/{args.kind}")
    if s >= 400:
        sys.stderr.write(f"failed [{s}]: {rows}\n")
        return 2
    if not rows:
        print(f"(no {args.kind} resources)")
        return 0
    for r in rows:
        _print_row(args.kind, r)
    return 0


def _print_row(kind: str, r: dict):
    name = r["metadata"]["name"]
    spec = r.get("spec", {})
    summary = ""
    if kind == "Service" or kind == "Task":
        rt = (spec.get("runtime") or {}).get("kind", "?")
        summary = f"runtime={rt}"
        if kind == "Service":
            rep = spec.get("replicas", 1)
            summary += f" replicas={rep}"
    elif kind == "Schedule":
        summary = f"cron='{spec.get('cron','?')}' task={spec.get('task') or '(inline)'}"
    elif kind == "Dataset":
        summary = f"locations={len(spec.get('locations') or [])}"
    elif kind == "Model":
        summary = f"variants={len(spec.get('variants') or [])}"
    elif kind == "Runtime":
        summary = f"kind={spec.get('runtime_kind')} url={spec.get('base_url')}"
    print(f"  {name:30}  {summary}")


def cmd_get(args) -> int:
    if "/" not in args.target:
        sys.stderr.write("target must be <Kind>/<Name>\n")
        return 1
    kind, name = args.target.split("/", 1)
    s, body = req("GET", f"/v1/resources/{kind}/{name}")
    if s == 404:
        sys.stderr.write(f"not found: {kind}/{name}\n")
        return 3
    if s >= 400:
        sys.stderr.write(f"failed [{s}]: {body}\n")
        return 2
    print(json.dumps(body, indent=2, sort_keys=True))
    return 0


def cmd_delete(args) -> int:
    if args.prefix:
        # When --prefix is given, the single positional argparse parsed as `target`
        # is actually the kind.
        kind = args.kind or args.target
        if not kind:
            sys.stderr.write("`--prefix` requires a kind, e.g. `manage delete --prefix demo- Service`\n")
            return 1
        s, rows = req("GET", f"/v1/resources/{kind}")
        if s >= 400:
            sys.stderr.write(f"list failed [{s}]: {rows}\n")
            return 2
        matches = [r["metadata"]["name"] for r in rows if r["metadata"]["name"].startswith(args.prefix)]
        if not matches:
            print(f"(no {kind} resources match prefix '{args.prefix}')")
            return 0
        if not args.yes:
            sys.stderr.write(f"About to delete {len(matches)} resource(s):\n")
            for m in matches:
                sys.stderr.write(f"  {kind}/{m}\n")
            sys.stderr.write("Proceed? [y/N] ")
            sys.stderr.flush()
            if input().strip().lower() not in ("y", "yes"):
                return 130
        for name in matches:
            s, body = req("DELETE", f"/v1/resources/{kind}/{name}")
            if s >= 400:
                sys.stderr.write(f"  FAIL {kind}/{name}: {body}\n")
            else:
                print(f"  deleted {kind}/{name}")
        return 0

    if not args.target or "/" not in args.target:
        sys.stderr.write("provide <Kind>/<Name> or `--prefix <p> <Kind>`\n")
        return 1
    kind, name = args.target.split("/", 1)
    if not args.yes:
        sys.stderr.write(f"Delete {kind}/{name}? [y/N] ")
        sys.stderr.flush()
        if input().strip().lower() not in ("y", "yes"):
            return 130
    s, body = req("DELETE", f"/v1/resources/{kind}/{name}")
    if s == 404:
        sys.stderr.write(f"not found: {kind}/{name}\n")
        return 3
    if s >= 400:
        sys.stderr.write(f"failed [{s}]: {body}\n")
        return 2
    print(f"deleted {kind}/{name}")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="OrionMesh resource CRUD")
    sub = p.add_subparsers(dest="cmd", required=True)

    pl = sub.add_parser("list", help="list resources")
    pl.add_argument("kind", nargs="?", help="resource kind (Service, Task, Schedule, …)")
    pl.add_argument("--all", dest="all_kinds", action="store_true", help="list every populated kind")
    pl.set_defaults(handler=cmd_list)

    pg = sub.add_parser("get", help="fetch one resource")
    pg.add_argument("target", help="<Kind>/<Name>")
    pg.set_defaults(handler=cmd_get)

    pd = sub.add_parser("delete", help="delete one or many resources")
    pd.add_argument("target", nargs="?", help="<Kind>/<Name>")
    pd.add_argument("kind", nargs="?", help="(when using --prefix)")
    pd.add_argument("--prefix", help="delete every resource whose name starts with this prefix")
    pd.add_argument("--yes", action="store_true", help="skip confirmation prompt")
    pd.set_defaults(handler=cmd_delete)

    args = p.parse_args()
    try:
        return args.handler(args)
    except urllib.error.URLError as e:
        sys.stderr.write(f"controller unreachable at {CTRL}: {e}\n")
        return 2


if __name__ == "__main__":
    sys.exit(main())
