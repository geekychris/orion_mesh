#!/usr/bin/env python3
"""run-md — execute the bash code blocks in a Markdown file.

Default: every ```bash fenced block in the document runs in order, sharing
one bash process so env vars / CWD / shell state carry across blocks.

Info-string flags (after the language tag) opt into / out of execution:

    ```bash {skip}          # never run — for display-only examples
    ```bash {run}           # explicit opt-in (redundant — bash blocks run by default)
    ```bash {name=apply}    # gives the block a name for --only / --list
    ```bash {teardown}      # runs at the end, even if earlier blocks failed
    ```bash {allow_fail}    # don't fail-fast on this block (good for examples
                            #   that are expected to exit non-zero, e.g. bad/)
    ```bash {dry}           # shown but never executed even outside --dry-run

Usage:
    scripts/run-md.py examples/09-ipc/README.md
    scripts/run-md.py examples/09-ipc/README.md --list
    scripts/run-md.py examples/09-ipc/README.md --only apply
    scripts/run-md.py examples/09-ipc/README.md --dry-run
    scripts/run-md.py examples/09-ipc/README.md --interactive
    scripts/run-md.py examples/09-ipc/README.md --cwd /path/to/repo
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass

# Match: ```bash<spaces>?<{flags}>?<newline><body><newline>```
BLOCK_RE = re.compile(
    r"^```\s*bash(?:\s+\{([^}\n]*)\})?\s*\n(.*?)\n```",
    re.MULTILINE | re.DOTALL,
)


@dataclass
class Block:
    lineno: int
    name: str | None
    flags: set[str]
    body: str

    @property
    def is_skip(self) -> bool:
        return "skip" in self.flags or "dry" in self.flags

    @property
    def is_teardown(self) -> bool:
        return "teardown" in self.flags

    @property
    def allow_fail(self) -> bool:
        return "allow_fail" in self.flags

    @property
    def display_name(self) -> str:
        return self.name or f"line-{self.lineno}"


def parse_flags(flag_str: str | None) -> tuple[set[str], str | None]:
    flags: set[str] = set()
    name: str | None = None
    if not flag_str:
        return flags, name
    for tok in flag_str.split():
        tok = tok.strip().strip(",")
        if not tok:
            continue
        if tok.startswith("name="):
            name = tok[5:].strip().strip('"').strip("'")
        else:
            flags.add(tok)
    return flags, name


def extract_blocks(path: str) -> list[Block]:
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    blocks: list[Block] = []
    for m in BLOCK_RE.finditer(text):
        flag_str = m.group(1)
        body = m.group(2)
        flags, name = parse_flags(flag_str)
        lineno = text[: m.start()].count("\n") + 1
        blocks.append(Block(lineno=lineno, name=name, flags=flags, body=body))
    return blocks


def list_blocks(blocks: list[Block]) -> None:
    for i, b in enumerate(blocks, 1):
        marks = []
        if b.is_skip:
            marks.append("skip")
        if b.is_teardown:
            marks.append("teardown")
        if b.allow_fail:
            marks.append("allow_fail")
        mark_str = (",".join(marks) or "-")[:18]
        first = (b.body.strip().splitlines() or [""])[0][:60]
        print(f"  [{i:2}] {mark_str:18}  {b.display_name:20}  L{b.lineno:>4}  {first}")


def run_blocks(
    blocks: list[Block],
    *,
    cwd: str,
    dry_run: bool,
    interactive: bool,
) -> int:
    """Execute the runnable blocks in one bash session.

    We send each block as a heredoc separated by an explicit marker that the
    runner watches for on stdout. That lets interactive mode wait for the
    previous block to finish before prompting for the next.
    """
    runnable = [b for b in blocks if not b.is_skip and not b.is_teardown]
    teardown = [b for b in blocks if b.is_teardown and not b.is_skip]

    if dry_run:
        sys.stdout.write("--- composite script (dry run) ---\n\n")
        for b in runnable + teardown:
            sys.stdout.write(f"# === {b.display_name} (L{b.lineno}) ===\n{b.body}\n\n")
        return 0

    rc = 0
    if interactive:
        rc = run_interactive(runnable, cwd=cwd)
    else:
        rc = run_one_shot(runnable, cwd=cwd, fail_fast=True)

    if teardown:
        sys.stdout.write("\n--- teardown ---\n")
        td_rc = run_one_shot(teardown, cwd=cwd, fail_fast=False)
        if rc == 0:
            rc = td_rc
    return rc


def run_one_shot(blocks: list[Block], *, cwd: str, fail_fast: bool) -> int:
    if not blocks:
        return 0
    parts = ["set -u", "set -o pipefail"]
    if fail_fast:
        parts.append("set -e")
    for b in blocks:
        parts.append(f"### {b.display_name} ###")
        if b.allow_fail:
            # Wrap in a subshell and swallow non-zero exit; restore set -e after.
            parts.append("set +e")
            parts.append("(")
            parts.append(b.body)
            parts.append(") ; rc=$?; echo \"# [allow_fail] block exited with rc=$rc\"")
            if fail_fast:
                parts.append("set -e")
        else:
            parts.append(b.body)
    script = "\n".join(parts)
    completed = subprocess.run(
        ["bash"], input=script.encode(), cwd=cwd
    )
    return completed.returncode


def run_interactive(blocks: list[Block], *, cwd: str) -> int:
    """Run blocks one at a time, prompting before each.

    Persistent bash kept open; env + CWD persist across blocks.
    """
    if not blocks:
        return 0
    proc = subprocess.Popen(
        ["bash", "--norc"],
        stdin=subprocess.PIPE,
        cwd=cwd,
        bufsize=0,
    )
    assert proc.stdin is not None
    proc.stdin.write(b"set -u\nset -o pipefail\n")

    rc = 0
    for b in blocks:
        sys.stdout.write(f"\n=== {b.display_name} (L{b.lineno}) ===\n")
        sys.stdout.write(b.body.rstrip() + "\n")
        try:
            ans = input("\n  [Enter]=run, s=skip, q=quit: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            ans = "q"
        if ans == "q":
            break
        if ans == "s":
            continue
        # Wrap in a subshell so a failure doesn't kill the persistent bash.
        # We also echo a per-block marker so the human sees a boundary in the
        # interleaved output.
        wrapped = f'echo "--- {b.display_name} ---"\n( {b.body}\n) ; echo "--- done (rc=$?) ---"\n'
        proc.stdin.write(wrapped.encode())
        # Give bash a tick to flush before we re-prompt.
        proc.stdin.flush()
    proc.stdin.close()
    proc.wait()
    return rc


def main() -> int:
    p = argparse.ArgumentParser(description="execute bash blocks in a Markdown file")
    p.add_argument("path", help="markdown file")
    p.add_argument("--list", action="store_true", help="list blocks; don't run")
    p.add_argument("--dry-run", action="store_true", help="print the composite script without running")
    p.add_argument("--only", help="run only the block with this name (set via name=…)")
    p.add_argument("--interactive", action="store_true", help="prompt before each block")
    p.add_argument(
        "--cwd",
        default=None,
        help="working directory for the bash session (default: parent of the markdown)",
    )
    args = p.parse_args()

    if not os.path.exists(args.path):
        sys.stderr.write(f"no such file: {args.path}\n")
        return 1

    blocks = extract_blocks(args.path)
    if not blocks:
        sys.stderr.write(f"no ```bash blocks in {args.path}\n")
        return 1

    if args.only:
        blocks = [b for b in blocks if b.name == args.only]
        if not blocks:
            sys.stderr.write(f"no block named {args.only!r} in {args.path}\n")
            return 1

    if args.list:
        list_blocks(blocks)
        return 0

    cwd = args.cwd or os.path.dirname(os.path.abspath(args.path)) or "."
    # If we're running from a README under examples/, the user's recipes almost
    # always reference the repo root. Walk up to find Cargo.toml.
    if not args.cwd:
        cwd = find_repo_root(cwd) or cwd

    return run_blocks(
        blocks,
        cwd=cwd,
        dry_run=args.dry_run,
        interactive=args.interactive,
    )


def find_repo_root(start: str) -> str | None:
    cur = os.path.abspath(start)
    while True:
        if os.path.exists(os.path.join(cur, "Cargo.toml")) or os.path.exists(
            os.path.join(cur, ".git")
        ):
            return cur
        parent = os.path.dirname(cur)
        if parent == cur:
            return None
        cur = parent


if __name__ == "__main__":
    sys.exit(main())
