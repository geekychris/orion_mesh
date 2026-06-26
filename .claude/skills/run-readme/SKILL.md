---
name: run-readme
description: Execute the bash blocks in a Markdown file as a single bash session (env carries across blocks). Use when the user says "run examples/X/README.md", "execute the IPC demo readme", "follow this walkthrough", or hands over a markdown file and wants the recipes inside it to actually run.
---

# run-readme

Drives `scripts/run-md.py` — a tiny pure-stdlib Python tool in the repo that extracts every ` ```bash ` fenced block from a Markdown file and pipes them through `bash` in order.

## How to use

```bash
# Print the runnable blocks (does not execute)
python3 scripts/run-md.py <path/to/README.md> --list

# Execute end-to-end (env carries across blocks so $CTRL etc. persist)
python3 scripts/run-md.py <path/to/README.md>

# Run only one named block
python3 scripts/run-md.py <path/to/README.md> --only apply

# Dry-run: print the composite script without executing
python3 scripts/run-md.py <path/to/README.md> --dry-run

# Step through interactively
python3 scripts/run-md.py <path/to/README.md> --interactive
```

The runner reads info-string flags on the fence:

| Tag | Meaning |
|---|---|
| (none) | Default — block runs in order |
| `{name=apply}` | Names the block; use with `--only` |
| `{skip}` | Never run — for display-only / illustrative blocks |
| `{allow_fail}` | Block may exit non-zero without stopping the run (used for `examples/bad/`) |
| `{teardown}` | Runs at the end, even if earlier blocks failed |
| `{dry}` | Like `{skip}` but shown in `--dry-run` |

## What to do when the user asks to run a README

1. Confirm the path exists and the file has bash blocks (`--list` shows them).
2. If a controller / agent / NATS broker is required, sanity-check that they're up before running — `curl -s $CTRL/health` returns `ok`.
3. Run the script. Show the user the tail of the output (the runner is chatty; truncate to the meaningful lines: success markers, key log entries, the teardown summary).
4. If a block has `{name=X}`, you can offer to run just that one with `--only X` instead of the whole thing.

## Verified working READMEs

These all run end-to-end against the live local stack today:

- `examples/README.md` — build CLI + validate good/bad + apply/dispatch sleeper + teardown
- `examples/bad/README.md` — every validation case (`{allow_fail}` so they don't stop the run)
- `examples/09-ipc/README.md` — IPC demo with publisher + subscriber + log tail + teardown
- `examples/09-ipc/jetstream/README.md` — durability + load-balanced JetStream demos
- `examples/09-ipc/polyglot/README.md` — Python pub + 2 Java + 3 Rust workers in one queue group

## When NOT to use this skill

- The user just wants to see what's in the README — `Read` the file directly.
- The user wants to author a new example — they need the `scripts/run-md.py` block-tag conventions; show them in the file rather than running.
- The user wants to run a `.sh` script directly — just call `bash <script>` via Bash.

## Exit codes

`scripts/run-md.py` passes through bash's exit code. `--dry-run` and `--list` always exit 0 on success.
