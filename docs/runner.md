# The Markdown runner — `scripts/run-md.py`

A small pure-stdlib Python script that extracts the `` ```bash `` blocks from any Markdown file and runs them in order, sharing one bash process so environment variables, CWD, and shell state carry across blocks.

Every example README in this repo is annotated for it. Running a README from top to bottom is the same as copy-pasting each shell block in order — but it's one command, and it includes a `{teardown}` step that cleans up after itself.

This doc is the single source of truth for the runner. Per-example READMEs link here rather than repeating the tag table.

---

## Usage

```bash
# Print the runnable blocks without executing
scripts/run-md.py examples/09-ipc/README.md --list

# Run end-to-end (one shared bash session — env carries across blocks)
scripts/run-md.py examples/09-ipc/README.md

# Print the composite script without running
scripts/run-md.py examples/09-ipc/README.md --dry-run

# Run a single named block (and only that one)
scripts/run-md.py examples/09-ipc/jetstream/README.md --only durability

# Step through interactively, pausing before each block
scripts/run-md.py examples/09-ipc/polyglot/README.md --interactive

# Override the working directory (defaults to repo root via Cargo.toml walk)
scripts/run-md.py path/to/some.md --cwd /elsewhere
```

The script's working directory defaults to the repo root (it walks up from the markdown file looking for `Cargo.toml` or `.git`). That makes paths like `examples/09-ipc/demo-pub.yaml` work regardless of where you invoke it from.

---

## Block info-strings

Append `{...}` after the language tag on a fenced block:

````markdown
```bash {name=apply}
curl -X POST $CTRL/v1/resources/apply --data-binary @resource.yaml
```
````

Recognised tags:

| Tag | Effect |
|---|---|
| (no tag) | Runs in order |
| `{name=X}` | Names the block for `--only X` and `--list` |
| `{skip}` | Never runs — for display-only or copy-paste-into-a-different-terminal examples |
| `{allow_fail}` | Block runs in a subshell with `set +e`; non-zero exit is reported but doesn't stop the whole script. Used for `examples/bad/` where the validate calls are *expected* to fail. |
| `{teardown}` | Runs at the end, after all other blocks, even if earlier blocks failed |
| `{dry}` | Like `{skip}` but shown in `--dry-run` output |

Tags can be combined: `{name=oopsie, allow_fail}`.

---

## What "shared bash session" actually means

Each block isn't a standalone `bash -c "..."` — they're concatenated into one script and run together. So:

````markdown
```bash {name=set-ctrl}
CTRL=http://127.0.0.1:7878
```

```bash {name=use-ctrl}
curl $CTRL/v1/nodes
```
````

…works. `CTRL` survives from the first block into the second.

Implementation-wise:

- For non-interactive runs (the default): the script concatenates all runnable blocks, prepends `set -u; set -o pipefail; set -e`, and pipes the whole composite to `bash` via stdin.
- For `--interactive`: it spawns a persistent `bash --norc` subprocess and feeds each block in one at a time, with an `( ... ) ; echo "--- done (rc=$?) ---"` wrapper so a failure in one block doesn't kill the shell.

`{allow_fail}` flips `set +e` for that block only and restores `set -e` after.

---

## Pattern: write a runnable README

A working template:

````markdown
# 0X — My example

What this teaches; when to use it; cross-links.

> **Runnable.** `scripts/run-md.py examples/0X-my-example/README.md` walks
> every recipe end-to-end. See [docs/runner.md](../../docs/runner.md) for
> tag conventions.

## Setup (idempotent)

```bash {name=build}
cargo build --release -p some-crate
```

## Apply a resource

```bash {name=apply}
CTRL=${ORION_CONTROLLER_URL:-http://127.0.0.1:7878}
curl -X POST --data-binary @examples/0X-my-example/foo.yaml $CTRL/v1/resources/apply
```

## Dispatch and watch

```bash {name=run}
CTRL=${ORION_CONTROLLER_URL:-http://127.0.0.1:7878}
curl -X POST $CTRL/v1/dispatch/Service/foo
sleep 3
curl $CTRL/v1/logs/Service/foo
```

## Display-only block (won't run)

```bash {skip}
# This is just illustrative — won't execute
docker run -d --rm -p 4222:4222 nats:2.10
```

## Tear down

```bash {teardown}
CTRL=${ORION_CONTROLLER_URL:-http://127.0.0.1:7878}
curl -sS -X DELETE $CTRL/v1/resources/Service/foo > /dev/null 2>&1 || true
echo "torn down"
```
````

Conventions worth following:

- **Always default `CTRL` from the env**: `${ORION_CONTROLLER_URL:-http://127.0.0.1:7878}`. Lets a CI script point at a remote controller without touching the README.
- **Idempotent setup**: `cargo build`, `mvn package`, and `pip install` are all incremental. Re-running the README is cheap.
- **Use `{name=…}`** so users can `--only foo` to skip the setup once they've run it.
- **Wrap a `{teardown}` around every recipe** so the controller's state is the same before and after.
- **Mark intentionally-failing blocks `{allow_fail}`** (e.g. `examples/bad/`).
- **Skip multi-terminal recipes** with `{skip}` — the runner is single-session.

---

## When `scripts/run-md.py` is the wrong tool

- You want to *see* what's in a README without running it → just `Read` it (or `cat`, or open in your editor).
- You're authoring a new example and want to test as you go → use `--list` repeatedly, then `--only X` to run one block at a time.
- The recipe needs two separate terminals → use `{skip}` so the runner doesn't try to run it, and leave the bash blocks as copy-paste reference for humans.
- You want CI-friendly idempotence → wrap each recipe in `{teardown}` and verify counts at the end (most READMEs do this — see `examples/09-ipc/README.md` for the pattern).

---

## Via Claude

The `run-readme` skill triggers on prompts like "run examples/X/README.md" or "execute the IPC demo readme" and wraps the script. SKILL.md is at [`.claude/skills/run-readme/SKILL.md`](../.claude/skills/run-readme/SKILL.md).

---

## Exit codes

| Code | Meaning |
|---|---|
| `0` | All runnable blocks (and the teardown) succeeded |
| `1` | Bad arguments (missing file, no matching block for `--only`) |
| Other | Pass-through from bash — e.g. `1` from a `set -e` non-zero exit |

`--dry-run` and `--list` always exit 0 on success.

---

## Verified-runnable READMEs

These all run end-to-end against a live local stack (controller + agent + NATS) with clean teardown — they're the reference patterns:

| README | What it walks |
|---|---|
| [`examples/README.md`](../examples/README.md) | build CLI, validate good + bad, apply + dispatch + teardown |
| [`examples/01-services/README.md`](../examples/01-services/README.md) | native + chatty Service with log preview |
| [`examples/02-tasks/README.md`](../examples/02-tasks/README.md) | snapshot-demo Task + a failing Task |
| [`examples/03-schedules/README.md`](../examples/03-schedules/README.md) | apply Schedule + wait for next minute mark + show fire_count |
| [`examples/04-capabilities/README.md`](../examples/04-capabilities/README.md) | apply caps + pure-Python matcher |
| [`examples/05-placement/README.md`](../examples/05-placement/README.md) | filter simulation against the live fleet |
| [`examples/06-data/README.md`](../examples/06-data/README.md) | Datasets / Models / Volumes / Secrets + plaintext secret |
| [`examples/07-peers/README.md`](../examples/07-peers/README.md) | Runtime catalog + delegating workloads |
| [`examples/08-canonical/README.md`](../examples/08-canonical/README.md) | validate + apply + `cargo test` |
| [`examples/09-ipc/README.md`](../examples/09-ipc/README.md) | pub/sub side-by-side log preview |
| [`examples/09-ipc/jetstream/README.md`](../examples/09-ipc/jetstream/README.md) | JetStream durability + load-balanced |
| [`examples/09-ipc/polyglot/README.md`](../examples/09-ipc/polyglot/README.md) | Python + Java + Rust workers in one queue group |
| [`examples/bad/README.md`](../examples/bad/README.md) | every validation failure (with `{allow_fail}`) |
