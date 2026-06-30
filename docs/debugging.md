# Debugging

There are three layers you might want to debug — the workload, the
OrionMesh components themselves, or the live cluster state. This doc
covers all three. For the deep dive on attaching a debugger to a
processor, see also [`./debugging-processors.md`](./debugging-processors.md).

## TL;DR — `orion doctor` first

When something looks wrong, always start here:

```bash
orion doctor
```

It probes the broker, the controller, the agent inventory, and the
JetStream layer in one shot. A green row = that layer is fine and you
can rule it out. A red row = that's almost certainly your problem.
Pair with `orion diag system` and `orion diag jetstream` for more
detail.

## Layer 1 — your workload

### Python processor

`orion gen processor row-cruncher --queue Q --lang python --debug` emits
a Service YAML that wraps the Python entry in `debugpy`:

```yaml
runtime:
  kind: native
  exec: python
  args: ['-m', 'debugpy', '--listen', '0.0.0.0:5678', '--wait-for-client', '<path>/processor.py']
ports:
  - { name: debugpy, port: 5678, protocol: tcp }
```

Then:

```bash
orion apply -f - <<< "$(orion gen processor row-cruncher --queue Q --lang python --debug --debug-suspend)"
orion dispatch Service row-cruncher
orion logs Service row-cruncher --follow
# wait for: "debugpy listening on 0.0.0.0:5678"
```

In VS Code: *Run* → *Add Configuration* → *Python: Remote Attach*,
host `localhost`, port `5678`. Set a breakpoint in `handle(row)` —
publish to the queue, hit the breakpoint, step through, inspect `row`.

`--debug-suspend` blocks the process until a debugger attaches — useful
when you want to break on the *first* message. Drop the flag to let the
process run normally and attach later.

### Java processor

Same with `--lang java`, default port 5005, attached as a Remote JVM
Debug from IntelliJ / Eclipse / VS Code Java Extension Pack.

### Rust processor (or any other Rust binary you launched as `kind: native`)

```bash
# 1. Make sure the binary has debug symbols (cargo build, not --release)
cargo build -p my-processor

# 2. Dispatch via OrionMesh
orion apply -f my-processor.yaml
orion dispatch Service my-processor

# 3. Find the PID
orion instances Service my-processor
# Note the instance id — the agent logs an "instance spawned pid=N" line

# Or grep ps:
ps -ef | grep my-processor

# 4. Attach
rust-lldb -p <pid>            # macOS / Linux
# or gdb -p <pid>             # Linux
```

Set a breakpoint, send work, step. The processor is just a child
process of the agent — nothing about OrionMesh prevents a normal
attach.

If you want to break *before* main runs, dispatch the service with the
exec wrapped in a launcher that pauses. Quickest way:

```yaml
runtime:
  kind: native
  exec: /bin/sh
  args: ['-c', 'echo PID=$$; read x < /tmp/orion-launch.fifo; exec /path/to/my-processor']
```

Then `orion logs Service ... | grep PID=` gives you the pid; attach
the debugger; in a third terminal write to the fifo to unblock. Ugly,
but reliable for "break on entry to main" scenarios where suspending the
launcher itself is fine.

### Catching panics / exceptions

For Rust workloads, add to the Service spec's runtime env:

```yaml
env:
  RUST_BACKTRACE: '1'        # or 'full' for everything
  RUST_LOG: debug            # if the workload uses tracing
```

For Python, `python -X dev` enables development mode (warnings + faulthandler).
For Java, `-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp` gets you a
heap dump when the JVM dies on OOM.

## Layer 2 — OrionMesh components

### Controller / agent / CLI under `cargo run`

Stop the installed ones first:

```bash
pkill -f orion-controller
pkill -f orion-agent
```

Then run from the workspace with full traces:

```bash
RUST_LOG=orion_controller=debug,orion_agent=debug,orion_bus=info,async_nats=warn \
RUST_BACKTRACE=1 \
ORION_AUTH_DISABLED=1 ORION_STORE_PATH=sqlite::memory: \
cargo run -p orion-controller -- --bind 127.0.0.1:7878
```

`RUST_LOG` accepts a comma-separated per-crate filter — set noisy
crates to `warn` and the one you're investigating to `debug` (or
`trace`).

### Attach a debugger to a component

```bash
# 1. Stop the installed binaries
pkill -f orion-controller orion-agent

# 2. Build with debug symbols (default for `cargo build`)
cargo build -p orion-controller

# 3. Either launch under the debugger directly:
rust-lldb -- target/debug/orion-controller --bind 127.0.0.1:7878
# (set breakpoints, then `run`)

# Or attach to an already-running one:
target/debug/orion-controller --bind 127.0.0.1:7878 &
rust-lldb -p $(pgrep -f orion-controller)
```

In VS Code: install the *CodeLLDB* extension; the "Debug" code-lens
above `main()` in `crates/orion-controller/src/main.rs` launches under
the debugger with breakpoints + variable inspection.

For IntelliJ + Rust plugin: *Run* → *Debug* on the auto-generated run
config; same flow.

### What the agent and controller log normally

| Subsystem | What you should see in `orion logs` / stderr |
|---|---|
| Agent startup | `orion-agent starting node_id=X nats_url=...` then `connected to NATS` then four `subscribed to control subject` lines |
| Heartbeat | nothing visible in normal logs — see them with `RUST_LOG=orion_agent=trace` |
| Dispatch reception | `control: run kind=Service name=X instance=<uuid>` on the agent |
| Workload spawn | the workload's own stdout/stderr, prefixed by `[orion logs]` |
| Workload exit | `instance N exited code=0` (or non-zero with reason) |
| Controller apply | `apply Kind/Name generation=N` |

If you don't see one of these at the expected time, that's the layer
that's broken.

### Reproducing controller crashes locally

Build with `--release` symbols + backtrace + a coredump path:

```bash
RUST_BACKTRACE=full cargo run --release -p orion-controller
```

If it panics, the stack trace goes to stderr. If it hangs, attach
lldb/gdb and `thread backtrace all`.

## Layer 3 — cluster state ("something's wrong, where?")

### The standard introspection sweep

```bash
orion doctor                           # broker / controller / agents pass-fail
orion diag system                      # controller pid, agent count, instances, log buffer
orion diag jetstream                   # streams + consumers + message backlog
orion get nodes                        # which agents reported, last_seen, inventory
orion get services                     # what's declared
orion instances                        # what's running, where, with which replica-id
orion queue ls                         # queues + msgs + active consumers
orion describe service <name>          # full YAML including computed status
```

### Live tail across multiple workloads

```bash
# Single workload
orion logs Service row-cruncher --follow

# Multiple — run a wrapper in another terminal per service, or:
for svc in row-cruncher watcher-py watcher-java; do
    orion logs Service $svc --follow | sed "s/^/[$svc] /" &
done
```

### Searching log history

The controller buffers the last ~10,000 lines per (kind, name) pair in
memory. To search across all of them:

```bash
orion logs Service row-cruncher | grep ERROR
orion logs Service row-cruncher | grep -i 'connection refused'
```

The controller also exposes a search endpoint that walks the buffer
server-side (faster, no client roundtrip per line):

```bash
curl 'http://127.0.0.1:7878/v1/logs/search?q=ERROR&limit=50' | jq .
```

The UI's Diag tab does this with regex + per-component filters; see
[`./diagnostics.md`](./diagnostics.md).

### When a queue is "stuck"

```bash
orion queue describe my-queue          # are there messages? are there consumers?
orion diag jetstream                   # full broker view
```

Common patterns:

| Symptom | Likely cause | Fix |
|---|---|---|
| `messages>0`, `consumers=0` | no subscriber has connected yet | `orion dispatch Service <processor>` |
| `messages>0`, `consumers>0`, no `pending_acks` | consumer is keeping up; nothing wrong | — |
| `messages>0`, big `pending_acks` | consumer is slow or handler is throwing | check workload logs for errors → handler bug |
| Pending stays constant, never drops | handler is naking everything | handler exception every time → fix the code |
| `messages=0` but you publish and nothing arrives | wrong subject / queue type mismatch | `orion queue describe` to verify the subject, check publisher's `--subject-from` flag |

### When dispatch doesn't actually launch

```bash
orion dispatch Service foo             # returns instance id
orion instances Service foo            # should show the instance, started_at = recent
orion logs Service foo                 # should show workload stdout

# If logs is empty:
orion get nodes                        # is the node still reporting?
# Look at the agent's own logs (the agent process's stderr — not orion logs):
journalctl --user -u orion-agent       # if running as a systemd user unit
# or just tail the file you redirected stderr to in `orion up`
```

The agent's own logs will show a `control: run` line on receive and a
`spawned pid=N` (or failure reason) immediately after. If you don't
see the `control: run`, the controller didn't pick this agent —
`orion get nodes` to see who was actually live at dispatch time.

### When `orion doctor` shows agents=0

The agent isn't reporting heartbeats. Diagnose:

```bash
# Is the agent process alive?
pgrep -lf orion-agent

# Is it actually connected?
# Run the agent with debug logging in the foreground:
RUST_LOG=orion_agent=debug,async_nats=info ORION_AUTH_DISABLED=1 \
    orion-agent --node-id local-dev --heartbeat-interval 2
# Look for: "connected to NATS" and periodic publish lines
```

If the agent says connected but the controller doesn't see it, the
heartbeat subject is mismatched (e.g. controller and agent on different
broker URLs) — `orion diag jetstream` will show no recent activity on
`orion.heartbeat`.

## Common gotchas

- **`orion logs` is empty but the workload is running.** The agent
  pipes stdout only when the Service was launched after the controller's
  log subscriber subscribed. If logs went missing mid-flight, that's
  the broker dropping or the controller crashed and lost its ring
  buffer. Restart picks up new lines but the old ones are gone.
- **`orion queue sub` says 0 messages but `orion queue describe` says
  messages>0.** Your durable name doesn't match. For work queues this
  is a feature (different durables = different consumer groups). Pass
  `--group <existing-durable>` to join an existing group.
- **`orion gen processor` was generated against an offline controller.**
  The env vars say `ORION_QUEUE_TYPE=work` even for a topic queue. Apply
  the Queue first, *then* generate the processor.
- **Native nats-server doesn't survive a `pkill -f orion`** when you
  started the stack with `orion up`. `pkill -f orion up` kills the
  parent group; `pkill -f orion-controller` only kills the controller
  and orphans nats. Use `pkill -INT -f 'orion up'` for a clean shutdown
  (sends SIGINT, which is what the Ctrl-C handler waits for).

## See also

- [`./debugging-processors.md`](./debugging-processors.md) — the deep dive on
  attaching to Python (debugpy) / Java (JDWP) processors.
- [`./diagnostics.md`](./diagnostics.md) — the UI's Diag tab + the JSON shape
  of every diagnostic endpoint.
- [`./runner.md`](./runner.md) — `scripts/run-md.py` for running an example
  README end-to-end with `--interactive` (which lets you pause between
  blocks and poke around).
- [`./runtime.md`](./runtime.md) — the native-first runtime model + adapter
  status (helps when "why is my Docker / Python runtime not launching?"
  is the question).
