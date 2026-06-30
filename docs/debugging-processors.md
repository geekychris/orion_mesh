# Debugging queue processors

A queue processor is a long-running Service that consumes from a [named
queue](./queues.md). The reference templates under
[`../examples/10-queues/`](../examples/10-queues/) are designed to be either:

1. **Started under a debugger** (the process suspends at launch until a
   client attaches), or
2. **Attached to live** (the process is already running and accepting
   message; the debugger latches on without restarting it).

Both modes work by enabling a per-language debug agent that listens on a TCP
port; an IDE / editor connects to that port and drives breakpoints.

## Python (debugpy)

```bash
# Start a 1-replica processor that waits for the IDE to attach
orion gen processor row-cruncher-debug \
    --queue ps-rows --lang python --debug --debug-suspend | \
  orion apply -f -
orion dispatch Service row-cruncher-debug

# Tail the logs to see the listen banner
orion logs Service row-cruncher-debug --follow
# → debugpy listening on 0.0.0.0:5678 — Waiting for client to attach...
```

Then in **VS Code**:

1. *Run* → *Add Configuration* → *Python: Remote Attach*.
2. Host: `localhost`, Port: `5678`.
3. Set a breakpoint in
   `examples/10-queues/python/processor.py` — the `handle(row)` function.
4. Pump some rows in:
   ```bash
   ps -ef | orion json | orion queue pub ps-rows
   ```
   Each row hits the breakpoint; `row` is a Python dict you can inspect.

Other editors / IDEs that speak the DAP debugpy protocol (PyCharm
Professional, Vim with `nvim-dap-python`, Emacs `dape`) work the same way —
they all attach over TCP.

To **disable suspend** (process starts running but accepts attach later),
drop the `--debug-suspend` flag. The processor starts normally; you can
attach mid-run and break on the next message.

To use a **non-default port**, pass `--debug-port 5901`. Useful when
multiple processors are running on the same node.

## Java (JDWP)

```bash
# Build the jar once
bash examples/10-queues/java/setup.sh

orion gen processor row-cruncher-java-debug \
    --queue ps-rows --lang java --debug --debug-suspend | \
  orion apply -f -
orion dispatch Service row-cruncher-java-debug
orion logs Service row-cruncher-java-debug --follow
# → Listening for transport dt_socket at address: 5005
```

In **IntelliJ IDEA**:

1. *Run* → *Edit Configurations…* → `+` → **Remote JVM Debug**.
2. Host: `localhost`, Port: `5005`.
3. Set a breakpoint in `Processor.handle(...)`.

In **Eclipse**: *Run* → *Debug Configurations* → *Remote Java Application*
with the same host/port. In **VS Code with the Java Extension Pack**:
*Run* → *Add Configuration* → *Java: Attach to Process*.

## Tips

- **Production safety.** `--debug` opens an unauthenticated debug port on
  every replica that runs the Service. Only use it in dev, or front the
  service with a network policy that restricts the port.
- **Multiple replicas.** With `--replicas N --debug`, every replica
  listens on the *same* port — that only works if the replicas are spread
  across nodes. For a single-node cluster, debug with `--replicas 1` and
  scale back up once the breakpoint isn't needed.
- **Editing the handler.** The `handle(row)` body is the user-editable
  part. Restart the Service (`orion restart Service <name>`) to pick up
  changes; hot-reload isn't wired up yet.
- **What's in `row`?** Whatever `orion json` (or whatever produced the
  ndjson on the publisher side) emitted, plus `_subject` — the JetStream
  subject the row arrived on. Useful when you publish with
  `--subject-from <field>` and want to know the per-row routing key.
