# 17 · Shell integrations — completions, env, exec

Three small commands round out the day-to-day shell experience.

## `orion completions <shell>`

Installs to the standard completion directory for your shell. Examples:

```bash
# bash
orion completions bash > /etc/bash_completion.d/orion
# zsh
mkdir -p ~/.zsh/completions
orion completions zsh > ~/.zsh/completions/_orion
# add `fpath=(~/.zsh/completions $fpath)` and `autoload -U compinit && compinit` to your .zshrc
# fish
orion completions fish > ~/.config/fish/completions/orion.fish
```

After this, `orion <Tab>` completes verbs, `orion get <Tab>` completes
kinds, `orion logs Service <Tab>` cycles through your live services
(static-only — dynamic completion is a follow-up).

## `orion env`

Emits `KEY=value` lines that an eval-able context can pick up so
scripts don't need to remember the controller URL. Default format is
`sh` (works for bash/zsh/sh); pass `--format fish` for fish, `--format
json` for tooling.

```bash
eval "$(orion env)"
# Now ORION_CONTROLLER_URL, NATS_URL, ORION_CLUSTER_TOKEN are set in
# the current shell. Other tooling (curl scripts, the Python client,
# CI scripts) just works without re-discovery.
```

JSON output is handy for editor integration:

```bash
orion env --format json | jq '.ORION_CONTROLLER_URL'
```

## `orion exec`

Wraps any command as a one-shot Task, dispatches it, tails its output,
deletes the Task on completion (unless `--keep`). The "I want to run
this somewhere on the cluster without writing a YAML" verb.

```bash
# Trivial — run on whatever node the scheduler picks
orion exec -- python -c 'print(2+2)'

# Pin to a node (set up a placement constraint via env or skip — coming soon)
orion exec --env FOO=bar -- /usr/bin/env

# Stay around for debugging
orion exec --keep -- /bin/sh -c 'echo hello && sleep 10'

# Docker
orion exec --runtime docker --image alpine -- /bin/echo "from a container"
```

The Task name is `oexec-<8 hex>` by default; pass `--name` to set it.

## End-to-end walkthrough

```bash {name=prereq}
docker ps --format '{{.Names}}' | grep -q orion-nats || \
    docker run -d --rm --name orion-nats -p 4222:4222 nats:2.10 -js
pkill -f orion-controller 2>/dev/null || true
pkill -f orion-agent 2>/dev/null || true
sleep 1
cargo build --workspace --quiet
ORION_AUTH_DISABLED=1 ORION_STORE_PATH=sqlite::memory: \
    target/debug/orion-controller --bind 127.0.0.1:7878 >/tmp/orion-ctrl.log 2>&1 &
sleep 1
ORION_AUTH_DISABLED=1 \
    target/debug/orion-agent --node-id local-dev --heartbeat-interval 2 >/tmp/orion-agent.log 2>&1 &
sleep 2
```

```bash {name=env}
target/debug/orion env
target/debug/orion env --format json
target/debug/orion env --format fish | head -3
```

```bash {name=exec}
target/debug/orion exec -- /bin/sh -c 'echo "Hello from $(hostname)!"; date'
```

```bash {name=exec-with-env}
target/debug/orion exec --env GREETING=ahoy -- /bin/sh -c 'echo "$GREETING world"'
```

```bash {name=completions}
target/debug/orion completions zsh | head -10
target/debug/orion completions bash | head -10
```

```bash {teardown}
pkill -f orion-controller 2>/dev/null || true
pkill -f orion-agent 2>/dev/null || true
docker stop orion-nats 2>/dev/null || true
echo "torn down"
```
