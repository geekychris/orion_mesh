# Bad examples

YAMLs that **intentionally fail** validation. Useful for confirming `orion validate` catches what serde alone wouldn't, and for showing what the error messages look like.

| File | What goes wrong |
|---|---|
| [schedule-both.yaml](schedule-both.yaml) | Schedule sets both `task:` and `task_template:` — only one allowed |
| [schedule-neither.yaml](schedule-neither.yaml) | Schedule sets neither `task:` nor `task_template:` |
| [unknown-kind.yaml](unknown-kind.yaml) | `kind: Slartibartfast` — not a valid resource kind |
| [unknown-runtime.yaml](unknown-runtime.yaml) | `runtime.kind: zigzag` — not a valid runtime kind |
| [bad-restart-policy.yaml](bad-restart-policy.yaml) | `restart_policy: maybe` — not a valid policy |

Try each one:

```bash
./target/debug/orion validate examples/bad/schedule-both.yaml
# → Error: validating resource
#   Caused by: schedule must set exactly one of `task` or `taskTemplate`
```

The serde-driven errors (unknown kind / runtime / restart policy) list the valid alternatives:

```bash
./target/debug/orion validate examples/bad/unknown-runtime.yaml
# → Error: parsing resource yaml
#   Caused by:
#     0: invalid resource yaml: unknown variant `zigzag`,
#        expected one of `native`, `docker`, `python`, `java`, `node`,
#                         `spark`, `llm`, `homeassistant`, `wasm`, `peer`
```
