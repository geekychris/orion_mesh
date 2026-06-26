# Capabilities

Services advertise *what they can do* via `capabilities:`. Workloads constrain placement by `requires:` selectors. The matcher recognizes three forms of attribute check based on JSON shape.

| File | Demonstrates |
|---|---|
| [advertise-search.yaml](advertise-search.yaml) | A Service advertising a `search` capability with nested attributes |
| [require-equals.yaml](require-equals.yaml) | Selector form: bare value → `Equals` |
| [require-oneof.yaml](require-oneof.yaml) | Selector form: JSON array → `OneOf` |
| [require-op.yaml](require-op.yaml) | Selector form: `{gte: 24}` → `Op` (numeric comparison) |
| [declared-schema.yaml](declared-schema.yaml) | A `Capability` resource declaring the attribute schema for `search` |

## The three forms in one place

```yaml
requires:
  search:
    dataset: amiga_schematics       # Equals  — bare value
    format: [pdf, png]              # OneOf   — array
  llm:
    gpu:
      min_vram_gb: { gte: 24 }      # Op      — numeric op
```

Custom `Deserialize` in `crates/orion-types/src/capability.rs` switches on JSON shape:

```
serde_json::Value::Array  → OneOf
serde_json::Value::Object whose keys are all in {eq, ne, gt, gte, lt, lte} → Op
anything else → Equals
```

`{}` doesn't become an empty `Op` (the guard requires at least one matching key).
