---
name: validate-resource
description: Validate an OrionMesh resource YAML file (Service, Task, Node, Schedule, Dataset, Model, Project, Secret, Volume, Network) against the resource model defined in crates/orion-types. Use when the user says "validate this", "check this resource", "is this orion yaml valid?", "lint <file>.yaml", or pastes/saves a candidate desired-state YAML.
---

# validate-resource

Run the `orion-cli` validator on an OrionMesh resource YAML and report what it found.

## How to use

1. Make sure `orion-cli` is built:
   - If `target/debug/orion` is missing, run `cargo build -p orion-cli` from the repo root.
2. Invoke `orion validate <path>` (the alias `target/debug/orion validate <path>` works from a fresh clone).
3. The CLI exits non-zero on parse failure and prints `ok: kind=<Kind> name=<name>` on success.

For multiple files, loop and aggregate. Don't run them in parallel — `orion validate` is cheap (<10 ms) and serial output is easier to read.

## What to report

- **On success**: one short line confirming the kind + name. If the user pasted YAML inline, also call out anything notable they should know:
  - The resource has no `placement` (will land anywhere — usually fine for `Service` but suspicious for `Task` on a GPU workload).
  - The resource has no `requires` block (won't be matched against capabilities — only OK if the runtime is self-contained).
  - Implicit defaults (e.g. `replicas: 1` when omitted on a Service).
- **On failure**: quote the serde error verbatim, then translate it into a one-sentence diagnosis pointing at the offending field. Common cases:
  - `missing field X` → the field is required; show the minimal correct shape inline.
  - `unknown variant X` → the `kind:` or `runtime.kind:` value is misspelled; list the valid options from `crates/orion-types/src/resource.rs` and `runtime.rs`.
  - `data did not match any variant` → the `runtime:` block doesn't match any `Runtime` variant; show the canonical shape for the kind they meant.

## Reference: canonical shape

The plan's `amiga-search` example, which the validator's roundtrip test verifies:

```yaml
kind: Service
metadata:
  name: amiga-search
spec:
  runtime:
    kind: docker
    image: amiga-search:latest
  replicas: 1
  placement:
    arch: [arm64, x86_64]
    os: [linux]
  requires:
    dataset: amiga_schematics
```

The full set of kinds, runtime variants, and placement keys is defined in `crates/orion-types/`. When the user is unsure what a field accepts, read the relevant module rather than guessing.

## When NOT to use this skill

- The user is editing the Rust resource model itself (resource.rs / runtime.rs / placement.rs) — they want compiler feedback (`cargo check -p orion-types`), not the YAML validator.
- The user wants to *apply* the resource to a running controller — that's `orion apply` (not yet implemented; flag it as out of scope and offer to validate locally instead).
- The file in question is a `devportal.yaml` — that's Dev Portal's asset manifest, validated by Dev Portal's schema at `~/code/claude_world/dev_portal/schema/devportal-asset.schema.json`. Different system, different validator.
