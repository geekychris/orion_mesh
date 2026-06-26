# Canonical examples

Verbatim from the architecture plan, kept here as a reference for the documented shape.

| File | Source |
|---|---|
| [amiga-search.yaml](amiga-search.yaml) | The example shown in `OrionMesh_Architecture_Plan.md` |
| [amiga-search-full.yaml](amiga-search-full.yaml) | The same service, fleshed out with health, capabilities, and labels |

`amiga-search.yaml` is the YAML the `service_amiga_search_roundtrip` test in `crates/orion-types/src/tests.rs` parses. Changing it changes the test.
