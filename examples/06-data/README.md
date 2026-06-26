# Datasets, Models, Volumes, Secrets

How OrionMesh tracks data that workloads consume.

| File | Demonstrates |
|---|---|
| [dataset-multi-location.yaml](dataset-multi-location.yaml) | A dataset with three locations across different nodes and access modes |
| [dataset-readonly.yaml](dataset-readonly.yaml) | Small single-location read-only dataset |
| [model-variants.yaml](model-variants.yaml) | Model with three variants (gguf q4, gguf q8, mlx int8) — scheduler picks one that fits |
| [model-served-by.yaml](model-served-by.yaml) | Model with a single variant, served by a single node |
| [volume.yaml](volume.yaml) | A shared scratch volume mounted on three nodes |
| [secret-plaintext.yaml](secret-plaintext.yaml) | Secret resolving via the `plaintext://` resolver — see also `docs/usage.md §3.10` |

`prefer_data_locality: true` on a Task asks the scheduler to score nodes that hold a referenced dataset higher. See [`05-placement/prefer-soft.yaml`](../05-placement/prefer-soft.yaml).
