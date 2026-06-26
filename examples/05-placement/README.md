# Placement

Hard constraints (`arch`, `os`, `gpu`, `acceleration`, `node_labels`) filter candidate nodes. Soft preferences (`prefer:`) score the survivors. Empty placement matches anything.

| File | Demonstrates |
|---|---|
| [arch-only.yaml](arch-only.yaml) | `arch: [arm64]` — Pi-only service |
| [gpu-required.yaml](gpu-required.yaml) | GPU vendor + min VRAM filter |
| [site-label.yaml](site-label.yaml) | Node label filter (`site: belmont`) |
| [prefer-soft.yaml](prefer-soft.yaml) | Soft preference (`prefer:`) — survivors get bonus points |
| [combined.yaml](combined.yaml) | All four together, plus inline capabilities |
