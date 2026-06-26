# Peer integration

OrionMesh treats Dev Portal, KQueue, and other OrionMesh instances as **peers, not parents**. Each is registered as a `Runtime` resource and can be referenced as the runtime for a Service or Task.

| File | Demonstrates |
|---|---|
| [orionmesh-belmont.yaml](orionmesh-belmont.yaml) | Register a peer OrionMesh controller at another site |
| [kqueue-default.yaml](kqueue-default.yaml) | Register a KQueue instance as a peer runtime |
| [devportal-local.yaml](devportal-local.yaml) | Register the local Dev Portal as a peer |
| [service-via-kqueue.yaml](service-via-kqueue.yaml) | A Service using `runtime: peer` to delegate to KQueue |
| [task-via-peer-mesh.yaml](task-via-peer-mesh.yaml) | A Task delegated to a peer OrionMesh in another site |

See [docs/architecture.md §6.4](../../docs/architecture.md#64-peer-integration--dev-portal-registration) for the registration sequence.
