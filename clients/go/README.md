# OrionMesh — Go client

Sync client mirroring the Python and Java surfaces. Single package
(`github.com/geekychris/orion_mesh/clients/go/orionmesh`), depends on
`nats.go` for JetStream and `gopkg.in/yaml.v3` for YAML encoding.

## Install

```bash
go get github.com/geekychris/orion_mesh/clients/go/orionmesh
```

## 60-second example

```go
package main

import (
    "context"
    "fmt"

    "github.com/geekychris/orion_mesh/clients/go/orionmesh"
)

func main() {
    c, err := orionmesh.New()
    if err != nil { panic(err) }
    defer c.Close()

    c.Apply(`apiVersion: orionmesh.dev/v1
kind: Queue
metadata: { name: events }
spec: { type: work }`)

    q := c.Queue("events")
    for i := 0; i < 5; i++ {
        q.Pub(map[string]any{"n": i, "msg": fmt.Sprintf("go-%d", i)})
    }

    ctx := context.Background()
    rows, errs := q.Sub(ctx, "reader", 5)
    for row := range rows {
        fmt.Println(row)
    }
    if e := <-errs; e != nil { fmt.Println("err:", e) }
}
```

## What's covered

| Surface | Method |
|---|---|
| Liveness | `c.Health()` |
| Get / list | `c.Get(kind, name)`, `c.List(kind)` |
| Apply | `c.Apply(yaml)` / `c.ApplyMap(m)` |
| Delete | `c.Delete(kind, name)` |
| Dispatch | `c.Dispatch(kind, name)` |
| Logs | `c.Logs(kind, name, since)` |
| Find | `c.Find(selectorMap)` |
| Doctor | `c.Doctor()` |
| Queue publish | `c.Queue(name).Pub(value)` |
| Queue batch publish | `c.Queue(name).PubMany(values)` |
| Queue subscribe | `rows, errs := c.Queue(name).Sub(ctx, group, limit)` |

Errors: `ResourceNotFoundError`, `ApplyFailedError`, `DispatchFailedError`,
`QueueNotFoundError`. Use `errors.As` to discriminate.

## Configuration

Same env vars as the other clients:

| Env | Default |
|---|---|
| `ORION_CONTROLLER_URL` | `http://127.0.0.1:7878` |
| `NATS_URL` | `nats://127.0.0.1:4222` |
| `ORION_CLUSTER_TOKEN` | (unset) |

Or override at construction with `orionmesh.New(opts...)` using
`WithController`, `WithNATSURL`, `WithToken`, `WithTimeout`.

## Tests

```bash
go test ./orionmesh/...
```

Unit tests stub the REST surface with `httptest.Server` — no broker, no
Docker, no external deps.

## Walkthrough

See [`examples/16-go-client/`](../../examples/16-go-client/) for an
end-to-end demo (declare queue → publish from Go → consume from a Go
binary running as an OrionMesh Service).
