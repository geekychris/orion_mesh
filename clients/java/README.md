# OrionMesh — Java client

Same API shape as the Python and Go clients. Sync surface; depends on
`jnats` for JetStream and Jackson for JSON/YAML.

## Install

```xml
<dependency>
    <groupId>io.orionmesh</groupId>
    <artifactId>orion-mesh-client</artifactId>
    <version>0.1.0</version>
</dependency>
```

For local development before this is published:

```bash
cd clients/java
mvn install                      # installs to ~/.m2/repository
```

## 60-second example

```java
import io.orionmesh.client.OrionClient;
import io.orionmesh.client.Queue;
import java.util.Map;

try (OrionClient c = new OrionClient()) {
    c.apply("""
        apiVersion: orionmesh.dev/v1
        kind: Queue
        metadata: { name: events }
        spec: { type: work }
        """);

    Queue q = c.queue("events");
    for (int i = 0; i < 5; i++) {
        q.pub(Map.of("n", i, "msg", "hello-" + i));
    }
    for (Map<String, Object> row : q.sub("reader", 5)) {
        System.out.println(row);
    }
}
```

## What's covered

| Surface | Method |
|---|---|
| Liveness | `c.health()` |
| Get / list | `c.get(kind, name)`, `c.list(kind)` |
| Apply | `c.apply(yamlString)` or `c.apply(Map<String,Object>)` |
| Delete | `c.delete(kind, name)` |
| Dispatch | `c.dispatch(kind, name)` |
| Logs | `c.logs(kind, name, since)` |
| Find | `c.find(Map<String,Object> selector)` |
| Doctor | `c.doctor()` |
| Queue publish | `c.queue(name).pub(value)` |
| Queue batch publish | `c.queue(name).pubMany(iterable)` |
| Queue subscribe | `for (Map row : c.queue(name).sub(group, limit)) { ... }` |

Errors hierarchy: `OrionException → ResourceNotFound / ApplyFailed /
DispatchFailed / QueueNotFound`.

## Configuration

| Env | Default |
|---|---|
| `ORION_CONTROLLER_URL` | `http://127.0.0.1:7878` |
| `NATS_URL` | `nats://127.0.0.1:4222` |
| `ORION_CLUSTER_TOKEN` | (unset → auth-disabled mode) |

Override at construction time: `new OrionClient(controllerUrl, natsUrl, token)`.

## Tests

```bash
mvn test                                  # unit tests (embedded HTTP server, no broker)
mvn test -Dgroups=integration             # against a live stack
```

Unit tests stub the REST surface with an embedded `com.sun.net.httpserver.HttpServer`
— no external dependencies, no Docker, runs in ~250ms.

## Walkthrough

See [`examples/15-java-client/`](../../examples/15-java-client/) for an
end-to-end demo (declare queue → publish from Java → consume as an
OrionMesh Service → tail logs back).

## Companion clients

| Language | Path | Status |
|---|---|---|
| Python | [`clients/python/`](../python/) | ✓ |
| Java   | (here)                          | ✓ |
| Go     | [`clients/go/`](../go/)         | (next) |
