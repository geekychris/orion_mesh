# Services

A `Service` is a long-running workload the controller keeps healthy. Demonstrates: native and Docker runtimes, named ports, health checks (HTTP / TCP / Exec), and restart policies.

| File | Demonstrates |
|---|---|
| [native-sleeper.yaml](native-sleeper.yaml) | Minimal Service: native runtime, one replica, always restart |
| [docker-nginx.yaml](docker-nginx.yaml) | Docker runtime, named ports, HTTP health check, on-failure restart |
| [docker-redis.yaml](docker-redis.yaml) | Docker runtime, TCP health check, env vars |
| [native-with-exec-health.yaml](native-with-exec-health.yaml) | Native runtime, custom exec health probe |
