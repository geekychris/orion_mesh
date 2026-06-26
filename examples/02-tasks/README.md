# Tasks

A `Task` is a one-shot workload — it runs to completion (success or failure). Demonstrates: retry policy, timeout, dataset locality preference, multiple runtimes.

| File | Demonstrates |
|---|---|
| [python-train.yaml](python-train.yaml) | Python runtime, GPU placement, retry + timeout, prefer data locality |
| [java-batch.yaml](java-batch.yaml) | Java runtime, longer timeout, x86 placement |
| [native-snapshot.yaml](native-snapshot.yaml) | Native runtime, minimal task — meant to be triggered by a Schedule |
