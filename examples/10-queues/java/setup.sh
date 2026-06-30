#!/usr/bin/env bash
# Build the orion-queue-processor jar via Maven.
set -euo pipefail
cd "$(dirname "$0")"
if ! command -v mvn >/dev/null 2>&1; then
    echo "Maven required. Install via 'brew install maven' or your package manager."
    exit 1
fi
mvn -q package
echo "ok: examples/10-queues/java/target/orion-queue-processor.jar"
echo "   processor: java -jar $(pwd)/target/orion-queue-processor.jar"
echo "   debug:     java -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005 -jar $(pwd)/target/orion-queue-processor.jar"
