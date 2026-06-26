#!/usr/bin/env bash
# Build the two Java fat jars. Idempotent (`mvn package` is fast on repeat runs).
set -euo pipefail
cd "$(dirname "$0")"
if ! command -v mvn >/dev/null; then
  echo "Maven not found. Install with `brew install maven` (macOS) or your distro's package manager."
  exit 1
fi
if ! command -v javac >/dev/null; then
  echo "JDK 17+ not found. Install with `brew install openjdk@17` (macOS) or your distro's JDK."
  exit 1
fi
mvn -q package
echo "Built:"
echo "  $(pwd)/target/orion-demo-pub.jar"
echo "  $(pwd)/target/orion-demo-sub.jar"
echo "Run with:"
echo "  java -jar $(pwd)/target/orion-demo-pub.jar --interval 1.0"
echo "  java -jar $(pwd)/target/orion-demo-sub.jar --queue-group ipc-workers"
