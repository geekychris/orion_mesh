#!/usr/bin/env bash
# Walk the example tree against a running controller. See examples/README.md.
#
# Defaults:
#   ORION_CONTROLLER_URL = http://127.0.0.1:7878
#   ORION_CLUSTER_TOKEN  = unset (dev / auth-disabled)
set -euo pipefail

CTRL="${ORION_CONTROLLER_URL:-http://127.0.0.1:7878}"
HEADERS=()
if [[ -n "${ORION_CLUSTER_TOKEN:-}" ]]; then
  HEADERS=(-H "Authorization: Bearer $ORION_CLUSTER_TOKEN")
fi

EXAMPLES_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$EXAMPLES_DIR/.." && pwd)"
ORION="$REPO_ROOT/target/debug/orion"

if [[ ! -x "$ORION" ]]; then
  echo "Building orion-cli..."
  (cd "$REPO_ROOT" && cargo build -p orion-cli --quiet)
fi

bold() { printf "\033[1m%s\033[0m\n" "$*"; }
ok()   { printf "  \033[32m✓\033[0m %s\n" "$*"; }
fail() { printf "  \033[31m✗\033[0m %s\n" "$*"; }

# ------------------------------------------------------------------- 1. validate

bold "Step 1: validate every YAML (good and bad)"

good=0
bad=0
for f in $(find "$EXAMPLES_DIR" -name '*.yaml' ! -path '*/bad/*' | sort); do
  if "$ORION" validate "$f" > /dev/null 2>&1; then
    ok "$(realpath --relative-to="$REPO_ROOT" "$f" 2>/dev/null || python3 -c "import os,sys;print(os.path.relpath('$f','$REPO_ROOT'))")"
    good=$((good+1))
  else
    fail "$(realpath --relative-to="$REPO_ROOT" "$f" 2>/dev/null || python3 -c "import os,sys;print(os.path.relpath('$f','$REPO_ROOT'))")"
    "$ORION" validate "$f" || true
    bad=$((bad+1))
  fi
done

echo
bold "Step 2: 'bad' YAMLs — these should ALL fail"
for f in "$EXAMPLES_DIR"/bad/*.yaml; do
  if ! "$ORION" validate "$f" > /dev/null 2>&1; then
    ok "$(basename "$f") rejected as expected"
  else
    fail "$(basename "$f") unexpectedly validated"
    bad=$((bad+1))
  fi
done

if [[ $bad -gt 0 ]]; then
  printf "\nValidation step had %d unexpected failures. Stopping before apply.\n" "$bad"
  exit 1
fi

# ------------------------------------------------------------------- 3. apply

bold "Step 3: apply a curated set against $CTRL"

# Skip /health auth (it's outside the layer); use auth headers on data routes.
if ! curl -sf "$CTRL/health" > /dev/null; then
  echo "Controller not reachable at $CTRL — start it (see docs/installation.md §6)."
  exit 2
fi

apply() {
  local f="$1"
  local rel
  rel="$(python3 -c "import os;print(os.path.relpath('$f','$REPO_ROOT'))")"
  local resp
  resp=$(curl -s "${HEADERS[@]}" -X POST --data-binary "@$f" "$CTRL/v1/resources/apply")
  printf "  %s\n    → %s\n" "$rel" "$resp"
}

for f in \
  "$EXAMPLES_DIR/08-canonical/amiga-search.yaml" \
  "$EXAMPLES_DIR/01-services/native-sleeper.yaml" \
  "$EXAMPLES_DIR/01-services/docker-nginx.yaml" \
  "$EXAMPLES_DIR/02-tasks/python-train.yaml" \
  "$EXAMPLES_DIR/03-schedules/inline-template.yaml" \
  "$EXAMPLES_DIR/06-data/dataset-multi-location.yaml" \
  "$EXAMPLES_DIR/06-data/model-variants.yaml" \
  "$EXAMPLES_DIR/07-peers/orionmesh-belmont.yaml" \
  "$EXAMPLES_DIR/07-peers/kqueue-default.yaml" \
  "$EXAMPLES_DIR/04-capabilities/declared-schema.yaml" \
  "$EXAMPLES_DIR/05-placement/combined.yaml" \
  ; do
  apply "$f"
done

# ------------------------------------------------------------------- 4. read back

bold "Step 4: read back"

for kind in Service Task Schedule Dataset Model Runtime Capability; do
  printf "  GET /v1/resources/%s  → " "$kind"
  count=$(curl -s "${HEADERS[@]}" "$CTRL/v1/resources/$kind" | python3 -c "import sys,json;print(len(json.load(sys.stdin)))")
  printf "%s rows\n" "$count"
done

printf "\n  GET /v1/nodes  →\n"
curl -s "${HEADERS[@]}" "$CTRL/v1/nodes" | python3 -m json.tool 2>/dev/null | head -30

bold "Walkthrough complete."
echo "  Open the UI at http://127.0.0.1:7879 to browse the node list."
