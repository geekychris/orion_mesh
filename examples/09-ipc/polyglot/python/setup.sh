#!/usr/bin/env bash
# Create a venv and install nats-py. Idempotent.
set -euo pipefail
cd "$(dirname "$0")"
if [[ ! -d .venv ]]; then
  python3 -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate
pip install -q -r requirements.txt
echo "Python venv ready at $(pwd)/.venv"
echo "Run with:"
echo "  $(pwd)/.venv/bin/python3 $(pwd)/pub.py --interval 1.0"
echo "  $(pwd)/.venv/bin/python3 $(pwd)/sub.py --queue-group ipc-workers"
