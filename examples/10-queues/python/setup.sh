#!/usr/bin/env bash
# Create a venv at .venv and install nats-py + debugpy.
set -euo pipefail
cd "$(dirname "$0")"
if [[ ! -d .venv ]]; then
    python3 -m venv .venv
fi
. .venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt
echo "ok: examples/10-queues/python/.venv ready"
echo "   processor: $(pwd)/.venv/bin/python processor.py"
echo "   debug:     $(pwd)/.venv/bin/python -m debugpy --listen :5678 --wait-for-client processor.py"
