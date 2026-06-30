#!/usr/bin/env bash
# Build all OrionMesh binaries in release mode and install them to
# $ORION_HOME/bin (default ~/.orion/bin). Idempotent — re-run after a
# `git pull` to upgrade.
#
# Adds $ORION_HOME/bin to PATH in ~/.zshrc inside a guarded block so it
# can be detected and skipped on later runs (or removed cleanly).
#
# Usage:
#     ./scripts/install-bins.sh                  # builds + installs + edits .zshrc
#     ./scripts/install-bins.sh --no-zshrc       # skip the .zshrc edit
#     ./scripts/install-bins.sh --debug          # install debug-mode binaries (faster build)
#     ./scripts/install-bins.sh --with-nats      # ALSO download native nats-server (no Docker needed)
#     ORION_HOME=~/orion-stuff ./scripts/install-bins.sh   # custom prefix
set -euo pipefail

ORION_HOME="${ORION_HOME:-$HOME/.orion}"
BIN="$ORION_HOME/bin"
MODE="release"
EDIT_ZSHRC=1
WITH_NATS=0

for arg in "$@"; do
    case "$arg" in
        --no-zshrc)  EDIT_ZSHRC=0 ;;
        --debug)     MODE="debug" ;;
        --with-nats) WITH_NATS=1 ;;
        -h|--help)
            sed -n '2,/^set -euo/p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) echo "unknown arg: $arg" >&2; exit 2 ;;
    esac
done

cd "$(dirname "$0")/.."
REPO="$(pwd)"

echo "==> building workspace ($MODE)"
if [[ "$MODE" == "release" ]]; then
    cargo build --workspace --release
    SRC="$REPO/target/release"
else
    cargo build --workspace
    SRC="$REPO/target/debug"
fi

mkdir -p "$BIN"
BINS=(orion orion-agent orion-controller orion-ui orion-mcp orion-demo-pub orion-demo-sub)

echo "==> installing into $BIN"
for b in "${BINS[@]}"; do
    if [[ -x "$SRC/$b" ]]; then
        install -m 0755 "$SRC/$b" "$BIN/$b"
        echo "    $b"
    else
        echo "    (skip: $b not built — $SRC/$b missing)" >&2
    fi
done

# Drop a tiny marker so we can detect the install (and the user can `cat` it).
cat > "$ORION_HOME/INSTALLED" <<EOF
mode=$MODE
repo=$REPO
installed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
EOF

if [[ $EDIT_ZSHRC -eq 1 && -f "$HOME/.zshrc" ]]; then
    MARKER_BEGIN='# >>> orion-mesh bin path >>>'
    MARKER_END='# <<< orion-mesh bin path <<<'
    if ! grep -Fq "$MARKER_BEGIN" "$HOME/.zshrc"; then
        echo "==> appending PATH stanza to $HOME/.zshrc"
        {
            echo ""
            echo "$MARKER_BEGIN"
            echo "# Added by orion-mesh scripts/install-bins.sh — safe to remove this block."
            echo "export ORION_HOME=\"$ORION_HOME\""
            echo "case \":\$PATH:\" in"
            echo "    *\":\$ORION_HOME/bin:\"*) ;;"
            echo "    *) export PATH=\"\$ORION_HOME/bin:\$PATH\" ;;"
            echo "esac"
            echo "$MARKER_END"
        } >> "$HOME/.zshrc"
    else
        echo "==> .zshrc already has the orion-mesh stanza — leaving it alone"
    fi
else
    echo "==> skipping .zshrc edit"
fi

# Optionally pull a native nats-server so OrionMesh doesn't need Docker.
if [[ $WITH_NATS -eq 1 ]]; then
    echo "==> installing native nats-server (no Docker)"
    bash "$(dirname "$0")/install-nats.sh"
fi

cat <<EOF

✓ done. Binaries installed:
$(ls -1 "$BIN" | sed 's/^/    /')

To use immediately in this shell:
    export PATH="$BIN:\$PATH"

Open a new shell (or 'source ~/.zshrc') to pick up the persistent change.
Then:
    orion --help
    orion get nodes
EOF
