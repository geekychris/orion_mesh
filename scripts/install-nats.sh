#!/usr/bin/env bash
# Download a native nats-server binary into $ORION_HOME/bin so OrionMesh
# never needs Docker. The released artifacts are official Go binaries
# from github.com/nats-io/nats-server — Apache-2.0, no install dance,
# no daemon manager, no package manager.
#
# Usage:
#     ./scripts/install-nats.sh              # latest tagged release
#     ./scripts/install-nats.sh v2.10.29     # pin a version
#     ./scripts/install-nats.sh --force      # overwrite even if already present
set -euo pipefail

ORION_HOME="${ORION_HOME:-$HOME/.orion}"
BIN="$ORION_HOME/bin"
mkdir -p "$BIN"

FORCE=0
VERSION=""
for arg in "$@"; do
    case "$arg" in
        --force) FORCE=1 ;;
        -h|--help)
            sed -n '2,/^set -euo/p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        v*) VERSION="$arg" ;;
        *) echo "unknown arg: $arg" >&2; exit 2 ;;
    esac
done

if [[ -x "$BIN/nats-server" && $FORCE -eq 0 ]]; then
    echo "==> nats-server already present at $BIN/nats-server"
    "$BIN/nats-server" --version
    echo "    (use --force to overwrite, or rm $BIN/nats-server)"
    exit 0
fi

# Resolve latest if no version pinned.
if [[ -z "$VERSION" ]]; then
    echo "==> resolving latest release"
    VERSION=$(curl -sSL https://api.github.com/repos/nats-io/nats-server/releases/latest \
        | grep -o '"tag_name": *"[^"]*"' | head -1 | sed 's/.*"\([^"]*\)"/\1/')
    if [[ -z "$VERSION" ]]; then
        # Fallback for rate-limited github API.
        VERSION="v2.10.29"
        echo "    (github API didn't respond — falling back to $VERSION)"
    fi
fi
echo "==> nats-server $VERSION"

# Detect platform.
OS=$(uname -s)
ARCH=$(uname -m)
case "$OS" in
    Darwin) GOOS=darwin ;;
    Linux)  GOOS=linux ;;
    *)      echo "unsupported OS: $OS" >&2; exit 1 ;;
esac
case "$ARCH" in
    arm64|aarch64) GOARCH=arm64 ;;
    x86_64|amd64)  GOARCH=amd64 ;;
    armv7l|armv6l) GOARCH=arm7 ;;
    *)             echo "unsupported arch: $ARCH" >&2; exit 1 ;;
esac

ASSET="nats-server-${VERSION}-${GOOS}-${GOARCH}.tar.gz"
URL="https://github.com/nats-io/nats-server/releases/download/${VERSION}/${ASSET}"
TMP=$(mktemp -d -t orion-nats-XXXXXX)
trap 'rm -rf "$TMP"' EXIT

echo "==> downloading $URL"
curl -fsSL -o "$TMP/nats.tgz" "$URL" \
    || { echo "download failed — check version + platform" >&2; exit 1; }

echo "==> extracting"
tar -C "$TMP" -xzf "$TMP/nats.tgz"

# The tarball lays out as nats-server-<ver>-<os>-<arch>/nats-server
SRC_BIN=$(find "$TMP" -name 'nats-server' -type f -perm -u+x | head -1)
if [[ -z "$SRC_BIN" ]]; then
    echo "no nats-server binary in archive" >&2
    exit 1
fi

install -m 0755 "$SRC_BIN" "$BIN/nats-server"
echo "==> installed $BIN/nats-server"
"$BIN/nats-server" --version

cat <<EOF

✓ done. Smoke test:
    $BIN/nats-server -js -DV &     # start with JetStream + debug
    sleep 1
    nc -z 127.0.0.1 4222 && echo "listening" && kill %1

Then \`orion up\` will pick this up automatically (PATH includes $BIN
via your .zshrc stanza). To force its use:
    orion up --nats native
EOF
