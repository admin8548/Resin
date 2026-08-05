#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
set -a
source "$ROOT/.env"
set +a

BIN="${RESIN_BIN:-$ROOT/resin}"
if [[ ! -x "$BIN" ]]; then
  echo "binary not found: $BIN" >&2
  echo "Build first: cd $ROOT && go build -tags \"with_quic with_wireguard with_grpc with_utls\" -o resin ./cmd/resin" >&2
  exit 1
fi

# Avoid clobbering prod on 2260
if [[ "${RESIN_PORT}" == "2260" ]]; then
  echo "REFUSING: RESIN_PORT=2260 collides with production" >&2
  exit 1
fi

exec "$BIN"
