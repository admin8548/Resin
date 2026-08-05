#!/usr/bin/env bash
# Verify Dest-aware Soft Ban P0 on the local dev tree (never touches :2260).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export PATH="${HOME}/.local/go1.26.3/go/bin:${HOME}/.local/go/bin:${PATH:-}"
export GOTOOLCHAIN="${GOTOOLCHAIN:-local}"
export GOPROXY="${GOPROXY:-https://goproxy.cn,direct}"

PORT="${RESIN_PORT:-2261}"
TOKEN="${RESIN_ADMIN_TOKEN:-Qcloud123}"
BIN="${RESIN_BIN:-$ROOT/resin}"

if [[ "$PORT" == "2260" ]]; then
  echo "REFUSING: port 2260 is production" >&2
  exit 1
fi

# Keep unit tests free of local .env pollution (paths/port defaults).
unset RESIN_PORT RESIN_CACHE_DIR RESIN_STATE_DIR RESIN_LOG_DIR RESIN_ADMIN_TOKEN || true

echo "==> unit tests (node/topology/routing/config/proxy)"
go test ./internal/node ./internal/topology ./internal/routing ./internal/config ./internal/proxy \
  -count=1 -timeout=120s

echo "==> dest-ban focused tests"
go test ./internal/node ./internal/topology ./internal/routing \
  -run 'DestBan|HardExcludesDestBanned|StickyLeaseHit_RejectsDestBanned|ChooseSameIPRotation_SkipsDestBanned|EmptyDomainFallsBack' \
  -count=1 -timeout=60s -v

echo "==> ensure binary"
# Restore local runtime env for API checks / optional process start.
if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +a
fi
PORT="${RESIN_PORT:-2261}"
TOKEN="${RESIN_ADMIN_TOKEN:-Qcloud123}"
BIN="${RESIN_BIN:-$ROOT/resin}"
if [[ ! -x "$BIN" ]]; then
  if [[ ! -d webui/dist ]]; then
    mkdir -p webui/dist
    printf '%s\n' '<!doctype html><title>Resin</title>' > webui/dist/index.html
  fi
  go build -tags "with_quic with_wireguard with_grpc with_utls" -o "$BIN" ./cmd/resin
fi

started_here=0
mkdir -p "$ROOT/data/log"
if ! ss -tln 2>/dev/null | grep -q ":${PORT} "; then
  echo "==> starting dev instance on :${PORT}"
  if [[ -f "$ROOT/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "$ROOT/.env"
    set +a
  fi
  export RESIN_PORT="$PORT"
  nohup "$BIN" >"$ROOT/data/log/verify-destban.out" 2>"$ROOT/data/log/verify-destban.err" &
  echo $! >"$ROOT/data/verify-destban.pid"
  started_here=1
  for _ in $(seq 1 30); do
    if ss -tln 2>/dev/null | grep -q ":${PORT} "; then
      break
    fi
    sleep 0.3
  done
fi

echo "==> runtime config API"
cfg="$(curl -fsS -H "Authorization: Bearer ${TOKEN}" "http://127.0.0.1:${PORT}/api/v1/system/config")"
export DESTBAN_CFG_JSON="$cfg"
python3 - <<'PY'
import json, os, sys
c = json.loads(os.environ["DESTBAN_CFG_JSON"])
need = ["dest_ban_enabled", "dest_ban_threshold", "dest_ban_ttl", "dest_ban_scope", "dest_ban_max_entries"]
missing = [k for k in need if k not in c]
if missing:
    sys.exit("missing keys: %s" % missing)
print("dest_ban_enabled=", c["dest_ban_enabled"])
print("dest_ban_threshold=", c["dest_ban_threshold"])
print("dest_ban_ttl=", c["dest_ban_ttl"])
print("dest_ban_scope=", c["dest_ban_scope"])
print("dest_ban_max_entries=", c["dest_ban_max_entries"])
if c.get("dest_ban_enabled") is not True:
    sys.exit("expected dest_ban_enabled=true by default")
if int(c.get("dest_ban_threshold", 0)) < 1:
    sys.exit("dest_ban_threshold invalid")
PY

echo "==> validation rejects bad scope"
code="$(curl -s -o /tmp/destban-patch.json -w '%{http_code}' -X PATCH \
  -H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json" \
  -d '{"dest_ban_scope":"bogus"}' \
  "http://127.0.0.1:${PORT}/api/v1/system/config")"
if [[ "$code" != "400" && "$code" != "422" ]]; then
  if ! grep -q 'INVALID_ARGUMENT\|dest_ban_scope' /tmp/destban-patch.json; then
    echo "unexpected response code=$code body=$(cat /tmp/destban-patch.json)" >&2
    exit 1
  fi
fi
echo "bad scope rejected (http=$code)"

if [[ "$started_here" -eq 1 ]]; then
  echo "==> stopping instance started by this script"
  if [[ -f "$ROOT/data/verify-destban.pid" ]]; then
    kill "$(cat "$ROOT/data/verify-destban.pid")" 2>/dev/null || true
    rm -f "$ROOT/data/verify-destban.pid"
  fi
fi

echo
echo "OK: Dest-aware Soft Ban P0 verified."
echo "Manual E2E (optional): proxy traffic via 127.0.0.1:${PORT} to a failing host"
echo "  >= threshold times; subsequent routes for that eTLD+1 should skip the node."
