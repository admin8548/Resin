#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROD_STATE_DB="${PROD_STATE_DB:-/home/ubuntu/.openclaw/workspace/resin-data/state/state.db}"
PROD_API="${PROD_API:-http://127.0.0.1:2260}"
TOKEN="${PROD_ADMIN_TOKEN:-Qcloud123}"

if ss -lntp 2>/dev/null | grep -q ':2261'; then
  echo "Stop resin-dev on 2261 before syncing state.db" >&2
  exit 1
fi

mkdir -p "$ROOT/data/state" "$ROOT/data/seed"
python3 - "$PROD_STATE_DB" "$ROOT/data/state/state.db" <<'PY'
import sqlite3, os, sys
src, dst = sys.argv[1], sys.argv[2]
os.makedirs(os.path.dirname(dst), exist_ok=True)
if os.path.exists(dst):
    os.remove(dst)
s=sqlite3.connect(f'file:{src}?mode=ro', uri=True)
d=sqlite3.connect(dst)
s.backup(d); d.close(); s.close()
print('synced state.db from', src)
PY

curl -sS -H "Authorization: Bearer $TOKEN" "$PROD_API/api/v1/platforms?limit=100" -o "$ROOT/data/seed/platforms.json"
curl -sS -H "Authorization: Bearer $TOKEN" "$PROD_API/api/v1/subscriptions?limit=100" -o "$ROOT/data/seed/subscriptions.json"
curl -sS -H "Authorization: Bearer $TOKEN" "$PROD_API/api/v1/system/config" -o "$ROOT/data/seed/system-config.json"
curl -sS -H "Authorization: Bearer $TOKEN" "$PROD_API/api/v1/account-header-rules?limit=100" -o "$ROOT/data/seed/account-header-rules.json"
echo "seed JSON refreshed under data/seed/"
