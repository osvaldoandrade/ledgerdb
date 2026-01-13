#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
WORK_DIR="${WORK_DIR:-$ROOT_DIR/tmp/ledgerdb-revert}"
REPO_DIR="$WORK_DIR/ledgerdb.git"

LEDGERDB_BIN="${LEDGERDB_BIN:-}"
if [ -z "$LEDGERDB_BIN" ]; then
  if [ -x "$ROOT_DIR/ledgerdb" ]; then
    LEDGERDB_BIN="$ROOT_DIR/ledgerdb"
  else
    LEDGERDB_BIN="ledgerdb"
  fi
fi

if [[ "$LEDGERDB_BIN" == */* ]]; then
  if [ ! -x "$LEDGERDB_BIN" ]; then
    echo "ledgerdb binary not found at $LEDGERDB_BIN"
    exit 1
  fi
else
  if ! command -v "$LEDGERDB_BIN" >/dev/null 2>&1; then
    echo "ledgerdb binary not found in PATH"
    exit 1
  fi
fi

PY_BIN="${PY_BIN:-}"
if [ -z "$PY_BIN" ]; then
  if command -v python3 >/dev/null 2>&1; then
    PY_BIN="python3"
  elif command -v python >/dev/null 2>&1; then
    PY_BIN="python"
  else
    echo "python is required to parse JSON output"
    exit 1
  fi
fi

if [ -e "$WORK_DIR" ]; then
  echo "Refusing to overwrite $WORK_DIR. Remove it or set WORK_DIR to a new path."
  exit 1
fi

mkdir -p "$WORK_DIR"

"$LEDGERDB_BIN" init --name "LedgerDB Revert Test" --repo "$REPO_DIR" --layout sharded --history-mode append

DOC_ID="task_0001"
PAYLOAD='{"title":"Task 1","status":"todo","priority":"low","updated_at":"2025-02-01T00:00:00Z"}'
PATCH='[{"op":"replace","path":"/status","value":"done"},{"op":"replace","path":"/updated_at","value":"2025-02-02T00:00:00Z"}]'

"$LEDGERDB_BIN" --repo "$REPO_DIR" doc put tasks "$DOC_ID" --payload "$PAYLOAD" >/dev/null
"$LEDGERDB_BIN" --repo "$REPO_DIR" doc patch tasks "$DOC_ID" --ops "$PATCH" >/dev/null

status_after_patch=$("$LEDGERDB_BIN" --repo "$REPO_DIR" --json doc get tasks "$DOC_ID" | "$PY_BIN" - <<'PY'
import json,sys
payload=json.load(sys.stdin)
print(payload.get("doc", {}).get("status", ""))
PY
)
if [ "$status_after_patch" != "done" ]; then
  echo "Expected status=done after patch, got: $status_after_patch"
  exit 1
fi

log_json=$("$LEDGERDB_BIN" --repo "$REPO_DIR" --json doc log tasks "$DOC_ID")
revert_tx_id=$(printf '%s' "$log_json" | "$PY_BIN" - <<'PY'
import json,sys
payload=json.load(sys.stdin)
entries=payload.get("entries", [])
if not entries:
    raise SystemExit(2)
print(entries[-1]["tx_id"])
PY
)

"$LEDGERDB_BIN" --repo "$REPO_DIR" doc revert tasks "$DOC_ID" --tx-id "$revert_tx_id" >/dev/null

status_after_revert=$("$LEDGERDB_BIN" --repo "$REPO_DIR" --json doc get tasks "$DOC_ID" | "$PY_BIN" - <<'PY'
import json,sys
payload=json.load(sys.stdin)
print(payload.get("doc", {}).get("status", ""))
PY
)

if [ "$status_after_revert" != "todo" ]; then
  echo "Expected status=todo after revert, got: $status_after_revert"
  exit 1
fi

echo "Revert OK: $DOC_ID restored to status=todo (tx_id=$revert_tx_id)"
