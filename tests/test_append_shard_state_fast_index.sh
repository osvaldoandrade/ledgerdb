#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
WORK_DIR="${WORK_DIR:-$ROOT_DIR/tmp/ledgerdb-append-sharded}"

: "${TASK_COUNT:=5000}"
: "${UPDATE_ROUNDS:=2}"
: "${UPDATE_EVERY:=4}"
: "${DELETE_EVERY:=50}"
: "${INDEX_BATCH_COMMITS:=500}"
: "${INDEX_FAST:=true}"
: "${INDEX_MODE:=state}"
: "${STREAM_LAYOUT:=sharded}"
: "${HISTORY_MODE:=append}"

if [ -e "$WORK_DIR" ]; then
  echo "Refusing to overwrite $WORK_DIR. Remove it or set WORK_DIR to a new path."
  exit 1
fi

WORK_DIR="$WORK_DIR" \
TASK_COUNT="$TASK_COUNT" \
UPDATE_ROUNDS="$UPDATE_ROUNDS" \
UPDATE_EVERY="$UPDATE_EVERY" \
DELETE_EVERY="$DELETE_EVERY" \
INDEX_BATCH_COMMITS="$INDEX_BATCH_COMMITS" \
INDEX_FAST="$INDEX_FAST" \
INDEX_MODE="$INDEX_MODE" \
STREAM_LAYOUT="$STREAM_LAYOUT" \
HISTORY_MODE="$HISTORY_MODE" \
"$ROOT_DIR/tests/test-ledgerdb.sh"
