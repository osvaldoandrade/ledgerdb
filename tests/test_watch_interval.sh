#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
WORK_DIR="${WORK_DIR:-$ROOT_DIR/tmp/ledgerdb-watch-interval}"

: "${TASK_COUNT:=1000}"
: "${UPDATE_ROUNDS:=1}"
: "${UPDATE_EVERY:=5}"
: "${DELETE_EVERY:=25}"
: "${INDEX_BATCH_COMMITS:=1}"
: "${INDEX_FAST:=true}"
: "${INDEX_MODE:=state}"
: "${STREAM_LAYOUT:=sharded}"
: "${HISTORY_MODE:=append}"
: "${INDEX_WATCH_INTERVAL:=250ms}"

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
INDEX_WATCH_INTERVAL="$INDEX_WATCH_INTERVAL" \
"$ROOT_DIR/tests/test-ledgerdb.sh"
