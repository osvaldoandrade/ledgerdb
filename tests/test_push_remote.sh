#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
WORK_DIR="${WORK_DIR:-$ROOT_DIR/tmp/ledgerdb-remote}"

: "${GITHUB_REMOTE_URL:=}"
: "${TASK_COUNT:=200}"
: "${UPDATE_ROUNDS:=1}"
: "${UPDATE_EVERY:=4}"
: "${DELETE_EVERY:=0}"
: "${INDEX_BATCH_COMMITS:=100}"
: "${INDEX_FAST:=true}"
: "${INDEX_MODE:=state}"
: "${STREAM_LAYOUT:=sharded}"
: "${HISTORY_MODE:=append}"
: "${ALLOW_GO_GIT_SYNC:=false}"
: "${AUTO_SYNC_EACH_WRITE:=false}"
: "${INDEX_WAIT_MODE:=none}"

if [ -z "$GITHUB_REMOTE_URL" ]; then
  echo "GITHUB_REMOTE_URL is required (e.g., https://github.com/org/repo.git)"
  exit 1
fi

if ! command -v git >/dev/null 2>&1; then
  echo "git is required"
  exit 1
fi

if ! git ls-remote --heads "$GITHUB_REMOTE_URL" >/dev/null 2>&1; then
  echo "Remote not reachable. Ensure the repo exists and auth is configured."
  exit 1
fi

if [ -e "$WORK_DIR" ]; then
  echo "Refusing to overwrite $WORK_DIR. Remove it or set WORK_DIR to a new path."
  exit 1
fi

WORK_DIR="$WORK_DIR" \
GITHUB_REMOTE_URL="$GITHUB_REMOTE_URL" \
TASK_COUNT="$TASK_COUNT" \
UPDATE_ROUNDS="$UPDATE_ROUNDS" \
UPDATE_EVERY="$UPDATE_EVERY" \
DELETE_EVERY="$DELETE_EVERY" \
INDEX_BATCH_COMMITS="$INDEX_BATCH_COMMITS" \
INDEX_FAST="$INDEX_FAST" \
INDEX_MODE="$INDEX_MODE" \
STREAM_LAYOUT="$STREAM_LAYOUT" \
HISTORY_MODE="$HISTORY_MODE" \
ALLOW_GO_GIT_SYNC="$ALLOW_GO_GIT_SYNC" \
AUTO_SYNC_EACH_WRITE="$AUTO_SYNC_EACH_WRITE" \
INDEX_WAIT_MODE="$INDEX_WAIT_MODE" \
"$ROOT_DIR/tests/test-ledgerdb.sh"

REMOTE_HEAD=$(git ls-remote "$GITHUB_REMOTE_URL" refs/heads/main | awk '{print $1}')
LOCAL_HEAD=$(git -C "$WORK_DIR/ledgerdb.git" rev-parse HEAD)

if [ -z "$REMOTE_HEAD" ]; then
  echo "Remote main not found after push."
  exit 1
fi
if [ "$REMOTE_HEAD" != "$LOCAL_HEAD" ]; then
  echo "Remote HEAD does not match local HEAD."
  echo "Remote: $REMOTE_HEAD"
  echo "Local:  $LOCAL_HEAD"
  exit 1
fi

echo "Remote push OK: $GITHUB_REMOTE_URL"
