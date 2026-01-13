#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
WORK_DIR="${WORK_DIR:-$ROOT_DIR/tmp/ledgerdb-demo}"
REPO_DIR="$WORK_DIR/ledgerdb.git"
SCHEMA_DIR="$WORK_DIR/schemas"
INDEX_DB="$WORK_DIR/index.db"
GITHUB_REMOTE_URL="${GITHUB_REMOTE_URL:-}"
TASK_COUNT="${TASK_COUNT:-5000}"
UPDATE_ROUNDS="${UPDATE_ROUNDS:-2}"
UPDATE_EVERY="${UPDATE_EVERY:-4}"
DELETE_EVERY="${DELETE_EVERY:-50}"
DELETE_PROGRESS_EVERY="${DELETE_PROGRESS_EVERY:-25}"
PROGRESS_EVERY="${PROGRESS_EVERY:-500}"
PAUSE_EVERY="${PAUSE_EVERY:-500}"
PAUSE_FOR="${PAUSE_FOR:-0.2}"
AUTO_SYNC_EACH_WRITE="${AUTO_SYNC_EACH_WRITE:-false}"
ALLOW_GO_GIT_SYNC="${ALLOW_GO_GIT_SYNC:-false}"
LEDGERDB_OUT="${LEDGERDB_OUT:-/dev/null}"
INDEX_WAIT_MAX="${INDEX_WAIT_MAX:-600}"
INDEX_WAIT_INTERVAL="${INDEX_WAIT_INTERVAL:-1}"
INDEX_WAIT_MODE="${INDEX_WAIT_MODE:-auto}"
INDEX_PROGRESS_EVERY="${INDEX_PROGRESS_EVERY:-15}"
INDEX_WATCH_LOG="${INDEX_WATCH_LOG:-$WORK_DIR/index-watch.log}"
INDEX_WATCH_INTERVAL="${INDEX_WATCH_INTERVAL:-1s}"
INDEX_BATCH_COMMITS="${INDEX_BATCH_COMMITS:-200}"
INDEX_FAST="${INDEX_FAST:-true}"
INDEX_MODE="${INDEX_MODE:-state}"
STREAM_LAYOUT="${STREAM_LAYOUT:-sharded}"
HISTORY_MODE="${HISTORY_MODE:-amend}"
EFFECTIVE_WAIT_MODE="$INDEX_WAIT_MODE"
if [ "$INDEX_WAIT_MODE" = "auto" ]; then
  if [ "$INDEX_MODE" = "state" ]; then
    EFFECTIVE_WAIT_MODE="count"
  elif [ "$HISTORY_MODE" = "append" ]; then
    EFFECTIVE_WAIT_MODE="count"
  else
    EFFECTIVE_WAIT_MODE="commit"
  fi
fi
PARTIAL_TREE_CHECK="${PARTIAL_TREE_CHECK:-true}"

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
    echo "Build with: make build (or go build ./cmd/ledgerdb)"
    exit 1
  fi
else
  if ! command -v "$LEDGERDB_BIN" >/dev/null 2>&1; then
    echo "ledgerdb binary not found in PATH."
    echo "Build with: make build (or go build ./cmd/ledgerdb)"
    exit 1
  fi
fi

if ! command -v sqlite3 >/dev/null 2>&1; then
  echo "sqlite3 is required for this script."
  exit 1
fi

if [ -e "$WORK_DIR" ]; then
  echo "Refusing to overwrite $WORK_DIR."
  echo "Remove it or set WORK_DIR to a new path."
  exit 1
fi

mkdir -p "$SCHEMA_DIR"

wait_for_table() {
  local table="$1"
  local elapsed=0
  local next_progress="$INDEX_PROGRESS_EVERY"
  while [ "$elapsed" -lt "$INDEX_WAIT_MAX" ]; do
    if [ "$INDEX_PROGRESS_EVERY" -gt 0 ] && [ "$elapsed" -ge "$next_progress" ]; then
      echo "Index wait: table $table not ready after ${elapsed}s"
      next_progress=$((next_progress + INDEX_PROGRESS_EVERY))
    fi
    if sqlite3 "$INDEX_DB" "select 1 from sqlite_master where type='table' and name='$table' limit 1;" | grep -q 1; then
      return 0
    fi
    sleep "$INDEX_WAIT_INTERVAL"
    elapsed=$((elapsed + INDEX_WAIT_INTERVAL))
  done
  echo "Timed out waiting for SQLite table $table."
  exit 1
}

wait_for_doc() {
  local table="$1"
  local doc_id="$2"
  local elapsed=0
  local next_progress="$INDEX_PROGRESS_EVERY"
  while [ "$elapsed" -lt "$INDEX_WAIT_MAX" ]; do
    if [ "$INDEX_PROGRESS_EVERY" -gt 0 ] && [ "$elapsed" -ge "$next_progress" ]; then
      echo "Index wait: $doc_id not in $table after ${elapsed}s"
      next_progress=$((next_progress + INDEX_PROGRESS_EVERY))
    fi
    if sqlite3 "$INDEX_DB" "select 1 from $table where doc_id='$doc_id' limit 1;" | grep -q 1; then
      return 0
    fi
    sleep "$INDEX_WAIT_INTERVAL"
    elapsed=$((elapsed + INDEX_WAIT_INTERVAL))
  done
  echo "Timed out waiting for $doc_id in $table."
  exit 1
}

wait_for_count() {
  local table="$1"
  local expected="$2"
  local elapsed=0
  local next_progress="$INDEX_PROGRESS_EVERY"
  while [ "$elapsed" -lt "$INDEX_WAIT_MAX" ]; do
    local count
    count=$(sqlite3 "$INDEX_DB" "select count(*) from $table;")
    if [ "$INDEX_PROGRESS_EVERY" -gt 0 ] && [ "$elapsed" -ge "$next_progress" ]; then
      echo "Index wait: $table rows $count/$expected after ${elapsed}s"
      next_progress=$((next_progress + INDEX_PROGRESS_EVERY))
    fi
    if [ "$count" -ge "$expected" ]; then
      return 0
    fi
    sleep "$INDEX_WAIT_INTERVAL"
    elapsed=$((elapsed + INDEX_WAIT_INTERVAL))
  done
  echo "Timed out waiting for $expected rows in $table."
  exit 1
}

has_json1() {
  sqlite3 "$INDEX_DB" "select json_extract('{\"a\":1}','$.a');" >/dev/null 2>&1
}

query_task_samples() {
  local limit="${1:-12}"
  if has_json1; then
    sqlite3 -header -column "$INDEX_DB" "
      select
        doc_id,
        json_extract(cast(payload as text), '\$.status') as status,
        json_extract(cast(payload as text), '\$.priority') as priority,
        json_extract(cast(payload as text), '\$.assignee.name') as assignee,
        json_extract(cast(payload as text), '\$.due_date') as due_date,
        deleted,
        updated_at
      from collection_tasks
      order by updated_at desc
      limit $limit;
    "
    return
  fi

  sqlite3 -header -column "$INDEX_DB" "
    select
      doc_id,
      cast(payload as text) as payload_json,
      deleted,
      updated_at
    from collection_tasks
    order by updated_at desc
    limit $limit;
  "
}

query_status_counts() {
  if ! has_json1; then
    echo "SQLite JSON1 extension not available; skipping status counts."
    return
  fi
  sqlite3 -header -column "$INDEX_DB" "
    select
      json_extract(cast(payload as text), '\$.status') as status,
      count(*) as total
    from collection_tasks
    where deleted = 0
    group by status
    order by total desc;
  "
}

ensure_watch_alive() {
  if ! kill -0 "$WATCH_PID" >/dev/null 2>&1; then
    echo "Index watcher stopped unexpectedly."
    if [ -f "$INDEX_WATCH_LOG" ]; then
      echo "Last index watcher logs:"
      tail -n 20 "$INDEX_WATCH_LOG" || true
    fi
    exit 1
  fi
}

wait_for_updated_at() {
  local table="$1"
  local doc_id="$2"
  local expected="$3"
  if ! has_json1; then
    sleep 2
    return 0
  fi
  local elapsed=0
  local next_progress="$INDEX_PROGRESS_EVERY"
  while [ "$elapsed" -lt "$INDEX_WAIT_MAX" ]; do
    local value
    value=$(sqlite3 "$INDEX_DB" "select json_extract(cast(payload as text), '\$.updated_at') from $table where doc_id='$doc_id' limit 1;")
    if [ "$INDEX_PROGRESS_EVERY" -gt 0 ] && [ "$elapsed" -ge "$next_progress" ]; then
      echo "Index wait: $doc_id updated_at=$value (waiting for $expected) after ${elapsed}s"
      next_progress=$((next_progress + INDEX_PROGRESS_EVERY))
    fi
    if [ "$value" = "$expected" ]; then
      return 0
    fi
    sleep "$INDEX_WAIT_INTERVAL"
    elapsed=$((elapsed + INDEX_WAIT_INTERVAL))
  done
  echo "Timed out waiting for $doc_id updated_at=$expected."
  exit 1
}

wait_for_deleted() {
  local table="$1"
  local doc_id="$2"
  local elapsed=0
  local next_progress="$INDEX_PROGRESS_EVERY"
  while [ "$elapsed" -lt "$INDEX_WAIT_MAX" ]; do
    local value
    value=$(sqlite3 "$INDEX_DB" "select deleted from $table where doc_id='$doc_id' limit 1;")
    if [ "$INDEX_PROGRESS_EVERY" -gt 0 ] && [ "$elapsed" -ge "$next_progress" ]; then
      echo "Index wait: $doc_id deleted=$value after ${elapsed}s"
      next_progress=$((next_progress + INDEX_PROGRESS_EVERY))
    fi
    if [ "$value" = "1" ]; then
      return 0
    fi
    sleep "$INDEX_WAIT_INTERVAL"
    elapsed=$((elapsed + INDEX_WAIT_INTERVAL))
  done
  echo "Timed out waiting for $doc_id to be marked deleted."
  exit 1
}

now_epoch() {
  date +%s
}

rate_per_sec() {
  local count="$1"
  local seconds="$2"
  awk -v c="$count" -v s="$seconds" 'BEGIN { if (s<=0) s=1; printf "%.2f", c/s }'
}

print_rate() {
  local label="$1"
  local count="$2"
  local seconds="$3"
  local rate
  rate=$(rate_per_sec "$count" "$seconds")
  printf "%s: %d writes in %ss (avg %s writes/sec)\n" "$label" "$count" "$seconds" "$rate"
}

list_doc_files() {
  if ! command -v git >/dev/null 2>&1; then
    return 1
  fi
  git -C "$REPO_DIR" ls-tree -r --name-only refs/heads/main -- "documents/tasks" 2>/dev/null || true
}

count_head_files() {
  list_doc_files | awk '/\/HEAD$/ {count++} END {print count + 0}'
}

count_tx_files() {
  list_doc_files | awk -F/ '$NF ~ /\.txpb$/ && $(NF-1)=="tx" {count++} END {print count + 0}'
}

count_compact_tx_files() {
  list_doc_files | awk -F/ '$NF == "current.txpb" && $(NF-1)=="tx" {count++} END {print count + 0}'
}

count_non_compact_tx_files() {
  list_doc_files | awk -F/ '$NF ~ /\.txpb$/ && $NF != "current.txpb" && $(NF-1)=="tx" {count++} END {print count + 0}'
}

verify_partial_trees() {
  if [ "$HISTORY_MODE" != "amend" ]; then
    return 0
  fi
  if [ "$PARTIAL_TREE_CHECK" != "true" ]; then
    return 0
  fi
  if ! command -v git >/dev/null 2>&1; then
    echo "==> Partial tree check skipped (git not available)."
    return 0
  fi

  local head_count
  local tx_count
  local compact_count
  local non_compact_count
  head_count=$(count_head_files)
  tx_count=$(count_tx_files)
  compact_count=$(count_compact_tx_files)
  non_compact_count=$(count_non_compact_tx_files)

  if [ "$head_count" -ne "$TASK_COUNT" ]; then
    echo "Partial tree check failed: expected $TASK_COUNT HEAD files, got $head_count"
    exit 1
  fi
  if [ "$tx_count" -ne "$TASK_COUNT" ]; then
    echo "Partial tree check failed: expected $TASK_COUNT tx files, got $tx_count"
    exit 1
  fi
  if [ "$compact_count" -ne "$TASK_COUNT" ]; then
    echo "Partial tree check failed: expected $TASK_COUNT tx/current.txpb files, got $compact_count"
    exit 1
  fi
  if [ "$non_compact_count" -ne 0 ]; then
    echo "Partial tree check failed: found $non_compact_count non-compact tx files"
    exit 1
  fi
  echo "==> Partial tree check OK (HEAD=$head_count tx/current=$compact_count)"
}

git_head_commit() {
  if ! command -v git >/dev/null 2>&1; then
    return 0
  fi
  git -C "$REPO_DIR" rev-parse refs/heads/main 2>/dev/null || true
}

index_last_commit() {
  sqlite3 "$INDEX_DB" "select last_commit from ledger_index_state where id = 1;" 2>/dev/null || true
}

wait_for_index_commit() {
  local expected="$1"
  if [ -z "$expected" ]; then
    return 0
  fi
  local elapsed=0
  local next_progress="$INDEX_PROGRESS_EVERY"
  while [ "$elapsed" -lt "$INDEX_WAIT_MAX" ]; do
    local current
    current=$(index_last_commit)
    if [ "$INDEX_PROGRESS_EVERY" -gt 0 ] && [ "$elapsed" -ge "$next_progress" ]; then
      if [ -n "$current" ]; then
        echo "Index wait: commit ${current:0:8} (waiting for ${expected:0:8}) after ${elapsed}s"
      else
        echo "Index wait: commit unknown (waiting for ${expected:0:8}) after ${elapsed}s"
      fi
      next_progress=$((next_progress + INDEX_PROGRESS_EVERY))
    fi
    if [ "$current" = "$expected" ]; then
      return 0
    fi
    sleep "$INDEX_WAIT_INTERVAL"
    elapsed=$((elapsed + INDEX_WAIT_INTERVAL))
  done
  echo "Timed out waiting for index to reach commit $expected."
  exit 1
}

SYNC_FLAGS=(--sync=false)
INDEX_FETCH="false"
REMOTE_MODE="offline"
POST_PUSH="false"
REMOTE_AVAILABLE="false"
REMOTE_FLAG=()

configure_remote() {
  if [ -z "$GITHUB_REMOTE_URL" ]; then
    return 0
  fi

  if ! command -v git >/dev/null 2>&1; then
    echo "git not found; skipping remote reachability check."
    REMOTE_MODE="github"
    POST_PUSH="true"
    return 0
  fi

  REMOTE_MODE="github"
  if git ls-remote --heads "$GITHUB_REMOTE_URL" >/dev/null 2>&1; then
    REMOTE_AVAILABLE="true"
  else
    REMOTE_AVAILABLE="false"
  fi

  if [ "$ALLOW_GO_GIT_SYNC" = "true" ]; then
    if [ "$REMOTE_AVAILABLE" = "true" ]; then
      INDEX_FETCH="true"
    else
      INDEX_FETCH="false"
    fi
    if [ "$AUTO_SYNC_EACH_WRITE" = "true" ]; then
      SYNC_FLAGS=()
      POST_PUSH="false"
    else
      SYNC_FLAGS=(--sync=false)
      POST_PUSH="true"
    fi
  else
    INDEX_FETCH="false"
    SYNC_FLAGS=(--sync=false)
    POST_PUSH="true"
  fi

  echo "Remote configured: $GITHUB_REMOTE_URL"
  if [ "$REMOTE_AVAILABLE" = "true" ]; then
    echo "Ensure your GitHub credentials allow push/pull for this URL."
  else
    echo "Remote not reachable right now; will attempt push at the end."
  fi
}

echo "==> Init repo"
if [ -n "$GITHUB_REMOTE_URL" ]; then
  REMOTE_FLAG=(--remote "$GITHUB_REMOTE_URL")
fi
"$LEDGERDB_BIN" init --name "LedgerDB Demo" --repo "$REPO_DIR" --layout "$STREAM_LAYOUT" --history-mode "$HISTORY_MODE" "${REMOTE_FLAG[@]:-}"

configure_remote
echo "==> Remote mode: $REMOTE_MODE"
if [ "$REMOTE_MODE" = "github" ]; then
  if [ "$ALLOW_GO_GIT_SYNC" = "true" ]; then
    if [ "$AUTO_SYNC_EACH_WRITE" = "true" ]; then
      echo "Autosync per write (go-git): enabled."
    else
      echo "Autosync per write (go-git): disabled (bulk writes, push once at end)."
    fi
  else
    echo "Go-git sync disabled; push will run via ledgerdb."
  fi
else
  echo "Tip: set GITHUB_REMOTE_URL=https://github.com/codecompany/demo-ledgerdb to test GitHub sync."
fi
echo "==> Load settings: tasks=$TASK_COUNT updates=$UPDATE_ROUNDS every=$UPDATE_EVERY deletes=$DELETE_EVERY index_wait=${INDEX_WAIT_MAX}s mode=${EFFECTIVE_WAIT_MODE} wait=${INDEX_WAIT_MODE} batch=${INDEX_BATCH_COMMITS} fast=${INDEX_FAST} watch=${INDEX_WATCH_INTERVAL} source=${INDEX_MODE} layout=${STREAM_LAYOUT} history=${HISTORY_MODE}"
if [ "$LEDGERDB_OUT" = "/dev/null" ]; then
  echo "==> LedgerDB stdout redirected to $LEDGERDB_OUT (set LEDGERDB_OUT=/dev/stdout to view)."
fi

cat > "$SCHEMA_DIR/task.json" <<'EOF'
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["title", "status", "priority", "assignee", "created_at", "updated_at"],
  "properties": {
    "title": {"type": "string", "minLength": 3},
    "description": {"type": "string"},
    "status": {"type": "string", "enum": ["todo", "progress", "review", "done", "blocked"]},
    "priority": {"type": "string", "enum": ["low", "medium", "high", "urgent"]},
    "assignee": {
      "type": "object",
      "required": ["id", "name"],
      "properties": {
        "id": {"type": "string"},
        "name": {"type": "string"},
        "email": {"type": "string"}
      },
      "additionalProperties": false
    },
    "tags": {"type": "array", "items": {"type": "string"}},
    "estimate_hours": {"type": "number", "minimum": 0},
    "story_points": {"type": "integer", "minimum": 0},
    "due_date": {"type": "string"},
    "created_at": {"type": "string"},
    "updated_at": {"type": "string"}
  },
  "additionalProperties": false
}
EOF

echo "==> Apply schema"
"$LEDGERDB_BIN" --repo "$REPO_DIR" "${SYNC_FLAGS[@]}" collection apply tasks \
  --schema "$SCHEMA_DIR/task.json" \
  --indexes "status,assignee"

FAST_FLAG=()
if [ "$INDEX_FAST" = "true" ]; then
  FAST_FLAG=(--fast)
fi

echo "==> Start index watcher (SQLite sidecar)"
"$LEDGERDB_BIN" --repo "$REPO_DIR" index watch \
  --db "$INDEX_DB" \
  --interval "$INDEX_WATCH_INTERVAL" \
  --only-changes \
  --quiet \
  --fetch="$INDEX_FETCH" \
  --batch-commits "$INDEX_BATCH_COMMITS" \
  --mode "$INDEX_MODE" \
  "${FAST_FLAG[@]}" \
  >> "$INDEX_WATCH_LOG" 2>&1 &
WATCH_PID=$!
trap 'kill "$WATCH_PID" 2>/dev/null || true' EXIT
echo "==> Index watcher log: $INDEX_WATCH_LOG"

if ! [[ "$TASK_COUNT" =~ ^[0-9]+$ ]] || [ "$TASK_COUNT" -le 0 ]; then
  echo "TASK_COUNT must be a positive integer."
  exit 1
fi
if ! [[ "$INDEX_WAIT_MAX" =~ ^[0-9]+$ ]] || [ "$INDEX_WAIT_MAX" -le 0 ]; then
  echo "INDEX_WAIT_MAX must be a positive integer (seconds)."
  exit 1
fi
if ! [[ "$INDEX_WAIT_INTERVAL" =~ ^[0-9]+$ ]] || [ "$INDEX_WAIT_INTERVAL" -le 0 ]; then
  echo "INDEX_WAIT_INTERVAL must be a positive integer (seconds)."
  exit 1
fi
if ! [[ "$INDEX_PROGRESS_EVERY" =~ ^[0-9]+$ ]] || [ "$INDEX_PROGRESS_EVERY" -lt 0 ]; then
  echo "INDEX_PROGRESS_EVERY must be a non-negative integer (seconds)."
  exit 1
fi
if ! [[ "$INDEX_BATCH_COMMITS" =~ ^[0-9]+$ ]] || [ "$INDEX_BATCH_COMMITS" -le 0 ]; then
  echo "INDEX_BATCH_COMMITS must be a positive integer."
  exit 1
fi
case "$INDEX_FAST" in
  true|false) ;;
  *)
    echo "INDEX_FAST must be true or false."
    exit 1
    ;;
esac
case "$INDEX_MODE" in
  history|state) ;;
  *)
    echo "INDEX_MODE must be one of: history, state."
    exit 1
    ;;
esac
case "$INDEX_WAIT_MODE" in
  auto|commit|count|none) ;;
  *)
    echo "INDEX_WAIT_MODE must be one of: auto, commit, count, none."
    exit 1
    ;;
esac
if ! [[ "$UPDATE_EVERY" =~ ^[0-9]+$ ]] || [ "$UPDATE_EVERY" -le 0 ]; then
  echo "UPDATE_EVERY must be a positive integer."
  exit 1
fi
if ! [[ "$UPDATE_ROUNDS" =~ ^[0-9]+$ ]] || [ "$UPDATE_ROUNDS" -lt 0 ]; then
  echo "UPDATE_ROUNDS must be a non-negative integer."
  exit 1
fi
if ! [[ "$DELETE_EVERY" =~ ^[0-9]+$ ]] || [ "$DELETE_EVERY" -lt 0 ]; then
  echo "DELETE_EVERY must be a non-negative integer."
  exit 1
fi
if ! [[ "$DELETE_PROGRESS_EVERY" =~ ^[0-9]+$ ]] || [ "$DELETE_PROGRESS_EVERY" -lt 0 ]; then
  echo "DELETE_PROGRESS_EVERY must be a non-negative integer."
  exit 1
fi
case "$STREAM_LAYOUT" in
  flat|sharded) ;;
  *)
    echo "STREAM_LAYOUT must be one of: flat, sharded."
    exit 1
    ;;
esac
case "$HISTORY_MODE" in
  append|amend) ;;
  *)
    echo "HISTORY_MODE must be one of: append, amend."
    exit 1
    ;;
esac
if [ "$INDEX_WAIT_MODE" = "auto" ]; then
  if [ "$HISTORY_MODE" = "append" ]; then
    EFFECTIVE_WAIT_MODE="count"
  else
    EFFECTIVE_WAIT_MODE="commit"
  fi
fi

STATUSES=(todo progress review done blocked)
PRIORITIES=(low medium high urgent)
ASSIGNEE_IDS=(u_ana u_bruno u_carla u_diego u_erika u_felipe)
ASSIGNEE_NAMES=("Ana Lima" "Bruno Dias" "Carla Souza" "Diego Rocha" "Erika Monteiro" "Felipe Ramos")
ASSIGNEE_EMAILS=("ana@example.com" "bruno@example.com" "carla@example.com" "diego@example.com" "erika@example.com" "felipe@example.com")
COMPONENTS=(core cli index ops sync release)

total_start=$(now_epoch)

echo "==> Insert $TASK_COUNT tasks"
insert_start=$(now_epoch)
for ((i = 1; i <= TASK_COUNT; i++)); do
  doc_id=$(printf "task_%05d" "$i")
  status="${STATUSES[$(( (i - 1) % ${#STATUSES[@]} ))]}"
  priority="${PRIORITIES[$(( (i - 1) % ${#PRIORITIES[@]} ))]}"
  assignee_idx=$(( (i - 1) % ${#ASSIGNEE_IDS[@]} ))
  component="${COMPONENTS[$(( (i - 1) % ${#COMPONENTS[@]} ))]}"
  sprint=$(( (i - 1) / 500 + 1 ))
  day=$(( (i - 1) % 28 + 1 ))
  hour=$(( (i - 1) % 24 ))
  minute=$(( (i * 7) % 60 ))
  created_at=$(printf "2025-02-%02dT%02d:%02d:00Z" "$day" "$hour" "$minute")
  updated_at="$created_at"
  due_day=$(( (day + 7) % 28 + 1 ))
  due_date=$(printf "2025-02-%02d" "$due_day")
  estimate_hours=$(( (i % 8) + 1 ))
  story_points=$(( (i % 13) + 1 ))

  payload=$(printf '{"title":"Task %05d","description":"Load test task %05d for %s","status":"%s","priority":"%s","assignee":{"id":"%s","name":"%s","email":"%s"},"tags":["%s","sprint-%02d"],"estimate_hours":%d,"story_points":%d,"due_date":"%s","created_at":"%s","updated_at":"%s"}' \
    "$i" "$i" "$component" "$status" "$priority" \
    "${ASSIGNEE_IDS[$assignee_idx]}" "${ASSIGNEE_NAMES[$assignee_idx]}" "${ASSIGNEE_EMAILS[$assignee_idx]}" \
    "$component" "$sprint" "$estimate_hours" "$story_points" "$due_date" "$created_at" "$updated_at")

  "$LEDGERDB_BIN" --repo "$REPO_DIR" "${SYNC_FLAGS[@]}" doc put tasks "$doc_id" --payload "$payload" >> "$LEDGERDB_OUT"
  if [ "$PROGRESS_EVERY" -gt 0 ] && (( i % PROGRESS_EVERY == 0 )); then
    echo "Inserted $i / $TASK_COUNT"
  fi
  if [ "$PAUSE_EVERY" -gt 0 ] && (( i % PAUSE_EVERY == 0 )); then
    sleep "$PAUSE_FOR"
  fi
done
insert_end=$(now_epoch)
insert_seconds=$((insert_end - insert_start))
insert_count=$TASK_COUNT
head_after_inserts=$(git_head_commit)
verify_partial_trees

wait_for_table "collection_tasks"
ensure_watch_alive
if [ "$EFFECTIVE_WAIT_MODE" = "commit" ]; then
  if [ -n "$head_after_inserts" ]; then
    wait_for_index_commit "$head_after_inserts"
  fi
  wait_for_count "collection_tasks" "$TASK_COUNT"
elif [ "$EFFECTIVE_WAIT_MODE" = "count" ]; then
  wait_for_count "collection_tasks" "$TASK_COUNT"
elif [ "$EFFECTIVE_WAIT_MODE" = "none" ]; then
  echo "==> Skipping index wait after inserts (INDEX_WAIT_MODE=none)"
fi

echo "==> SQLite after inserts"
query_task_samples 12
query_status_counts

last_updated_doc_id=""
last_updated_at=""
last_deleted_doc_id=""
update_total=0
update_seconds=0
delete_total=0
delete_seconds=0

if [ "$UPDATE_ROUNDS" -gt 0 ]; then
  echo "==> Update tasks ($UPDATE_ROUNDS rounds, every $UPDATE_EVERY)"
  for ((round = 1; round <= UPDATE_ROUNDS; round++)); do
    echo "==> Update round $round"
    updated=0
    if [ "$round" -eq 1 ]; then
      update_start=$(now_epoch)
    fi
    for ((i = 1; i <= TASK_COUNT; i += UPDATE_EVERY)); do
      doc_id=$(printf "task_%05d" "$i")
      status="${STATUSES[$(( (i + round) % ${#STATUSES[@]} ))]}"
      priority="${PRIORITIES[$(( (i + round) % ${#PRIORITIES[@]} ))]}"
      assignee_idx=$(( (i + round) % ${#ASSIGNEE_IDS[@]} ))
      day=$(( (i + round) % 28 + 1 ))
      hour=$(( (i + round * 3) % 24 ))
      minute=$(( (i * 11 + round) % 60 ))
      updated_at=$(printf "2025-02-%02dT%02d:%02d:00Z" "$day" "$hour" "$minute")

      ops=$(printf '[{"op":"replace","path":"/status","value":"%s"},{"op":"replace","path":"/priority","value":"%s"},{"op":"replace","path":"/assignee","value":{"id":"%s","name":"%s","email":"%s"}},{"op":"replace","path":"/updated_at","value":"%s"}]' \
        "$status" "$priority" \
        "${ASSIGNEE_IDS[$assignee_idx]}" "${ASSIGNEE_NAMES[$assignee_idx]}" "${ASSIGNEE_EMAILS[$assignee_idx]}" \
        "$updated_at")

      "$LEDGERDB_BIN" --repo "$REPO_DIR" "${SYNC_FLAGS[@]}" doc patch tasks "$doc_id" --ops "$ops" >> "$LEDGERDB_OUT"
      last_updated_doc_id="$doc_id"
      last_updated_at="$updated_at"
      updated=$((updated + 1))
      update_total=$((update_total + 1))
      if [ "$PROGRESS_EVERY" -gt 0 ] && (( updated % PROGRESS_EVERY == 0 )); then
        echo "Updated $updated tasks (round $round)"
      fi
      if [ "$PAUSE_EVERY" -gt 0 ] && (( updated % PAUSE_EVERY == 0 )); then
        sleep "$PAUSE_FOR"
      fi
    done
  done
  update_end=$(now_epoch)
  update_seconds=$((update_end - update_start))
  head_after_updates=$(git_head_commit)
  verify_partial_trees
else
  echo "==> Updates skipped (UPDATE_ROUNDS=0)"
fi

if [ "$DELETE_EVERY" -gt 0 ]; then
  echo "==> Delete every $DELETE_EVERY task"
  deleted=0
  delete_start=$(now_epoch)
  for ((i = DELETE_EVERY; i <= TASK_COUNT; i += DELETE_EVERY)); do
    doc_id=$(printf "task_%05d" "$i")
    "$LEDGERDB_BIN" --repo "$REPO_DIR" "${SYNC_FLAGS[@]}" doc delete tasks "$doc_id" >> "$LEDGERDB_OUT"
    last_deleted_doc_id="$doc_id"
    deleted=$((deleted + 1))
    delete_total=$((delete_total + 1))
    if [ "$DELETE_PROGRESS_EVERY" -gt 0 ] && (( deleted % DELETE_PROGRESS_EVERY == 0 )); then
      echo "Deleted $deleted tasks"
    fi
    if [ "$PAUSE_EVERY" -gt 0 ] && (( deleted % PAUSE_EVERY == 0 )); then
      sleep "$PAUSE_FOR"
    fi
  done
  delete_end=$(now_epoch)
  delete_seconds=$((delete_end - delete_start))
  head_after_deletes=$(git_head_commit)
  verify_partial_trees
else
  echo "==> Deletes skipped (DELETE_EVERY=0)"
fi

total_end=$(now_epoch)
total_seconds=$((total_end - total_start))

ensure_watch_alive
if [ "$EFFECTIVE_WAIT_MODE" = "commit" ]; then
  if [ -n "${head_after_updates:-}" ]; then
    wait_for_index_commit "$head_after_updates"
  fi
  if [ -n "${head_after_deletes:-}" ]; then
    wait_for_index_commit "$head_after_deletes"
  fi
  if [ -n "$last_updated_doc_id" ]; then
    wait_for_updated_at "collection_tasks" "$last_updated_doc_id" "$last_updated_at"
  fi
  if [ -n "$last_deleted_doc_id" ]; then
    wait_for_deleted "collection_tasks" "$last_deleted_doc_id"
  fi
elif [ "$EFFECTIVE_WAIT_MODE" = "count" ]; then
  if [ -n "$last_updated_doc_id" ]; then
    wait_for_updated_at "collection_tasks" "$last_updated_doc_id" "$last_updated_at"
  fi
  if [ -n "$last_deleted_doc_id" ]; then
    wait_for_deleted "collection_tasks" "$last_deleted_doc_id"
  fi
elif [ "$EFFECTIVE_WAIT_MODE" = "none" ]; then
  echo "==> Skipping index wait after updates/deletes (INDEX_WAIT_MODE=none)"
fi

if [ "$REMOTE_MODE" = "github" ] && [ "$POST_PUSH" = "true" ]; then
  echo "==> Push to GitHub (bulk)"
  "$LEDGERDB_BIN" --repo "$REPO_DIR" push
fi

echo "==> SQLite after updates"
query_task_samples 12
query_status_counts

total_writes=$((insert_count + update_total + delete_total))

echo "==> Write metrics (avg writes/sec)"
print_rate "Inserts" "$insert_count" "$insert_seconds"
if [ "$update_total" -gt 0 ]; then
  print_rate "Updates" "$update_total" "$update_seconds"
fi
if [ "$delete_total" -gt 0 ]; then
  print_rate "Deletes" "$delete_total" "$delete_seconds"
fi
print_rate "Total" "$total_writes" "$total_seconds"

echo "Done. SQLite DB at: $INDEX_DB"
