#!/usr/bin/env bash
set -euo pipefail

# Publish local wiki markdown pages to GitHub Wiki repository.
# Usage: ./wiki/publish.sh [owner/repo]

REPO="${1:-osvaldoandrade/ledgerdb}"
WIKI_URL="https://github.com/${REPO}.wiki.git"

TMP_DIR="$(mktemp -d)"
cleanup() { rm -rf "$TMP_DIR"; }
trap cleanup EXIT

if git ls-remote "$WIKI_URL" >/dev/null 2>&1; then
  git clone "$WIKI_URL" "$TMP_DIR/wiki" >/dev/null
  rsync -a --delete \
    --exclude='.git/' \
    --include='*/' \
    --include='*.md' \
    --exclude='*' \
    wiki/ "$TMP_DIR/wiki/"

  cd "$TMP_DIR/wiki"
  git add -A
  if git diff --cached --quiet; then
    echo "No wiki changes to publish."
    exit 0
  fi

  git commit -m "docs: publish ledgerdb wiki" >/dev/null
  git push origin master
else
  echo "Wiki repository is not available: $WIKI_URL"
  exit 1
fi
