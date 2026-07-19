#!/usr/bin/env bash
set -euo pipefail

# Auto git add / commit / push setiap 3 prompt selesai (hook event: stop).
# Counter disimpan di .cursor/auto-git-counter (local, di-gitignore).

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$ROOT"

COUNTER_FILE=".cursor/auto-git-counter"
mkdir -p .cursor

count="$(cat "$COUNTER_FILE" 2>/dev/null || echo 0)"
count=$((count + 1))
echo "$count" > "$COUNTER_FILE"

if [ "$count" -lt 3 ]; then
  exit 0
fi

echo 0 > "$COUNTER_FILE"

if ! git rev-parse --git-dir >/dev/null 2>&1; then
  exit 0
fi

if git diff --quiet && git diff --cached --quiet; then
  if [ -z "$(git ls-files --others --exclude-standard)" ]; then
    exit 0
  fi
fi

git add .

timestamp="$(date -u +"%Y-%m-%d %H:%M:%S UTC")"
if git commit -m "$(cat <<EOF
chore: auto-commit after 3 prompts ($timestamp)
EOF
)"; then
  git push || git push -u origin HEAD
fi

exit 0
