#!/usr/bin/env bash
# Auto git add / commit / push setiap 3 prompt selesai (hook event: stop).
# Counter: .cursor/auto-git-counter | Log: .cursor/auto-git.log

set -u

# Consume stdin JSON from Cursor hooks (avoid pipe blocking).
cat >/dev/null || true

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$ROOT" || exit 0

COUNTER_FILE=".cursor/auto-git-counter"
LOG_FILE=".cursor/auto-git.log"
mkdir -p .cursor

log() {
  printf '%s %s\n' "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "$*" >> "$LOG_FILE"
}

count="$(cat "$COUNTER_FILE" 2>/dev/null || echo 0)"
# sanitize non-numeric
case "$count" in
  ''|*[!0-9]*) count=0 ;;
esac
count=$((count + 1))
echo "$count" > "$COUNTER_FILE"
log "stop: counter=$count"

if [ "$count" -lt 3 ]; then
  exit 0
fi

echo 0 > "$COUNTER_FILE"
log "stop: counter reached 3 — running git workflow"

if ! git rev-parse --git-dir >/dev/null 2>&1; then
  log "skip: not a git repo"
  exit 0
fi

# Skip if nothing to commit
if git diff --quiet && git diff --cached --quiet; then
  if [ -z "$(git ls-files --others --exclude-standard)" ]; then
    log "skip: clean working tree"
    exit 0
  fi
fi

# Avoid committing secrets
if [ -f .env ] && git check-ignore -q .env 2>/dev/null; then
  :
fi

if ! git add . >>"$LOG_FILE" 2>&1; then
  log "error: git add failed"
  exit 0
fi

# Unstage secrets if accidentally staged
git reset HEAD -- .env 2>/dev/null || true
git reset HEAD -- '**/.env' 2>/dev/null || true
git reset HEAD -- '**/credentials.json' 2>/dev/null || true

if git diff --cached --quiet; then
  log "skip: nothing staged after add"
  exit 0
fi

timestamp="$(date -u +"%Y-%m-%d %H:%M:%S UTC")"
msg="chore: auto-commit after 3 prompts ($timestamp)"

if git commit -m "$msg" >>"$LOG_FILE" 2>&1; then
  log "commit ok: $msg"
  if git push >>"$LOG_FILE" 2>&1; then
    log "push ok"
  elif git push -u origin HEAD >>"$LOG_FILE" 2>&1; then
    log "push ok (set upstream)"
  else
    log "error: git push failed"
  fi
else
  log "error: git commit failed"
fi

exit 0
