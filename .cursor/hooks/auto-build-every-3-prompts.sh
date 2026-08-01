#!/usr/bin/env bash
# Auto ./build.sh (release + PM2 restart) setiap 3 prompt selesai (hook event: stop).
# Counter: .cursor/auto-build-counter | Log: .cursor/auto-build.log

set -u

cat >/dev/null || true

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$ROOT" || exit 0

COUNTER_FILE=".cursor/auto-build-counter"
LOG_FILE=".cursor/auto-build.log"
BUILD_SCRIPT="./build.sh"
mkdir -p .cursor

log() {
  printf '%s %s\n' "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "$*" >> "$LOG_FILE"
}

count="$(cat "$COUNTER_FILE" 2>/dev/null || echo 0)"
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
log "stop: counter reached 3 — running $BUILD_SCRIPT"

if [ ! -x "$BUILD_SCRIPT" ]; then
  if [ -f "$BUILD_SCRIPT" ]; then
    chmod +x "$BUILD_SCRIPT" 2>/dev/null || true
  fi
fi

if [ ! -f "$BUILD_SCRIPT" ]; then
  log "error: $BUILD_SCRIPT not found"
  exit 0
fi

export BUILD_SKIP_PM2_LOGS=1
if "$BUILD_SCRIPT" >>"$LOG_FILE" 2>&1; then
  log "build ok"
else
  log "error: $BUILD_SCRIPT failed (exit $?)"
fi

exit 0
