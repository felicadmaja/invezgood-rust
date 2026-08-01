#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$ROOT/.env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "mcp-invezgo: .env tidak ditemukan di $ENV_FILE" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

if [[ -z "${INVEZGO_BEARER_TOKEN:-}" ]]; then
  echo "mcp-invezgo: INVEZGO_BEARER_TOKEN kosong di $ENV_FILE" >&2
  exit 1
fi

exec npx -y mcp-remote@latest "https://mcp.invezgo.com" \
  --transport http-only \
  --header "Authorization: Bearer ${INVEZGO_BEARER_TOKEN}"
