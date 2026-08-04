#!/usr/bin/env bash
# Backup keyspace ScyllaDB `invezgood` → backup_database/backup_invezgood_YYYY-MM-DD.gz
# Cara: snapshot nodetool + schema cqlsh, lalu tar|gzip. Snapshot dihapus setelah arsip selesai.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +a
fi

KEYSPACE="${SCYLLA_KEYSPACE:-invezgood}"
SCYLLA_HOST="${SCYLLA_HOST:-}"
SCYLLA_URI="${SCYLLA_URI:-127.0.0.1:9042}"
SCYLLA_USER="${SCYLLA_USER:-}"
SCYLLA_PASSWORD="${SCYLLA_PASSWORD:-}"
SCYLLA_DATA_DIR="${SCYLLA_DATA_DIR:-/var/lib/scylla/data}"

if [[ -z "$SCYLLA_HOST" ]]; then
  SCYLLA_HOST="${SCYLLA_URI%%:*}"
fi

DATE="$(date +%Y-%m-%d)"
TAG="backup_invezgood_${DATE}"
OUT_DIR="$ROOT/backup_database"
OUT_FILE="$OUT_DIR/backup_invezgood_${DATE}.gz"
DATA_KS_DIR="$SCYLLA_DATA_DIR/$KEYSPACE"

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Error: perintah '$1' tidak ditemukan" >&2
    exit 1
  }
}

need_cmd nodetool
need_cmd cqlsh
need_cmd tar
need_cmd gzip
need_cmd find
need_cmd mktemp

if [[ ! -d "$DATA_KS_DIR" ]]; then
  echo "Error: direktori data keyspace tidak ada: $DATA_KS_DIR" >&2
  echo "Set SCYLLA_DATA_DIR bila data Scylla di path lain." >&2
  exit 1
fi

if [[ ! -r "$DATA_KS_DIR" ]]; then
  echo "Error: tidak bisa membaca $DATA_KS_DIR (izin akses)" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/backup_invezgood.XXXXXX")"
cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

echo "==> Snapshot keyspace '$KEYSPACE' (tag=$TAG)..."
nodetool snapshot -t "$TAG" "$KEYSPACE"

echo "==> Dump schema via cqlsh..."
CQLSH_ARGS=(-u "${SCYLLA_USER}" )
if [[ -n "${SCYLLA_PASSWORD}" ]]; then
  CQLSH_ARGS+=(-p "${SCYLLA_PASSWORD}")
fi
# cqlsh tanpa -u/-p bila user kosong
if [[ -z "${SCYLLA_USER}" ]]; then
  cqlsh "$SCYLLA_HOST" -e "DESCRIBE KEYSPACE ${KEYSPACE};" >"$TMP_DIR/schema.cql"
else
  cqlsh "$SCYLLA_HOST" "${CQLSH_ARGS[@]}" -e "DESCRIBE KEYSPACE ${KEYSPACE};" >"$TMP_DIR/schema.cql"
fi

echo "==> Kumpulkan file snapshot..."
STAGE="$TMP_DIR/data"
mkdir -p "$STAGE"
found=0
while IFS= read -r -d '' snap_dir; do
  table_dir="$(dirname "$(dirname "$snap_dir")")"
  table_name="$(basename "$table_dir")"
  dest="$STAGE/$table_name"
  mkdir -p "$dest"
  cp -a "$snap_dir"/. "$dest"/
  found=$((found + 1))
done < <(find "$DATA_KS_DIR" -type d -path "*/snapshots/${TAG}" -print0)

if [[ "$found" -eq 0 ]]; then
  echo "Error: tidak ada folder snapshot tag=$TAG di $DATA_KS_DIR" >&2
  nodetool clearsnapshot -t "$TAG" "$KEYSPACE" || true
  exit 1
fi

echo "==> Arsip gzip → $OUT_FILE ($found tabel/MV)..."
# Nama file .gz sesuai permintaan; isi = tar stream (schema + SSTables snapshot).
tar -C "$TMP_DIR" -cf - schema.cql data | gzip -c >"$OUT_FILE"

echo "==> Hapus snapshot Scylla (tag=$TAG)..."
nodetool clearsnapshot -t "$TAG" "$KEYSPACE"

SIZE="$(du -h "$OUT_FILE" | awk '{print $1}')"
echo "OK: backup selesai → $OUT_FILE ($SIZE)"
