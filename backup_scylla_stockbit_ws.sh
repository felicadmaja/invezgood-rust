#!/usr/bin/env bash
# Backup ScyllaDB keyspace `stockbit` via nodetool snapshot + schema dump.
#
# Contoh:
#   ./backup_scylla_stockbit_ws.sh
#   BACKUP_ROOT=/mnt/backups ./backup_scylla_stockbit_ws.sh
#   KEEP_SNAPSHOT=1 ./backup_scylla_stockbit_ws.sh   # jangan clearsnapshot di Scylla
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# --- Konfigurasi (bisa di-override via env) ---
KEYSPACE="${SCYLLA_KEYSPACE:-stockbit}"
SCYLLA_DATA_DIR="${SCYLLA_DATA_DIR:-/var/lib/scylla/data}"
BACKUP_ROOT="${BACKUP_ROOT:-$SCRIPT_DIR/backup_scylla_stockbit_ws}"
KEEP_SNAPSHOT="${KEEP_SNAPSHOT:-0}"
DOTENV_FILE="${DOTENV_FILE:-$SCRIPT_DIR/.env}"

TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
TAG="stockbit_ws_${TIMESTAMP}"
OUT_DIR="${BACKUP_ROOT}/${KEYSPACE}_${TIMESTAMP}"
ARCHIVE="${BACKUP_ROOT}/${KEYSPACE}_${TIMESTAMP}.tar.gz"

log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }
die() { log "ERROR: $*"; exit 1; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "perintah '$1' tidak ditemukan"
}

# Muat SCYLLA_* dari .env (sumber tunggal — tidak perlu export di bash).
load_dotenv_scylla() {
  [[ -f "$DOTENV_FILE" ]] || die "file .env tidak ditemukan: $DOTENV_FILE"
  local line key val
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ "$line" =~ ^[[:space:]]*# ]] && continue
    [[ "$line" =~ ^[[:space:]]*$ ]] && continue
    key="${line%%=*}"
    val="${line#*=}"
    key="$(echo "$key" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    case "$key" in
      SCYLLA_URI|SCYLLA_USER|SCYLLA_PASSWORD|SCYLLA_KEYSPACE)
        # Hapus kutip luar bila ada.
        val="${val#\'}"; val="${val%\'}"
        val="${val#\"}"; val="${val%\"}"
        export "$key=$val"
        ;;
    esac
  done < "$DOTENV_FILE"
  KEYSPACE="${SCYLLA_KEYSPACE:-stockbit}"
  [[ -n "${SCYLLA_URI:-}" ]] || die ".env wajib punya SCYLLA_URI"
  [[ -n "${SCYLLA_USER:-}" ]] || die ".env wajib punya SCYLLA_USER"
  [[ -n "${SCYLLA_PASSWORD:-}" ]] || die ".env wajib punya SCYLLA_PASSWORD"
}

parse_cqlsh_host_port() {
  local uri="${SCYLLA_URI:-127.0.0.1:9042}"
  CQLSH_HOST="${uri%%:*}"
  CQLSH_PORT="${uri##*:}"
  # Jangan pakai `[[ ... ]] &&` di bawah `set -e`: bila URI sudah
  # punya port, perbandingan false dan bash langsung exit tanpa pesan.
  if [[ "$CQLSH_HOST" == "$uri" ]]; then
    CQLSH_PORT="9042"
  fi
}

need_cmd nodetool
need_cmd tar
need_cmd find
need_cmd cqlsh

load_dotenv_scylla
parse_cqlsh_host_port

[[ -d "$SCYLLA_DATA_DIR/$KEYSPACE" ]] || die "data keyspace tidak ada: $SCYLLA_DATA_DIR/$KEYSPACE"

mkdir -p "$OUT_DIR"/{snapshots,schema}
log "Keyspace : $KEYSPACE"
log "Tag      : $TAG"
log "Output   : $OUT_DIR"

# 1) Snapshot (flush memtable termasuk)
log "Mengambil snapshot nodetool..."
nodetool snapshot --keyspaces "$KEYSPACE" -t "$TAG" \
  || die "nodetool snapshot gagal"

# 2) Salin hardlink/copy file snapshot ke folder backup
KS_DATA="$SCYLLA_DATA_DIR/$KEYSPACE"
COPIED=0
while IFS= read -r -d '' snap_dir; do
  # .../stockbit/<table-uuid>/snapshots/<tag>
  table_uuid="$(basename "$(dirname "$(dirname "$snap_dir")")")"
  dest="$OUT_DIR/snapshots/$table_uuid"
  mkdir -p "$dest"
  # cp -a agar permission/mtime terjaga; butuh read access ke data Scylla
  if ! cp -a "$snap_dir"/. "$dest/" 2>/dev/null; then
    die "gagal menyalin $snap_dir → $dest (butuh permission baca $SCYLLA_DATA_DIR)"
  fi
  COPIED=$((COPIED + 1))
done < <(find "$KS_DATA" -type d -path "*/snapshots/$TAG" -print0)

[[ "$COPIED" -gt 0 ]] || die "tidak ada folder snapshots/$TAG di $KS_DATA"
log "Disalin $COPIED tabel snapshot."

# 3) Schema dump
SCHEMA_FILE="$OUT_DIR/schema/${KEYSPACE}_schema.cql"
log "Dump schema → $SCHEMA_FILE"
CQLSH_ARGS=(cqlsh "$CQLSH_HOST" "$CQLSH_PORT")
if [[ -n "${SCYLLA_USER:-}" ]]; then
  CQLSH_ARGS+=(-u "$SCYLLA_USER")
fi
if [[ -n "${SCYLLA_PASSWORD:-}" ]]; then
  CQLSH_ARGS+=(-p "$SCYLLA_PASSWORD")
fi

"${CQLSH_ARGS[@]}" -e "DESC KEYSPACE ${KEYSPACE};" >"$SCHEMA_FILE" 2>/tmp/scylla_backup_cqlsh.err \
  || {
    cat /tmp/scylla_backup_cqlsh.err >&2 || true
    die "cqlsh DESC KEYSPACE gagal"
  }
rm -f /tmp/scylla_backup_cqlsh.err

# Metadata
cat >"$OUT_DIR/backup_meta.txt" <<EOF
keyspace=$KEYSPACE
tag=$TAG
created_at=$(date -Iseconds)
host=$(hostname)
scylla_uri=${SCYLLA_URI:-}
scylla_data_dir=$SCYLLA_DATA_DIR
tables_snapshotted=$COPIED
EOF

# 4) Arsip tar.gz
mkdir -p "$BACKUP_ROOT"
log "Membuat arsip $ARCHIVE ..."
tar -C "$BACKUP_ROOT" -czf "$ARCHIVE" "$(basename "$OUT_DIR")"
log "Arsip siap: $ARCHIVE ($(du -h "$ARCHIVE" | awk '{print $1}'))"

# Hapus folder unpacked — semua isi sudah di dalam .tar.gz
rm -rf "$OUT_DIR"
log "Folder sementara dihapus: $OUT_DIR"

# 5) Bersihkan snapshot di node (hemat disk) kecuali KEEP_SNAPSHOT=1
if [[ "$KEEP_SNAPSHOT" == "1" ]]; then
  log "KEEP_SNAPSHOT=1 — snapshot Scylla tag=$TAG dibiarkan."
else
  log "Menghapus snapshot Scylla tag=$TAG ..."
  nodetool clearsnapshot -t "$TAG" --keyspaces "$KEYSPACE" \
    || log "WARN: clearsnapshot gagal (boleh diabaikan; hapus manual bila perlu)"
fi

log "Selesai."
log "Arsip  : $ARCHIVE"
