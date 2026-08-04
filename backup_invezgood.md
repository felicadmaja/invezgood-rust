# Restore backup ScyllaDB keyspace `invezgood`

File backup dibuat oleh `./backup_invezgood.sh` dan tersimpan di:

```text
backup_database/backup_invezgood_YYYY-MM-DD.gz
```

Isi arsip (tar + gzip):

| Path | Isi |
|------|-----|
| `schema.cql` | DDL keyspace + tabel + MV |
| `data/<table>-<uuid>/` | SSTables hasil snapshot `nodetool` |

Restore memakai **`cqlsh`** (schema) + **`sstableloader`** (data).

---

## Prasyarat

- ScyllaDB berjalan dan `nodetool status` menunjukkan node **UN**
- Perintah tersedia: `cqlsh`, `sstableloader`, `gzip`/`tar`
- Kredensial sama seperti di `.env` (`SCYLLA_URI`, `SCYLLA_USER`, `SCYLLA_PASSWORD`)
- Versi Scylla target sedapat mungkin sama / kompatibel dengan node sumber backup
- Disk cukup untuk extract + load

---

## 1. Pilih file backup

```bash
cd /home/baki1/invezgood_rust
ls -lh backup_database/backup_invezgood_*.gz
```

Contoh: `backup_database/backup_invezgood_2026-08-04.gz`

---

## 2. Extract ke folder sementara

```bash
BACKUP=backup_database/backup_invezgood_2026-08-04.gz
RESTORE_DIR=/tmp/restore_invezgood_$(date +%Y%m%d)

mkdir -p "$RESTORE_DIR"
gzip -dc "$BACKUP" | tar -x -C "$RESTORE_DIR"
ls "$RESTORE_DIR"
# diharapkan: schema.cql  data/
```

---

## 3. (Opsional) Drop keyspace lama

Hanya jika ingin **replace penuh**. Ini menghapus semua data keyspace saat ini.

```bash
# load .env bila perlu
set -a && source .env && set +a
HOST="${SCYLLA_URI%%:*}"

cqlsh "$HOST" -u "$SCYLLA_USER" -p "$SCYLLA_PASSWORD" \
  -e "DROP KEYSPACE IF EXISTS invezgood;"
```

Jika keyspace masih dipakai app (`invezgood` / PM2), **hentikan app dulu** agar tidak ada write selama restore.

```bash
pm2 stop invezgood   # opsional, disarankan
```

---

## 4. Restore schema

```bash
cqlsh "$HOST" -u "$SCYLLA_USER" -p "$SCYLLA_PASSWORD" -f "$RESTORE_DIR/schema.cql"
```

Cek:

```bash
cqlsh "$HOST" -u "$SCYLLA_USER" -p "$SCYLLA_PASSWORD" \
  -e "DESCRIBE KEYSPACE invezgood;"
```

---

## 5. Load SSTables dengan `sstableloader`

Jalankan **per folder tabel** di `data/` (nama folder = `nama_tabel-<uuid>`):

```bash
HOST="${SCYLLA_URI%%:*}"
# biasanya port native 9042; sstableloader memakai host cluster
for tbl_dir in "$RESTORE_DIR"/data/*/; do
  echo "==> Loading $(basename "$tbl_dir") ..."
  sstableloader -d "$HOST" "$tbl_dir"
done
```

Catatan:

- Materialized View ikut ter-load jika folder MV ada di arsip; setelah load, biarkan Scylla rebuild/konsisten MV bila perlu.
- Jika `sstableloader` minta autentikasi, tambahkan opsi yang didukung versi Anda (mis. `--username` / `--password`), selaras dengan `.env`.

---

## 6. Verifikasi

```bash
cqlsh "$HOST" -u "$SCYLLA_USER" -p "$SCYLLA_PASSWORD" -e "
SELECT COUNT(*) FROM invezgood.stock_list;
SELECT COUNT(*) FROM invezgood.portofolio;
"
```

Sesuaikan query dengan tabel yang ingin dicek. Lalu start ulang app:

```bash
pm2 start invezgood   # atau BUILD_SKIP_PM2_LOGS=1 ./build.sh
```

---

## 7. Bersihkan extract sementara

```bash
rm -rf "$RESTORE_DIR"
```

---

## Ringkas (copy-paste)

```bash
cd /home/baki1/invezgood_rust
set -a && source .env && set +a
HOST="${SCYLLA_URI%%:*}"
BACKUP=backup_database/backup_invezgood_2026-08-04.gz   # ganti tanggal
RESTORE_DIR=/tmp/restore_invezgood

rm -rf "$RESTORE_DIR"
mkdir -p "$RESTORE_DIR"
gzip -dc "$BACKUP" | tar -x -C "$RESTORE_DIR"

# Hati-hati: hapus keyspace lama
# cqlsh "$HOST" -u "$SCYLLA_USER" -p "$SCYLLA_PASSWORD" -e "DROP KEYSPACE IF EXISTS invezgood;"

cqlsh "$HOST" -u "$SCYLLA_USER" -p "$SCYLLA_PASSWORD" -f "$RESTORE_DIR/schema.cql"

for tbl_dir in "$RESTORE_DIR"/data/*/; do
  sstableloader -d "$HOST" "$tbl_dir"
done

rm -rf "$RESTORE_DIR"
```

---


