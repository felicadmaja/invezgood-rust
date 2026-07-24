# Panduan backup Scylla keyspace `stockbit`

Script: [`backup_scylla_stockbit_ws.sh`](./backup_scylla_stockbit_ws.sh)

Script ini membuat **snapshot** keyspace Scylla `stockbit`, menyalin file snapshot ke folder lokal, men-dump **schema CQL**, lalu mengemas semuanya menjadi `.tar.gz`.

Kredensial CQL (`SCYLLA_URI`, `SCYLLA_USER`, `SCYLLA_PASSWORD`, `SCYLLA_KEYSPACE`) **dibaca otomatis dari file `.env`** di root repo. Tidak perlu `export` atau mengisi variabel di bash.

## Prasyarat

- ScyllaDB berjalan di mesin ini (`nodetool status` OK).
- Perintah tersedia: `nodetool`, `cqlsh`, `tar`, `find`.
- User yang menjalankan script harus bisa **membaca** `/var/lib/scylla/data/stockbit`.
- File `.env` di root repo sudah berisi `SCYLLA_*` (sama seperti yang dipakai aplikasi).

## Cara menjalankan

Dari root repo:

```bash
cd /home/baki1/stockbit_ws
chmod +x backup_scylla_stockbit_ws.sh
./backup_scylla_stockbit_ws.sh
```

Output default:

| Item | Lokasi |
|------|--------|
| Folder backup | `/home/baki1/stockbit_ws/backup_scylla_stockbit_ws/stockbit_YYYYMMDD_HHMMSS/` |
| Arsip | `/home/baki1/stockbit_ws/backup_scylla_stockbit_ws/stockbit_YYYYMMDD_HHMMSS.tar.gz` |
| Schema | `.../schema/stockbit_schema.cql` |
| Snapshot files | `.../snapshots/<table-uuid>/` |
| Meta | `.../backup_meta.txt` |

## Opsi opsional (bukan kredensial)

Hanya untuk mengubah lokasi/perilaku backup; kredensial tetap dari `.env`.

```bash
# Folder tujuan arsip/folder backup
BACKUP_ROOT=/mnt/disk2/scylla_backups ./backup_scylla_stockbit_ws.sh

# Path data Scylla non-default
SCYLLA_DATA_DIR=/var/lib/scylla/data ./backup_scylla_stockbit_ws.sh

# Jangan hapus snapshot di dalam Scylla setelah copy (default: di-clear)
KEEP_SNAPSHOT=1 ./backup_scylla_stockbit_ws.sh
```

## Verifikasi singkat

```bash
# Cek arsip terbaru
ls -lht /home/baki1/stockbit_ws/backup_scylla_stockbit_ws/*.tar.gz | head

# Isi arsip
tar -tzf /home/baki1/stockbit_ws/backup_scylla_stockbit_ws/stockbit_*.tar.gz | head

# Schema
less /home/baki1/stockbit_ws/backup_scylla_stockbit_ws/stockbit_*/schema/stockbit_schema.cql
```

## Catatan restore (ringkas)

Restore **bukan** sekadar extract tar ke sembarang folder. Alur tipikal:

1. Restore/create keyspace + schema dari `stockbit_schema.cql` (`cqlsh`).
2. Letakkan SSTable hasil snapshot ke directory `upload/` tabel yang sesuai di data dir Scylla.
3. Jalankan `nodetool refresh -- stockbit <table>`.

Untuk production, ikuti dokumentasi resmi Scylla: [Backup and restore](https://docs.scylladb.com/manual/stable/operating-scylla/procedures/backup-restore/).

