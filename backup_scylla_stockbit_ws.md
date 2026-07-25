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

Output default (hanya arsip; folder sementara dihapus setelah `.tar.gz` dibuat):

| Item | Lokasi |
|------|--------|
| Arsip | `/home/baki1/stockbit_ws/backup_scylla_stockbit_ws/stockbit_YYYYMMDD_HHMMSS.tar.gz` |

Permission: folder backup `700` (hanya owner), file `.tar.gz` `600`. Script menerapkan ini otomatis tiap run.

Isi di dalam `.tar.gz`: `backup_meta.txt`, `schema/stockbit_schema.cql`, `snapshots/<table-uuid>/`.


## Catatan restore (ringkas)

Restore **bukan** sekadar extract tar ke sembarang folder. Alur tipikal:

1. Restore/create keyspace + schema dari `stockbit_schema.cql` (`cqlsh`).
2. Letakkan SSTable hasil snapshot ke directory `upload/` contoh `/var/lib/scylla/data/stockbit/<nama_tabel>-<uuid>/upload/` tabel yang sesuai di data dir Scylla.
3. Jalankan `nodetool refresh -- stockbit <table>`.

Untuk production, ikuti dokumentasi resmi Scylla: [Backup and restore](https://docs.scylladb.com/manual/stable/operating-scylla/procedures/backup-restore/).



## Jadwal otomatis (crontab)

Jalankan sebagai user **`baki1`** (bukan root), agar arsip tetap milik Anda dan folder backup tetap `700`.

### 1. Pastikan script executable

```bash
chmod +x /home/baki1/stockbit_ws/backup_scylla_stockbit_ws.sh
```

### 2. Uji sekali secara manual

```bash
/home/baki1/stockbit_ws/backup_scylla_stockbit_ws.sh
```

Pastikan muncul arsip baru di `backup_scylla_stockbit_ws/` dan exit code `0`.

### 3. Buka crontab user

```bash
crontab -e
```

### 4. Tambahkan baris jadwal

Contoh: **setiap hari jam 04:15** (waktu lokal server), log ke file:

```cron
15 4 * * * /home/baki1/stockbit_ws/backup_scylla_stockbit_ws.sh >> /home/baki1/stockbit_ws/backup_scylla_stockbit_ws.log 2>&1
```


`PATH` cron sering sempit; bila `nodetool`/`cqlsh` tidak ketemu, pakai bentuk ini:

```cron
15 4 * * * PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin /home/baki1/stockbit_ws/backup_scylla_stockbit_ws.sh >> /home/baki1/stockbit_ws/backup_scylla_stockbit_ws.log 2>&1
```


**Catatan:** pastikan user cron bisa membaca `/var/lib/scylla/data/stockbit` (sama seperti saat menjalankan manual). Script membaca `.env` dari root repo — jangan pindahkan `.env` tanpa update `DOTENV_FILE`.
