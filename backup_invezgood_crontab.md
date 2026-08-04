# Crontab — backup harian `invezgood`

Jalankan `./backup_invezgood.sh` otomatis tiap hari. Output: `backup_database/backup_invezgood_YYYY-MM-DD.gz`.

## 1. Pastikan script bisa dijalankan

```bash
cd /home/baki1/invezgood_rust
chmod +x backup_invezgood.sh
./backup_invezgood.sh   # uji manual sekali
```

## 2. Pasang crontab

```bash
crontab -e
```

Tambahkan **satu** baris (sesuaikan path & jam):

```cron
# Backup Scylla keyspace invezgood setiap hari jam 02:00
0 17 * * * cd /home/baki1/invezgood_rust && PATH=/usr/bin:/bin:/usr/sbin:/sbin /usr/bin/flock -n /tmp/backup_invezgood.lock ./backup_invezgood.sh >> logs/backup_invezgood.log 2>&1
```

| Bagian | Arti |
|--------|------|
| `0 17 * * *` | Setiap hari pukul 17:00 (waktu server) |
| `cd …` | Working directory project (agar `.env` & `backup_database/` ketemu) |
| `flock -n …` | Cegah 2 backup jalan bersamaan |
| `>> logs/…` | Simpan stdout/stderr ke log |

Buat folder log sekali:

```bash
mkdir -p /home/baki1/invezgood_rust/logs
```


## 3. Cek

```bash
crontab -l
tail -f /home/baki1/invezgood_rust/logs/backup_invezgood.log
ls -lh /home/baki1/invezgood_rust/backup_database/
```




