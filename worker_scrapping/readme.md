# Worker Scraping Stockbit (`stockbit_scrapper_worker`)

Binary: `worker_scrapping` (crate `worker_scrapping`).

Worker ini **bukan** proses PM2 `stockbit_ws`. Ia dijalankan terpisah (manual atau cron).

## Prasyarat

- Rust toolchain + build release workspace
- Chromium/Chrome (`CHROME_EXECUTABLE_PATH` opsional)
- ScyllaDB, Redis, GCS (lihat `.env-example` di root repo)
- PM2 app `stockbit_ws` sudah terdaftar
- File `.env` di root workspace (`stockbit_ws/.env`)

## Build

Dari root repo:

```bash
cargo build --release -p worker_scrapping
```

Binary:

```text
/home/baki1/stockbit_ws/target/release/worker_scrapping
```



## Jalankan manual (uji)

```bash
./target/release/worker_scrapping
```

Pastikan `.env` terisi (`STOCKBIT_EMAIL`, `STOCKBIT_PASSWORD`, `STOCKBUT_PIN`, Scylla, Redis, GCS, dll.).

## Deploy via cron (2× sehari: 04:00 & 21:00)

Zona waktu disarankan **Asia/Jakarta** (WIB). Cron memanggil binary langsung (tanpa wrapper script).

### 1. Siapkan folder log

```bash
mkdir -p /home/baki1/stockbit_ws/logs
```



### 2. Crontab

```bash
crontab -e
```

Isi (WIB):

```cron
SHELL=/bin/bash
PATH=/home/baki1/.cargo/bin:/usr/local/bin:/usr/bin:/bin
CRON_TZ=Asia/Jakarta

# Stockbit scrapper — jam 04:00 dan 21:00
0 4 * * * cd /home/baki1/stockbit_ws && ./target/release/worker_scrapping >> /home/baki1/stockbit_ws/logs/scrapper_$(date +\%Y\%m\%d_\%H\%M\%S).log 2>&1
0 21 * * * cd /home/baki1/stockbit_ws && ./target/release/worker_scrapping >> /home/baki1/stockbit_ws/logs/scrapper_$(date +\%Y\%m\%d_\%H\%M\%S).log 2>&1
```

Jika `CRON_TZ` tidak didukung di sistem Anda, set timezone OS ke `Asia/Jakarta`, atau hitung offset manual.

Catatan: `%` di crontab harus di-escape (`\%`) agar `date` dijalankan saat job, bukan saat parse crontab.

### 3. Verifikasi cron

```bash
crontab -l
grep CRON /var/log/syslog 2>/dev/null || journalctl -u cron -n 50
ls -lt /home/baki1/stockbit_ws/logs/scrapper_*.log | head
```



## Catatan operasional


| Topik       | Keterangan                                                                                                          |
| ----------- | ------------------------------------------------------------------------------------------------------------------- |
| Durasi      | Bisa lama (banyak emiten + bandarmology + portfolio). Hindari overlap: jangan jalankan scrape manual saat jam cron. |
| PM2         | Worker stop/start `stockbit_ws` sendiri.                                                                            |
| HTTP 4xx    | Worker abort + `pm2 start stockbit_ws` agar tidak diblokir.                                                         |
| Sesi Chrome | Profil di `worker_scrapping/browser_data/` — jangan hapus sembarangan.                                              |
| 2FA         | Jika muncul trusted device, set `STOCKBIT_2FA_TIMEOUT_SECS` dan approve di HP.                                      |
| Rebuild     | Setelah `git pull`, jalankan ulang `cargo build --release -p worker_scrapping`.                                     |




## Env penting (ringkas)

Salin dari root `.env-example` → `.env`:

- `STOCKBIT_EMAIL`, `STOCKBIT_PASSWORD`, `STOCKBUT_PIN`
- `SCYLLA_*`, `REDIS_URL`
- `GCS_*`, `CHROME_EXECUTABLE_PATH` (opsional)



## Troubleshooting

- Cron jalan tapi gagal segera: cek log `logs/scrapper_*.log` (path `.env`, Chrome, Scylla).
- `stockbit_ws` tetap stopped: jalankan `pm2 start stockbit_ws` manual; pastikan worker sempat exit (bukan hang).
- Job overlap: pastikan run sebelumnya sudah selesai sebelum jam cron berikutnya.

