# Crontab — restart PM2 `invezgood` setiap 18:00

Jalankan sebagai user yang menjalankan PM2 (biasanya user login server):

```bash
crontab -e
```

Tambahkan baris ini (18:00 **waktu server** — pastikan timezone server sudah WIB bila perlu):

```cron
0 18 * * * /home/baki1/.nvm/versions/node/v22.16.0/bin/pm2 restart invezgood >> /home/baki1/invezgood_rust/cron-pm2-restart.log 2>&1
```

Cek crontab terpasang:

```bash
crontab -l
```

## Catatan

- `0 18 * * *` = setiap hari jam 18:00 (menit 0).
- Path PM2 memakai NVM; bila versi Node diganti, update path di crontab (`which pm2`).
- Log restart: `cron-pm2-restart.log` di root repo.
