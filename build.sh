#!/bin/bash

# Konfigurasi - GANTI SESUAI NAMA PROYEKMU
NAMA_APP="stockbit_ws"

echo "🚀 Memulai proses deployment..."

source $HOME/.cargo/env

# 0. Cargo check terlebih dahulu
# echo "🔍 Menjalankan cargo check..."
# cargo check
# if [ $? -ne 0 ]; then
#     echo "❌ Cargo check gagal! Membatalkan deploy."
#     exit 1
# fi
# echo "✅ Cargo check berhasil."

# 1. Jalankan Build Release
echo "📦 Mengompilasi kode dalam mode release..."
cargo build --release

if [ $? -eq 0 ]; then
    echo "✅ Build berhasil."
else
    echo "❌ Build gagal! Membatalkan deploy."
    exit 1
fi

mkdir -p logs

# 2. Start atau restart PM2
echo "🔄 Merestart PM2 app $NAMA_APP..."
if pm2 describe "$NAMA_APP" >/dev/null 2>&1; then
    pm2 flush "$NAMA_APP" || true
    pm2 restart "$NAMA_APP" --update-env
else
    echo "ℹ️  Proses $NAMA_APP belum ada — start dari ecosystem.config.js"
    pm2 start ecosystem.config.js
    pm2 save
fi

# 3. Cek Status
echo "📊 Status PM2 saat ini:"
pm2 status "$NAMA_APP"

echo "✨ Selesai! Aplikasi kamu sudah versi terbaru."

pm2 logs "$NAMA_APP"