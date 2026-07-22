//! Abort scrap segera bila API mengembalikan HTTP 4xx (hindari diblokir server).
//!
//! `std::process::exit` tidak menjalankan `Drop`, jadi PM2 `stockbit_ws` di-start
//! kembali di sini sebelum exit (app biasanya di-`stop` saat worker jalan).
//!
//! Parsing / jeda rate-limit: lihat [`crate::rate_limit_delay`].

use reqwest::StatusCode;
use std::process::Command;

const PM2_APP_NAME: &str = "stockbit_ws";

pub fn is_http_4xx(status: StatusCode) -> bool {
    status.is_client_error()
}

fn pm2_start_stockbit_ws() {
    eprintln!("PM2: start {PM2_APP_NAME} (abort karena HTTP 4xx)...");
    match Command::new("pm2").args(["start", PM2_APP_NAME]).output() {
        Ok(out) if out.status.success() => {
            eprintln!("PM2: {PM2_APP_NAME} di-start.");
        }
        Ok(out) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            let stdout = String::from_utf8_lossy(&out.stdout);
            eprintln!(
                "Peringatan: pm2 start gagal (exit {:?}): {}{}",
                out.status.code(),
                stderr.trim(),
                if stdout.trim().is_empty() {
                    String::new()
                } else {
                    format!(" | {}", stdout.trim())
                }
            );
        }
        Err(e) => eprintln!("Peringatan: gagal menjalankan pm2 start: {e}"),
    }
}

/// Jika `status` adalah 4xx: log, start PM2 `stockbit_ws`, lalu `process::exit(1)`.
/// Tidak return bila 4xx (kecuali 404 — ticker/resource tidak ada; biarkan caller handle).
pub fn abort_app_if_http_4xx(status: StatusCode, context: &str) {
    if !is_http_4xx(status) {
        return;
    }
    // 404: biasanya emiten/kode tidak ditemukan — jangan bunuh seluruh proses.
    if status == StatusCode::NOT_FOUND {
        return;
    }
    let code = status.as_u16();
    eprintln!(
        "FATAL: API HTTP {code} (4xx) — hentikan worker agar tidak diblokir server.\n  context: {context}"
    );
    pm2_start_stockbit_ws();
    std::process::exit(1);
}
