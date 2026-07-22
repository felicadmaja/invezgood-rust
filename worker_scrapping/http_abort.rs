//! Abort scrap segera bila API mengembalikan HTTP 4xx (hindari diblokir server).
//!
//! Hanya aktif bila env `STOCKBIT_WORKER_ABORT_ON_HTTP_4XX=1` (di-set oleh bin
//! `worker_scrapping`). Invoke dari RPC / `stockbit_ws` **tidak** menghentikan proses —
//! caller menangani error HTTP seperti biasa.
//!
//! `std::process::exit` tidak menjalankan `Drop`, jadi PM2 `stockbit_ws` di-start
//! kembali di sini sebelum exit (app biasanya di-`stop` saat worker jalan).
//!
//! Parsing / jeda rate-limit: lihat [`crate::rate_limit_delay`].

use reqwest::StatusCode;
use std::process::Command;

const PM2_APP_NAME: &str = "stockbit_ws";
const ABORT_ENV: &str = "STOCKBIT_WORKER_ABORT_ON_HTTP_4XX";

pub fn is_http_4xx(status: StatusCode) -> bool {
    status.is_client_error()
}

/// True bila proses ini adalah worker scrap yang boleh abort-on-4xx.
pub fn worker_abort_on_http_4xx_enabled() -> bool {
    matches!(
        std::env::var(ABORT_ENV).as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
    )
}

/// Aktifkan abort-on-4xx (panggil di `main` bin `worker_scrapping`).
pub fn enable_worker_abort_on_http_4xx() {
    std::env::set_var(ABORT_ENV, "1");
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

/// Jika `status` adalah 4xx (kecuali 404):
/// - **worker** (`STOCKBIT_WORKER_ABORT_ON_HTTP_4XX=1`): log, `pm2 start`, `process::exit(1)`
/// - **RPC / stockbit_ws**: hanya log peringatan; tidak exit (caller handle error).
pub fn abort_app_if_http_4xx(status: StatusCode, context: &str) {
    if !is_http_4xx(status) {
        return;
    }
    // 404: biasanya emiten/kode tidak ditemukan — jangan bunuh seluruh proses.
    if status == StatusCode::NOT_FOUND {
        return;
    }
    let code = status.as_u16();
    if !worker_abort_on_http_4xx_enabled() {
        eprintln!(
            "HTTP {code} (4xx) di konteks RPC — tidak abort app.\n  context: {context}"
        );
        return;
    }
    eprintln!(
        "FATAL: API HTTP {code} (4xx) — hentikan worker agar tidak diblokir server.\n  context: {context}"
    );
    pm2_start_stockbit_ws();
    std::process::exit(1);
}
