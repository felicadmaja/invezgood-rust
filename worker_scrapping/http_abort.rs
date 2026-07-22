//! Abort scrap segera bila API mengembalikan HTTP 4xx (hindari diblokir server).
//!
//! `std::process::exit` tidak menjalankan `Drop`, jadi PM2 `stockbit_ws` di-start
//! kembali di sini sebelum exit (app biasanya di-`stop` saat worker jalan).

use reqwest::header::HeaderMap;
use reqwest::StatusCode;
use std::process::Command;

const PM2_APP_NAME: &str = "stockbit_ws";

pub fn is_http_4xx(status: StatusCode) -> bool {
    status.is_client_error()
}

/// Snapshot header kuota Stockbit (`x-rate-limit-*`).
#[derive(Debug, Clone, Copy, Default)]
pub struct RateLimitInfo {
    pub limit: Option<i64>,
    pub remaining: Option<i64>,
    pub reset_secs: u64,
}

impl RateLimitInfo {
    pub fn from_headers(headers: &HeaderMap) -> Self {
        Self {
            limit: header_i64(headers, "x-rate-limit-limit"),
            remaining: header_i64(headers, "x-rate-limit-remaining"),
            reset_secs: header_i64(headers, "x-rate-limit-reset")
                .and_then(|v| u64::try_from(v).ok())
                .unwrap_or(0),
        }
    }

    pub fn log_line(&self) -> String {
        format!(
            "x-rate-limit-limit={} x-rate-limit-remaining={} x-rate-limit-reset={}",
            self.limit
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".into()),
            self.remaining
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".into()),
            self.reset_secs
        )
    }

    /// `true` bila kuota menipis: `remaining <= limit` (atau `remaining <= 2` jika limit absen).
    pub fn is_quota_thin(&self) -> bool {
        match (self.remaining, self.limit) {
            (Some(r), Some(lim)) if lim > 0 => r <= lim,
            (Some(r), _) => r <= 2,
            _ => false,
        }
    }

    /// Jeda antar emiten (ms): 0 jika kuota tebal; 200–1000 ms jika menipis.
    /// `prev_remaining`: remaining response sebelumnya — bila belum naik / turun, jeda lebih panjang.
    pub fn inter_emiten_delay_ms(&self) -> u64 {
        self.inter_emiten_delay_ms_with_prev(None)
    }

    pub fn inter_emiten_delay_ms_with_prev(&self, prev_remaining: Option<i64>) -> u64 {
        if !self.is_quota_thin() {
            return 0;
        }
        let Some(r) = self.remaining else {
            return 200;
        };
        // Remaining 0, atau belum naik dibanding request sebelumnya → jeda lebih panjang.
        let not_recovering = r <= 0 || prev_remaining.is_some_and(|p| r <= p);
        if !not_recovering {
            return 200;
        }
        // Pakai reset window bila ada; clamp 200–1000 ms.
        if self.reset_secs > 0 {
            return self
                .reset_secs
                .saturating_mul(1000)
                .clamp(200, 1000);
        }
        // remaining=0 tanpa reset → 1000 ms; tipis tapi >0 → skala 300–800 ms.
        if r <= 0 {
            1000
        } else {
            (300 + (5u64.saturating_sub(r as u64)) * 100).clamp(200, 1000)
        }
    }
}

/// Ringkas header kuota umum Stockbit (`x-rate-limit-*`, `retry-after`).
/// Nilai `-` jika header tidak dikirim server.
pub fn rate_limit_headers_log(headers: &HeaderMap) -> String {
    let info = RateLimitInfo::from_headers(headers);
    let retry_after = header_str(headers, "retry-after");
    format!("{} retry-after={retry_after}", info.log_line())
}

fn header_str(headers: &HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "-".into())
}

fn header_i64(headers: &HeaderMap, name: &str) -> Option<i64> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok())
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
