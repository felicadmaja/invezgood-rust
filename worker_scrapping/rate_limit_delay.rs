//! Spesifikasi jeda adaptif berdasarkan header Stockbit `x-rate-limit-remaining`.
//!
//! Dipakai portofolio history, bandarmology, dan scrape API lain yang menghormati kuota.
//!
//! | `x-rate-limit-remaining` | Jeda |
//! |--------------------------|------|
//! | ≥ 4                      | 0 (tanpa jeda) |
//! | 3                        | 200 ms |
//! | 2                        | 300 ms |
//! | 1                        | 400 ms |
//! | ≤ 0                      | 1000 ms |
//!
//! Header terkait yang dilog: `x-rate-limit-limit`, `x-rate-limit-reset`, `retry-after`.

use reqwest::header::HeaderMap;

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

    /// `true` bila `remaining` di bawah 4 (perlu jeda antar request).
    pub fn is_quota_thin(&self) -> bool {
        matches!(self.remaining, Some(r) if r < 4)
    }

    /// Jeda (ms) menurut tabel spesifikasi di modul ini.
    pub fn inter_emiten_delay_ms(&self) -> u64 {
        delay_ms_for_remaining(self.remaining)
    }
}

/// Jeda (ms) dari nilai `x-rate-limit-remaining`.
///
/// Lihat tabel di dokumentasi modul.
pub fn delay_ms_for_remaining(remaining: Option<i64>) -> u64 {
    match remaining {
        Some(r) if r <= 0 => 1000,
        Some(1) => 400,
        Some(2) => 300,
        Some(3) => 200,
        _ => 0,
    }
}

/// Ringkas header kuota (`x-rate-limit-*`, `retry-after`). Nilai `-` jika absen.
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

#[cfg(test)]
mod tests {
    use super::delay_ms_for_remaining;

    #[test]
    fn delay_table() {
        assert_eq!(delay_ms_for_remaining(None), 0);
        assert_eq!(delay_ms_for_remaining(Some(10)), 0);
        assert_eq!(delay_ms_for_remaining(Some(4)), 0);
        assert_eq!(delay_ms_for_remaining(Some(3)), 200);
        assert_eq!(delay_ms_for_remaining(Some(2)), 300);
        assert_eq!(delay_ms_for_remaining(Some(1)), 400);
        assert_eq!(delay_ms_for_remaining(Some(0)), 1000);
        assert_eq!(delay_ms_for_remaining(Some(-1)), 1000);
    }
}
