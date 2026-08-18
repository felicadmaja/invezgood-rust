//! Ambil kalender libur nasional + cuti bersama dari
//! `GET https://api.kemendesa.link/libur-nasional/api/holidays/{tahun}.json`
//! (sumber SKB 3 Menteri), lalu upsert ke `invezgood.hari_libur`.

use std::sync::Arc;
use std::time::Duration;

use chrono::{Datelike, NaiveDate, Utc};
use scylla::client::session::Session;
use serde::Deserialize;

use crate::model::{HariLiburRow, KEYSPACE, TABLE};

const HOLIDAY_API_BASE_URL: &str = "https://api.kemendesa.link/libur-nasional/api/holidays";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Deserialize)]
struct ApiResponse {
    #[serde(default)]
    data: Vec<ApiHoliday>,
}

#[derive(Debug, Deserialize)]
struct ApiHoliday {
    date: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    is_civic: bool,
    #[serde(default)]
    is_religious: bool,
    #[serde(default)]
    is_cuti_bersama: bool,
}

/// `Ok(None)` bila tahun tidak tersedia di API (HTTP 404 — bukan error).
async fn fetch_tahun(tahun: &str) -> Result<Option<Vec<HariLiburRow>>, String> {
    let url = format!("{HOLIDAY_API_BASE_URL}/{tahun}.json");
    let response = reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .map_err(|e| format!("HTTP client hari libur: {e}"))?
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("request {url}: {e}"))?;

    let status = response.status();
    if status.as_u16() == 404 {
        return Ok(None);
    }

    let body = response
        .text()
        .await
        .map_err(|e| format!("baca body {url}: {e}"))?;

    if !status.is_success() {
        let preview: String = body.chars().take(160).collect();
        return Err(format!("HTTP {status} {url}: {preview}"));
    }

    let parsed: ApiResponse =
        serde_json::from_str(&body).map_err(|e| format!("parse JSON {url}: {e}"))?;

    let now = Utc::now();
    let mut rows = Vec::with_capacity(parsed.data.len());
    for item in parsed.data {
        let Ok(date) = NaiveDate::parse_from_str(item.date.trim(), "%Y-%m-%d") else {
            eprintln!("hari libur {tahun}: date '{}' dilewati (bukan YYYY-MM-DD)", item.date);
            continue;
        };

        rows.push(HariLiburRow {
            date,
            tahun: Some(date.year().to_string()),
            name: Some(item.name.trim().to_string()),
            is_civic: Some(item.is_civic),
            is_religious: Some(item.is_religious),
            is_cuti_bersama: Some(item.is_cuti_bersama),
            updated_at: Some(now),
        });
    }

    rows.sort_by_key(|row| row.date);
    Ok(Some(rows))
}

/// Fetch satu tahun dari API lalu upsert ke `invezgood.hari_libur`.
/// `Ok(None)` bila tahun tidak ada di API (HTTP 404) — pemanggil balas stream kosong.
/// Cache libur nasional `market_holiday` dibuang per tanggal supaya perubahan langsung terpakai.
pub async fn fetch_and_save(
    session: Arc<Session>,
    tahun: &str,
) -> Result<Option<Vec<HariLiburRow>>, String> {
    let Some(rows) = fetch_tahun(tahun).await? else {
        return Ok(None);
    };

    for row in &rows {
        crate::repository::upsert(session.as_ref(), row).await?;
        market_holiday::invalidate_national_holiday(row.date).await;
    }

    println!(
        "Hari libur {tahun}: {} tanggal di-upsert ke {KEYSPACE}.{TABLE}",
        rows.len()
    );
    Ok(Some(rows))
}
