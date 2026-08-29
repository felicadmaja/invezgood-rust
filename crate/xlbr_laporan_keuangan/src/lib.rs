pub mod pb {
    tonic::include_proto!("xlbr_laporan_keuangan");
}

pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("xlbr_laporan_keuangan_descriptor");

mod database;
pub mod model;
mod parser;
pub mod repository;
mod service;

pub use database::connect;
pub use model::{KEYSPACE, QUARTERS, TABLE, XlbrLaporanKeuanganRow};
pub use pb::xlbr_laporan_keuangan_server::{XlbrLaporanKeuangan, XlbrLaporanKeuanganServer};
pub use service::XlbrLaporanKeuanganService;

use std::sync::Arc;

use scylla::client::session::Session;

use model::{quarter_index, required_prior_quarters, XlbrLaporanKeuanganRow as Row};
use parser::parse_zip_bytes;
use repository::{row_from_standalone, standalone_sum, upsert, list_for_year};

/// Alur inti UploadFromUrl: download → parse → dekumulasi → upsert.
pub async fn upload_from_url(session: Arc<Session>, url: &str) -> Result<Row, String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()
        .map_err(|e| format!("reqwest client: {e}"))?;

    let mut req = client
        .get(url.trim())
        .header(
            "User-Agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        .header("Accept", "*/*")
        .header("Accept-Language", "en-US,en;q=0.9,id;q=0.8")
        .header(
            "Referer",
            "https://www.idx.co.id/id/perusahaan-tercatat/laporan-keuangan-dan-tahunan",
        );

    if let Ok(cookie) = std::env::var("IDX_COOKIE") {
        if !cookie.trim().is_empty() {
            req = req.header("Cookie", cookie.trim());
        }
    }

    let response = req
        .send()
        .await
        .map_err(|e| format!("curl {url} gagal: {e}"))?;

    if !response.status().is_success() {
        return Err(format!("curl {url} HTTP {}", response.status()));
    }

    let bytes = response
        .bytes()
        .await
        .map_err(|e| format!("baca body: {e}"))?;

    if bytes.is_empty() {
        return Err("zip kosong".into());
    }

    let parsed = parse_zip_bytes(&bytes)?;

    let existing = list_for_year(session.as_ref(), &parsed.meta.code, parsed.meta.fiscal_year).await?;
    validate_serial_upload(&existing, &parsed.meta.quarter)?;

    let prior_sum = standalone_sum(&existing);
    let standalone = parsed.ytd.deaccumulate(&prior_sum);

    let row = row_from_standalone(
        &parsed.meta.code,
        parsed.meta.fiscal_year,
        &parsed.meta.quarter,
        parsed.meta.period_end,
        &parsed.meta.presentation_currency,
        parsed.meta.unit_scale,
        standalone,
        &parsed.source_zip_hash,
    );

    upsert(session.as_ref(), &row).await?;
    Ok(row)
}

fn validate_serial_upload(existing: &[Row], quarter: &str) -> Result<(), String> {
    let idx = quarter_index(quarter).ok_or_else(|| format!("quarter tidak valid: {quarter}"))?;

    for &req in required_prior_quarters(quarter)? {
        if !existing.iter().any(|r| r.quarter.eq_ignore_ascii_case(req)) {
            return Err(format!(
                "upload {quarter} membutuhkan {req} sudah di-upload untuk tahun buku yang sama"
            ));
        }
    }

    if existing.iter().any(|r| r.quarter.eq_ignore_ascii_case(quarter)) {
        return Err(format!("{quarter} sudah ada untuk tahun buku ini"));
    }

    for later in &QUARTERS[idx + 1..] {
        if existing.iter().any(|r| r.quarter.eq_ignore_ascii_case(later)) {
            return Err(format!(
                "tidak bisa upload {quarter}: {later} sudah ada — upload harus serial TW1→TW4"
            ));
        }
    }

    Ok(())
}
