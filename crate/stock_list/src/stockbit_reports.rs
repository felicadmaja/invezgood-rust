//! Fetch & parse Stockbit stream reports API → upsert kolom stockbit_reports di Scylla.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use scylla::client::session::Session;
use serde::Deserialize;
use stockbit_browser::ensure_stockbit_bearer;

use crate::model::{StockbitReportStreamDb, StockbitReportsDb};

const STREAM_REPORTS_URL: &str = "https://exodus.stockbit.com/stream/v3/symbol";
const REPORTS_LIMIT: u32 = 20;
const STOCKBIT_REPORTS_MAX_AGE_SECS: i64 = 24 * 60 * 60;

#[derive(Debug, Deserialize)]
struct ApiReportsResponse {
    data: ApiReportsData,
}

#[derive(Debug, Deserialize)]
struct ApiReportsData {
    #[serde(default)]
    stream: Vec<StockbitReportStreamDb>,
}

/// Perlu GET Stockbit API bila `stockbit_reports_updated_at` kosong atau ≥ 1 hari.
pub fn needs_stockbit_reports_refresh(updated_at: Option<DateTime<Utc>>) -> bool {
    let Some(updated_at) = updated_at else {
        return true;
    };
    Utc::now()
        .signed_duration_since(updated_at)
        .num_seconds()
        >= STOCKBIT_REPORTS_MAX_AGE_SECS
}

pub async fn fetch_stockbit_reports(code: &str) -> Result<StockbitReportsDb, String> {
    let code = code.trim().to_ascii_uppercase();
    let bearer = ensure_stockbit_bearer()
        .await
        .map_err(|e| format!("Stockbit bearer gagal: {e}"))?;

    let url = format!(
        "{STREAM_REPORTS_URL}/{code}?category=STREAM_CATEGORY_REPORTS&last_stream_id=0&limit={REPORTS_LIMIT}&report_type=REPORT_TYPE_ALL"
    );
    let http = reqwest::Client::new();
    let resp = http
        .get(&url)
        .header("Authorization", format!("Bearer {bearer}"))
        .header("Accept", "application/json")
        .header("Origin", "https://stockbit.com")
        .header("Referer", "https://stockbit.com/")
        .header("x-platform", "web")
        .send()
        .await
        .map_err(|e| format!("stream reports {code} request: {e}"))?;

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if status == reqwest::StatusCode::NOT_FOUND || status == reqwest::StatusCode::BAD_REQUEST {
        return Err(format!("Emiten {code} tidak ditemukan di Stockbit"));
    }
    if !status.is_success() {
        let preview: String = body.chars().take(280).collect();
        return Err(format!("stream reports {code} HTTP {status}: {preview}"));
    }

    let parsed: ApiReportsResponse =
        serde_json::from_str(&body).map_err(|e| format!("stream reports {code} JSON: {e}"))?;
    if parsed.data.stream.is_empty() {
        Ok(None)
    } else {
        Ok(Some(parsed.data.stream))
    }
}

pub async fn fetch_and_save_stockbit_reports(
    session: Arc<Session>,
    code: &str,
) -> Result<(StockbitReportsDb, DateTime<Utc>), String> {
    let reports = fetch_stockbit_reports(code).await?;
    let updated_at = Utc::now();
    crate::repository::update_stockbit_reports(session.as_ref(), code, reports.clone(), updated_at)
        .await?;
    Ok((reports, updated_at))
}
