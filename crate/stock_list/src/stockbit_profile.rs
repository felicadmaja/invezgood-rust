//! Fetch & parse Stockbit emitten profile API → upsert kolom stockbit_profile di Scylla.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use scylla::client::session::Session;
use stockbit_browser::ensure_stockbit_bearer;

use crate::model::StockbitProfileDb;

const PROFILE_URL: &str = "https://exodus.stockbit.com/emitten";
const STOCKBIT_PROFILE_MAX_AGE_SECS: i64 = 30 * 24 * 60 * 60;

/// Perlu GET Stockbit API bila `stockbit_profile` kosong atau `stockbit_profile_updated_at` ≥ 30 hari.
pub fn needs_stockbit_profile_refresh(
    profile: Option<&StockbitProfileDb>,
    updated_at: Option<DateTime<Utc>>,
) -> bool {
    if profile.is_none() {
        return true;
    }
    let Some(updated_at) = updated_at else {
        return true;
    };
    Utc::now()
        .signed_duration_since(updated_at)
        .num_seconds()
        >= STOCKBIT_PROFILE_MAX_AGE_SECS
}

pub async fn fetch_stockbit_profile(code: &str) -> Result<StockbitProfileDb, String> {
    let code = code.trim().to_ascii_uppercase();
    let bearer = ensure_stockbit_bearer()
        .await
        .map_err(|e| format!("Stockbit bearer gagal: {e}"))?;

    let url = format!("{PROFILE_URL}/{code}/profile");
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
        .map_err(|e| format!("stockbit profile {code} request: {e}"))?;

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if status == reqwest::StatusCode::NOT_FOUND || status == reqwest::StatusCode::BAD_REQUEST {
        return Err(format!("Emiten {code} tidak ditemukan di Stockbit"));
    }
    if !status.is_success() {
        let preview: String = body.chars().take(280).collect();
        return Err(format!("stockbit profile {code} HTTP {status}: {preview}"));
    }

    serde_json::from_str(&body).map_err(|e| format!("stockbit profile {code} JSON: {e}"))
}

pub async fn fetch_and_save_stockbit_profile(
    session: Arc<Session>,
    code: &str,
) -> Result<(StockbitProfileDb, DateTime<Utc>), String> {
    let profile = fetch_stockbit_profile(code).await?;
    let updated_at = Utc::now();
    crate::repository::update_stockbit_profile(session.as_ref(), code, Some(profile.clone()), updated_at)
        .await?;
    Ok((profile, updated_at))
}
