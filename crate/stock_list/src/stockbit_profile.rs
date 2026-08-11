//! Fetch & parse Stockbit emitten profile API → upsert kolom stockbit_profile di Scylla.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use scylla::client::session::Session;
use serde::Deserialize;
use stockbit_browser::ensure_stockbit_bearer;

use crate::model::StockbitProfileDb;

const PROFILE_URL: &str = "https://exodus.stockbit.com/emitten";
const STOCKBIT_PROFILE_MAX_AGE_SECS: i64 = 30 * 24 * 60 * 60;
const API_BODY_LOG_MAX_CHARS: usize = 2000;

#[derive(Debug, Deserialize)]
struct ApiProfileResponse {
    data: StockbitProfileDb,
}

fn parse_stockbit_profile_response(body: &str, code: &str) -> Result<StockbitProfileDb, String> {
    serde_json::from_str::<ApiProfileResponse>(body)
        .map(|parsed| parsed.data)
        .map_err(|e| {
            let preview: String = body.chars().take(500).collect();
            eprintln!(
                "\x1b[33mstockbit profile API {code} JSON parse error: {e}\nbody preview: {preview}\x1b[0m"
            );
            format!("stockbit profile {code} JSON: {e}")
        })
}

fn log_stockbit_profile_api(code: &str, body: &str, profile: &StockbitProfileDb) {
    let char_count = body.chars().count();
    let preview: String = body.chars().take(API_BODY_LOG_MAX_CHARS).collect();
    let truncated = char_count > API_BODY_LOG_MAX_CHARS;
    eprintln!(
        "\x1b[36mstockbit profile API {code} response ({} bytes{}):\n{preview}{}\x1b[0m",
        body.len(),
        if truncated { ", preview truncated" } else { "" },
        if truncated { "\n..." } else { "" },
    );
    eprintln!(
        "\x1b[36mstockbit profile API {code} parsed: background_chars={} address={} shareholder={} sdc={} shareholder_numbers={} beneficiary={} key_executive_nonempty={}\x1b[0m",
        profile.background.chars().count(),
        profile.address.as_ref().map(|v| v.len()).unwrap_or(0),
        profile.shareholder.as_ref().map(|v| v.len()).unwrap_or(0),
        profile
            .shareholder_director_commissioner
            .as_ref()
            .map(|v| v.len())
            .unwrap_or(0),
        profile
            .shareholder_numbers
            .as_ref()
            .map(|v| v.len())
            .unwrap_or(0),
        profile.beneficiary.as_ref().map(|v| v.len()).unwrap_or(0),
        key_executive_has_entries(&profile.key_executive),
    );
}

fn vec_has_items<T>(value: &Option<Vec<T>>) -> bool {
    value.as_ref().is_some_and(|items| !items.is_empty())
}

fn key_executive_has_entries(key_executive: &crate::model::StockbitProfileKeyExecutiveDb) -> bool {
    [
        &key_executive.commissioner,
        &key_executive.director,
        &key_executive.independent_commissioner,
        &key_executive.president_commissioner,
        &key_executive.president_director,
        &key_executive.vice_president,
        &key_executive.vice_president_commissioner,
        &key_executive.independent_vice_president_commissioner,
        &key_executive.independent_president_commissioner,
    ]
    .into_iter()
    .any(vec_has_items)
}

/// Profil dianggap kosong bila tidak ada konten utama (background, alamat, pemegang saham, jajaran).
pub fn is_stockbit_profile_empty(profile: &StockbitProfileDb) -> bool {
    profile.background.trim().is_empty()
        && !vec_has_items(&profile.address)
        && !vec_has_items(&profile.shareholder)
        && !vec_has_items(&profile.shareholder_director_commissioner)
        && !vec_has_items(&profile.beneficiary)
        && !key_executive_has_entries(&profile.key_executive)
}

/// Perlu GET Stockbit API bila `stockbit_profile` kosong/null atau `stockbit_profile_updated_at` ≥ 30 hari.
pub fn needs_stockbit_profile_refresh(
    profile: Option<&StockbitProfileDb>,
    updated_at: Option<DateTime<Utc>>,
) -> bool {
    let Some(profile) = profile else {
        return true;
    };
    if is_stockbit_profile_empty(profile) {
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

    let profile = parse_stockbit_profile_response(&body, &code)?;
    log_stockbit_profile_api(&code, &body, &profile);
    Ok(profile)
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
