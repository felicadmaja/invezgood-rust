//! Fetch & parse Stockbit emitten profile API → upsert kolom stockbit_profile di Scylla.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use scylla::client::session::Session;
use serde::Deserialize;
use stockbit_browser::ensure_stockbit_bearer;

use crate::model::StockbitProfileDb;

const PROFILE_URL: &str = "https://exodus.stockbit.com/emitten";
const STOCKBIT_PROFILE_MAX_AGE_SECS: i64 = 30 * 24 * 60 * 60;

#[derive(Debug, Deserialize)]
struct ApiProfileResponse {
    #[serde(default)]
    data: Option<StockbitProfileDb>,
}

fn parse_stockbit_profile_response(body: &str, code: &str) -> Result<StockbitProfileDb, String> {
    serde_json::from_str::<ApiProfileResponse>(body)
        .map_err(|e| format!("stockbit profile {code} JSON: {e}"))
        .and_then(|parsed| {
            parsed
                .data
                .ok_or_else(|| format!("stockbit profile {code}: field data null atau tidak ada"))
        })
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

    parse_stockbit_profile_response(&body, &code).map_err(|e| {
        let preview: String = body.chars().take(280).collect();
        if let Ok(path) = std::env::var("STOCKBIT_PROFILE_DEBUG_BODY") {
            let _ = std::fs::write(&path, &body);
            eprintln!("stockbit profile {code}: body disimpan ke {path}");
        }
        let at = e
            .find("column ")
            .and_then(|i| e[i + 7..].split_whitespace().next())
            .and_then(|c| c.parse::<usize>().ok());
        if let Some(col) = at {
            let snippet: String = body
                .chars()
                .skip(col.saturating_sub(80))
                .take(160)
                .collect();
            eprintln!("stockbit profile {code} JSON near col {col}: {snippet}");
        }
        format!("{e} (body preview: {preview})")
    })
}

pub async fn fetch_and_save_stockbit_profile(
    session: Arc<Session>,
    code: &str,
) -> Result<(StockbitProfileDb, DateTime<Utc>), String> {
    let code = code.trim().to_ascii_uppercase();
    let profile = fetch_stockbit_profile(&code).await.map_err(|e| {
        eprintln!("GetStockbitProfileByCode {code} fetch gagal: {e}");
        e
    })?;
    let updated_at = Utc::now();
    crate::repository::update_stockbit_profile(
        session.as_ref(),
        &code,
        profile.clone(),
        updated_at,
    )
    .await
    .map_err(|e| {
        eprintln!("GetStockbitProfileByCode {code} upsert Scylla gagal: {e}");
        e
    })?;

    let row = crate::repository::get_stockbit_profile_by_code(session.as_ref(), &code)
        .await
        .map_err(|e| {
            eprintln!("GetStockbitProfileByCode {code} read-back gagal: {e}");
            e
        })?
        .ok_or_else(|| format!("stockbit profile {code}: baris tidak ditemukan setelah upsert"))?;
    if row.stockbit_profile.is_none() {
        return Err(format!("stockbit profile {code}: kolom stockbit_profile masih null setelah upsert"));
    }
    let saved_at = row
        .stockbit_profile_updated_at
        .ok_or_else(|| format!("stockbit profile {code}: stockbit_profile_updated_at masih null setelah upsert"))?;
    eprintln!(
        "GetStockbitProfileByCode {code} upsert Scylla OK updated_at={saved_at}"
    );
    Ok((profile, updated_at))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_stockbit_profile_contoh_json() {
        let inner = include_str!("stockbit_profile_contoh.json");
        let body = format!(r#"{{"data":{inner}}}"#);
        parse_stockbit_profile_response(&body, "ABDA").expect("contoh ABDA harus parse");
    }

    #[test]
    fn parse_stockbit_profile_abda_subsidiary_company_field() {
        let body = r#"{"data":{"subsidiary":[{"company":"PT Asuransi Bina Dana Arta, Tbk","percentage":"100%"}]}}"#;
        let profile = parse_stockbit_profile_response(body, "ABDA").expect("subsidiary company alias");
        let sub = profile
            .subsidiary
            .expect("subsidiary")
            .into_iter()
            .next()
            .expect("one sub");
        assert_eq!(sub.name, "PT Asuransi Bina Dana Arta, Tbk");
        assert_eq!(sub.percentage, "100%");
    }

    #[test]
    fn parse_stockbit_profile_abda_classification_object() {
        let inner = include_str!("stockbit_profile_abda_classification.json");
        let body = format!(r#"{{"data":{inner}}}"#);
        let profile =
            parse_stockbit_profile_response(&body, "ABDA").expect("ABDA classification object");
        let class = profile
            .sector_classification
            .expect("sector_classification harus terisi");
        assert_eq!(class.sector.name, "Keuangan");
        assert_eq!(class.sub_sector.name, "Asuransi");
    }
}
