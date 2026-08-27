//! GET Invezgo shareholder classification → upsert `invezgood.shareholder_composition`.

use std::collections::HashMap;
use std::sync::Arc;

use scylla::client::session::Session;
use serde::Deserialize;
use serde_json::Value;

use crate::model::ShareholderCompositionRow;

const INVEZGO_CLASSIFICATION_URL: &str =
    "https://api.invezgo.com/analysis/shareholder/classification";
const DEFAULT_RANGE: u32 = 6;

#[derive(Debug, Deserialize)]
struct ApiClassificationItem {
    code: String,
    date: String,
    #[serde(flatten)]
    fields: HashMap<String, Value>,
}

fn normalize_code(raw: &str) -> String {
    raw.trim().to_ascii_uppercase()
}

fn date_to_tahun_bulan(date_raw: &str) -> Result<String, String> {
    let trimmed = date_raw.trim();
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(trimmed) {
        return Ok(dt.format("%Y-%m").to_string());
    }
    let date_part = trimmed.split('T').next().unwrap_or(trimmed);
    let parsed = chrono::NaiveDate::parse_from_str(date_part, "%Y-%m-%d")
        .map_err(|_| format!("date invalid: {date_raw}"))?;
    Ok(parsed.format("%Y-%m").to_string())
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn api_item_to_row(item: ApiClassificationItem) -> Result<ShareholderCompositionRow, String> {
    let code = normalize_code(&item.code);
    if code.is_empty() {
        return Err("code kosong dari Invezgo classification".into());
    }
    let tahun_bulan = date_to_tahun_bulan(&item.date)?;
    let mut detail = HashMap::new();
    for (key, value) in item.fields {
        detail.insert(key, value_to_string(&value));
    }
    Ok(ShareholderCompositionRow {
        code,
        tahun_bulan,
        detail: Some(detail),
    })
}

/// GET classification range=6 → upsert semua periode ke Scylla. Return jumlah baris tersimpan.
pub async fn fetch_and_save(session: Arc<Session>, code: &str) -> Result<usize, String> {
    let code = normalize_code(code);
    if code.is_empty() {
        return Err("code wajib diisi".into());
    }

    let url = format!("{INVEZGO_CLASSIFICATION_URL}/{code}?range={DEFAULT_RANGE}");

    let body = invezgo_http::get(&url).await?;
    let parsed: Vec<ApiClassificationItem> = serde_json::from_str(&body)
        .map_err(|e| format!("parse JSON Invezgo classification {code}: {e}"))?;

    let mut saved = 0usize;
    for item in parsed {
        let row = api_item_to_row(item)?;
        crate::repository::upsert(session.as_ref(), &row).await?;
        saved += 1;
    }

    eprintln!("shareholder_composition Invezgo {code}: {saved} baris di-upsert");
    Ok(saved)
}
