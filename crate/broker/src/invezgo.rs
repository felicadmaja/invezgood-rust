use std::collections::HashMap;
use std::sync::Arc;

use chrono::{Datelike, Days, Months, NaiveDate};
use scylla::client::session::Session;
use serde::Deserialize;

use crate::model::{BrokerRow, BrokerStalkerRow};

const INVEZGO_LIST_BROKER_URL: &str = "https://api.invezgo.com/analysis/list/broker";
const INVEZGO_STALKER_LIST_URL: &str = "https://api.invezgo.com/analysis/stalker/list";

#[derive(Debug, Deserialize)]
struct ApiBrokerItem {
    code: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct ApiStalkerResponse {
    summary: serde_json::Value,
    list: Vec<serde_json::Value>,
}

pub async fn fetch_and_save(session: Arc<Session>) -> Result<Vec<BrokerRow>, String> {
    let token = std::env::var("INVEZGO_BEARER_TOKEN")
        .map_err(|_| "INVEZGO_BEARER_TOKEN belum diset".to_string())?;

    let response = reqwest::Client::new()
        .get(INVEZGO_LIST_BROKER_URL)
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| format!("request Invezgo list/broker: {e}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| format!("baca body Invezgo list/broker: {e}"))?;

    if !status.is_success() {
        return Err(format!("Invezgo HTTP {status} list/broker: {body}"));
    }

    let parsed: Vec<ApiBrokerItem> = serde_json::from_str(&body)
        .map_err(|e| format!("parse JSON Invezgo list/broker: {e}"))?;

    let now = chrono::Utc::now();
    let mut saved = Vec::with_capacity(parsed.len());

    for item in parsed {
        let broker_code = item.code.trim().to_ascii_uppercase();
        if broker_code.is_empty() {
            continue;
        }

        let row = BrokerRow {
            broker_code: broker_code.clone(),
            name: Some(item.name),
            tipe: Some(0),
            asosiasi: None,
            catatan: None,
            is_huge: Some(false),
            is_top: Some(false),
            updated_at: Some(now),
        };

        crate::repository::upsert(session.as_ref(), &row).await?;
        saved.push(row);
    }

    saved.sort_by(|a, b| a.broker_code.cmp(&b.broker_code));
    Ok(saved)
}

/// Parse `YYYY-MM` → (from=YYYY-MM-01, to=hari terakhir bulan).
pub fn month_range(tahun_bulan: &str) -> Result<(NaiveDate, NaiveDate), String> {
    let from = NaiveDate::parse_from_str(&format!("{tahun_bulan}-01"), "%Y-%m-%d").map_err(|_| {
        format!("tahun_bulan tidak valid (harus YYYY-MM): {tahun_bulan}")
    })?;

    let to = from
        .checked_add_months(Months::new(1))
        .and_then(|d| d.checked_sub_days(Days::new(1)))
        .ok_or_else(|| format!("gagal hitung akhir bulan untuk tahun_bulan={tahun_bulan}"))?;

    if from.day() != 1 {
        return Err(format!("tahun_bulan tidak valid (harus YYYY-MM): {tahun_bulan}"));
    }

    Ok((from, to))
}

pub async fn fetch_stalker_and_save(
    session: Arc<Session>,
    broker_code: &str,
    tahun_bulan: &str,
) -> Result<BrokerStalkerRow, String> {
    let token = std::env::var("INVEZGO_BEARER_TOKEN")
        .map_err(|_| "INVEZGO_BEARER_TOKEN belum diset".to_string())?;

    let (from, to) = month_range(tahun_bulan)?;
    let url = format!(
        "{INVEZGO_STALKER_LIST_URL}/{broker_code}?from={}&to={}&investor=all&market=RG",
        from.format("%Y-%m-%d"),
        to.format("%Y-%m-%d")
    );

    let response = reqwest::Client::new()
        .get(&url)
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| format!("request Invezgo stalker/list: {e}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| format!("baca body Invezgo stalker/list: {e}"))?;

    if !status.is_success() {
        return Err(format!("Invezgo HTTP {status} stalker/list: {body}"));
    }

    let parsed: ApiStalkerResponse = serde_json::from_str(&body)
        .map_err(|e| format!("parse JSON Invezgo stalker/list: {e}"))?;

    let summary = json_object_to_string_map(parsed.summary);
    let list: Vec<HashMap<String, String>> = parsed
        .list
        .into_iter()
        .map(json_object_to_string_map)
        .collect();

    let row = BrokerStalkerRow {
        broker_code: broker_code.to_string(),
        tahun_bulan: tahun_bulan.to_string(),
        summary: Some(summary),
        list: Some(list),
    };

    crate::repository::upsert_stalker(session.as_ref(), &row).await?;
    Ok(row)
}

fn json_object_to_string_map(value: serde_json::Value) -> HashMap<String, String> {
    let serde_json::Value::Object(map) = value else {
        return HashMap::new();
    };

    map.into_iter()
        .map(|(key, val)| (key, json_value_to_string(val)))
        .collect()
}

fn json_value_to_string(value: serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s,
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}
