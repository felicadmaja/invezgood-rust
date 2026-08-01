use std::sync::Arc;

use crate::model::Keystats;
use scylla::client::session::Session;
use serde::Deserialize;

const INVEZGO_STOCK_LIST_URL: &str = "https://api.invezgo.com/analysis/list/stock";

fn keystat_url(code: &str) -> String {
    format!("https://api.invezgo.com/analysis/keystat/{code}?type=Q")
}


#[derive(Debug, Deserialize)]
struct InvezgoStockItem {
    code: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    sector: Option<String>,
    #[serde(default)]
    logo: Option<String>,
}

#[derive(Debug, Deserialize)]
struct InvezgoStockListWrapped {
    data: Vec<InvezgoStockItem>,
}

#[derive(Debug, Deserialize)]
struct InvezgoKeystatsResponse {
    rows: Vec<InvezgoKeystatsRow>,
    columns: Vec<InvezgoKeystatsColumn>,
}

#[derive(Debug, Deserialize)]
struct InvezgoKeystatsRow {
    id: String,
    name: String,
    #[serde(default)]
    values: Vec<InvezgoKeystatsValue>,
}

#[derive(Debug, Deserialize)]
struct InvezgoKeystatsValue {
    col: String,
    year: i32,
    amount: f64,
    period: String,
}

#[derive(Debug, Deserialize)]
struct InvezgoKeystatsColumn {
    year: i32,
    label: String,
    period: String,
}

pub async fn fetch_keystats(code: &str) -> Result<Keystats, String> {
    let token = std::env::var("INVEZGO_BEARER_TOKEN")
        .map_err(|_| "INVEZGO_BEARER_TOKEN belum diset".to_string())?;

    let response = reqwest::Client::new()
        .get(keystat_url(code))
        .header("Accept", "application/json")
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| format!("request Invezgo keystat code={code} gagal: {e}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| format!("baca body Invezgo keystat code={code} gagal: {e}"))?;

    if !status.is_success() {
        return Err(format!("Invezgo keystat HTTP {status} code={code}: {body}"));
    }

    parse_keystats(&body)
}

pub async fn fetch_and_save_keystats(
    session: Arc<Session>,
    code: &str,
) -> Result<(Keystats, chrono::DateTime<chrono::Utc>), String> {
    let keystats = fetch_keystats(code).await?;
    let updated_at = chrono::Utc::now();
    let keystats_db = crate::model::StockListKeystatsDb::from(keystats.clone());

    crate::repository::update_keystats(session.as_ref(), code, keystats_db, updated_at).await?;

    Ok((keystats, updated_at))
}

fn parse_keystats(body: &str) -> Result<Keystats, String> {
    let parsed: InvezgoKeystatsResponse = serde_json::from_str(body)
        .map_err(|e| format!("parse JSON Invezgo keystat gagal: {e}"))?;

    Ok(Keystats {
        rows: parsed
            .rows
            .into_iter()
            .map(|row| crate::model::KeystatsRow {
                id: row.id,
                name: row.name,
                values: row
                    .values
                    .into_iter()
                    .map(|v| crate::model::KeystatsValue {
                        col: v.col,
                        year: v.year,
                        amount: v.amount,
                        period: v.period,
                    })
                    .collect(),
            })
            .collect(),
        columns: parsed
            .columns
            .into_iter()
            .map(|c| crate::model::KeystatsColumn {
                year: c.year,
                label: c.label,
                period: c.period,
            })
            .collect(),
    })
}

pub async fn fetch_and_save(session: Arc<Session>) -> Result<usize, String> {
    let token = std::env::var("INVEZGO_BEARER_TOKEN")
        .map_err(|_| "INVEZGO_BEARER_TOKEN belum diset".to_string())?;

    let response = reqwest::Client::new()
        .get(INVEZGO_STOCK_LIST_URL)
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| format!("request Invezgo gagal: {e}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| format!("baca body Invezgo gagal: {e}"))?;

    if !status.is_success() {
        return Err(format!("Invezgo HTTP {status}: {body}"));
    }

    let items = parse_stock_list(&body)?;
    let mut saved = 0usize;

    for item in items {
        crate::repository::upsert(
            session.as_ref(),
            &item.code,
            item.name.as_deref(),
            item.sector.as_deref(),
            item.logo.as_deref(),
        )
        .await?;
        saved += 1;
    }

    Ok(saved)
}

fn parse_stock_list(body: &str) -> Result<Vec<InvezgoStockItem>, String> {
    if let Ok(wrapped) = serde_json::from_str::<InvezgoStockListWrapped>(body) {
        return Ok(wrapped.data);
    }

    serde_json::from_str::<Vec<InvezgoStockItem>>(body)
        .map_err(|e| format!("parse JSON Invezgo gagal: {e}"))
}
