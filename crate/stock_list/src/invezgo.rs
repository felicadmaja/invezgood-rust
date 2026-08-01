use std::sync::Arc;

use scylla::client::session::Session;
use serde::Deserialize;

const INVEZGO_STOCK_LIST_URL: &str = "https://api.invezgo.com/analysis/list/stock";

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
            None,
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
