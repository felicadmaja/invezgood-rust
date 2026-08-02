use std::sync::Arc;

use chrono::Utc;
use scylla::client::session::Session;
use serde::Deserialize;

use crate::model::BrokerRow;

const INVEZGO_LIST_BROKER_URL: &str = "https://api.invezgo.com/analysis/list/broker";

#[derive(Debug, Deserialize)]
struct ApiBrokerItem {
    code: String,
    name: String,
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

    let now = Utc::now();
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
            updated_at: Some(now),
        };

        crate::repository::upsert(session.as_ref(), &row).await?;
        saved.push(row);
    }

    saved.sort_by(|a, b| a.broker_code.cmp(&b.broker_code));
    Ok(saved)
}
