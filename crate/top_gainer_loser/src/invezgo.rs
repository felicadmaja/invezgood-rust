use std::sync::Arc;

use chrono::Local;
use scylla::client::session::Session;
use serde::Deserialize;

use crate::model::{GraphPointDb, TopGainerLoserRow};

const INVEZGO_TOP_CHANGE_URL: &str = "https://api.invezgo.com/analysis/top/change";

#[derive(Debug, Deserialize)]
struct ApiGraphPoint {
    date: String,
    value: f64,
}

#[derive(Debug, Deserialize)]
struct ApiTopItem {
    code: String,
    name: String,
    price: f64,
    change: f64,
    value: String,
    volume: String,
    logo: String,
    calculated_value: f64,
    graph: Vec<ApiGraphPoint>,
}

#[derive(Debug, Deserialize)]
struct ApiTopChangeResponse {
    gain: Vec<ApiTopItem>,
    #[serde(default)]
    loss: Vec<ApiTopItem>,
}

fn body_preview(body: &str, max_chars: usize) -> String {
    let trimmed = body.trim();
    if trimmed.chars().count() <= max_chars {
        return trimmed.to_string();
    }
    let preview: String = trimmed.chars().take(max_chars).collect();
    format!("{preview}… ({} chars)", trimmed.len())
}

pub async fn fetch_and_save(
    session: Arc<Session>,
    trade_date: chrono::NaiveDate,
) -> Result<Vec<TopGainerLoserRow>, String> {
    let token = std::env::var("INVEZGO_BEARER_TOKEN")
        .map_err(|_| "INVEZGO_BEARER_TOKEN belum diset".to_string())?;

    let date_param = trade_date.format("%Y-%m-%d").to_string();
    let url = format!("{INVEZGO_TOP_CHANGE_URL}?date={date_param}");

    eprintln!("top_gainer_loser Invezgo GET {url}");

    let response = reqwest::Client::new()
        .get(&url)
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| format!("request Invezgo gagal: {e}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| format!("baca body Invezgo gagal: {e}"))?;

    eprintln!(
        "top_gainer_loser Invezgo HTTP {status} url={url} body={}",
        body_preview(&body, 2000)
    );

    if !status.is_success() {
        return Err(format!("Invezgo HTTP {status}: {body}"));
    }

    let parsed: ApiTopChangeResponse = serde_json::from_str(&body).map_err(|e| {
        format!(
            "parse JSON Invezgo gagal: {e}; body={}",
            body_preview(&body, 500)
        )
    })?;

    let gain_n = parsed.gain.len();
    let loss_n = parsed.loss.len();
    eprintln!("top_gainer_loser Invezgo parsed date={date_param} gain={gain_n} loss={loss_n}");

    let mut saved = Vec::new();

    for item in parsed.gain {
        let row = api_item_to_row(trade_date, "gain", item);
        crate::repository::upsert(session.as_ref(), &row).await?;
        saved.push(row);
    }

    for item in parsed.loss {
        let row = api_item_to_row(trade_date, "loser", item);
        crate::repository::upsert(session.as_ref(), &row).await?;
        saved.push(row);
    }

    if saved.is_empty() {
        eprintln!(
            "top_gainer_loser Invezgo kosong untuk {date_param}: tidak ada baris di-upsert"
        );
    }

    Ok(saved)
}

pub fn resolve_trade_date(tahun_bulan_tanggal: Option<String>) -> Result<chrono::NaiveDate, String> {
    match tahun_bulan_tanggal {
        Some(date) if !date.is_empty() => chrono::NaiveDate::parse_from_str(&date, "%Y-%m-%d")
            .map_err(|_| format!("tahun_bulan_tanggal invalid: {date} (harus YYYY-MM-DD)")),
        _ => Ok(Local::now().date_naive()),
    }
}

fn api_item_to_row(trade_date: chrono::NaiveDate, tipe: &str, item: ApiTopItem) -> TopGainerLoserRow {
    TopGainerLoserRow {
        tahun_bulan_tanggal: trade_date,
        code: item.code,
        name: Some(item.name),
        price: Some(item.price),
        change_pct: Some(item.change),
        value: Some(item.value),
        volume: Some(item.volume),
        logo: Some(item.logo),
        calculated_value: Some(item.calculated_value),
        tipe: Some(tipe.to_string()),
        graph: Some(
            item.graph
                .into_iter()
                .map(|p| GraphPointDb {
                    date: p.date,
                    value: p.value,
                })
                .collect(),
        ),
    }
}
