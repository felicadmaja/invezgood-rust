use std::sync::Arc;

use chrono::Local;
use chrono::Datelike;
use scylla::client::session::Session;
use serde::Deserialize;

use crate::model::TopForeignFlowRow;

const INVEZGO_TOP_FOREIGN_URL: &str = "https://api.invezgo.com/analysis/top/foreign";

#[derive(Debug, Deserialize)]
struct ApiTopForeignItem {
    code: String,
    name: String,
    price: i32,
    change: f64,
    value: String,
    volume: String,
}

#[derive(Debug, Deserialize)]
struct ApiTopForeignResponse {
    #[serde(default)]
    accum: Vec<ApiTopForeignItem>,
    #[serde(default)]
    dist: Vec<ApiTopForeignItem>,
}

fn body_preview(body: &str, max_chars: usize) -> String {
    let trimmed = body.trim();
    if trimmed.chars().count() <= max_chars {
        return trimmed.to_string();
    }
    let preview: String = trimmed.chars().take(max_chars).collect();
    format!("{preview}… ({} chars)", trimmed.len())
}

fn parse_i64_field(label: &str, code: &str, raw: &str) -> Result<i64, String> {
    raw.parse::<i64>().map_err(|_| {
        format!("{label} invalid untuk code={code}: {raw} (harus angka bulat)")
    })
}

pub fn resolve_trade_date(tahun_bulan_tanggal: Option<String>) -> Result<chrono::NaiveDate, String> {
    match tahun_bulan_tanggal {
        Some(date) if !date.is_empty() => chrono::NaiveDate::parse_from_str(&date, "%Y-%m-%d")
            .map_err(|_| format!("tahun_bulan_tanggal invalid: {date} (harus YYYY-MM-DD)")),
        _ => Ok(Local::now().date_naive()),
    }
}

pub fn ensure_not_weekend(trade_date: chrono::NaiveDate) -> Result<(), String> {
    if matches!(trade_date.weekday(), chrono::Weekday::Sat | chrono::Weekday::Sun) {
        return Err("Sabtu/Minggi Libur".into());
    }
    Ok(())
}

fn api_item_to_row(
    trade_date: chrono::NaiveDate,
    accum_or_dist: &str,
    item: ApiTopForeignItem,
) -> Result<TopForeignFlowRow, String> {
    let value = parse_i64_field("value", &item.code, &item.value)?;
    let volume = parse_i64_field("volume", &item.code, &item.volume)?;
    Ok(TopForeignFlowRow {
        tahun_bulan_tanggal: trade_date,
        value,
        code: item.code,
        name: Some(item.name),
        price: Some(item.price),
        change: Some(item.change),
        volume: Some(volume),
        accum_or_dist: Some(accum_or_dist.to_string()),
    })
}

pub async fn fetch_and_save(
    session: Arc<Session>,
    trade_date: chrono::NaiveDate,
) -> Result<usize, String> {
    let token = std::env::var("INVEZGO_BEARER_TOKEN")
        .map_err(|_| "INVEZGO_BEARER_TOKEN belum diset".to_string())?;

    let date_param = trade_date.format("%Y-%m-%d").to_string();
    let url = format!("{INVEZGO_TOP_FOREIGN_URL}?date={date_param}&filter_column=value");

    eprintln!("top_foreign_flow Invezgo GET {url}");

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
        "top_foreign_flow Invezgo HTTP {status} url={url} body={}",
        body_preview(&body, 2000)
    );

    if !status.is_success() {
        return Err(format!("Invezgo HTTP {status}: {body}"));
    }

    let parsed: ApiTopForeignResponse = serde_json::from_str(&body).map_err(|e| {
        format!(
            "parse JSON Invezgo gagal: {e}; body={}",
            body_preview(&body, 500)
        )
    })?;

    let accum_n = parsed.accum.len();
    let dist_n = parsed.dist.len();
    eprintln!("top_foreign_flow Invezgo parsed date={date_param} accum={accum_n} dist={dist_n}");

    crate::repository::delete_by_date(session.as_ref(), trade_date).await?;

    let mut saved = 0usize;

    for item in parsed.accum {
        let row = api_item_to_row(trade_date, "accum", item)?;
        crate::repository::upsert(session.as_ref(), &row).await?;
        saved += 1;
    }

    for item in parsed.dist {
        let row = api_item_to_row(trade_date, "dist", item)?;
        crate::repository::upsert(session.as_ref(), &row).await?;
        saved += 1;
    }

    if saved == 0 {
        eprintln!("top_foreign_flow Invezgo kosong untuk {date_param}: tidak ada baris di-upsert");
    }

    Ok(saved)
}
