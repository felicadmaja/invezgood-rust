use chrono::NaiveDate;
use scylla::client::session::Session;
use serde::Deserialize;

use crate::model::ChartRow;
use crate::pb::ChartBar;
use crate::repository;

const INVEZGO_CHART_STOCK_URL: &str = "https://api.invezgo.com/analysis/chart/stock";

#[derive(Debug, Deserialize)]
struct ApiChartBar {
    date: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: String,
}

/// Parse ISO-8601 API date → `NaiveDate` (tanpa jam/menit/detik).
fn parse_api_date(raw: &str) -> Result<NaiveDate, String> {
    let trimmed = raw.trim();
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(trimmed) {
        return Ok(dt.date_naive());
    }
    if let Some(date_part) = trimmed.split('T').next() {
        if let Ok(date) = NaiveDate::parse_from_str(date_part, "%Y-%m-%d") {
            return Ok(date);
        }
    }
    NaiveDate::parse_from_str(trimmed, "%Y-%m-%d")
        .map_err(|_| format!("date API tidak valid: {raw}"))
}

fn f64_to_i32(field: &str, value: f64) -> Result<i32, String> {
    if !value.is_finite() {
        return Err(format!("{field} tidak valid: {value}"));
    }
    Ok(value.round() as i32)
}

fn parse_volume(raw: &str) -> Result<i32, String> {
    let trimmed = raw.trim();
    trimmed
        .parse::<i32>()
        .or_else(|_| trimmed.parse::<f64>().map(|v| v.round() as i32))
        .map_err(|_| format!("volume tidak valid: {raw}"))
}

fn api_bar_to_row(code: &str, bar: ApiChartBar) -> Result<ChartRow, String> {
    Ok(ChartRow {
        code: code.to_string(),
        date: parse_api_date(&bar.date)?,
        open: f64_to_i32("open", bar.open)?,
        high: f64_to_i32("high", bar.high)?,
        low: f64_to_i32("low", bar.low)?,
        close: f64_to_i32("close", bar.close)?,
        volume: parse_volume(&bar.volume)?,
    })
}

fn row_to_proto(row: &ChartRow) -> ChartBar {
    ChartBar {
        date: row.date.format("%Y-%m-%d").to_string(),
        open: f64::from(row.open),
        high: f64::from(row.high),
        low: f64::from(row.low),
        close: f64::from(row.close),
        volume: row.volume.to_string(),
    }
}

pub async fn fetch_and_save_chart(
    session: &Session,
    code: &str,
    from_date: &str,
    to_date: &str,
) -> Result<Vec<ChartBar>, String> {
    let token = std::env::var("INVEZGO_BEARER_TOKEN")
        .map_err(|_| "INVEZGO_BEARER_TOKEN belum diset".to_string())?;

    let url = format!("{INVEZGO_CHART_STOCK_URL}/{code}?from={from_date}&to={to_date}");

    eprintln!("chart Invezgo GET {url}");

    let response = reqwest::Client::new()
        .get(&url)
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| format!("request Invezgo chart/stock gagal: {e}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| format!("baca body Invezgo chart/stock gagal: {e}"))?;

    if !status.is_success() {
        return Err(format!("Invezgo HTTP {status} chart/stock: {body}"));
    }

    let parsed: Vec<ApiChartBar> = serde_json::from_str(&body)
        .map_err(|e| format!("parse JSON Invezgo chart/stock gagal: {e}"))?;

    let mut items = Vec::with_capacity(parsed.len());
    for bar in parsed {
        let row = api_bar_to_row(code, bar)?;
        repository::upsert(session, &row).await?;
        items.push(row_to_proto(&row));
    }

    eprintln!("chart: {code} — {} baris di-upsert ke invezgood.chart", items.len());
    Ok(items)
}
