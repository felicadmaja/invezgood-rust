use chrono::NaiveDate;
use serde::Deserialize;

use crate::pb::ChartBar;

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

/// Parse ISO-8601 API date → `YYYY-MM-DD`.
fn normalize_api_date(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    let date = if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(trimmed) {
        dt.date_naive()
    } else if let Some(date_part) = trimmed.split('T').next() {
        NaiveDate::parse_from_str(date_part, "%Y-%m-%d")
            .map_err(|_| format!("date API tidak valid: {raw}"))?
    } else {
        NaiveDate::parse_from_str(trimmed, "%Y-%m-%d")
            .map_err(|_| format!("date API tidak valid: {raw}"))?
    };
    Ok(date.format("%Y-%m-%d").to_string())
}

fn api_bar_to_proto(bar: ApiChartBar) -> Result<ChartBar, String> {
    Ok(ChartBar {
        date: normalize_api_date(&bar.date)?,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
        volume: bar.volume,
    })
}

pub async fn fetch_from_api(
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

    parsed.into_iter().map(api_bar_to_proto).collect()
}
