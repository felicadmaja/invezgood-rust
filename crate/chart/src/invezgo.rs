use chrono::NaiveDate;
use serde::Deserialize;

use crate::pb::{ChartBar, GetCurrentDayChartFromInvezgoResponse};

const INVEZGO_CHART_STOCK_URL: &str = "https://api.invezgo.com/analysis/chart/stock";
const INVEZGO_INTRADAY_DATA_URL: &str = "https://api.invezgo.com/analysis/intraday-data";

#[derive(Debug, Deserialize)]
struct ApiChartBar {
    date: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    #[serde(deserialize_with = "deserialize_volume")]
    volume: String,
}

fn deserialize_volume<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    Ok(match value {
        serde_json::Value::String(s) => s,
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    })
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

impl GetCurrentDayChartFromInvezgoResponse {
    /// Normalisasi harga nol dari API sebelum return ke client / cache:
    /// `open` = 0 → `close`; `low` = 0 dan `high` = 0 → keduanya `close`.
    pub fn normalize_intraday_prices(&mut self) {
        if self.open == 0.0 {
            self.open = self.close;
        }
        if self.low == 0.0 && self.high == 0.0 {
            self.low = self.close;
            self.high = self.close;
        }
    }
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
    let url = format!("{INVEZGO_CHART_STOCK_URL}/{code}?from={from_date}&to={to_date}");

    eprintln!("\x1b[32mchart Invezgo GET {url}\x1b[0m");

    let body = invezgo_http::get(&url).await?;

    let parsed: Vec<ApiChartBar> = serde_json::from_str(&body)
        .map_err(|e| format!("parse JSON Invezgo chart/stock gagal: {e}"))?;

    parsed.into_iter().map(api_bar_to_proto).collect()
}

#[derive(Debug, Deserialize)]
struct ApiIntradayData {
    code: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    avg: f64,
    volume: i64,
    freq: i64,
    value: i64,
    prev: f64,
    bid_price: f64,
    bid_lot: i64,
    bid_freq: i64,
    offer_price: f64,
    offer_lot: i64,
    offer_freq: i64,
    iep: f64,
    iev: i64,
}

pub async fn fetch_intraday_data(code: &str) -> Result<GetCurrentDayChartFromInvezgoResponse, String> {
    let url = format!("{INVEZGO_INTRADAY_DATA_URL}/{code}?market=RG");

    let body = invezgo_http::get(&url).await?;

    let parsed: ApiIntradayData = serde_json::from_str(&body)
        .map_err(|e| format!("parse JSON Invezgo intraday-data gagal: {e}"))?;

    let mut resp = GetCurrentDayChartFromInvezgoResponse {
        code: parsed.code,
        open: parsed.open,
        high: parsed.high,
        low: parsed.low,
        close: parsed.close,
        avg: parsed.avg,
        volume: parsed.volume,
        freq: parsed.freq,
        value: parsed.value,
        prev: parsed.prev,
        bid_price: parsed.bid_price,
        bid_lot: parsed.bid_lot,
        bid_freq: parsed.bid_freq,
        offer_price: parsed.offer_price,
        offer_lot: parsed.offer_lot,
        offer_freq: parsed.offer_freq,
        iep: parsed.iep,
        iev: parsed.iev,
        success: true,
        message: "ok".to_string(),
    };
    resp.normalize_intraday_prices();
    Ok(resp)
}
