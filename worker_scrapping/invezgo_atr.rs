//! Deteksi spike via Invezgo API.
//! Di luar 09:00–09:05: GET `analysis/intraday-data/{code}?market=RG` — `(close - open) / open * 100`.
//! 09:00–09:05: GET `analysis/chart/stock/{code}?from=&to=` (lookback 7 hari) —
//!   `(close_hari_ini - open_kemarin) / open_kemarin * 100`; ambang opening `.env`.

use chrono::{Duration as ChronoDuration, Local, NaiveDate};
use serde::Deserialize;
use std::time::Duration;

use crate::yahoo_atr::{active_spike_thresholds, in_opening_spike_window, SpikeEmiten};

const INVEZGO_CHART_STOCK_URL: &str = "https://api.invezgo.com/analysis/chart/stock";
const INVEZGO_INTRADAY_DATA_URL: &str = "https://api.invezgo.com/analysis/intraday-data";

#[derive(Debug, Deserialize)]
struct ApiChartBar {
    date: String,
    open: f64,
    close: f64,
}

#[derive(Debug, Deserialize)]
struct ApiIntradayData {
    open: f64,
    close: f64,
}

fn spike_at_now() -> String {
    Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

fn spike_from_change(
    base: f64,
    value: f64,
    up_pct: f64,
    down_pct: f64,
) -> Option<(&'static str, f64)> {
    if base <= 0.0 {
        return None;
    }
    let change = (value - base) / base;
    let pct = change * 100.0;
    if change >= up_pct / 100.0 {
        Some(("up", pct))
    } else if change <= -(down_pct / 100.0) {
        Some(("down", pct))
    } else {
        None
    }
}

fn bar_date(raw: &str) -> Option<NaiveDate> {
    let trimmed = raw.trim();
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(trimmed) {
        return Some(dt.date_naive());
    }
    trimmed
        .split('T')
        .next()
        .and_then(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())
}

fn today_and_prev_bars(bars: &[ApiChartBar]) -> Option<(&ApiChartBar, &ApiChartBar)> {
    let today = Local::now().date_naive();
    if let Some(today_idx) = bars.iter().rposition(|b| bar_date(&b.date) == Some(today)) {
        if today_idx == 0 {
            return None;
        }
        return Some((&bars[today_idx], &bars[today_idx - 1]));
    }
    if bars.len() < 2 {
        return None;
    }
    Some((bars.last()?, &bars[bars.len() - 2]))
}

async fn invezgo_get(url: &str) -> Result<String, String> {
    let token = std::env::var("INVEZGO_BEARER_TOKEN")
        .map_err(|_| "INVEZGO_BEARER_TOKEN belum diset".to_string())?;
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| format!("Invezgo HTTP client: {e}"))?
        .get(url)
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| format!("Invezgo GET {url}: {e}"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| format!("Invezgo body {url}: {e}"))?;
    if !status.is_success() {
        return Err(format!("Invezgo HTTP {status} {url}: {body}"));
    }
    Ok(body)
}

async fn fetch_intraday_open_close(code: &str) -> Result<(f64, f64), String> {
    let url = format!("{INVEZGO_INTRADAY_DATA_URL}/{code}?market=RG");
    let body = invezgo_get(&url).await?;
    let parsed: ApiIntradayData = serde_json::from_str(&body)
        .map_err(|e| format!("parse Invezgo intraday-data {code}: {e}"))?;
    let mut open = parsed.open;
    let close = parsed.close;
    if open == 0.0 {
        open = close;
    }
    Ok((open, close))
}

async fn fetch_chart_bars(code: &str, lookback_days: i64) -> Result<Vec<ApiChartBar>, String> {
    let today = Local::now().date_naive();
    let from = today - ChronoDuration::days(lookback_days.max(0));
    let url = format!(
        "{INVEZGO_CHART_STOCK_URL}/{code}?from={}&to={}",
        from.format("%Y-%m-%d"),
        today.format("%Y-%m-%d")
    );
    let body = invezgo_get(&url).await?;
    serde_json::from_str(&body).map_err(|e| format!("parse Invezgo chart/stock {code}: {e}"))
}

/// Scan emiten plan-to-trade; GET Invezgo per emiten tanpa jeda antar emiten.
pub async fn find_spike_emitens(emitens: &[String]) -> Vec<SpikeEmiten> {
    if emitens.is_empty() {
        return Vec::new();
    }

    let opening = in_opening_spike_window();
    let lookback_days = if opening { 7 } else { 0 };
    if opening {
        let (up, down, _) = active_spike_thresholds();
        println!(
            "Invezgo spike: mode 09:00-09:05 — close hari ini vs open kemarin (UP>={up}% DOWN>={down}%)"
        );
    }

    let mut spikes = Vec::new();

    for raw in emitens.iter() {
        let code = raw.trim().to_ascii_uppercase();
        if code.len() != 4 || !code.chars().all(|c| c.is_ascii_alphabetic()) {
            continue;
        }

        let detected = if opening {
            match fetch_chart_bars(&code, lookback_days).await {
                Ok(bars) => {
                    let Some((today, prev)) = today_and_prev_bars(&bars) else {
                        continue;
                    };
                    let (up_pct, down_pct, _) = active_spike_thresholds();
                    spike_from_change(prev.open, today.close, up_pct, down_pct).map(|hit| {
                        (
                            hit,
                            format!(
                                "close hari ini={:.2} vs open kemarin={:.2}",
                                today.close, prev.open
                            ),
                        )
                    })
                }
                Err(e) => {
                    eprintln!("Invezgo spike {code}: {e}");
                    None
                }
            }
        } else {
            match fetch_intraday_open_close(&code).await {
                Ok((open, close)) => {
                    let (up_pct, down_pct, _) = active_spike_thresholds();
                    spike_from_change(open, close, up_pct, down_pct).map(|hit| {
                        (hit, format!("open={open:.2} close={close:.2}"))
                    })
                }
                Err(e) => {
                    eprintln!("Invezgo spike {code}: {e}");
                    None
                }
            }
        };

        let Some(((jenis, pct), detail)) = detected else {
            continue;
        };
        println!("\x1b[32mInvezgo spike {code} {jenis}: {pct:+.2}% ({detail})\x1b[0m");
        spikes.push(SpikeEmiten {
            spike_at: spike_at_now(),
            emiten_name: code,
            jenis_spike: jenis.to_string(),
            value_spike_percentage: pct,
        });
    }
    spikes
}
