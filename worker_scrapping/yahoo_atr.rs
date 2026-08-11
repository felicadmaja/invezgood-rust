//! Fetch Yahoo Finance daily chart + hitung ATR; deteksi lonjakan (high-low) hari ini.

use chrono::{Duration as ChronoDuration, Local};
use serde_json::Value;
use std::time::Duration;
use tokio::time::sleep;

const YAHOO_CHART_URL: &str = "https://query2.finance.yahoo.com/v8/finance/chart";
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
const ATR_PERIOD: usize = 14;
const SPREAD_ATR_MULTIPLIER: f64 = 1.5;
const INTER_EMITEN_DELAY: Duration = Duration::from_millis(300);

#[derive(Debug, Clone)]
struct Candle {
    high: f64,
    low: f64,
    close: f64,
}

/// True bila spread (high-low) candle terakhir >= 1.5 × ATR(14).
fn is_price_spike(candles: &[Candle]) -> bool {
    if candles.len() < ATR_PERIOD + 1 {
        return false;
    }
    let Some(atr) = wilder_atr(candles, ATR_PERIOD) else {
        return false;
    };
    if atr <= 0.0 {
        return false;
    }
    let last = &candles[candles.len() - 1];
    let spread = last.high - last.low;
    spread >= SPREAD_ATR_MULTIPLIER * atr
}

fn wilder_atr(candles: &[Candle], period: usize) -> Option<f64> {
    if candles.len() < period + 1 {
        return None;
    }
    let mut trs = Vec::with_capacity(candles.len() - 1);
    for i in 1..candles.len() {
        let h = candles[i].high;
        let l = candles[i].low;
        let prev_c = candles[i - 1].close;
        let tr = (h - l)
            .max((h - prev_c).abs())
            .max((l - prev_c).abs());
        trs.push(tr);
    }
    if trs.len() < period {
        return None;
    }
    let mut atr: f64 = trs[..period].iter().sum::<f64>() / period as f64;
    for &tr in &trs[period..] {
        atr = (atr * (period as f64 - 1.0) + tr) / period as f64;
    }
    Some(atr)
}

fn parse_candles(body: &str) -> Result<Vec<Candle>, String> {
    let v: Value =
        serde_json::from_str(body).map_err(|e| format!("yahoo JSON: {e}"))?;
    let result = v
        .pointer("/chart/result/0")
        .ok_or_else(|| "yahoo: chart.result kosong".to_string())?;
    let quote = result
        .pointer("/indicators/quote/0")
        .ok_or_else(|| "yahoo: indicators.quote kosong".to_string())?;
    let highs = quote
        .get("high")
        .and_then(|x| x.as_array())
        .ok_or_else(|| "yahoo: high missing".to_string())?;
    let lows = quote
        .get("low")
        .and_then(|x| x.as_array())
        .ok_or_else(|| "yahoo: low missing".to_string())?;
    let closes = quote
        .get("close")
        .and_then(|x| x.as_array())
        .ok_or_else(|| "yahoo: close missing".to_string())?;

    let n = highs.len().min(lows.len()).min(closes.len());
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let (Some(h), Some(l), Some(c)) = (
            highs[i].as_f64(),
            lows[i].as_f64(),
            closes[i].as_f64(),
        ) else {
            continue;
        };
        out.push(Candle {
            high: h,
            low: l,
            close: c,
        });
    }
    Ok(out)
}

fn unix_range_one_month() -> (i64, i64) {
    let now = Local::now();
    let period2 = now.timestamp();
    let period1 = (now - ChronoDuration::days(31)).timestamp();
    (period1, period2)
}

async fn fetch_candles(
    http: &reqwest::Client,
    emiten: &str,
) -> Result<Vec<Candle>, String> {
    let (period1, period2) = unix_range_one_month();
    let url = format!(
        "{YAHOO_CHART_URL}/{emiten}.JK?period1={period1}&period2={period2}&interval=1d"
    );
    let resp = http
        .get(&url)
        .header("User-Agent", USER_AGENT)
        .header("Accept", "application/json")
        .send()
        .await
        .map_err(|e| format!("yahoo request {emiten}: {e}"))?;
    let status = resp.status();
    let body = resp
        .text()
        .await
        .map_err(|e| format!("yahoo body {emiten}: {e}"))?;
    if !status.is_success() {
        let preview: String = body.chars().take(160).collect();
        return Err(format!("yahoo HTTP {status} {emiten}: {preview}"));
    }
    parse_candles(&body)
}

/// Untuk setiap emiten: GET Yahoo chart (jeda 300ms), hitung ATR, kembalikan yang lonjakan.
pub async fn find_spike_emitens(emitens: &[String]) -> Vec<String> {
    if emitens.is_empty() {
        return Vec::new();
    }
    let http = match reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(30))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("yahoo ATR: gagal buat HTTP client: {e}");
            return Vec::new();
        }
    };

    let mut spikes = Vec::new();
    for (i, raw) in emitens.iter().enumerate() {
        if i > 0 {
            sleep(INTER_EMITEN_DELAY).await;
        }
        let code = raw.trim().to_ascii_uppercase();
        if code.len() != 4 || !code.chars().all(|c| c.is_ascii_alphabetic()) {
            continue;
        }
        match fetch_candles(&http, &code).await {
            Ok(candles) => {
                if is_price_spike(&candles) {
                    let last = candles.last().unwrap();
                    let spread = last.high - last.low;
                    println!(
                        "\x1b[32myahoo ATR spike {code}: spread={spread:.2} (high={:.2} low={:.2})\x1b[0m",
                        last.high, last.low
                    );
                    spikes.push(code);
                }
            }
            Err(e) => eprintln!("yahoo ATR {code}: {e}"),
        }
    }
    spikes
}
