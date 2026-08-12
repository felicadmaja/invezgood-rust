//! Fetch Yahoo Finance daily chart; deteksi spike close vs open hari ini (UP ≥ 16%, DOWN ≥ 8%).

use chrono::{Duration as ChronoDuration, Local};
use serde_json::Value;
use std::time::Duration;
use tokio::time::sleep;

const YAHOO_CHART_URL: &str = "https://query2.finance.yahoo.com/v8/finance/chart";
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
const SPIKE_UP_PCT: f64 = 0.16;
const SPIKE_DOWN_PCT: f64 = 0.08;
const INTER_EMITEN_DELAY: Duration = Duration::from_millis(50);
const RATE_LIMIT_RETRY_DELAY: Duration = Duration::from_millis(300);
const RATE_LIMIT_MAX_RETRIES: u32 = 20;

#[derive(Debug, Clone)]
struct Candle {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
}

/// Hasil deteksi lonjakan: emiten + arah + % close vs open.
#[derive(Debug, Clone, PartialEq)]
pub struct SpikeEmiten {
    pub emiten_name: String,
    /// `up` | `down` dari close vs open (UP ≥ 16%, DOWN ≥ 8%).
    pub jenis_spike: String,
    /// Persentase (positif naik, negatif turun), mis. `8.52` / `-10.1`.
    pub value_spike_percentage: f64,
}

/// `up`/`down` + persen bila change vs open memenuhi ambang (UP ≥ 16%, DOWN ≥ 8%).
fn spike_from_candle(c: &Candle) -> Option<(&'static str, f64)> {
    if c.open <= 0.0 {
        return None;
    }
    let change = (c.close - c.open) / c.open;
    let pct = change * 100.0;
    if change >= SPIKE_UP_PCT {
        Some(("up", pct))
    } else if change <= -SPIKE_DOWN_PCT {
        Some(("down", pct))
    } else {
        None
    }
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
    let opens = quote
        .get("open")
        .and_then(|x| x.as_array())
        .ok_or_else(|| "yahoo: open missing".to_string())?;
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

    let n = opens
        .len()
        .min(highs.len())
        .min(lows.len())
        .min(closes.len());
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let (Some(o), Some(h), Some(l), Some(c)) = (
            opens[i].as_f64(),
            highs[i].as_f64(),
            lows[i].as_f64(),
            closes[i].as_f64(),
        ) else {
            continue;
        };
        out.push(Candle {
            open: o,
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

fn is_too_many_requests(status: reqwest::StatusCode) -> bool {
    // Yahoo kadang 409 Conflict / 429 Too Many Requests.
    status.as_u16() == 409 || status.as_u16() == 429
}

async fn fetch_candles(
    http: &reqwest::Client,
    emiten: &str,
) -> Result<Vec<Candle>, String> {
    let (period1, period2) = unix_range_one_month();
    let url = format!(
        "{YAHOO_CHART_URL}/{emiten}.JK?period1={period1}&period2={period2}&interval=1d"
    );
    let mut attempt = 0u32;
    loop {
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
        if is_too_many_requests(status) {
            attempt += 1;
            eprintln!(
                "\x1b[31myahoo HTTP {status} Too Many Request {emiten} — jeda 300ms lalu retry ({attempt})\x1b[0m"
            );
            if attempt > RATE_LIMIT_MAX_RETRIES {
                return Err(format!(
                    "yahoo HTTP {status} Too Many Request {emiten}: gagal setelah {RATE_LIMIT_MAX_RETRIES} retry"
                ));
            }
            sleep(RATE_LIMIT_RETRY_DELAY).await;
            continue;
        }
        if !status.is_success() {
            let preview: String = body.chars().take(160).collect();
            return Err(format!("yahoo HTTP {status} {emiten}: {preview}"));
        }
        return parse_candles(&body);
    }
}

/// Untuk setiap emiten: GET Yahoo chart (jeda 50ms), kembalikan yang close naik ≥ 16% atau turun ≥ 8% vs open.
pub async fn find_spike_emitens(emitens: &[String]) -> Vec<SpikeEmiten> {
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
            eprintln!("yahoo spike: gagal buat HTTP client: {e}");
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
                let Some(last) = candles.last() else {
                    continue;
                };
                if let Some((jenis, pct)) = spike_from_candle(last) {
                    println!(
                        "\x1b[32myahoo spike {code} {jenis}: {pct:+.2}% (o={:.2} h={:.2} l={:.2} c={:.2})\x1b[0m",
                        last.open, last.high, last.low, last.close
                    );
                    spikes.push(SpikeEmiten {
                        emiten_name: code,
                        jenis_spike: jenis.to_string(),
                        value_spike_percentage: pct,
                    });
                }
            }
            Err(e) => eprintln!("yahoo spike {code}: {e}"),
        }
    }
    spikes
}
