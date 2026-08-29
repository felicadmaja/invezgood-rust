//! Fetch fundamental Yahoo Finance (.JK) — crumb + quoteSummary + fundamentals-timeseries.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::Client;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio::time::sleep;

const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
const FC_YAHOO_URL: &str = "https://fc.yahoo.com";
const CRUMB_URL: &str = "https://query2.finance.yahoo.com/v1/test/getcrumb";
const QUOTE_SUMMARY_URL: &str = "https://query2.finance.yahoo.com/v10/finance/quoteSummary";
const TIMESERIES_URL: &str =
    "https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries";
const TIMESERIES_PERIOD1: i64 = 1_609_459_200;

const TS_TYPES: &str = "quarterlyOperatingIncome,quarterlyEBIT,quarterlyReconciledDepreciation,\
quarterlyTotalDebt,quarterlyCashAndCashEquivalents,quarterlyMinorityInterest";

#[derive(Debug, Clone)]
pub struct EmitenMetrics {
    pub ev_ebit: Option<f64>,
    pub ev_ebitda: Option<f64>,
}

struct YahooSession {
    client: Client,
    crumb: Option<String>,
}

pub struct YahooClient {
    inner: Mutex<YahooSession>,
}

impl YahooClient {
    pub fn new() -> Result<Arc<Self>, String> {
        let client = Client::builder()
            .cookie_store(true)
            .user_agent(USER_AGENT)
            .build()
            .map_err(|e| format!("yahoo http client: {e}"))?;
        Ok(Arc::new(Self {
            inner: Mutex::new(YahooSession {
                client,
                crumb: None,
            }),
        }))
    }

    pub async fn fetch_emiten(&self, code: &str) -> Result<EmitenMetrics, String> {
        let symbol = format!("{code}.JK");
        let mcap = self.market_cap(&symbol).await?;
        let series = self.fetch_timeseries(&symbol).await?;

        let ebit = ttm_sum(series.get("quarterlyOperatingIncome"))
            .or_else(|| ttm_sum(series.get("quarterlyEBIT")))
            .ok_or_else(|| "no EBIT TTM".to_string())?;

        let da = ttm_sum(series.get("quarterlyReconciledDepreciation")).unwrap_or(0.0);
        let ebitda = ebit + da;

        let debt = latest(series.get("quarterlyTotalDebt"));
        let cash = latest(series.get("quarterlyCashAndCashEquivalents"));
        let nci = latest(series.get("quarterlyMinorityInterest"));

        let ev = mcap + debt + nci - cash;
        Ok(EmitenMetrics {
            ev_ebit: (ebit > 0.0).then_some(ev / ebit),
            ev_ebitda: (ebitda > 0.0).then_some(ev / ebitda),
        })
    }

    async fn market_cap(&self, symbol: &str) -> Result<f64, String> {
        let body = self
            .quote_summary(symbol, "summaryDetail,defaultKeyStatistics")
            .await?;
        let result = body
            .pointer("/quoteSummary/result/0")
            .ok_or_else(|| format!("quoteSummary kosong {symbol}"))?;

        if let Some(raw) = result
            .pointer("/summaryDetail/marketCap/raw")
            .and_then(|v| v.as_f64())
        {
            return Ok(raw);
        }

        let price = result
            .pointer("/defaultKeyStatistics/currentPrice/raw")
            .or_else(|| result.pointer("/summaryDetail/regularMarketPrice/raw"))
            .and_then(|v| v.as_f64());
        let shares = result
            .pointer("/defaultKeyStatistics/sharesOutstanding/raw")
            .and_then(|v| v.as_f64());

        match (price, shares) {
            (Some(p), Some(s)) if p > 0.0 && s > 0.0 => Ok(p * s),
            _ => Err(format!("no marketCap {symbol}")),
        }
    }

    async fn fetch_timeseries(
        &self,
        symbol: &str,
    ) -> Result<HashMap<String, Vec<(String, f64)>>, String> {
        let period2 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| format!("system time: {e}"))?
            .as_secs() as i64
            + 86_400;

        let url = format!(
            "{TIMESERIES_URL}/{symbol}?period1={TIMESERIES_PERIOD1}&period2={period2}\
             &type={TS_TYPES}&merge=false&padTimeSeries=true"
        );
        let body = self.get_json_with_crumb(&url).await?;
        parse_timeseries(&body)
    }

    async fn quote_summary(&self, symbol: &str, modules: &str) -> Result<Value, String> {
        let url = format!("{QUOTE_SUMMARY_URL}/{symbol}?modules={modules}");
        self.get_json_with_crumb(&url).await
    }

    async fn get_json_with_crumb(&self, url: &str) -> Result<Value, String> {
        self.get_json_with_crumb_retry(url, true).await
    }

    async fn get_json_with_crumb_retry(
        &self,
        url: &str,
        may_refresh_crumb: bool,
    ) -> Result<Value, String> {
        let mut session = self.inner.lock().await;
        Self::ensure_crumb(&mut session).await?;

        let crumb = session.crumb.clone().unwrap_or_default();
        let full_url = if url.contains('?') {
            format!("{url}&crumb={crumb}")
        } else {
            format!("{url}?crumb={crumb}")
        };

        let resp = session
            .client
            .get(&full_url)
            .header("User-Agent", USER_AGENT)
            .send()
            .await
            .map_err(|e| format!("yahoo GET {url}: {e}"))?;

        if resp.status().as_u16() == 401 || resp.status().as_u16() == 403 {
            if may_refresh_crumb {
                session.crumb = None;
                drop(session);
                return Box::pin(self.get_json_with_crumb_retry(url, false)).await;
            }
            return Err(format!("yahoo HTTP {} {url}", resp.status()));
        }

        if !resp.status().is_success() {
            return Err(format!("yahoo HTTP {} {url}", resp.status()));
        }

        resp.json::<Value>()
            .await
            .map_err(|e| format!("yahoo JSON {url}: {e}"))
    }

    async fn ensure_crumb(session: &mut YahooSession) -> Result<(), String> {
        if session.crumb.is_some() {
            return Ok(());
        }
        session
            .client
            .get(FC_YAHOO_URL)
            .header("User-Agent", USER_AGENT)
            .send()
            .await
            .map_err(|e| format!("yahoo fc.yahoo.com: {e}"))?;

        let resp = session
            .client
            .get(CRUMB_URL)
            .header("User-Agent", USER_AGENT)
            .send()
            .await
            .map_err(|e| format!("yahoo getcrumb: {e}"))?;

        let crumb = resp
            .text()
            .await
            .map_err(|e| format!("yahoo crumb body: {e}"))?
            .trim()
            .to_string();

        if crumb.is_empty() {
            return Err("yahoo crumb kosong".into());
        }
        session.crumb = Some(crumb);
        Ok(())
    }
}

fn parse_timeseries(body: &Value) -> Result<HashMap<String, Vec<(String, f64)>>, String> {
    let mut out = HashMap::new();
    let Some(results) = body.pointer("/timeseries/result").and_then(|v| v.as_array()) else {
        return Ok(out);
    };

    for block in results {
        let Some(type_name) = block
            .pointer("/meta/type/0")
            .and_then(|v| v.as_str())
            .map(str::to_string)
        else {
            continue;
        };
        let mut points = Vec::new();
        if let Some(entries) = block.get(&type_name).and_then(|v| v.as_array()) {
            for entry in entries {
                let Some(date) = entry.get("asOfDate").and_then(|v| v.as_str()) else {
                    continue;
                };
                let Some(raw) = entry
                    .pointer("/reportedValue/raw")
                    .and_then(|v| v.as_f64())
                else {
                    continue;
                };
                points.push((date.to_string(), raw));
            }
        }
        points.sort_by(|a, b| a.0.cmp(&b.0));
        out.insert(type_name, points);
    }
    Ok(out)
}

fn ttm_sum(points: Option<&Vec<(String, f64)>>) -> Option<f64> {
    let points = points?;
    if points.len() < 4 {
        return if points.is_empty() { None } else { None };
    }
    let last4: f64 = points.iter().rev().take(4).map(|(_, v)| v).sum();
    Some(last4)
}

fn latest(points: Option<&Vec<(String, f64)>>) -> f64 {
    points
        .and_then(|p| p.last())
        .map(|(_, v)| *v)
        .unwrap_or(0.0)
}

pub fn sleep_between_emiten() -> Duration {
    let secs = std::env::var("EVTOEBIT_SLEEP_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.4);
    Duration::from_secs_f64(secs)
}

pub async fn throttle() {
    sleep(sleep_between_emiten()).await;
}
