//! Portofolio history via API `carina.stockbit.com/order/v2/list?filter_criteria.stock_code=`.
//! Upsert ke tabel Scylla `portofolio_history` (bukan kolom `portofolio.history`).
//!
//! Alur: START TRADING/PIN bila perlu → Bearer trading → GET order list per emiten
//! (jeda adaptif dari x-rate-limit-*: 200–1000 ms bila kuota menipis / remaining belum naik, 0 bila tebal)
//! → INSERT `(emiten_name, tahun_bulan_tanggal=today, history)`.

use chrono::{DateTime, Local, Utc};
use chromiumoxide::page::Page;
use scylla::client::session::Session;
use scylla::{DeserializeValue, SerializeValue};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use crate::http_abort::RateLimitInfo;
use crate::portofolio_worker::{ensure_trading_session, extract_trading_bearer_after_pin};

const ORDER_LIST_API_URL: &str = "https://carina.stockbit.com/order/v2/list";
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

/// UDT `portofolio_history_item` untuk INSERT/SELECT map history.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct PortofolioHistoryItemUd {
    #[scylla(default_when_null)]
    pub order_id: String,
    #[scylla(default_when_null)]
    pub message: String,
    #[scylla(default_when_null)]
    pub symbol: String,
    #[scylla(default_when_null)]
    pub side: String,
    #[scylla(default_when_null)]
    pub lot_done: i32,
    #[scylla(default_when_null)]
    pub price_average: f64,
    #[scylla(default_when_null)]
    pub amount_matched: f64,
}

fn json_f64(v: &Value, path: &[&str]) -> f64 {
    let mut cur = v;
    for p in path {
        cur = match cur.get(*p) {
            Some(x) => x,
            None => return 0.0,
        };
    }
    cur.as_f64()
        .or_else(|| cur.as_i64().map(|i| i as f64))
        .or_else(|| cur.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or(0.0)
}

fn json_i32(v: &Value, path: &[&str]) -> i32 {
    json_f64(v, path) as i32
}

fn urlencoding_simple(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' => c.to_string(),
            _ => format!("%{:02X}", c as u8),
        })
        .collect()
}

fn parse_order_time_key(item: &Value) -> Option<DateTime<Utc>> {
    for path in ["/time/match", "/time/open", "/time/order"] {
        let s = item.pointer(path).and_then(|x| x.as_str()).unwrap_or("").trim();
        if s.is_empty() || s.starts_with("0001-01-01") {
            continue;
        }
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            return Some(dt.with_timezone(&Utc));
        }
    }
    None
}

/// Parse `data[]` dari order/v2/list → map history (key = waktu match/open/order).
pub fn parse_order_history_map(
    v: &Value,
) -> HashMap<DateTime<Utc>, PortofolioHistoryItemUd> {
    let items = v
        .get("data")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();

    let mut out = HashMap::new();
    for item in items {
        let Some(ts) = parse_order_time_key(&item) else {
            continue;
        };
        let order_id = item
            .get("order_id")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        if order_id.is_empty() {
            continue;
        }
        let symbol = item
            .get("symbol")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim()
            .to_ascii_uppercase();
        let message = item
            .get("message")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        let side = item
            .get("side")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        out.insert(
            ts,
            PortofolioHistoryItemUd {
                order_id,
                message,
                symbol,
                side,
                lot_done: json_i32(&item, &["qty", "lot_done"]),
                price_average: json_f64(&item, &["price", "average", "price"]),
                amount_matched: json_f64(&item, &["amount", "matched"]),
            },
        );
    }
    out
}

async fn fetch_order_history_by_stock_code(
    http: &reqwest::Client,
    bearer: &str,
    stock_code: &str,
) -> Result<(HashMap<DateTime<Utc>, PortofolioHistoryItemUd>, RateLimitInfo), Box<dyn std::error::Error>>
{
    let url = format!(
        "{ORDER_LIST_API_URL}?filter_criteria.stock_code={}",
        urlencoding_simple(stock_code)
    );
    let resp = http
        .get(&url)
        .header("Authorization", format!("Bearer {bearer}"))
        .header("Accept", "application/json")
        .header("Origin", "https://stockbit.com")
        .header("Referer", "https://stockbit.com/")
        .header("Cache-Control", "no-cache")
        .header("Pragma", "no-cache")
        .send()
        .await?;

    let status = resp.status();
    let rate = RateLimitInfo::from_headers(resp.headers());
    let rate_log = crate::http_abort::rate_limit_headers_log(resp.headers());
    println!("  order/v2/list {stock_code} → HTTP {status} | {rate_log}");
    let body = resp.text().await.unwrap_or_default();
    // Jangan abort seluruh app: 4xx (mis. 429) di-handle caller — hentikan batch history saja.
    if crate::http_abort::is_http_4xx(status) && status != reqwest::StatusCode::NOT_FOUND {
        let preview: String = body.chars().take(280).collect();
        return Err(format!(
            "PORTFOLIO_HISTORY_HTTP_4XX {status} order/v2/list stock={stock_code}: {preview} | {rate_log}"
        )
        .into());
    }
    if !status.is_success() {
        let preview: String = body.chars().take(280).collect();
        return Err(format!(
            "order/v2/list (stock={stock_code}) HTTP {status}: {preview} | {rate_log}"
        )
        .into());
    }
    let v: Value = serde_json::from_str(&body)
        .map_err(|e| format!("order/v2/list JSON: {e}"))?;
    Ok((parse_order_history_map(&v), rate))
}

/// Baca baris `portofolio_history` untuk emiten + tanggal.
pub async fn load_portofolio_history(
    session: &Session,
    keyspace: &str,
    emiten_name: &str,
    tahun_bulan_tanggal: chrono::NaiveDate,
) -> Result<Option<HashMap<DateTime<Utc>, PortofolioHistoryItemUd>>, Box<dyn std::error::Error>>
{
    #[derive(scylla::DeserializeRow)]
    struct Row {
        history: HashMap<DateTime<Utc>, PortofolioHistoryItemUd>,
    }

    let code = emiten_name.trim().to_ascii_uppercase();
    let stmt = session
        .prepare(format!(
            "SELECT history FROM {keyspace}.portofolio_history \
             WHERE emiten_name = ? AND tahun_bulan_tanggal = ? LIMIT 1"
        ))
        .await?;
    let result = session
        .execute_unpaged(&stmt, (code.as_str(), tahun_bulan_tanggal))
        .await?
        .into_rows_result()?;
    Ok(result
        .maybe_first_row::<Row>()?
        .map(|r| r.history))
}

/// Upsert ke `portofolio_history` (emiten_name, today, history map).
pub async fn upsert_portofolio_history_today(
    session: &Session,
    keyspace: &str,
    emiten_name: &str,
    history: &HashMap<DateTime<Utc>, PortofolioHistoryItemUd>,
) -> Result<chrono::NaiveDate, Box<dyn std::error::Error>> {
    let code = emiten_name.trim().to_ascii_uppercase();
    let today = Local::now().date_naive();
    let insert = session
        .prepare(format!(
            "INSERT INTO {keyspace}.portofolio_history (\
                emiten_name, tahun_bulan_tanggal, history\
            ) VALUES (?, ?, ?)"
        ))
        .await?;
    session
        .execute_unpaged(&insert, (code.as_str(), today, history))
        .await?;
    Ok(today)
}

/// START TRADING/PIN → Bearer trading → GET order/v2/list?filter_criteria.stock_code=
/// → upsert `portofolio_history` hari ini. Returns (jumlah entri, tanggal, map history).
pub async fn scrape_and_replace_portofolio_history(
    page: &Page,
    session: &Session,
    keyspace: &str,
    emiten_name: &str,
) -> Result<(usize, chrono::NaiveDate, HashMap<DateTime<Utc>, PortofolioHistoryItemUd>), Box<dyn std::error::Error>> {
    let code = emiten_name.trim().to_ascii_uppercase();
    if code.len() != 4 || !code.chars().all(|c| c.is_ascii_alphabetic()) {
        return Err("emiten_name harus tepat 4 huruf alfabet".into());
    }

    let pin_entered = ensure_trading_session(page).await?;
    if pin_entered {
        println!("Portofolio history: jeda 1 detik setelah input PIN...");
        sleep(Duration::from_secs(1)).await;
    }

    let bearer = extract_trading_bearer_after_pin(page).await?;
    let http = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(60))
        .build()?;

    println!(
        "Portofolio history API: GET {ORDER_LIST_API_URL}?filter_criteria.stock_code={code}..."
    );
    let (history, _rate) = fetch_order_history_by_stock_code(&http, &bearer, &code).await?;
    let n = history.len();
    let today = upsert_portofolio_history_today(session, keyspace, &code, &history).await?;
    println!(
        "OK: portofolio_history {code} {today} diupsert dengan {n} entri."
    );
    Ok((n, today, history))
}

/// Scrape + upsert history untuk banyak emiten (satu sesi trading/Bearer).
/// Returns jumlah emiten yang berhasil di-upsert.
pub async fn scrape_and_upsert_portofolio_history_for_emitens(
    page: &Page,
    session: &Arc<Session>,
    keyspace: &str,
    emitens: &[String],
) -> Result<usize, Box<dyn std::error::Error>> {
    if emitens.is_empty() {
        return Ok(0);
    }

    let pin_entered = ensure_trading_session(page).await?;
    if pin_entered {
        println!("Portofolio history batch: jeda 1 detik setelah input PIN...");
        sleep(Duration::from_secs(1)).await;
    }

    let bearer = extract_trading_bearer_after_pin(page).await?;
    let http = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(60))
        .build()?;

    let mut ok = 0usize;
    let mut last_rate = RateLimitInfo::default();
    let mut prev_remaining: Option<i64> = None;
    for (i, raw) in emitens.iter().enumerate() {
        if i > 0 {
            let delay = last_rate.inter_emiten_delay_ms_with_prev(prev_remaining);
            if delay > 0 {
                println!(
                    "  jeda adaptif {delay}ms (kuota menipis; {})",
                    last_rate.log_line()
                );
                sleep(Duration::from_millis(delay)).await;
            }
        }
        let code = raw.trim().to_ascii_uppercase();
        if code.len() != 4 || !code.chars().all(|c| c.is_ascii_alphabetic()) {
            continue;
        }
        if i > 0 {
            println!();
        }
        println!(
            "Portofolio history [{}/{}] {code}: GET order/v2/list...",
            i + 1,
            emitens.len()
        );
        match fetch_order_history_by_stock_code(&http, &bearer, &code).await {
            Ok((history, rate)) => {
                prev_remaining = last_rate.remaining;
                last_rate = rate;
                let n = history.len();
                match upsert_portofolio_history_today(session.as_ref(), keyspace, &code, &history)
                    .await
                {
                    Ok(today) => {
                        ok += 1;
                        println!(
                            "OK: portofolio_history {code} {today} diupsert dengan {n} entri."
                        );
                    }
                    Err(e) => eprintln!("portofolio_history [{code}]: upsert gagal: {e}"),
                }
            }
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("PORTFOLIO_HISTORY_HTTP_4XX") {
                    eprintln!(
                        "Portofolio history: hentikan batch GET API ({msg}) — lanjut proses berikutnya."
                    );
                    break;
                }
                eprintln!("portofolio_history [{code}]: fetch gagal: {e}");
            }
        }
    }
    Ok(ok)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_order_history_maps_fields() {
        let v: Value = serde_json::from_str(
            r#"{
                "data": [
                    {
                        "order_id": "XL123",
                        "message": "Matched",
                        "symbol": "bbca",
                        "side": "SIDE_BUY",
                        "time": { "match": "2026-07-20T07:20:55Z" },
                        "qty": { "lot_done": 10 },
                        "price": { "average": { "price": 8500.5 } },
                        "amount": { "matched": 8500500.0 }
                    }
                ]
            }"#,
        )
        .unwrap();
        let map = parse_order_history_map(&v);
        assert_eq!(map.len(), 1);
        let item = map.values().next().unwrap();
        assert_eq!(item.order_id, "XL123");
        assert_eq!(item.symbol, "BBCA");
        assert_eq!(item.lot_done, 10);
        assert!((item.price_average - 8500.5).abs() < 1e-9);
    }
}
