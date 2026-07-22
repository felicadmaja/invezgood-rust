//! Portofolio history via API `carina.stockbit.com/order/v2/list?filter_criteria.stock_code=`.
//! Upsert ke tabel Scylla `portofolio_history` (bukan kolom `portofolio.history`).
//!
//! Alur: START TRADING/PIN bila perlu → Bearer trading → GET order list per emiten
//! (jeda adaptif dari x-rate-limit-remaining: ≥4=0, 3=200ms, 2=300ms, 1=400ms, ≤0=1000ms)
//! → INSERT `(emiten_name, tahun_bulan_tanggal=today, history)`.

use chrono::{DateTime, Local, Utc};
use chromiumoxide::page::Page;
use scylla::client::session::Session;
use scylla::{DeserializeValue, SerializeValue};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use crate::rate_limit_delay::RateLimitInfo;
use crate::portofolio_worker::{ensure_trading_session, extract_trading_bearer_after_pin};

const ORDER_LIST_API_URL: &str = "https://carina.stockbit.com/order/v2/list";
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

/// UDT `portofolio_history_item` untuk INSERT/SELECT list history.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct PortofolioHistoryItemUd {
    #[scylla(default_when_null)]
    pub command: String,
    #[scylla(default_when_null)]
    pub symbol: String,
    #[scylla(default_when_null)]
    pub price: f64,
    #[scylla(default_when_null)]
    pub lot: f64,
    #[scylla(default_when_null)]
    pub amount: f64,
    #[scylla(default_when_null)]
    pub status: String,
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

fn json_str(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .trim()
        .to_string()
}

/// Parse `data[]` dari order/v2/list → list history (hanya item dengan time valid).
pub fn parse_order_history_list(v: &Value) -> Vec<PortofolioHistoryItemUd> {
    let items = v
        .get("data")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();

    let mut out = Vec::new();
    for item in items {
        // Skip entri tanpa waktu match/open/order valid.
        if parse_order_time_key(&item).is_none() {
            continue;
        }
        let symbol = json_str(&item, "symbol").to_ascii_uppercase();
        if symbol.is_empty() {
            continue;
        }
        // `command` API bila ada; fallback `side` (SIDE_BUY / SIDE_SELL).
        let command = {
            let c = json_str(&item, "command");
            if c.is_empty() {
                json_str(&item, "side")
            } else {
                c
            }
        };
        let status = {
            let s = json_str(&item, "status_text");
            if s.is_empty() {
                let s2 = json_str(&item, "status");
                if s2.is_empty() {
                    json_str(&item, "message")
                } else {
                    s2
                }
            } else {
                s
            }
        };
        let price = {
            let p = json_f64(&item, &["price", "average", "price"]);
            if p == 0.0 {
                json_f64(&item, &["price", "order"])
            } else {
                p
            }
        };
        let lot = {
            let l = json_f64(&item, &["qty", "lot_done"]);
            if l == 0.0 {
                json_f64(&item, &["qty", "lot_open"])
            } else {
                l
            }
        };
        let amount = {
            let a = json_f64(&item, &["amount", "matched"]);
            if a == 0.0 {
                json_f64(&item, &["amount", "matched_total"])
            } else {
                a
            }
        };
        out.push(PortofolioHistoryItemUd {
            command,
            symbol,
            price,
            lot,
            amount,
            status,
        });
    }
    out
}

async fn fetch_order_history_by_stock_code(
    http: &reqwest::Client,
    bearer: &str,
    stock_code: &str,
) -> Result<(Vec<PortofolioHistoryItemUd>, RateLimitInfo), Box<dyn std::error::Error>> {
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
    let rate_log = crate::rate_limit_delay::rate_limit_headers_log(resp.headers());
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
    Ok((parse_order_history_list(&v), rate))
}

/// Baca baris `portofolio_history` untuk emiten + tanggal.
pub async fn load_portofolio_history(
    session: &Session,
    keyspace: &str,
    emiten_name: &str,
    tahun_bulan_tanggal: chrono::NaiveDate,
) -> Result<Option<Vec<PortofolioHistoryItemUd>>, Box<dyn std::error::Error>> {
    #[derive(scylla::DeserializeRow)]
    struct Row {
        history: Vec<PortofolioHistoryItemUd>,
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
    Ok(result.maybe_first_row::<Row>()?.map(|r| r.history))
}

/// Upsert ke `portofolio_history` (emiten_name, today, history list).
pub async fn upsert_portofolio_history_today(
    session: &Session,
    keyspace: &str,
    emiten_name: &str,
    history: &[PortofolioHistoryItemUd],
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
/// → upsert `portofolio_history` hari ini. Returns (jumlah entri, tanggal, list history).
pub async fn scrape_and_replace_portofolio_history(
    page: &Page,
    session: &Session,
    keyspace: &str,
    emiten_name: &str,
) -> Result<(usize, chrono::NaiveDate, Vec<PortofolioHistoryItemUd>), Box<dyn std::error::Error>> {
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
    for (i, raw) in emitens.iter().enumerate() {
        if i > 0 {
            let delay = last_rate.inter_emiten_delay_ms();
            if delay > 0 {
                println!(
                    "  jeda adaptif {delay}ms (remaining={}; {})",
                    last_rate
                        .remaining
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "-".into()),
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
        let (history, rate) = match fetch_order_history_by_stock_code(&http, &bearer, &code).await
        {
            Ok(v) => v,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("PORTFOLIO_HISTORY_HTTP_4XX") {
                    eprintln!(
                        "Portofolio history: hentikan batch GET API ({msg}) — lanjut proses berikutnya."
                    );
                    break;
                }
                eprintln!("portofolio_history [{code}]: fetch gagal: {msg}");
                continue;
            }
        };
        last_rate = rate;
        let n = history.len();
        match upsert_portofolio_history_today(session.as_ref(), keyspace, &code, &history).await {
            Ok(today) => {
                ok += 1;
                println!(
                    "OK: portofolio_history {code} {today} diupsert dengan {n} entri."
                );
            }
            Err(e) => eprintln!("portofolio_history [{code}]: upsert gagal: {e}"),
        }
    }
    Ok(ok)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_order_history_list_fields() {
        let v: Value = serde_json::from_str(
            r#"{
                "data": [
                    {
                        "order_id": "XL123",
                        "message": "Matched",
                        "symbol": "bbca",
                        "side": "SIDE_BUY",
                        "status_text": "MATCH",
                        "time": { "match": "2026-07-20T07:20:55Z" },
                        "qty": { "lot_done": 10 },
                        "price": { "average": { "price": 8500.5 } },
                        "amount": { "matched": 8500500.0 }
                    }
                ]
            }"#,
        )
        .unwrap();
        let list = parse_order_history_list(&v);
        assert_eq!(list.len(), 1);
        let item = &list[0];
        assert_eq!(item.command, "SIDE_BUY");
        assert_eq!(item.symbol, "BBCA");
        assert_eq!(item.status, "MATCH");
        assert!((item.lot - 10.0).abs() < 1e-9);
        assert!((item.price - 8500.5).abs() < 1e-9);
        assert!((item.amount - 8500500.0).abs() < 1e-9);
    }
}
