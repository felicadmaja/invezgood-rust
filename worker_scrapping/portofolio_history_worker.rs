//! Portofolio history via API `carina.stockbit.com/history?stock=`.
//! Upsert ke tabel Scylla `portofolio_history` (bukan kolom `portofolio.history`).
//!
//! Alur: START TRADING/PIN bila perlu → Bearer trading → GET `/history` per emiten
//! (paginate `page` s/d `meta.max_page`; jeda adaptif rate-limit)
//! → group by tanggal transaksi (`date`, mis. `20 Jul 2026`)
//! → INSERT `(emiten_name=symbol, tahun_bulan_tanggal=date, history=list item hari itu)`.

use chrono::{Local, NaiveDate};
use chromiumoxide::page::Page;
use scylla::client::session::Session;
use scylla::{DeserializeValue, SerializeValue};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use crate::portofolio_worker::{ensure_trading_session, extract_trading_bearer_after_pin};
use crate::rate_limit_delay::RateLimitInfo;

const HISTORY_API_URL: &str = "https://carina.stockbit.com/history";
const HISTORY_PAGE_LIMIT: u32 = 200;
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

fn json_f64_leaf(v: &Value) -> f64 {
    v.as_f64()
        .or_else(|| v.as_i64().map(|i| i as f64))
        .or_else(|| v.as_u64().map(|i| i as f64))
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or(0.0)
}

fn json_str(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .trim()
        .to_string()
}

fn urlencoding_simple(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' => c.to_string(),
            _ => format!("%{:02X}", c as u8),
        })
        .collect()
}

/// Parse tanggal API seperti `20 Jul 2026` → `NaiveDate`.
pub fn parse_history_item_date(s: &str) -> Option<NaiveDate> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    if let Ok(d) = NaiveDate::parse_from_str(s, "%d %b %Y") {
        return Some(d);
    }
    if let Ok(d) = NaiveDate::parse_from_str(s, "%d %B %Y") {
        return Some(d);
    }
    // Fallback manual: "20 Jul 2026"
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() != 3 {
        return None;
    }
    let day: u32 = parts[0].parse().ok()?;
    let year: i32 = parts[2].parse().ok()?;
    let mon = match parts[1].to_ascii_lowercase().as_str() {
        "jan" | "january" => 1,
        "feb" | "february" => 2,
        "mar" | "march" => 3,
        "apr" | "april" => 4,
        "may" => 5,
        "jun" | "june" => 6,
        "jul" | "july" => 7,
        "aug" | "august" => 8,
        "sep" | "sept" | "september" => 9,
        "oct" | "october" => 10,
        "nov" | "november" => 11,
        "dec" | "december" => 12,
        _ => return None,
    };
    NaiveDate::from_ymd_opt(year, mon, day)
}

fn parse_history_item(item: &Value) -> Option<(NaiveDate, PortofolioHistoryItemUd)> {
    let date = parse_history_item_date(&json_str(item, "date"))?;
    let symbol = json_str(item, "symbol").to_ascii_uppercase();
    if symbol.is_empty() {
        return None;
    }
    let command = json_str(item, "command");
    let status = json_str(item, "status");
    let price = item.get("price").map(json_f64_leaf).unwrap_or(0.0);
    let lot = item.get("lot").map(json_f64_leaf).unwrap_or(0.0);
    let amount = item.get("amount").map(json_f64_leaf).unwrap_or(0.0);
    Some((
        date,
        PortofolioHistoryItemUd {
            command,
            symbol,
            price,
            lot,
            amount,
            status,
        },
    ))
}

/// Parse body `/history` → map tanggal transaksi → list item (urutan API).
pub fn parse_carina_history_by_date(
    v: &Value,
) -> BTreeMap<NaiveDate, Vec<PortofolioHistoryItemUd>> {
    let mut out: BTreeMap<NaiveDate, Vec<PortofolioHistoryItemUd>> = BTreeMap::new();
    let months = v
        .pointer("/data/history")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();
    for month in months {
        let list = month
            .get("history_list")
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default();
        for item in list {
            if let Some((date, ud)) = parse_history_item(&item) {
                out.entry(date).or_default().push(ud);
            }
        }
    }
    out
}

fn history_max_page(v: &Value) -> u32 {
    v.pointer("/data/meta/max_page")
        .and_then(|x| x.as_u64().or_else(|| x.as_i64().map(|i| i as u64)))
        .unwrap_or(1)
        .max(1) as u32
}

fn history_url(stock: &str, page: u32) -> String {
    format!(
        "{HISTORY_API_URL}?page={page}&limit={HISTORY_PAGE_LIMIT}&period=all&start=&end=&action=&stock={}",
        urlencoding_simple(stock)
    )
}

async fn fetch_history_page(
    http: &reqwest::Client,
    bearer: &str,
    stock_code: &str,
    page: u32,
) -> Result<(Value, RateLimitInfo), Box<dyn std::error::Error>> {
    let url = history_url(stock_code, page);
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
    println!("  /history {stock_code} page={page} → HTTP {status} | {rate_log}");
    let body = resp.text().await.unwrap_or_default();
    if crate::http_abort::is_http_4xx(status) && status != reqwest::StatusCode::NOT_FOUND {
        let preview: String = body.chars().take(280).collect();
        return Err(format!(
            "PORTFOLIO_HISTORY_HTTP_4XX {status} /history stock={stock_code} page={page}: {preview} | {rate_log}"
        )
        .into());
    }
    if !status.is_success() {
        let preview: String = body.chars().take(280).collect();
        return Err(format!(
            "/history (stock={stock_code} page={page}) HTTP {status}: {preview} | {rate_log}"
        )
        .into());
    }
    let v: Value =
        serde_json::from_str(&body).map_err(|e| format!("/history JSON: {e}"))?;
    Ok((v, rate))
}

/// Fetch semua halaman `/history` untuk satu kode → group by tanggal transaksi.
pub async fn fetch_history_by_stock_code(
    http: &reqwest::Client,
    bearer: &str,
    stock_code: &str,
) -> Result<(BTreeMap<NaiveDate, Vec<PortofolioHistoryItemUd>>, RateLimitInfo), Box<dyn std::error::Error>>
{
    let (first, mut last_rate) = fetch_history_page(http, bearer, stock_code, 1).await?;
    let max_page = history_max_page(&first);
    let mut by_date = parse_carina_history_by_date(&first);

    for page in 2..=max_page {
        let delay = last_rate.inter_emiten_delay_ms();
        if delay > 0 {
            sleep(Duration::from_millis(delay)).await;
        }
        let (v, rate) = fetch_history_page(http, bearer, stock_code, page).await?;
        last_rate = rate;
        for (date, mut items) in parse_carina_history_by_date(&v) {
            by_date.entry(date).or_default().append(&mut items);
        }
    }
    Ok((by_date, last_rate))
}

/// Baca baris `portofolio_history` untuk emiten + tanggal.
pub async fn load_portofolio_history(
    session: &Session,
    keyspace: &str,
    emiten_name: &str,
    tahun_bulan_tanggal: NaiveDate,
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

/// Upsert satu hari transaksi ke `portofolio_history`.
pub async fn upsert_portofolio_history_for_date(
    session: &Session,
    keyspace: &str,
    emiten_name: &str,
    tahun_bulan_tanggal: NaiveDate,
    history: &[PortofolioHistoryItemUd],
) -> Result<(), Box<dyn std::error::Error>> {
    let code = emiten_name.trim().to_ascii_uppercase();
    let insert = session
        .prepare(format!(
            "INSERT INTO {keyspace}.portofolio_history (\
                emiten_name, tahun_bulan_tanggal, history\
            ) VALUES (?, ?, ?)"
        ))
        .await?;
    session
        .execute_unpaged(
            &insert,
            (code.as_str(), tahun_bulan_tanggal, history),
        )
        .await?;
    Ok(())
}

/// Upsert semua tanggal dari hasil `/history`. Returns (jumlah entri, tanggal terbaru).
pub async fn upsert_portofolio_history_by_dates(
    session: &Session,
    keyspace: &str,
    emiten_name: &str,
    by_date: &BTreeMap<NaiveDate, Vec<PortofolioHistoryItemUd>>,
) -> Result<(usize, Option<NaiveDate>), Box<dyn std::error::Error>> {
    let mut total = 0usize;
    let mut latest: Option<NaiveDate> = None;
    for (date, items) in by_date {
        upsert_portofolio_history_for_date(session, keyspace, emiten_name, *date, items).await?;
        total += items.len();
        latest = Some(latest.map_or(*date, |l| l.max(*date)));
    }
    Ok((total, latest))
}

/// START TRADING/PIN → Bearer → GET `/history?stock=` → upsert per tanggal transaksi.
/// Returns (jumlah entri, tanggal terbaru, list history tanggal terbaru).
pub async fn scrape_and_replace_portofolio_history(
    page: &Page,
    session: &Session,
    keyspace: &str,
    emiten_name: &str,
) -> Result<(usize, NaiveDate, Vec<PortofolioHistoryItemUd>), Box<dyn std::error::Error>> {
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
        "Portofolio history API: GET {HISTORY_API_URL}?page=1&limit={HISTORY_PAGE_LIMIT}&period=all&stock={code}..."
    );
    let (by_date, _rate) = fetch_history_by_stock_code(&http, &bearer, &code).await?;
    let (n, latest) = upsert_portofolio_history_by_dates(session, keyspace, &code, &by_date).await?;
    let latest = latest.unwrap_or_else(|| Local::now().date_naive());
    let history = by_date.get(&latest).cloned().unwrap_or_default();
    println!(
        "OK: portofolio_history {code} — {n} entri di {} tanggal (terbaru {latest}).",
        by_date.len()
    );
    Ok((n, latest, history))
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
            "Portofolio history [{}/{}] {code}: GET /history...",
            i + 1,
            emitens.len()
        );
        let (by_date, rate) = match fetch_history_by_stock_code(&http, &bearer, &code).await {
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
        match upsert_portofolio_history_by_dates(session.as_ref(), keyspace, &code, &by_date).await
        {
            Ok((n, latest)) => {
                ok += 1;
                println!(
                    "OK: portofolio_history {code} — {n} entri / {} tanggal (terbaru {:?}).",
                    by_date.len(),
                    latest
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
    fn parse_item_date_examples() {
        assert_eq!(
            parse_history_item_date("20 Jul 2026"),
            Some(NaiveDate::from_ymd_opt(2026, 7, 20).unwrap())
        );
        assert_eq!(
            parse_history_item_date("08 Jul 2026"),
            Some(NaiveDate::from_ymd_opt(2026, 7, 8).unwrap())
        );
    }

    #[test]
    fn parse_carina_history_groups_by_date() {
        let v: Value = serde_json::from_str(
            r#"{
                "data": {
                  "history": [{
                    "date": "Jul 2026",
                    "history_list": [
                      {"command":"BUY","symbol":"UNVR","price":1765,"lot":6,"amount":1059000,"status":"MATCH","date":"20 Jul 2026"},
                      {"command":"BUY","symbol":"UNVR","price":1665,"lot":3,"amount":499500,"status":"MATCH","date":"13 Jul 2026"},
                      {"command":"BUY","symbol":"UNVR","price":1730,"lot":3,"amount":519000,"status":"MATCH","date":"08 Jul 2026"}
                    ]
                  }],
                  "meta": {"max_page": 1}
                }
              }"#,
        )
        .unwrap();
        let by_date = parse_carina_history_by_date(&v);
        assert_eq!(by_date.len(), 3);
        let d20 = NaiveDate::from_ymd_opt(2026, 7, 20).unwrap();
        let items = by_date.get(&d20).expect("20 Jul");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].command, "BUY");
        assert_eq!(items[0].symbol, "UNVR");
        assert!((items[0].price - 1765.0).abs() < 1e-9);
        assert!((items[0].lot - 6.0).abs() < 1e-9);
        assert!((items[0].amount - 1059000.0).abs() < 1e-9);
        assert_eq!(items[0].status, "MATCH");
    }
}
