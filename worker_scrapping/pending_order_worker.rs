//! Pending order via API `carina.stockbit.com/order/v2/list` → upsert `pending_order`.
//!
//! Alur sama portofolio: START TRADING + PIN (`STOCKBUT_PIN` / `STOCKBIT_PIN`) bila perlu,
//! lalu Bearer trading pasca-PIN → GET order list (log + jeda `rate_limit_delay`) → upsert Scylla.
//!
//! Mapping JSON → kolom (lihat `pending_order.cql` / `contoh_data.json`):
//! - `order_id` ← `order_id`
//! - `emiten_name` ← `symbol`
//! - `status` ← `status_text`
//! - `message` ← `message`
//! - `side` ← `side`
//! - `time_open` ← `time.open` (timestamp RFC3339)
//! - `tahun_bulan_tanggal` ← tanggal dari `time_open` (YYYY-MM-DD)
//! - `lot_open` ← `qty.lot_open`
//! - `lot_done` ← `qty.lot_done`
//! - `price_order` ← `price.order`
//! - `amount_open` ← `amount.open`
//! - `amount_match` ← `amount.matched`
//! - `amount_match_total` ← `amount.matched_total`
//! - `is_gtc` ← `gtc.is_gtc`
//! - `updated_at` ← waktu upsert (UTC now)

use chrono::{DateTime, NaiveDate, Utc};
use chromiumoxide::page::Page;
use scylla::client::session::Session;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use crate::portofolio_worker;
use crate::rate_limit_delay::RateLimitInfo;

const ORDER_LIST_API_URL: &str = "https://carina.stockbit.com/order/v2/list";
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

#[derive(Debug, Clone)]
struct PendingOrderRow {
    order_id: String,
    tahun_bulan_tanggal: NaiveDate,
    emiten_name: String,
    status: String,
    message: String,
    side: String,
    time_open: DateTime<Utc>,
    lot_open: f64,
    lot_done: f64,
    price_order: f64,
    amount_open: f64,
    amount_match: f64,
    amount_match_total: f64,
    is_gtc: bool,
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
        .or_else(|| cur.as_i64().map(|n| n as f64))
        .or_else(|| cur.as_u64().map(|n| n as f64))
        .unwrap_or(0.0)
}

fn json_str(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .trim()
        .to_string()
}

fn json_bool(v: &Value, path: &[&str]) -> bool {
    let mut cur = v;
    for p in path {
        cur = match cur.get(*p) {
            Some(x) => x,
            None => return false,
        };
    }
    cur.as_bool().unwrap_or(false)
}

/// Parse `time.open` ISO (`2026-07-20T13:04:25Z`) → `DateTime<Utc>`.
fn parse_time_open(item: &Value) -> Option<DateTime<Utc>> {
    let s = item
        .pointer("/time/open")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .trim();
    if s.is_empty() || s.starts_with("0001-01-01") {
        return None;
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    // Fallback: tanggal saja → midnight UTC
    let date_part = s.get(..10)?;
    let naive = chrono::NaiveDate::parse_from_str(date_part, "%Y-%m-%d").ok()?;
    Some(DateTime::from_naive_utc_and_offset(
        naive.and_hms_opt(0, 0, 0)?,
        Utc,
    ))
}

fn parse_order_list_json(v: &Value) -> Vec<PendingOrderRow> {
    let items = v
        .get("data")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();

    let mut rows = Vec::with_capacity(items.len());
    for item in items {
        let order_id = json_str(&item, "order_id");
        if order_id.is_empty() {
            continue;
        }
        let emiten_name = json_str(&item, "symbol").to_ascii_uppercase();
        if emiten_name.is_empty() {
            continue;
        }
        let Some(time_open) = parse_time_open(&item) else {
            println!(
                "Pending order: skip {order_id} — time.open tidak valid ({:?})",
                item.pointer("/time/open")
            );
            continue;
        };
        rows.push(PendingOrderRow {
            order_id,
            tahun_bulan_tanggal: time_open.date_naive(),
            emiten_name,
            status: json_str(&item, "status_text"),
            message: json_str(&item, "message"),
            side: json_str(&item, "side"),
            time_open,
            lot_open: json_f64(&item, &["qty", "lot_open"]),
            lot_done: json_f64(&item, &["qty", "lot_done"]),
            price_order: json_f64(&item, &["price", "order"]),
            amount_open: json_f64(&item, &["amount", "open"]),
            amount_match: json_f64(&item, &["amount", "matched"]),
            amount_match_total: json_f64(&item, &["amount", "matched_total"]),
            is_gtc: json_bool(&item, &["gtc", "is_gtc"]),
        });
    }
    rows
}

async fn fetch_order_list(
    http: &reqwest::Client,
    bearer: &str,
) -> Result<(Vec<PendingOrderRow>, RateLimitInfo), Box<dyn std::error::Error>> {
    let resp = http
        .get(ORDER_LIST_API_URL)
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
    println!("  order/v2/list (pending_order) → HTTP {status} | {rate_log}");
    crate::http_abort::abort_app_if_http_4xx(status, "order/v2/list");
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        let preview: String = body.chars().take(280).collect();
        return Err(format!("order/v2/list HTTP {status}: {preview} | {rate_log}").into());
    }

    let v: Value =
        serde_json::from_str(&body).map_err(|e| format!("order/v2/list JSON: {e}"))?;
    Ok((parse_order_list_json(&v), rate))
}

async fn upsert_pending_orders(
    session: &Session,
    keyspace: &str,
    rows: &[PendingOrderRow],
) -> Result<usize, Box<dyn std::error::Error>> {
    let insert = session
        .prepare(format!(
            "INSERT INTO {keyspace}.pending_order (\
                order_id, tahun_bulan_tanggal, tahun_bulan, emiten_name, status, message, side, time_open, \
                lot_open, lot_done, price_order, amount_open, amount_match, \
                amount_match_total, is_gtc, updated_at\
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        ))
        .await?;

    let updated_at = Utc::now();
    let mut n = 0usize;
    println!(
        "Pending order: upsert {n_rows} baris ke {keyspace}.pending_order...",
        n_rows = rows.len()
    );
    for row in rows {
        let tahun_bulan = row.tahun_bulan_tanggal.format("%Y-%m").to_string();
        session
            .execute_unpaged(
                &insert,
                (
                    row.order_id.as_str(),
                    row.tahun_bulan_tanggal,
                    tahun_bulan.as_str(),
                    row.emiten_name.as_str(),
                    row.status.as_str(),
                    row.message.as_str(),
                    row.side.as_str(),
                    row.time_open,
                    row.lot_open,
                    row.lot_done,
                    row.price_order,
                    row.amount_open,
                    row.amount_match,
                    row.amount_match_total,
                    row.is_gtc,
                    updated_at,
                ),
            )
            .await?;
        n += 1;
    }
    Ok(n)
}

/// START TRADING (opsional) → PIN (opsional) → Bearer trading → order/v2/list → upsert `pending_order`.
pub async fn scrape_and_insert_pending_order(
    page: &Page,
    session: &Arc<Session>,
    keyspace: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let pin_entered = portofolio_worker::ensure_trading_session(page).await?;
    if pin_entered {
        println!("Pending order: jeda 1 detik setelah input PIN...");
        sleep(Duration::from_secs(1)).await;
    }

    let bearer = portofolio_worker::extract_trading_bearer_after_pin(page).await?;
    let http = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(60))
        .build()?;

    println!("\x1b[32mPending order API: GET {ORDER_LIST_API_URL}...\x1b[0m");
    let (rows, rate) = fetch_order_list(&http, &bearer).await?;
    let delay_ms = rate.inter_emiten_delay_ms();
    if delay_ms > 0 {
        println!(
            "  jeda adaptif {delay_ms}ms (remaining={}; {})",
            rate.remaining
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".into()),
            rate.log_line()
        );
        sleep(Duration::from_millis(delay_ms)).await;
    }
    println!("Pending order: {} baris dari API.", rows.len());

    if rows.is_empty() {
        println!("INFO pending_order: data kosong — tidak ada insert.");
        return Ok(0);
    }

    let n = upsert_pending_orders(session.as_ref(), keyspace, &rows).await?;
    println!("OK: {n} baris di-upsert ke pending_order.");
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_order_list_maps_fields() {
        let v: Value = serde_json::from_str(
            r#"{
                "message": "Orders data retrieved",
                "data": [
                    {
                        "order_id": "XL4637614QBOkCmZbPu7",
                        "symbol": "RBMS",
                        "status_text": "PENDING",
                        "message": "waiting",
                        "side": "SIDE_BUY",
                        "time": {
                            "open": "2026-07-20T13:04:25Z"
                        },
                        "qty": { "lot_done": 0, "lot_open": 157 },
                        "price": { "order": 63 },
                        "amount": {
                            "matched": 0,
                            "matched_total": 10,
                            "open": 989100
                        },
                        "gtc": { "is_gtc": true }
                    }
                ]
            }"#,
        )
        .unwrap();

        let rows = parse_order_list_json(&v);
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.order_id, "XL4637614QBOkCmZbPu7");
        assert_eq!(r.emiten_name, "RBMS");
        assert_eq!(r.status, "PENDING");
        assert_eq!(r.message, "waiting");
        assert_eq!(r.side, "SIDE_BUY");
        assert_eq!(
            r.time_open.to_rfc3339(),
            "2026-07-20T13:04:25+00:00"
        );
        assert_eq!(r.tahun_bulan_tanggal.to_string(), "2026-07-20");
        assert_eq!(r.lot_open, 157.0);
        assert_eq!(r.lot_done, 0.0);
        assert_eq!(r.price_order, 63.0);
        assert_eq!(r.amount_open, 989100.0);
        assert_eq!(r.amount_match, 0.0);
        assert_eq!(r.amount_match_total, 10.0);
        assert!(r.is_gtc);
    }
}
