//! Top Gainer / Top Loser via `exodus.stockbit.com/order-trade/market-mover`
//! → upsert `invezgood.emiten_trending`.
//! Icon/nama diisi dari `stock_list` bila ada, else dari API movers.

use chrono::{Local, Utc};
use chromiumoxide::page::Page;
use scylla::client::session::Session;
use scylla::DeserializeRow;
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;
use stockbit_browser::extract_stockbit_bearer;
use tokio::time::sleep;

use crate::rate_limit_delay::RateLimitInfo;

const MARKET_MOVER_URL: &str = "https://exodus.stockbit.com/order-trade/market-mover";
const FILTER_STOCKS_QUERY: &str = concat!(
    "filter_stocks=FILTER_STOCKS_TYPE_MAIN_BOARD",
    "&filter_stocks=FILTER_STOCKS_TYPE_DEVELOPMENT_BOARD",
    "&filter_stocks=FILTER_STOCKS_TYPE_ACCELERATION_BOARD",
    "&filter_stocks=FILTER_STOCKS_TYPE_NEW_ECONOMY_BOARD",
);

#[derive(Debug, DeserializeRow)]
struct StockListMetaRow {
    #[scylla(default_when_null)]
    name: Option<String>,
    #[scylla(default_when_null)]
    logo: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct MoversRow {
    symbol: String,
    long_name: String,
    emiten_icon: String,
    price: String,
    price_change: String,
    value: String,
    volume: String,
    freq: String,
}

fn normalize_emiten_name(raw: &str) -> String {
    raw.chars()
        .filter(|c| c.is_ascii_alphabetic())
        .map(|c| c.to_ascii_uppercase())
        .collect()
}

fn parse_price(raw: &str) -> f64 {
    raw.trim().replace(',', "").parse().unwrap_or(0.0)
}

fn parse_price_change(raw: &str) -> f64 {
    let cleaned: String = raw
        .chars()
        .filter(|c| *c != '(' && *c != ')' && *c != '%' && !c.is_whitespace())
        .collect();
    cleaned.parse().unwrap_or(0.0)
}

async fn fetch_market_mover(
    http: &reqwest::Client,
    bearer: &str,
    mover_type: &str,
) -> Result<(Vec<MoversRow>, RateLimitInfo), Box<dyn std::error::Error>> {
    let url = format!("{MARKET_MOVER_URL}?mover_type={mover_type}&{FILTER_STOCKS_QUERY}");
    let resp = http
        .get(&url)
        .header("Authorization", format!("Bearer {bearer}"))
        .header("Accept", "application/json, text/plain, */*")
        .header("Origin", "https://stockbit.com")
        .header("Referer", "https://stockbit.com/")
        .header("x-platform", "web")
        .send()
        .await?;

    let status = resp.status();
    let rate = RateLimitInfo::from_headers(resp.headers());
    let rate_log = crate::rate_limit_delay::rate_limit_headers_log(resp.headers());
    println!("  market-mover {mover_type} → HTTP {status} | {rate_log}");
    crate::http_abort::abort_app_if_http_4xx(status, &format!("market-mover {mover_type}"));
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        let preview: String = body.chars().take(280).collect();
        return Err(
            format!("market-mover {mover_type} HTTP {status}: {preview} | {rate_log}").into(),
        );
    }

    let v: Value = serde_json::from_str(&body)
        .map_err(|e| format!("market-mover {mover_type} JSON: {e}"))?;
    let list = v
        .pointer("/data/mover_list")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();

    let mut rows = Vec::with_capacity(list.len());
    for item in list {
        let symbol = item
            .pointer("/stock_detail/code")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim()
            .to_uppercase();
        if symbol.is_empty() {
            continue;
        }
        let emiten_icon = item
            .pointer("/stock_detail/icon_url")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        let long_name = item
            .pointer("/stock_detail/name")
            .or_else(|| item.pointer("/stock_detail/company_name"))
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        let price = match item.get("price") {
            Some(Value::Number(n)) => n.to_string(),
            Some(Value::String(s)) => s.trim().to_string(),
            _ => String::new(),
        };
        let pct = item
            .pointer("/change/percentage")
            .and_then(|x| x.as_f64())
            .unwrap_or(0.0);
        let price_change = format!("{pct:.2}%");
        let value = item
            .pointer("/value/formatted")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        let volume = item
            .pointer("/volume/formatted")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        let freq = item
            .pointer("/frequency/formatted")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim()
            .to_string();

        rows.push(MoversRow {
            symbol,
            long_name,
            emiten_icon,
            price,
            price_change,
            value,
            volume,
            freq,
        });
    }
    Ok((rows, rate))
}

async fn apply_rate_limit_delay(rate: &RateLimitInfo, context: &str) {
    let delay_ms = rate.inter_emiten_delay_ms();
    if delay_ms == 0 {
        return;
    }
    println!(
        "  jeda adaptif {delay_ms}ms ({context}; remaining={}; {})",
        rate.remaining
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".into()),
        rate.log_line()
    );
    sleep(Duration::from_millis(delay_ms)).await;
}

async fn resolve_meta_from_stock_list(
    session: &Session,
    stmt: &scylla::statement::prepared::PreparedStatement,
    emiten: &str,
    api_long_name: &str,
    api_icon: &str,
) -> (String, String) {
    match session.execute_unpaged(stmt, (emiten,)).await {
        Ok(res) => match res.into_rows_result() {
            Ok(rows) => {
                if let Ok(Some(meta)) = rows.maybe_first_row::<StockListMetaRow>() {
                    let name = meta
                        .name
                        .unwrap_or_default()
                        .trim()
                        .to_string();
                    let logo = meta.logo.unwrap_or_default().trim().to_string();
                    let long_name = if !name.is_empty() {
                        name
                    } else {
                        api_long_name.trim().to_string()
                    };
                    let icon = if !logo.is_empty() {
                        logo
                    } else if !api_icon.trim().is_empty() {
                        api_icon.trim().to_string()
                    } else {
                        format!("https://assets.stockbit.com/logos/companies/{emiten}.png")
                    };
                    return (long_name, icon);
                }
            }
            Err(e) => eprintln!("Peringatan: parse stock_list {emiten}: {e}"),
        },
        Err(e) => eprintln!("Peringatan: baca stock_list {emiten}: {e}"),
    }

    let long_name = api_long_name.trim().to_string();
    let icon = if !api_icon.trim().is_empty() {
        api_icon.trim().to_string()
    } else {
        format!("https://assets.stockbit.com/logos/companies/{emiten}.png")
    };
    (long_name, icon)
}

async fn insert_emiten_trending(
    session: &Session,
    keyspace: &str,
    rows: &[MoversRow],
    gainer_or_loser: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let today = Local::now().date_naive();
    let date_str = today.format("%Y-%m-%d").to_string();

    let insert = session
        .prepare(format!(
            "INSERT INTO {keyspace}.emiten_trending (\
                agg_tahun_bulan_tanggal_emiten_name, \
                tahun_bulan_tanggal, \
                gainer_or_loser, \
                emiten_name, \
                long_name, \
                emiten_icon, \
                sector, \
                price, \
                price_change, \
                value, \
                volume, \
                freq, \
                updated_at\
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        ))
        .await?;

    let stock_list_stmt = session
        .prepare(format!(
            "SELECT name, logo FROM {keyspace}.stock_list WHERE code = ?"
        ))
        .await?;

    let mut n = 0usize;
    for row in rows {
        let emiten = normalize_emiten_name(&row.symbol);
        if emiten.is_empty() {
            continue;
        }
        let agg = format!("{date_str}_{emiten}");

        let price_change = parse_price_change(&row.price_change);
        let price = parse_price(&row.price);
        let (long_name, emiten_icon) = resolve_meta_from_stock_list(
            session,
            &stock_list_stmt,
            &emiten,
            &row.long_name,
            &row.emiten_icon,
        )
        .await;
        let sector: Option<i8> = None;
        let updated_at = Utc::now();

        session
            .execute_unpaged(
                &insert,
                (
                    agg.as_str(),
                    today,
                    gainer_or_loser,
                    emiten.as_str(),
                    long_name.as_str(),
                    emiten_icon.as_str(),
                    sector,
                    price,
                    price_change,
                    row.value.as_str(),
                    row.volume.as_str(),
                    row.freq.as_str(),
                    updated_at,
                ),
            )
            .await?;
        n += 1;
    }
    Ok(n)
}

/// Returns `(inserted_gainer, inserted_loser)`.
pub async fn scrape_and_insert_movers(
    page: &Page,
    session: &Session,
    keyspace: &str,
) -> Result<(usize, usize), Box<dyn std::error::Error>> {
    println!("Market mover: ambil Bearer dari sesi browser...");
    let bearer = extract_stockbit_bearer(page)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;
    println!("Bearer OK (len={}).", bearer.len());

    let http = reqwest::Client::builder()
        .user_agent(
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        .timeout(Duration::from_secs(60))
        .build()?;

    println!("Market mover: TOP_GAINER...");
    let (gainer_rows, gainer_rate) =
        fetch_market_mover(&http, &bearer, "MOVER_TYPE_TOP_GAINER").await?;
    apply_rate_limit_delay(&gainer_rate, "market-mover TOP_GAINER").await;
    println!("Top Gainer: {} baris dari API.", gainer_rows.len());
    if gainer_rows.is_empty() {
        return Err("Top Gainer kosong dari API market-mover".into());
    }

    let inserted_gainer =
        insert_emiten_trending(session, keyspace, &gainer_rows, "gainer").await?;
    println!("OK: {inserted_gainer} baris diinsert ke emiten_trending (gainer).");

    println!("Market mover: TOP_LOSER...");
    let (loser_rows, loser_rate) =
        fetch_market_mover(&http, &bearer, "MOVER_TYPE_TOP_LOSER").await?;
    apply_rate_limit_delay(&loser_rate, "market-mover TOP_LOSER").await;
    println!("Top Loser: {} baris dari API.", loser_rows.len());
    let inserted_loser = if loser_rows.is_empty() {
        eprintln!(
            "Peringatan: Top Loser kosong dari API market-mover — skip insert loser."
        );
        0
    } else {
        let n = insert_emiten_trending(session, keyspace, &loser_rows, "loser").await?;
        println!("OK: {n} baris diinsert ke emiten_trending (loser).");
        n
    };

    Ok((inserted_gainer, inserted_loser))
}
