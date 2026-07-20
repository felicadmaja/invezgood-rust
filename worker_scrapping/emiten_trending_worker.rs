//! Top Gainer / Top Loser via API `exodus.stockbit.com/order-trade/market-mover`
//! → insert `emiten_trending`. Bearer dari sesi browser (login Stockbit).
//! Bila baris hari ini benar-benar baru (belum ada PK), ikut upsert
//! `emiten_trending_count_by_name` (`appearance_count + 1`).

use chrono::{Local, NaiveDate, Utc};
use chromiumoxide::page::Page;
use gcs::{download_and_upload_emiten_icon, GcsOAuthTokenCache, GcsSignedUrlRuntime};
use scylla::client::session::Session;
use scylla::DeserializeRow;
use serde::Deserialize;
use serde_json::Value;
use std::sync::OnceLock;
use std::time::Duration;
use stockbit_browser::extract_stockbit_bearer;

const MARKET_MOVER_URL: &str = "https://exodus.stockbit.com/order-trade/market-mover";
const FILTER_STOCKS_QUERY: &str = concat!(
    "filter_stocks=FILTER_STOCKS_TYPE_MAIN_BOARD",
    "&filter_stocks=FILTER_STOCKS_TYPE_DEVELOPMENT_BOARD",
    "&filter_stocks=FILTER_STOCKS_TYPE_ACCELERATION_BOARD",
    "&filter_stocks=FILTER_STOCKS_TYPE_NEW_ECONOMY_BOARD",
);

#[derive(Debug, DeserializeRow)]
struct TrendingCountRow {
    #[scylla(default_when_null)]
    appearance_count: i64,
}

#[derive(Debug, Clone, Deserialize)]
struct MoversRow {
    symbol: String,
    /// Dari API `stock_detail.name` / `company_name` (opsional).
    long_name: String,
    emiten_icon: String,
    price: String,
    price_change: String,
    value: String,
    volume: String,
    freq: String,
}

/// Normalisasi symbol: huruf saja, uppercase (contoh `kblv` → `KBLV`).
fn normalize_emiten_name(raw: &str) -> String {
    raw.chars()
        .filter(|c| c.is_ascii_alphabetic())
        .map(|c| c.to_ascii_uppercase())
        .collect()
}

/// Harga saham: `"108"`, `"1,235"` → double.
fn parse_price(raw: &str) -> f64 {
    raw.trim().replace(',', "").parse().unwrap_or(0.0)
}

/// `emiten_trending.price_change` (double).
/// Contoh API/UI: `"27.49%"` / `"(+26.85%)"` → `26.85`, `"(-1.08%)"` → `-1.08`.
/// Buang `(`, `)`, `%`, spasi; tanda `+`/`-` tetap (positif tanpa `+`).
fn parse_price_change(raw: &str) -> f64 {
    let cleaned: String = raw
        .chars()
        .filter(|c| *c != '(' && *c != ')' && *c != '%' && !c.is_whitespace())
        .collect();
    cleaned.parse().unwrap_or(0.0)
}

fn gcs_upload_ctx() -> Result<(&'static GcsSignedUrlRuntime, &'static GcsOAuthTokenCache), String> {
    static RUNTIME: OnceLock<Result<GcsSignedUrlRuntime, String>> = OnceLock::new();
    static OAUTH: OnceLock<GcsOAuthTokenCache> = OnceLock::new();
    let runtime = match RUNTIME.get_or_init(gcs::load_gcs_signed_url_runtime) {
        Ok(r) => r,
        Err(e) => return Err(e.clone()),
    };
    let oauth = OAUTH.get_or_init(GcsOAuthTokenCache::new);
    Ok((runtime, oauth))
}

/// Download icon dari Stockbit assets lalu upload ke GCS (`stoksaham/icon/{CODE}.ext`).
/// Path object GCS disimpan ke DB (bukan path lokal).
async fn upload_emiten_icon_to_gcs(
    emiten: &str,
    url: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if url.trim().is_empty() {
        return Ok(String::new());
    }
    let (runtime, oauth) = gcs_upload_ctx()?;
    download_and_upload_emiten_icon(emiten, url, runtime, oauth).await
}

#[derive(Debug, DeserializeRow)]
struct EmitenListIconRow {
    #[scylla(default_when_null)]
    emiten_icon: String,
}

#[derive(Debug, DeserializeRow)]
struct EmitenListLongNameRow {
    #[scylla(default_when_null)]
    long_name: String,
}

/// Ambil `emiten_list.emiten_icon` bila sudah terisi (hindari download/upload GCS ulang).
async fn fetch_emiten_icon_from_list(
    session: &Session,
    stmt: &scylla::statement::prepared::PreparedStatement,
    emiten: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let result = session
        .execute_unpaged(stmt, (emiten,))
        .await?
        .into_rows_result()?;
    Ok(result
        .maybe_first_row::<EmitenListIconRow>()?
        .map(|r| r.emiten_icon)
        .unwrap_or_default()
        .trim()
        .to_string())
}

async fn fetch_emiten_long_name_from_list(
    session: &Session,
    stmt: &scylla::statement::prepared::PreparedStatement,
    emiten: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let result = session
        .execute_unpaged(stmt, (emiten,))
        .await?
        .into_rows_result()?;
    Ok(result
        .maybe_first_row::<EmitenListLongNameRow>()?
        .map(|r| r.long_name)
        .unwrap_or_default()
        .trim()
        .to_string())
}

/// Prefer Redis → `emiten_list.long_name` → nama dari API movers (lalu cache Redis).
async fn resolve_trending_long_name(
    session: &Session,
    list_long_name_stmt: &scylla::statement::prepared::PreparedStatement,
    emiten: &str,
    api_long_name: &str,
) -> String {
    if let Some(cached) = crate::redis_long_name::get_long_name(emiten).await {
        return cached;
    }
    match fetch_emiten_long_name_from_list(session, list_long_name_stmt, emiten).await {
        Ok(name) if !name.is_empty() && name.to_ascii_uppercase() != emiten => {
            return name;
        }
        Ok(_) => {}
        Err(e) => {
            eprintln!("Peringatan: baca emiten_list.long_name {emiten}: {e}");
        }
    }
    let from_api = api_long_name.trim().to_string();
    if !from_api.is_empty() {
        crate::redis_long_name::set_long_name(emiten, &from_api).await;
    }
    from_api
}

/// Prefer icon dari `emiten_list`; fallback download
/// `https://assets.stockbit.com/logos/companies/{CODE}.png` → upload GCS.
async fn resolve_trending_emiten_icon(
    session: &Session,
    list_icon_stmt: &scylla::statement::prepared::PreparedStatement,
    emiten: &str,
    _movers_icon_url: &str,
) -> String {
    match fetch_emiten_icon_from_list(session, list_icon_stmt, emiten).await {
        Ok(path) if !path.is_empty() => return path,
        Ok(_) => {}
        Err(e) => {
            eprintln!("Peringatan: baca emiten_list.emiten_icon {emiten}: {e}");
        }
    }
    let url = format!("https://assets.stockbit.com/logos/companies/{emiten}.png");
    match upload_emiten_icon_to_gcs(emiten, &url).await {
        Ok(path) => path,
        Err(e) => {
            eprintln!("Peringatan: gagal upload icon GCS {emiten}: {e}");
            String::new()
        }
    }
}

async fn fetch_market_mover(
    http: &reqwest::Client,
    bearer: &str,
    mover_type: &str,
) -> Result<Vec<MoversRow>, Box<dyn std::error::Error>> {
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
    crate::http_abort::abort_app_if_http_4xx(
        status,
        &format!("market-mover {mover_type}"),
    );
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        let preview: String = body.chars().take(280).collect();
        return Err(format!("market-mover {mover_type} HTTP {status}: {preview}").into());
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
    Ok(rows)
}

async fn emiten_trending_exists_today(
    session: &Session,
    exists_stmt: &scylla::statement::prepared::PreparedStatement,
    agg: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let result = session
        .execute_unpaged(exists_stmt, (agg,))
        .await?
        .into_rows_result()?;
    Ok(result.rows_num() > 0)
}

/// Upsert count hanya dipanggil saat baris `emiten_trending` hari ini baru (insert murni).
async fn upsert_emiten_trending_count(
    session: &Session,
    select_count: &scylla::statement::prepared::PreparedStatement,
    upsert_count: &scylla::statement::prepared::PreparedStatement,
    emiten_name: &str,
    today: NaiveDate,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = session
        .execute_unpaged(select_count, (emiten_name,))
        .await?
        .into_rows_result()?;
    let prev = result
        .maybe_first_row::<TrendingCountRow>()?
        .map(|r| r.appearance_count)
        .unwrap_or(0);
    let next = prev.saturating_add(1);
    let now = Utc::now();
    session
        .execute_unpaged(upsert_count, (emiten_name, next, today, now))
        .await?;
    Ok(())
}

async fn insert_emiten_trending(
    session: &Session,
    keyspace: &str,
    rows: &[MoversRow],
    gainer_or_loser: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let today = Local::now().date_naive();
    let date_str = today.format("%Y-%m-%d").to_string();

    let exists_stmt = session
        .prepare(format!(
            "SELECT agg_tahun_bulan_tanggal_emiten_name FROM {keyspace}.emiten_trending \
             WHERE agg_tahun_bulan_tanggal_emiten_name = ?"
        ))
        .await?;

    let insert = session
        .prepare(format!(
            "INSERT INTO {keyspace}.emiten_trending (\
                agg_tahun_bulan_tanggal_emiten_name, \
                tahun_bulan_tanggal, \
                gainer_or_loser, \
                emiten_name, \
                long_name, \
                emiten_icon, \
                price, \
                price_change, \
                value, \
                volume, \
                freq, \
                updated_at\
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, toTimestamp(now()))"
        ))
        .await?;

    let select_count = session
        .prepare(format!(
            "SELECT appearance_count FROM {keyspace}.emiten_trending_count_by_name \
             WHERE emiten_name = ?"
        ))
        .await?;
    let upsert_count = session
        .prepare(format!(
            "INSERT INTO {keyspace}.emiten_trending_count_by_name (\
                emiten_name, appearance_count, last_tahun_bulan_tanggal, updated_at\
            ) VALUES (?, ?, ?, ?)"
        ))
        .await?;

    let list_icon_stmt = session
        .prepare(format!(
            "SELECT emiten_icon FROM {keyspace}.emiten_list WHERE code_name = ?"
        ))
        .await?;
    let list_long_name_stmt = session
        .prepare(format!(
            "SELECT long_name FROM {keyspace}.emiten_list WHERE code_name = ?"
        ))
        .await?;

    let mut n = 0usize;
    let mut new_count_bumps = 0usize;
    for row in rows {
        let emiten = normalize_emiten_name(&row.symbol);
        if emiten.is_empty() {
            continue;
        }
        let agg = format!("{date_str}_{emiten}");
        // Hanya baris hari ini yang belum ada yang dihitung sebagai insert murni.
        let is_new_today = !emiten_trending_exists_today(session, &exists_stmt, &agg).await?;

        let price_change = parse_price_change(&row.price_change);
        let price = parse_price(&row.price);
        let emiten_icon = resolve_trending_emiten_icon(
            session,
            &list_icon_stmt,
            &emiten,
            &row.emiten_icon,
        )
        .await;
        let long_name = resolve_trending_long_name(
            session,
            &list_long_name_stmt,
            &emiten,
            &row.long_name,
        )
        .await;
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
                    price,
                    price_change,
                    row.value.as_str(),
                    row.volume.as_str(),
                    row.freq.as_str(),
                ),
            )
            .await?;
        println!(
            "\nemiten_trending insert {emiten} ({long_name}) ({gainer_or_loser}): \
             price={price} change={price_change} value={} volume={} freq={}",
            row.value, row.volume, row.freq
        );
        n += 1;

        if is_new_today {
            if let Err(e) = upsert_emiten_trending_count(
                session,
                &select_count,
                &upsert_count,
                &emiten,
                today,
            )
            .await
            {
                eprintln!(
                    "Peringatan: gagal upsert emiten_trending_count_by_name {emiten}: {e}"
                );
            } else {
                new_count_bumps += 1;
            }
        }
    }
    if new_count_bumps > 0 {
        println!(
            "emiten_trending_count_by_name: +{new_count_bumps} emiten baru ({gainer_or_loser}) untuk {date_str}"
        );
    }
    Ok(n)
}

/// Ambil Top Gainer + Top Loser via API market-mover → insert `emiten_trending`.
/// Bearer dari sesi browser (login Stockbit).
/// Returns `(inserted_gainer, inserted_loser, mover_codes)` — `mover_codes` unik uppercase.
pub async fn scrape_and_insert_movers(
    page: &Page,
    session: &Session,
    keyspace: &str,
) -> Result<(usize, usize, Vec<String>), Box<dyn std::error::Error>> {
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
    let gainer_rows = fetch_market_mover(&http, &bearer, "MOVER_TYPE_TOP_GAINER").await?;
    println!("Top Gainer: {} baris dari API.", gainer_rows.len());
    if gainer_rows.is_empty() {
        return Err("Top Gainer kosong dari API market-mover".into());
    }

    let inserted_gainer =
        insert_emiten_trending(session, keyspace, &gainer_rows, "gainer").await?;
    println!("OK: {inserted_gainer} baris diinsert ke emiten_trending (gainer).");

    println!("Market mover: TOP_LOSER...");
    let loser_rows = fetch_market_mover(&http, &bearer, "MOVER_TYPE_TOP_LOSER").await?;
    println!("Top Loser: {} baris dari API.", loser_rows.len());
    if loser_rows.is_empty() {
        return Err("Top Loser kosong dari API market-mover".into());
    }

    let inserted_loser = insert_emiten_trending(session, keyspace, &loser_rows, "loser").await?;
    println!("OK: {inserted_loser} baris diinsert ke emiten_trending (loser).");

    let mut mover_codes: Vec<String> = gainer_rows
        .iter()
        .chain(loser_rows.iter())
        .map(|r| normalize_emiten_name(&r.symbol))
        .filter(|c| !c.is_empty())
        .collect();
    mover_codes.sort();
    mover_codes.dedup();

    Ok((inserted_gainer, inserted_loser, mover_codes))
}

#[cfg(test)]
mod tests {
    use super::{parse_price, parse_price_change};

    #[test]
    fn parse_price_from_movers_table() {
        assert_eq!(parse_price("108"), 108.0);
        assert_eq!(parse_price("1,235"), 1235.0);
        assert_eq!(parse_price("24,775"), 24775.0);
    }

    #[test]
    fn parse_price_change_from_movers_table() {
        // Contoh UI Top Gainer: <span class="green">(+26.85%)</span>
        assert_eq!(parse_price_change("(+26.85%)"), 26.85);
        assert_eq!(parse_price_change("(+27.06%)"), 27.06);
        assert_eq!(parse_price_change("+27.06%"), 27.06);
        // Top Loser / nilai turun → negatif
        assert_eq!(parse_price_change("(-1.08%)"), -1.08);
        assert_eq!(parse_price_change("-1.08%"), -1.08);
        assert_eq!(parse_price_change("(-12.50%)"), -12.50);
        // Format dari API market-mover (`change.percentage`)
        assert_eq!(parse_price_change("27.49%"), 27.49);
        assert_eq!(parse_price_change("-14.35%"), -14.35);
    }
}
