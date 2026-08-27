//! GET Invezgo `analysis/top/change` → upsert `invezgood.emiten_trending`.
//! Dipakai poller `IsStockbitReady` dan RPC `GetLatestEmitenTrendingFromInvezgo`.

use std::sync::Arc;

use chrono::{Local, NaiveDate, Utc};
use scylla::client::session::Session;
use serde::Deserialize;

const INVEZGO_TOP_CHANGE_URL: &str = "https://api.invezgo.com/analysis/top/change";

const UPSERT: &str = "INSERT INTO invezgood.emiten_trending \
    (agg_tahun_bulan_tanggal_emiten_name, tahun_bulan_tanggal, gainer_or_loser, emiten_name, \
    long_name, emiten_icon, sector, price, price_change, value, volume, freq, updated_at) \
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

const SECTOR_BY_CODE: &str = "SELECT sector FROM invezgood.stock_list WHERE code = ?";

#[derive(Debug, Deserialize)]
struct ApiGraphPoint {
    #[allow(dead_code)]
    date: String,
    #[allow(dead_code)]
    value: f64,
}

#[derive(Debug, Deserialize)]
struct ApiTopItem {
    code: String,
    name: String,
    price: f64,
    change: f64,
    value: String,
    volume: String,
    #[allow(dead_code)]
    logo: String,
    #[allow(dead_code)]
    calculated_value: f64,
    #[allow(dead_code)]
    graph: Vec<ApiGraphPoint>,
}

#[derive(Debug, Deserialize)]
struct ApiTopChangeResponse {
    gain: Vec<ApiTopItem>,
    #[serde(default)]
    loss: Vec<ApiTopItem>,
}

fn agg_key(trade_date: NaiveDate, code: &str) -> String {
    format!("{}_{code}", trade_date.format("%Y-%m-%d"))
}

fn normalize_code(raw: &str) -> String {
    raw.trim().to_ascii_uppercase()
}

fn emiten_icon_url(code: &str) -> String {
    format!("https://assets.stockbit.com/logos/companies/{code}.png")
}

async fn lookup_sector_i8(session: &Session, code: &str) -> Result<Option<i8>, String> {
    let rows = session
        .query_unpaged(SECTOR_BY_CODE, (code,))
        .await
        .map_err(|e| format!("sector lookup stock_list code={code}: {e}"))?
        .into_rows_result()
        .map_err(|e| format!("sector rows stock_list code={code}: {e}"))?;

    let Some((raw,)) = rows
        .maybe_first_row::<(Option<String>,)>()
        .map_err(|e| format!("sector row stock_list code={code}: {e}"))?
    else {
        return Ok(None);
    };
    let Some(raw) = raw else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    Ok(trimmed.parse::<i8>().ok())
}

async fn upsert_row(
    session: &Session,
    trade_date: NaiveDate,
    gainer_or_loser: &str,
    item: ApiTopItem,
) -> Result<(), String> {
    let code = normalize_code(&item.code);
    if code.is_empty() {
        return Err("code kosong dari Invezgo top/change".into());
    }
    let sector = lookup_sector_i8(session, &code).await?;

    session
        .query_unpaged(
            UPSERT,
            (
                agg_key(trade_date, &code).as_str(),
                trade_date,
                gainer_or_loser,
                code.as_str(),
                item.name.trim(),
                emiten_icon_url(&code).as_str(),
                sector,
                item.price,
                item.change,
                item.value.as_str(),
                item.volume.as_str(),
                "",
                Some(Utc::now()),
            ),
        )
        .await
        .map_err(|e| format!("upsert emiten_trending code={code}: {e}"))?;

    Ok(())
}

/// GET Invezgo top/change hari ini → upsert semua gainer/loser ke Scylla. Return jumlah baris tersimpan.
pub async fn fetch_and_save(session: Arc<Session>) -> Result<usize, String> {
    let trade_date = Local::now().date_naive();
    let date_param = trade_date.format("%Y-%m-%d").to_string();
    let url = format!("{INVEZGO_TOP_CHANGE_URL}?date={date_param}&filter_column=change");

    let body = invezgo_http::get(&url).await?;
    let parsed: ApiTopChangeResponse = serde_json::from_str(&body)
        .map_err(|e| format!("parse JSON Invezgo top/change: {e}"))?;

    let gain_n = parsed.gain.len();
    let loss_n = parsed.loss.len();
    let mut saved = 0usize;

    for item in parsed.gain {
        upsert_row(&session, trade_date, "gainer", item).await?;
        saved += 1;
    }

    for item in parsed.loss {
        upsert_row(&session, trade_date, "loser", item).await?;
        saved += 1;
    }

    eprintln!(
        "emiten_trending Invezgo {date_param}: {saved} baris di-upsert (gain={gain_n} loss={loss_n})"
    );

    Ok(saved)
}
