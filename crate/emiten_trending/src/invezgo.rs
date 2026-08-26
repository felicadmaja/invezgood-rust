//! Fetch top gainer/loser dari Invezgo `GET /analysis/top/change` → upsert `invezgood.emiten_trending` saja.
//! Tidak mengembalikan baris API ke caller RPC; handler baca ulang dari Scylla MV setelah upsert.

use std::sync::Arc;

use chrono::{Local, NaiveDate, Utc};
use scylla::client::session::Session;
use serde::Deserialize;

use crate::model::{agg_tahun_bulan_tanggal_emiten_name, EmitenTrending};
use crate::repository::EmitenTrendingRepository;

const INVEZGO_TOP_CHANGE_URL: &str = "https://api.invezgo.com/analysis/top/change";

#[derive(Debug, Deserialize)]
struct ApiGraphPoint {
    date: String,
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

fn normalize_code(raw: &str) -> String {
    raw.trim().to_ascii_uppercase()
}

fn emiten_icon_url(code: &str) -> String {
    format!("https://assets.stockbit.com/logos/companies/{code}.png")
}

async fn api_item_to_row(
    repo: &EmitenTrendingRepository,
    trade_date: NaiveDate,
    gainer_or_loser: &str,
    item: ApiTopItem,
) -> Result<EmitenTrending, String> {
    let code = normalize_code(&item.code);
    if code.is_empty() {
        return Err("code kosong dari Invezgo top/change".into());
    }
    let sector = repo.lookup_sector_i8(&code).await?;

    Ok(EmitenTrending {
        agg_tahun_bulan_tanggal_emiten_name: agg_tahun_bulan_tanggal_emiten_name(trade_date, &code),
        tahun_bulan_tanggal: trade_date,
        gainer_or_loser: gainer_or_loser.to_string(),
        emiten_name: code.clone(),
        long_name: item.name.trim().to_string(),
        emiten_icon: emiten_icon_url(&code),
        sector,
        price: item.price,
        price_change: item.change,
        value: item.value,
        volume: item.volume,
        freq: String::new(),
        updated_at: Some(Utc::now()),
    })
}

/// GET Invezgo top/change hari ini → upsert semua gainer/loser ke Scylla. Return jumlah baris tersimpan.
pub async fn fetch_and_save(session: Arc<Session>) -> Result<usize, String> {
    let token = std::env::var("INVEZGO_BEARER_TOKEN")
        .map_err(|_| "INVEZGO_BEARER_TOKEN belum diset".to_string())?;

    let trade_date = Local::now().date_naive();
    let date_param = trade_date.format("%Y-%m-%d").to_string();
    let url = format!("{INVEZGO_TOP_CHANGE_URL}?date={date_param}&filter_column=change");

    eprintln!("emiten_trending Invezgo GET {url}");

    let response = reqwest::Client::new()
        .get(&url)
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| format!("request Invezgo top/change: {e}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| format!("baca body Invezgo top/change: {e}"))?;

    if !status.is_success() {
        let preview: String = body.chars().take(300).collect();
        return Err(format!("Invezgo HTTP {status} top/change: {preview}"));
    }

    let parsed: ApiTopChangeResponse = serde_json::from_str(&body)
        .map_err(|e| format!("parse JSON Invezgo top/change: {e}"))?;

    let gain_n = parsed.gain.len();
    let loss_n = parsed.loss.len();
    let repo = EmitenTrendingRepository::new(session);
    let mut saved = 0usize;

    for item in parsed.gain {
        let row = api_item_to_row(&repo, trade_date, "gainer", item).await?;
        repo.upsert(&row).await?;
        saved += 1;
    }

    for item in parsed.loss {
        let row = api_item_to_row(&repo, trade_date, "loser", item).await?;
        repo.upsert(&row).await?;
        saved += 1;
    }

    eprintln!(
        "emiten_trending Invezgo {date_param}: {saved} baris di-upsert (gain={gain_n} loss={loss_n})"
    );

    Ok(saved)
}
