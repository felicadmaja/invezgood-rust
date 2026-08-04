use std::sync::Arc;

use chrono::Utc;
use scylla::client::session::Session;
use serde::Deserialize;

use crate::model::{BandarmologyEntryDb, BandarmologyRow};

fn summary_stock_url(code: &str, date: &str) -> String {
    format!(
        "https://api.invezgo.com/analysis/summary/stock/{code}?from={date}&to={date}&investor=all&market=RG"
    )
}

#[derive(Debug, Deserialize)]
struct ApiSummaryEntry {
    code: String,
    #[serde(default, deserialize_with = "deserialize_string_field")]
    buy_freq: String,
    #[serde(default, deserialize_with = "deserialize_string_field")]
    buy_volume: String,
    #[serde(default, deserialize_with = "deserialize_string_field")]
    buy_value: String,
    #[serde(default, deserialize_with = "deserialize_string_field")]
    sell_freq: String,
    #[serde(default, deserialize_with = "deserialize_string_field")]
    sell_volume: String,
    #[serde(default, deserialize_with = "deserialize_string_field")]
    sell_value: String,
    #[serde(default, deserialize_with = "deserialize_optional_f64")]
    buy_avg: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_optional_f64")]
    sell_avg: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_string_field")]
    net_value: String,
    #[serde(default, deserialize_with = "deserialize_string_field")]
    net_volume: String,
    #[serde(default, deserialize_with = "deserialize_string_field")]
    net_freq: String,
    #[serde(default)]
    name: String,
}

fn deserialize_string_field<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    Ok(match value {
        serde_json::Value::String(s) => s,
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    })
}

fn deserialize_optional_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::Number(n) => n
            .as_f64()
            .map(Some)
            .ok_or_else(|| serde::de::Error::custom("invalid number for f64")),
        serde_json::Value::String(s) if s.trim().is_empty() => Ok(None),
        serde_json::Value::String(s) => s
            .trim()
            .parse::<f64>()
            .map(Some)
            .map_err(|_| serde::de::Error::custom(format!("invalid f64 string: {s}"))),
        _ => Err(serde::de::Error::custom("expected number, string, or null")),
    }
}

pub async fn fetch_and_save(
    session: Arc<Session>,
    code: &str,
    trade_date: chrono::NaiveDate,
) -> Result<BandarmologyRow, String> {
    let token = std::env::var("INVEZGO_BEARER_TOKEN")
        .map_err(|_| "INVEZGO_BEARER_TOKEN belum diset".to_string())?;

    let date_param = trade_date.format("%Y-%m-%d").to_string();
    let url = summary_stock_url(code, &date_param);

    eprintln!("\x1b[32mbandarmology Invezgo GET {url}\x1b[0m");

    let response = reqwest::Client::new()
        .get(&url)
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| format!("request Invezgo summary/stock code={code} date={date_param}: {e}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| format!("baca body Invezgo summary/stock: {e}"))?;

    if !status.is_success() {
        return Err(format!("Invezgo HTTP {status} summary/stock code={code} date={date_param}: {body}"));
    }

    let parsed: Vec<ApiSummaryEntry> = serde_json::from_str(&body).map_err(|e| {
        format!("parse JSON Invezgo summary/stock code={code} date={date_param}: {e}")
    })?;

    let row = BandarmologyRow {
        code: code.to_string(),
        tahun_bulan_tanggal: trade_date,
        bandarmology: Some(parsed.into_iter().map(api_entry_to_db).collect()),
        updated_at: Some(Utc::now()),
    };

    crate::repository::upsert(session.as_ref(), &row).await?;
    Ok(row)
}

fn api_entry_to_db(item: ApiSummaryEntry) -> BandarmologyEntryDb {
    BandarmologyEntryDb {
        code: item.code,
        buy_freq: item.buy_freq,
        buy_volume: item.buy_volume,
        buy_value: item.buy_value,
        sell_freq: item.sell_freq,
        sell_volume: item.sell_volume,
        sell_value: item.sell_value,
        buy_avg: item.buy_avg,
        sell_avg: item.sell_avg,
        net_value: item.net_value,
        net_volume: item.net_volume,
        net_freq: item.net_freq,
        name: item.name,
    }
}
