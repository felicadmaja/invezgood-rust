use chrono::NaiveDate;
use serde::Deserialize;

use crate::model::{agg_code_tahun_bulan_tanggal, HakaHakiRow};
use crate::pb::HakaHakiPoint;

const INVEZGO_MOMENTUM_CHART_URL: &str = "https://api.invezgo.com/analysis/momentum-chart";

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ApiHakaHakiPoint {
    time: String,
    #[serde(default, deserialize_with = "deserialize_i64_field")]
    value: i64,
    #[serde(default, deserialize_with = "deserialize_i64_field")]
    buy: i64,
    #[serde(default, deserialize_with = "deserialize_i64_field")]
    sell: i64,
}

fn deserialize_i64_field<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::Number(n) => n
            .as_i64()
            .ok_or_else(|| serde::de::Error::custom("invalid number for i64")),
        serde_json::Value::String(s) => s
            .trim()
            .parse::<i64>()
            .map_err(|_| serde::de::Error::custom(format!("invalid i64 string: {s}"))),
        serde_json::Value::Null => Ok(0),
        _ => Err(serde::de::Error::custom("expected number or string")),
    }
}

fn i64_to_scylla_int(value: i64, field: &str) -> Result<i32, String> {
    i32::try_from(value).map_err(|_| format!("{field}={value} melebihi batas int Scylla"))
}

pub fn api_point_to_proto(point: &ApiHakaHakiPoint) -> HakaHakiPoint {
    HakaHakiPoint {
        time: point.time.clone(),
        value: point.value,
        buy: point.buy,
        sell: point.sell,
    }
}

pub fn api_point_to_row(
    code: &str,
    trade_date: NaiveDate,
    point: &ApiHakaHakiPoint,
) -> Result<HakaHakiRow, String> {
    Ok(HakaHakiRow {
        code: code.to_string(),
        tahun_bulan_tanggal: trade_date,
        jam_menit: point.time.clone(),
        agg_code_tahun_bulan_tanggal: agg_code_tahun_bulan_tanggal(code, trade_date),
        volume: i64_to_scylla_int(point.value, "value")?,
        buy: i64_to_scylla_int(point.buy, "buy")?,
        sell: i64_to_scylla_int(point.sell, "sell")?,
    })
}

pub async fn fetch_momentum_chart(
    code: &str,
    trade_date: NaiveDate,
    range: i32,
) -> Result<Vec<ApiHakaHakiPoint>, String> {
    let token = std::env::var("INVEZGO_BEARER_TOKEN")
        .map_err(|_| "INVEZGO_BEARER_TOKEN belum diset".to_string())?;

    let date = trade_date.format("%Y-%m-%d");
    let url = format!(
        "{INVEZGO_MOMENTUM_CHART_URL}/{code}?date={date}&range={range}&scope=volume"
    );

    let response = reqwest::Client::new()
        .get(&url)
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| format!("request Invezgo momentum-chart gagal: {e}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| format!("baca body Invezgo momentum-chart gagal: {e}"))?;

    if !status.is_success() {
        return Err(format!("Invezgo HTTP {status} momentum-chart: {body}"));
    }

    serde_json::from_str(&body).map_err(|e| format!("parse JSON Invezgo momentum-chart gagal: {e}"))
}
