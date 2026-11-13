//! Parse Stockbit fundachart PBV band JSON → protobuf response + upsert Scylla `pbv_band`.

use std::sync::Arc;

use chrono::{DateTime, Local, Utc};
use scylla::client::session::Session;
use serde::Deserialize;

use crate::model::{FundachartChartEntryDb, FundachartSeriesValueDb, PbvBandDataDb};
use crate::pb::{
    FundachartChartEntry, FundachartData, FundachartSeriesValue, GetStockPbvBandResponse,
};

#[derive(Debug, Deserialize)]
struct ApiFundachartRoot {
    #[serde(default)]
    message: String,
    data: Option<ApiFundachartData>,
}

#[derive(Debug, Deserialize)]
struct ApiFundachartData {
    #[serde(default)]
    interval: i32,
    #[serde(default)]
    chart_entry: Vec<ApiChartEntry>,
}

#[derive(Debug, Deserialize)]
struct ApiChartEntry {
    #[serde(default)]
    title: String,
    value: Option<ApiSeriesValue>,
    #[serde(default)]
    color: String,
    #[serde(default)]
    date: i64,
    #[serde(default)]
    item_id: i32,
    #[serde(default)]
    timeframe: String,
    #[serde(default)]
    suffix: String,
}

#[derive(Debug, Deserialize)]
struct ApiSeriesValue {
    #[serde(default)]
    yaxis_id: Vec<f64>,
    #[serde(default)]
    xaxis_id: Vec<i64>,
}

pub fn parse_fundachart_response(code: &str, raw_json: &str) -> Result<GetStockPbvBandResponse, String> {
    let parsed: ApiFundachartRoot = serde_json::from_str(raw_json)
        .map_err(|e| format!("fundachart JSON {code}: {e}"))?;

    let Some(data) = parsed.data else {
        return Ok(GetStockPbvBandResponse {
            code: code.to_string(),
            success: false,
            message: format!(
                "fundachart {code}: field data kosong — {}",
                parsed.message
            ),
            api_message: parsed.message,
            ..Default::default()
        });
    };

    let chart_entry = data
        .chart_entry
        .into_iter()
        .map(|e| FundachartChartEntry {
            title: e.title,
            value: e.value.map(|v| FundachartSeriesValue {
                yaxis_id: v.yaxis_id,
                xaxis_id: v.xaxis_id,
            }),
            color: e.color,
            date: e.date,
            item_id: e.item_id,
            timeframe: e.timeframe,
            suffix: e.suffix,
        })
        .collect();

    Ok(GetStockPbvBandResponse {
        code: code.to_string(),
        success: true,
        message: format!("fundachart PBV band {code} OK"),
        api_message: parsed.message,
        data: Some(FundachartData {
            interval: data.interval,
            chart_entry,
        }),
    })
}

pub async fn fetch_stock_pbv_band(code: &str) -> Result<GetStockPbvBandResponse, String> {
    let code = code.trim().to_ascii_uppercase();
    stockbit_browser::require_stockbit_ready()
        .await
        .map_err(|e| format!("Stockbit readiness: {e}"))?;
    let raw = stockbit_browser::fetch_fundachart_pbv_json(&code)
        .await
        .map_err(|e| format!("GET fundachart {code}: {e}"))?;
    parse_fundachart_response(&code, &raw)
}

pub fn pbv_band_data_from_proto(data: &FundachartData, api_message: &str) -> PbvBandDataDb {
    let chart_entry = data
        .chart_entry
        .iter()
        .map(|e| FundachartChartEntryDb {
            title: e.title.clone(),
            value: e.value.as_ref().map(|v| FundachartSeriesValueDb {
                yaxis_id: Some(v.yaxis_id.clone()),
                xaxis_id: Some(v.xaxis_id.clone()),
            }),
            color: e.color.clone(),
            date: e.date,
            item_id: e.item_id,
            timeframe: e.timeframe.clone(),
            suffix: e.suffix.clone(),
        })
        .collect();
    PbvBandDataDb {
        interval: data.interval,
        api_message: Some(api_message.to_string()),
        chart_entry: Some(chart_entry),
    }
}

pub fn fundachart_data_from_db(db: &PbvBandDataDb) -> FundachartData {
    FundachartData {
        interval: db.interval,
        chart_entry: db
            .chart_entry
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|e| FundachartChartEntry {
                title: e.title,
                value: e.value.map(|v| FundachartSeriesValue {
                    yaxis_id: v.yaxis_id.unwrap_or_default(),
                    xaxis_id: v.xaxis_id.unwrap_or_default(),
                }),
                color: e.color,
                date: e.date,
                item_id: e.item_id,
                timeframe: e.timeframe,
                suffix: e.suffix,
            })
            .collect(),
    }
}

pub fn pbv_band_scraped_today(updated_at: Option<DateTime<Utc>>) -> bool {
    let Some(updated_at) = updated_at else {
        return false;
    };
    Local::now().date_naive() == updated_at.with_timezone(&Local).date_naive()
}

pub fn response_from_pbv_band_db(code: &str, db: &PbvBandDataDb) -> GetStockPbvBandResponse {
    GetStockPbvBandResponse {
        code: code.to_string(),
        success: true,
        message: format!("fundachart PBV band {code} dari Scylla (hari ini)"),
        api_message: db.api_message.clone().unwrap_or_default(),
        data: Some(fundachart_data_from_db(db)),
    }
}

pub async fn fetch_and_save_stock_pbv_band(
    session: Arc<Session>,
    code: &str,
) -> Result<GetStockPbvBandResponse, String> {
    let resp = fetch_stock_pbv_band(code).await?;
    if resp.success {
        if let Some(ref data) = resp.data {
            let db = pbv_band_data_from_proto(data, &resp.api_message);
            crate::repository::update_pbv_band(
                session.as_ref(),
                &resp.code,
                db,
                Utc::now(),
            )
            .await?;
        }
    }
    Ok(resp)
}

/// Cache harian: bila `pbv_band_updated_at` masih hari kalender lokal yang sama, baca Scylla saja.
pub async fn get_or_fetch_and_save_stock_pbv_band(
    session: Arc<Session>,
    code: &str,
) -> Result<GetStockPbvBandResponse, String> {
    let code = code.trim().to_ascii_uppercase();
    if let Some(row) = crate::repository::get_pbv_band_by_code(session.as_ref(), &code).await? {
        if pbv_band_scraped_today(row.pbv_band_updated_at) {
            if let Some(ref db) = row.pbv_band {
                return Ok(response_from_pbv_band_db(&row.code, db));
            }
        }
    }
    fetch_and_save_stock_pbv_band(session, &code).await
}
