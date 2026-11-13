//! Parse Stockbit fundachart PBV band JSON → protobuf response.

use serde::Deserialize;

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
