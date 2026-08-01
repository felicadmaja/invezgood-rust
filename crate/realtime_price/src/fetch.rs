//! GET `https://exodus.stockbit.com/emitten/{CODE}/info`.

use serde::Deserialize;
use serde_json::Value;

use crate::{CorpActionInfo, GetRealtimePriceFromStockbitResponse};

const USER_AGENT: &str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

#[derive(Debug, Deserialize)]
struct InfoResponse {
    data: InfoData,
}

#[derive(Debug, Deserialize)]
struct InfoData {
    symbol: Option<String>,
    price: Option<Value>,
    formatted_price: Option<Value>,
    date: Option<String>,
    time: Option<String>,
    volume: Option<Value>,
    corp_action: Option<CorpActionJson>,
}

#[derive(Debug, Deserialize)]
struct CorpActionJson {
    active: Option<bool>,
    icon: Option<String>,
    text: Option<String>,
    detail: Option<Value>,
}

fn parse_i64(v: &Option<Value>) -> i64 {
    match v {
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0),
        Some(Value::String(s)) => {
            let cleaned: String = s.chars().filter(|c| c.is_ascii_digit() || *c == '-').collect();
            cleaned.parse().unwrap_or(0)
        }
        _ => 0,
    }
}

fn detail_to_string(v: &Option<Value>) -> String {
    match v {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
    }
}

/// GET emitten info → proto response.
pub async fn fetch_realtime_price(
    emiten: &str,
    bearer: &str,
) -> Result<GetRealtimePriceFromStockbitResponse, String> {
    let code = emiten.trim().to_ascii_uppercase();
    let url = format!("https://exodus.stockbit.com/emitten/{code}/info");

    let http = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| e.to_string())?;

    let resp = http
        .get(&url)
        .header("Authorization", format!("Bearer {bearer}"))
        .header("Accept", "application/json")
        .header("Origin", "https://stockbit.com")
        .header("Referer", "https://stockbit.com/")
        .send()
        .await
        .map_err(|e| format!("HTTP emitten/{code}/info: {e}"))?;

    let status = resp.status();
    let body = resp
        .text()
        .await
        .map_err(|e| format!("baca body emitten/{code}/info: {e}"))?;
    if !status.is_success() {
        let snippet: String = body.chars().take(200).collect();
        return Err(format!(
            "emitten/{code}/info HTTP {status}: {snippet}"
        ));
    }

    let parsed: InfoResponse = serde_json::from_str(&body)
        .map_err(|e| format!("parse JSON emitten/{code}/info: {e}"))?;
    let d = parsed.data;
    let corp = d.corp_action.unwrap_or(CorpActionJson {
        active: Some(false),
        icon: None,
        text: None,
        detail: None,
    });

    Ok(GetRealtimePriceFromStockbitResponse {
        symbol: d
            .symbol
            .unwrap_or_else(|| code.clone())
            .trim()
            .to_ascii_uppercase(),
        price: parse_i64(&d.price),
        formatted_price: parse_i64(&d.formatted_price),
        time: d.time.unwrap_or_default(),
        volume: parse_i64(&d.volume),
        corp_action: Some(CorpActionInfo {
            active: corp.active.unwrap_or(false),
            icon: corp.icon.unwrap_or_default(),
            text: corp.text.unwrap_or_default(),
            detail: detail_to_string(&corp.detail),
        }),
        date: d.date.unwrap_or_default(),
    })
}
