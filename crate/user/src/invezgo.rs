use serde::Deserialize;

use crate::pb::UsageResponse;

const INVEZGO_USAGE_URL: &str = "https://api.invezgo.com/usage/api";

#[derive(Debug, Deserialize)]
struct ApiUsageResponse {
    usage: i64,
    remaining: i64,
    limit: i64,
    #[serde(rename = "isBlocked")]
    is_blocked: bool,
    expire: String,
}

pub async fn fetch_usage() -> Result<UsageResponse, String> {
    let token = std::env::var("INVEZGO_BEARER_TOKEN")
        .map_err(|_| "INVEZGO_BEARER_TOKEN belum diset".to_string())?;

    let response = reqwest::Client::new()
        .get(INVEZGO_USAGE_URL)
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| format!("request Invezgo usage/api: {e}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| format!("baca body Invezgo usage/api: {e}"))?;

    if !status.is_success() {
        eprintln!("CekUsage Invezgo API gagal: HTTP {status} body={body}");
        return Err(format!("Invezgo HTTP {status} usage/api: {body}"));
    }

    let parsed: ApiUsageResponse = serde_json::from_str(&body)
        .map_err(|e| format!("parse JSON Invezgo usage/api: {e}; body={body}"))?;

    Ok(UsageResponse {
        usage: parsed.usage,
        remaining: parsed.remaining,
        limit: parsed.limit,
        is_blocked: parsed.is_blocked,
        expire: parsed.expire,
    })
}
