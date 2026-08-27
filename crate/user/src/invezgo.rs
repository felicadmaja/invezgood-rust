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
    let body = invezgo_http::get(INVEZGO_USAGE_URL).await?;
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
