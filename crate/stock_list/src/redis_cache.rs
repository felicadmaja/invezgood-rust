use chrono::{DateTime, Utc};
use redis::AsyncCommands;

pub const UPDATED_AT_KEY: &str = "StockListUpdatedAt";
const MAX_AGE_SECS: i64 = 30 * 24 * 60 * 60;

pub fn client_from_env() -> Result<redis::Client, String> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    redis::Client::open(url).map_err(|e| format!("redis client: {e}"))
}

pub async fn should_refresh_from_api(
    conn: &mut redis::aio::MultiplexedConnection,
) -> Result<bool, String> {
    let raw: Option<String> = conn
        .get(UPDATED_AT_KEY)
        .await
        .map_err(|e| format!("redis GET {UPDATED_AT_KEY}: {e}"))?;

    let Some(raw) = raw else {
        return Ok(true);
    };

    let updated_at = parse_timestamp(&raw)?;
    let age = Utc::now().timestamp() - updated_at.timestamp();
    Ok(age > MAX_AGE_SECS)
}

pub async fn set_updated_at(conn: &mut redis::aio::MultiplexedConnection) -> Result<(), String> {
    let now = Utc::now().timestamp().to_string();
    conn.set(UPDATED_AT_KEY, now)
        .await
        .map_err(|e| format!("redis SET {UPDATED_AT_KEY}: {e}"))
}

fn parse_timestamp(raw: &str) -> Result<DateTime<Utc>, String> {
    if let Ok(secs) = raw.parse::<i64>() {
        return DateTime::from_timestamp(secs, 0)
            .ok_or_else(|| format!("timestamp unix invalid: {raw}"));
    }

    raw.parse::<DateTime<Utc>>()
        .map_err(|_| format!("timestamp invalid: {raw}"))
}
