//! Koneksi Redis shared untuk cache spike (Invezgo + Yahoo).
//! Retry sekali + invalidate koneksi stale saat broken pipe / reset.

use std::sync::OnceLock;

use redis::aio::ConnectionManager;
use tokio::sync::Mutex;

const REDIS_RETRY_MAX: usize = 2;

static REDIS: OnceLock<Mutex<Option<ConnectionManager>>> = OnceLock::new();

fn redis_slot() -> &'static Mutex<Option<ConnectionManager>> {
    REDIS.get_or_init(|| Mutex::new(None))
}

pub fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string())
}

fn is_reconnect_error(err: &redis::RedisError) -> bool {
    let msg = err.to_string().to_ascii_lowercase();
    msg.contains("broken pipe")
        || msg.contains("connection reset")
        || msg.contains("connection refused")
        || msg.contains("connection closed")
        || msg.contains("connection lost")
        || msg.contains("not connected")
        || msg.contains("eof")
}

async fn connection(reconnect: bool) -> Result<ConnectionManager, String> {
    let mut guard = redis_slot().lock().await;
    if reconnect {
        *guard = None;
    }
    if let Some(conn) = guard.as_ref() {
        return Ok(conn.clone());
    }
    let client = redis::Client::open(redis_url()).map_err(|e| e.to_string())?;
    let mgr = ConnectionManager::new(client)
        .await
        .map_err(|e| e.to_string())?;
    *guard = Some(mgr.clone());
    Ok(mgr)
}

/// Jalankan operasi Redis; saat koneksi stale, invalidate + retry sekali.
pub async fn with_retry<T, F, Fut>(log_prefix: &str, mut op: F) -> Result<T, ()>
where
    F: FnMut(ConnectionManager) -> Fut,
    Fut: std::future::Future<Output = Result<T, redis::RedisError>>,
{
    for attempt in 0..REDIS_RETRY_MAX {
        let reconnect = attempt > 0;
        let conn = match connection(reconnect).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("{log_prefix}: koneksi gagal ({e})");
                return Err(());
            }
        };
        match op(conn).await {
            Ok(v) => return Ok(v),
            Err(e) if attempt + 1 < REDIS_RETRY_MAX && is_reconnect_error(&e) => {
                eprintln!("{log_prefix}: {e} — reconnect & retry");
            }
            Err(e) => {
                eprintln!("{log_prefix}: {e}");
                return Err(());
            }
        }
    }
    Err(())
}
