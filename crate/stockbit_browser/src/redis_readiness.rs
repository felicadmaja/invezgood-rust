//! State readiness poller di Redis (bukan memory proses).
//!
//! Env: `REDIS_URL` (default `redis://localhost:6379`).
//! Key hash: `stockbit:readiness` — field `ready` (`0`/`1`), `message`.
//! Tanpa TTL agar survive restart. Bila Redis down: get → None; set → log saja.

use std::collections::HashMap;
use std::sync::OnceLock;

use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use tokio::sync::Mutex;

use crate::ReadinessUpdate;

const KEY: &str = "stockbit:readiness";

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string())
}

static REDIS: OnceLock<Mutex<Option<ConnectionManager>>> = OnceLock::new();

fn redis_slot() -> &'static Mutex<Option<ConnectionManager>> {
    REDIS.get_or_init(|| Mutex::new(None))
}

async fn connection() -> Result<ConnectionManager, String> {
    let mut guard = redis_slot().lock().await;
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

/// Baca status readiness dari Redis. `None` = belum ada / Redis error.
pub async fn get() -> Option<ReadinessUpdate> {
    let mut conn = match connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Redis readiness get: koneksi gagal ({e})");
            return None;
        }
    };
    let map: HashMap<String, String> = match conn.hgetall(KEY).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Redis readiness get: {e}");
            return None;
        }
    };
    if map.is_empty() {
        return None;
    }
    let ready_raw = map.get("ready").map(String::as_str).unwrap_or("0");
    let ready = ready_raw == "1" || ready_raw.eq_ignore_ascii_case("true");
    let message = map.get("message").cloned().unwrap_or_default();
    Some(ReadinessUpdate {
        ready,
        message,
        poll_seq: 0,
    })
}

/// Simpan status readiness ke Redis. Error hanya di-log.
pub async fn set(update: &ReadinessUpdate) {
    let mut conn = match connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Redis readiness set: koneksi gagal ({e})");
            return;
        }
    };
    let ready = if update.ready { "1" } else { "0" };
    if let Err(e) = redis::cmd("HSET")
        .arg(KEY)
        .arg("ready")
        .arg(ready)
        .arg("message")
        .arg(update.message.as_str())
        .query_async::<()>(&mut conn)
        .await
    {
        eprintln!("Redis readiness set: {e}");
    }
}
