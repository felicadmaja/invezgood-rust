//! Cache Redis untuk `long_name` emiten (hasil API).
//!
//! Env: `REDIS_URL` (default `redis://localhost:6379`).
//! Key: `stockbit:emiten:long_name:{CODE}` — TTL 1 tahun.
//! Bila Redis down: treat sebagai cache miss (lanjut API), jangan gagalkan scrape.

use std::sync::OnceLock;
use std::time::Duration;

use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use tokio::sync::Mutex;

/// TTL cache long_name: 1 tahun.
pub const LONG_NAME_TTL: Duration = Duration::from_secs(365 * 24 * 60 * 60);

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string())
}

fn long_name_key(code: &str) -> String {
    format!(
        "stockbit:emiten:long_name:{}",
        code.trim().to_ascii_uppercase()
    )
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

/// Baca `long_name` dari Redis. `None` = miss / Redis error.
pub async fn get_long_name(code: &str) -> Option<String> {
    let code = code.trim();
    if code.is_empty() {
        return None;
    }
    let mut conn = match connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Redis long_name get: koneksi gagal ({e}) — cache miss");
            return None;
        }
    };
    let key = long_name_key(code);
    match conn.get::<_, Option<String>>(&key).await {
        Ok(Some(v)) => {
            let v = v.trim().to_string();
            if v.is_empty() {
                None
            } else {
                Some(v)
            }
        }
        Ok(None) => None,
        Err(e) => {
            eprintln!("Redis long_name get {code}: {e} — cache miss");
            None
        }
    }
}

/// Simpan `long_name` ke Redis dengan TTL 1 tahun. Error hanya di-log.
pub async fn set_long_name(code: &str, long_name: &str) {
    let code = code.trim();
    let name = long_name.trim();
    if code.is_empty() || name.is_empty() {
        return;
    }
    let mut conn = match connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Redis long_name set: koneksi gagal ({e})");
            return;
        }
    };
    let key = long_name_key(code);
    let secs = LONG_NAME_TTL.as_secs();
    if let Err(e) = conn.set_ex::<_, _, ()>(&key, name, secs).await {
        eprintln!("Redis long_name set {code}: {e}");
    }
}
