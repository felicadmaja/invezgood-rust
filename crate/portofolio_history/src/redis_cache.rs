//! Cache Redis untuk response `GetPortofolioHistoryByEmitenNameFromStockbit`.
//!
//! Env: `REDIS_URL` (default `redis://localhost:6379`).
//! Key: `stockbit:portofolio_history:stockbit:{EMITEN}` — TTL 5 menit.
//! Payload: prost bytes `GetPortofolioHistoryByEmitenNameFromStockbitResponse`.
//! Bila Redis down: treat sebagai cache miss (lanjut scrape), jangan gagalkan RPC.

use std::sync::OnceLock;
use std::time::Duration;

use prost::Message;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use tokio::sync::Mutex;

use crate::GetPortofolioHistoryByEmitenNameFromStockbitResponse;

/// TTL cache response history Stockbit per emiten.
pub const HISTORY_STOCKBIT_CACHE_TTL: Duration = Duration::from_secs(5 * 60);

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string())
}

fn cache_key(emiten: &str) -> String {
    format!(
        "stockbit:portofolio_history:stockbit:{}",
        emiten.trim().to_ascii_uppercase()
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

/// Baca response cache. `None` = miss / Redis error / decode gagal.
pub async fn get(emiten: &str) -> Option<GetPortofolioHistoryByEmitenNameFromStockbitResponse> {
    let emiten = emiten.trim();
    if emiten.is_empty() {
        return None;
    }
    let mut conn = match connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Redis portofolio_history get: koneksi gagal ({e}) — cache miss");
            return None;
        }
    };
    let key = cache_key(emiten);
    let bytes: Option<Vec<u8>> = match conn.get(&key).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Redis portofolio_history get {emiten}: {e} — cache miss");
            return None;
        }
    };
    let Some(bytes) = bytes else {
        return None;
    };
    match GetPortofolioHistoryByEmitenNameFromStockbitResponse::decode(bytes.as_slice()) {
        Ok(resp) => Some(resp),
        Err(e) => {
            eprintln!("Redis portofolio_history decode {emiten}: {e} — cache miss");
            None
        }
    }
}

/// Simpan response sukses ke Redis (TTL 5 menit). Error hanya di-log.
pub async fn set(emiten: &str, resp: &GetPortofolioHistoryByEmitenNameFromStockbitResponse) {
    let emiten = emiten.trim();
    if emiten.is_empty() || !resp.success {
        return;
    }
    let mut conn = match connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Redis portofolio_history set: koneksi gagal ({e})");
            return;
        }
    };
    let key = cache_key(emiten);
    let bytes = resp.encode_to_vec();
    let secs = HISTORY_STOCKBIT_CACHE_TTL.as_secs();
    if let Err(e) = conn.set_ex::<_, _, ()>(&key, bytes, secs).await {
        eprintln!("Redis portofolio_history set {emiten}: {e}");
    }
}
