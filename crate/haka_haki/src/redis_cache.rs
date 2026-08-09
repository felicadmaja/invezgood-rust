//! Cache Redis untuk `GetHakaHakiFromInvezgo` bila `tahun_bulan_tanggal` bukan hari ini.
//!
//! Key: `invezgood:haka_haki:invezgo:{code}:{YYYY-MM-DD}:{range}`
//! Hanya disimpan/dibaca bila `items` tidak kosong.
//! Env: `REDIS_URL`, `HAKA_HAKI_CACHE_TTL_SECS` (default 7 hari).

use std::sync::OnceLock;

use prost::Message;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use tokio::sync::Mutex;

use crate::pb::GetHakaHakiFromInvezgoResponse;

const DEFAULT_CACHE_TTL_SECS: u64 = 7 * 24 * 60 * 60;

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string())
}

fn cache_ttl_secs() -> u64 {
    std::env::var("HAKA_HAKI_CACHE_TTL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_CACHE_TTL_SECS)
}

pub fn cache_key(code: &str, tahun_bulan_tanggal: &str, range: i32) -> String {
    format!(
        "invezgood:haka_haki:invezgo:{}:{}:{range}",
        code.trim().to_ascii_uppercase(),
        tahun_bulan_tanggal.trim()
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

pub async fn get(
    code: &str,
    tahun_bulan_tanggal: &str,
    range: i32,
) -> Option<GetHakaHakiFromInvezgoResponse> {
    let mut conn = match connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Redis haka_haki get: koneksi gagal ({e}) — cache miss");
            return None;
        }
    };
    let key = cache_key(code, tahun_bulan_tanggal, range);
    let bytes: Option<Vec<u8>> = match conn.get(&key).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Redis haka_haki get {key}: {e} — cache miss");
            return None;
        }
    };
    let Some(bytes) = bytes else {
        return None;
    };
    match GetHakaHakiFromInvezgoResponse::decode(bytes.as_slice()) {
        Ok(resp) if !resp.items.is_empty() => Some(resp),
        Ok(_) => None,
        Err(e) => {
            eprintln!("Redis haka_haki decode {key}: {e} — cache miss");
            None
        }
    }
}

pub async fn set(
    code: &str,
    tahun_bulan_tanggal: &str,
    range: i32,
    resp: &GetHakaHakiFromInvezgoResponse,
) {
    if !resp.success || resp.items.is_empty() {
        return;
    }
    let mut conn = match connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Redis haka_haki set: koneksi gagal ({e})");
            return;
        }
    };
    let key = cache_key(code, tahun_bulan_tanggal, range);
    let bytes = resp.encode_to_vec();
    let secs = cache_ttl_secs();
    if let Err(e) = conn.set_ex::<_, _, ()>(&key, bytes, secs).await {
        eprintln!("Redis haka_haki set {key}: {e}");
    }
}
