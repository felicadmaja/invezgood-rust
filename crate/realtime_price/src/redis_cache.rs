//! Cache Redis response terakhir + flag hari libur (tanggal merah / cuti bersama).
//!
//! Env: `REDIS_URL` (default `redis://localhost:6379`).
//! Key harga: `stockbit:realtime_price:last:{EMITEN}` — tanpa TTL.
//! Key libur: `stockbit:realtime_price:holiday:{YYYY-MM-DD}` — TTL ~2 hari.

use std::sync::OnceLock;

use chrono::NaiveDate;
use prost::Message;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use tokio::sync::Mutex;

use crate::hours::today_local;
use crate::GetRealtimePriceFromStockbitResponse;

/// TTL flag libur (detik) — cukup melewati hari itu.
const HOLIDAY_TTL_SECS: u64 = 2 * 24 * 60 * 60;

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string())
}

fn cache_key(emiten: &str) -> String {
    format!(
        "stockbit:realtime_price:last:{}",
        emiten.trim().to_ascii_uppercase()
    )
}

fn holiday_key(day: NaiveDate) -> String {
    format!("stockbit:realtime_price:holiday:{}", day.format("%Y-%m-%d"))
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

/// Baca response terakhir. `None` = miss / Redis error / decode gagal.
pub async fn get(emiten: &str) -> Option<GetRealtimePriceFromStockbitResponse> {
    let emiten = emiten.trim();
    if emiten.is_empty() {
        return None;
    }
    let mut conn = match connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Redis realtime_price get: koneksi gagal ({e}) — cache miss");
            return None;
        }
    };
    let key = cache_key(emiten);
    let bytes: Option<Vec<u8>> = match conn.get(&key).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Redis realtime_price get {emiten}: {e} — cache miss");
            return None;
        }
    };
    let Some(bytes) = bytes else {
        return None;
    };
    match GetRealtimePriceFromStockbitResponse::decode(bytes.as_slice()) {
        Ok(resp) => Some(resp),
        Err(e) => {
            eprintln!("Redis realtime_price decode {emiten}: {e} — cache miss");
            None
        }
    }
}

/// Simpan response terakhir jam operasional. Error hanya di-log.
pub async fn set(emiten: &str, resp: &GetRealtimePriceFromStockbitResponse) {
    let emiten = emiten.trim();
    if emiten.is_empty() {
        return;
    }
    let mut conn = match connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Redis realtime_price set: koneksi gagal ({e})");
            return;
        }
    };
    let key = cache_key(emiten);
    let bytes = resp.encode_to_vec();
    if let Err(e) = conn.set::<_, _, ()>(&key, bytes).await {
        eprintln!("Redis realtime_price set {emiten}: {e}");
    }
}

/// Apakah hari lokal ini sudah ditandai libur (tanggal merah / cuti bersama).
pub async fn is_holiday_today() -> bool {
    let day = today_local();
    let mut conn = match connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Redis realtime_price holiday get: koneksi gagal ({e})");
            return false;
        }
    };
    let key = holiday_key(day);
    match conn.get::<_, Option<String>>(&key).await {
        Ok(Some(v)) => {
            let v = v.trim();
            v == "1" || v.eq_ignore_ascii_case("true")
        }
        Ok(None) => false,
        Err(e) => {
            eprintln!("Redis realtime_price holiday get: {e}");
            false
        }
    }
}

/// Tandai hari lokal sebagai libur (hentikan poll API untuk sisa hari).
pub async fn declare_holiday_today() {
    let day = today_local();
    let mut conn = match connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Redis realtime_price holiday set: koneksi gagal ({e})");
            return;
        }
    };
    let key = holiday_key(day);
    if let Err(e) = conn.set_ex::<_, _, ()>(&key, "1", HOLIDAY_TTL_SECS).await {
        eprintln!("Redis realtime_price holiday set {key}: {e}");
        return;
    }
    println!(
        "RealtimePrice: hari {} ditandai LIBUR (volume=0 setelah 09:10) — poller API dihentikan untuk hari ini",
        day.format("%Y-%m-%d")
    );
}
