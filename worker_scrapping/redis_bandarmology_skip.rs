//! Cache Redis untuk state skip scrape bandarmology (hindari hit Scylla berulang).
//!
//! Env: `REDIS_URL` (default `redis://localhost:6379`).
//!
//! Key:
//! - `stockbit:bandarmology:skip:current:{agg}` — bulan berjalan sudah `updated_at` hari ini
//! - `stockbit:bandarmology:skip:exists:{agg}` — baris historis (agg) sudah ada
//! - `stockbit:bandarmology:skip:weeks:{agg}` — kolom minggu invoke-slot sudah lengkap hari ini
//!
//! TTL: sampai 23:59:59 waktu lokal hari ini. Redis down → treat sebagai cache miss.

use std::sync::OnceLock;

use chrono::{Local, TimeZone};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use tokio::sync::Mutex;

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string())
}

fn key_current(agg: &str) -> String {
    format!("stockbit:bandarmology:skip:current:{agg}")
}

fn key_exists(agg: &str) -> String {
    format!("stockbit:bandarmology:skip:exists:{agg}")
}

fn key_weeks(agg: &str) -> String {
    format!("stockbit:bandarmology:skip:weeks:{agg}")
}

/// Detik sampai 23:59:59 lokal hari ini (min 1).
pub fn ttl_secs_until_local_2359() -> u64 {
    let now = Local::now();
    let eod_naive = match now.date_naive().and_hms_opt(23, 59, 59) {
        Some(t) => t,
        None => return 1,
    };
    let eod = match Local.from_local_datetime(&eod_naive) {
        chrono::LocalResult::Single(dt) | chrono::LocalResult::Ambiguous(dt, _) => dt,
        chrono::LocalResult::None => return 1,
    };
    let secs = (eod - now).num_seconds();
    if secs <= 0 {
        1
    } else {
        secs as u64
    }
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

async fn get_flag(key: &str) -> Option<bool> {
    let mut conn = match connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Redis bandarmology skip get: koneksi gagal ({e}) — cache miss");
            return None;
        }
    };
    match conn.get::<_, Option<String>>(key).await {
        Ok(Some(v)) => {
            let v = v.trim();
            Some(v == "1" || v.eq_ignore_ascii_case("true"))
        }
        Ok(None) => None,
        Err(e) => {
            eprintln!("Redis bandarmology skip get {key}: {e} — cache miss");
            None
        }
    }
}

async fn set_flag(key: &str) {
    let mut conn = match connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Redis bandarmology skip set: koneksi gagal ({e})");
            return;
        }
    };
    let ttl = ttl_secs_until_local_2359();
    if let Err(e) = conn.set_ex::<_, _, ()>(key, "1", ttl).await {
        eprintln!("Redis bandarmology skip set {key}: {e}");
    }
}

/// `Some(true)` = skip current (cache hit). `None` = miss / Redis error.
pub async fn get_skip_current(agg: &str) -> Option<bool> {
    let agg = agg.trim();
    if agg.is_empty() {
        return None;
    }
    get_flag(&key_current(agg)).await.filter(|v| *v)
}

pub async fn set_skip_current(agg: &str) {
    let agg = agg.trim();
    if agg.is_empty() {
        return;
    }
    set_flag(&key_current(agg)).await;
}

/// `Some(true)` = baris historis sudah ada (cache hit).
pub async fn get_skip_exists(agg: &str) -> Option<bool> {
    let agg = agg.trim();
    if agg.is_empty() {
        return None;
    }
    get_flag(&key_exists(agg)).await.filter(|v| *v)
}

pub async fn set_skip_exists(agg: &str) {
    let agg = agg.trim();
    if agg.is_empty() {
        return;
    }
    set_flag(&key_exists(agg)).await;
}

/// `Some(true)` = kolom minggu invoke-slot sudah OK hari ini (cache hit).
pub async fn get_skip_weeks(agg: &str) -> Option<bool> {
    let agg = agg.trim();
    if agg.is_empty() {
        return None;
    }
    get_flag(&key_weeks(agg)).await.filter(|v| *v)
}

pub async fn set_skip_weeks(agg: &str) {
    let agg = agg.trim();
    if agg.is_empty() {
        return;
    }
    set_flag(&key_weeks(agg)).await;
}
