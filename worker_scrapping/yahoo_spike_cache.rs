//! Cache Redis: emiten lonjakan Yahoo ATR yang sudah pernah di-output hari ini.
//!
//! Env: `REDIS_URL` (default `redis://localhost:6379`).
//! Key SET: `invezgood:yahoo_atr:spike_reported` — member = emiten_name.
//! TTL: sampai 23:59:59 hari lokal — ganti hari → key expire (invalidate).
//! Redis down → treat sebagai cache kosong (tetap cek Yahoo).

use std::collections::HashSet;
use std::sync::OnceLock;

use chrono::{Local, TimeZone};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use tokio::sync::Mutex;

const KEY: &str = "invezgood:yahoo_atr:spike_reported";

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string())
}

/// Detik sampai 23:59:59 waktu lokal hari ini (minimal 1).
fn ttl_until_end_of_day_secs() -> u64 {
    let now = Local::now();
    let end_naive = now
        .date_naive()
        .and_hms_opt(23, 59, 59)
        .expect("23:59:59 valid");
    let end = Local
        .from_local_datetime(&end_naive)
        .single()
        .unwrap_or(now);
    (end - now).num_seconds().max(1) as u64
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

/// Emiten yang sudah di-output hari ini (skip Yahoo + skip stream ulang).
pub async fn already_reported() -> HashSet<String> {
    let mut conn = match connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Redis yahoo_atr spike get: koneksi gagal ({e}) — cache miss");
            return HashSet::new();
        }
    };
    let members: Vec<String> = match conn.smembers(KEY).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Redis yahoo_atr spike SMEMBERS: {e} — cache miss");
            return HashSet::new();
        }
    };
    members
        .into_iter()
        .map(|s| s.trim().to_ascii_uppercase())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Tandai emiten sudah di-output; TTL di-refresh sampai akhir hari.
pub async fn mark_reported(emitens: &[String]) {
    if emitens.is_empty() {
        return;
    }
    let mut conn = match connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Redis yahoo_atr spike set: koneksi gagal ({e})");
            return;
        }
    };
    let codes: Vec<String> = emitens
        .iter()
        .map(|s| s.trim().to_ascii_uppercase())
        .filter(|s| !s.is_empty())
        .collect();
    if codes.is_empty() {
        return;
    }
    if let Err(e) = conn.sadd::<_, _, ()>(KEY, &codes).await {
        eprintln!("Redis yahoo_atr spike SADD: {e}");
        return;
    }
    let secs = ttl_until_end_of_day_secs();
    if let Err(e) = conn.expire::<_, ()>(KEY, secs as i64).await {
        eprintln!("Redis yahoo_atr spike EXPIRE: {e}");
    }
}
