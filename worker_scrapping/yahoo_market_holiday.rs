//! Deteksi market libur via Yahoo Finance v8 chart BBCA (1d, hanya hari ini;
//! volume titik terakhir = 0 → market libur).
//!
//! Dipakai poller IsStockbitReady: mulai 09:15, bila libur skip semua auto-scrape.
//! Cache Redis + in-memory per hari (TTL s/d 23:59:59 lokal).

use std::sync::OnceLock;

use chrono::{Local, TimeZone, Timelike};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use tokio::sync::Mutex;

const BENCHMARK_EMITEN: &str = "BBCA";
const KEY_PREFIX: &str = "invezgood:yahoo:market_holiday:";

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string())
}

fn today_key_suffix() -> String {
    Local::now().format("%Y-%m-%d").to_string()
}

fn redis_key(date: &str) -> String {
    format!("{KEY_PREFIX}{date}")
}

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
static MEM: OnceLock<Mutex<Option<(String, bool)>>> = OnceLock::new();

fn redis_slot() -> &'static Mutex<Option<ConnectionManager>> {
    REDIS.get_or_init(|| Mutex::new(None))
}

fn mem_slot() -> &'static Mutex<Option<(String, bool)>> {
    MEM.get_or_init(|| Mutex::new(None))
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

async fn mem_get(today: &str) -> Option<bool> {
    let guard = mem_slot().lock().await;
    guard
        .as_ref()
        .filter(|(date, _)| date == today)
        .map(|(_, holiday)| *holiday)
}

async fn mem_set(today: &str, holiday: bool) {
    let mut guard = mem_slot().lock().await;
    *guard = Some((today.to_string(), holiday));
}

async fn redis_get(today: &str) -> Option<bool> {
    let mut conn = connection().await.ok()?;
    let key = redis_key(today);
    let raw: Option<String> = conn.get(&key).await.ok()?;
    let raw = raw?;
    match raw.as_str() {
        "1" => Some(true),
        "0" => Some(false),
        _ => None,
    }
}

async fn redis_set(today: &str, holiday: bool) {
    let Ok(mut conn) = connection().await else {
        return;
    };
    let key = redis_key(today);
    let value = if holiday { "1" } else { "0" };
    if conn
        .set_ex::<_, _, ()>(&key, value, ttl_until_end_of_day_secs())
        .await
        .is_err()
    {
        eprintln!("Redis yahoo market_holiday set {key} gagal");
    }
}

/// True bila sudah >= 09:15 waktu server lokal (cek BBCA boleh dijalankan).
pub fn can_check_market_holiday() -> bool {
    let now = Local::now();
    now.hour() * 60 + now.minute() >= 9 * 60 + 15
}

async fn cached_market_holiday(today: &str) -> Option<bool> {
    if let Some(holiday) = mem_get(today).await {
        return Some(holiday);
    }
    if let Some(holiday) = redis_get(today).await {
        mem_set(today, holiday).await;
        return Some(holiday);
    }
    None
}

async fn store_market_holiday(today: &str, holiday: bool) {
    mem_set(today, holiday).await;
    redis_set(today, holiday).await;
}

/// Poller: mulai 09:15 cek Yahoo BBCA; volume terakhir = 0 → market libur hari ini.
/// Sebelum 09:15 selalu `false` (poller scrapes tetap jalan bila jam operasional).
/// Error Yahoo → `false` (jangan blokir poller).
pub async fn is_poller_market_holiday() -> bool {
    if !can_check_market_holiday() {
        return false;
    }

    let today = today_key_suffix();
    if let Some(holiday) = cached_market_holiday(&today).await {
        return holiday;
    }

    match crate::yahoo_atr::fetch_today_volume(BENCHMARK_EMITEN).await {
        Ok(volume) => {
            let holiday = volume == 0;
            store_market_holiday(&today, holiday).await;
            if holiday {
                println!(
                    "\x1b[33mPoller market libur: Yahoo {BENCHMARK_EMITEN} volume=0 (tanggal {today})\x1b[0m"
                );
            } else {
                println!(
                    "Poller market buka: Yahoo {BENCHMARK_EMITEN} volume={volume} (tanggal {today})"
                );
            }
            holiday
        }
        Err(e) => {
            eprintln!(
                "Poller cek market libur Yahoo {BENCHMARK_EMITEN} gagal: {e} — lanjut scrape"
            );
            false
        }
    }
}
