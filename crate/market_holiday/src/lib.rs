//! Deteksi market libur untuk semua RPC.
//!
//! Sabtu/Minggu selalu hari libur (tanpa cek Yahoo).
//! Tanggal di `ARRAY_HOLIDAY` (.env, JSON `["YYYY-MM-DD", ...]`) = libur nasional (tanpa cek Yahoo).
//! Senin–Jumat setelah jam **10:00** waktu lokal: GET Yahoo Finance v8 chart **BBCA**.JK (1d, hari ini).
//! Volume titik terakhir = 0 → hari libur.
//! Cache Redis `invezgood:market_holiday:{YYYY-MM-DD}` (`1`=libur, `0`=buka; TTL s/d 23:59:59).
//! Senin–Jumat sebelum 10:00 (bukan tanggal ARRAY_HOLIDAY) selalu `false`. Error fetch → `false`.

use std::collections::HashSet;
use std::sync::OnceLock;
use std::time::Duration;

use chrono::{Datelike, Local, NaiveDate, TimeZone, Timelike, Weekday};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio::time::sleep;

const BENCHMARK_EMITEN: &str = "BBCA";
const KEY_PREFIX: &str = "invezgood:market_holiday:";
const YAHOO_CHART_URL: &str = "https://query2.finance.yahoo.com/v8/finance/chart";
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
const RATE_LIMIT_RETRY_DELAY: Duration = Duration::from_millis(300);
const RATE_LIMIT_MAX_RETRIES: u32 = 20;

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
    match raw?.as_str() {
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
        eprintln!("Redis market_holiday set {key} gagal");
    }
}

/// True bila `date` Sabtu atau Minggu.
pub fn is_weekend_date(date: NaiveDate) -> bool {
    matches!(date.weekday(), Weekday::Sat | Weekday::Sun)
}

/// True bila hari ini Sabtu atau Minggu (waktu server lokal).
pub fn is_weekend() -> bool {
    is_weekend_date(Local::now().date_naive())
}

fn parse_array_holiday(raw: &str) -> HashSet<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return HashSet::new();
    }

    let items: Vec<String> = serde_json::from_str(trimmed).unwrap_or_else(|_| {
        trimmed
            .trim_matches(|c| c == '[' || c == ']')
            .split(',')
            .map(|s| s.trim().trim_matches('"').to_string())
            .filter(|s| !s.is_empty())
            .collect()
    });

    items
        .into_iter()
        .map(|s| s.trim().to_string())
        .filter(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").is_ok())
        .collect()
}

fn national_holiday_dates() -> &'static HashSet<String> {
    static DATES: OnceLock<HashSet<String>> = OnceLock::new();
    DATES.get_or_init(|| {
        parse_array_holiday(&std::env::var("ARRAY_HOLIDAY").unwrap_or_default())
    })
}

/// True bila `date` (YYYY-MM-DD) ada di `ARRAY_HOLIDAY`.
pub fn is_national_holiday_date(date: NaiveDate) -> bool {
    national_holiday_dates().contains(&date.format("%Y-%m-%d").to_string())
}

/// True bila hari ini ada di `ARRAY_HOLIDAY` (.env, libur nasional Indonesia).
pub fn is_national_holiday() -> bool {
    is_national_holiday_date(Local::now().date_naive())
}

/// True bila sudah >= 10:00 waktu server lokal.
pub fn can_check_market_holiday() -> bool {
    let now = Local::now();
    now.hour() * 60 + now.minute() >= 10 * 60
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

fn unix_range_today() -> (i64, i64) {
    let now = Local::now();
    let period2 = now.timestamp();
    let start_naive = now.date_naive().and_hms_opt(0, 0, 0).expect("00:00 valid");
    let period1 = Local
        .from_local_datetime(&start_naive)
        .single()
        .unwrap_or(now)
        .timestamp();
    (period1, period2)
}

fn parse_last_volume(body: &str) -> Result<i64, String> {
    let v: Value = serde_json::from_str(body).map_err(|e| format!("yahoo JSON: {e}"))?;
    let volumes = v
        .pointer("/chart/result/0/indicators/quote/0/volume")
        .and_then(|x| x.as_array())
        .ok_or_else(|| "yahoo: volume missing".to_string())?;
    for item in volumes.iter().rev() {
        match item {
            Value::Null => continue,
            Value::Number(n) => {
                if let Some(v) = n.as_i64() {
                    return Ok(v);
                }
                if let Some(v) = n.as_f64() {
                    return Ok(v as i64);
                }
            }
            _ => continue,
        }
    }
    Ok(0)
}

async fn fetch_bbca_volume() -> Result<i64, String> {
    let (period1, period2) = unix_range_today();
    let url = format!(
        "{YAHOO_CHART_URL}/{BENCHMARK_EMITEN}.JK?period1={period1}&period2={period2}&interval=1d"
    );
    let http = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| format!("yahoo HTTP client: {e}"))?;

    let mut attempt = 0u32;
    loop {
        let resp = http
            .get(&url)
            .header("User-Agent", USER_AGENT)
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|e| format!("yahoo request {BENCHMARK_EMITEN}: {e}"))?;
        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| format!("yahoo body {BENCHMARK_EMITEN}: {e}"))?;
        let too_many = status.as_u16() == 409 || status.as_u16() == 429;
        if too_many {
            attempt += 1;
            eprintln!(
                "\x1b[31myahoo HTTP {status} Too Many Request {BENCHMARK_EMITEN} — jeda 300ms lalu retry ({attempt})\x1b[0m"
            );
            if attempt > RATE_LIMIT_MAX_RETRIES {
                return Err(format!(
                    "yahoo HTTP {status} Too Many Request {BENCHMARK_EMITEN}: gagal setelah {RATE_LIMIT_MAX_RETRIES} retry"
                ));
            }
            sleep(RATE_LIMIT_RETRY_DELAY).await;
            continue;
        }
        if !status.is_success() {
            let preview: String = body.chars().take(160).collect();
            return Err(format!("yahoo HTTP {status} {BENCHMARK_EMITEN}: {preview}"));
        }
        return parse_last_volume(&body);
    }
}

/// `true` bila `date` hari libur: Sabtu/Minggu, ARRAY_HOLIDAY, atau (bila hari ini) BBCA volume=0 mulai 10:00.
pub async fn is_market_holiday_on(date: NaiveDate) -> bool {
    if is_weekend_date(date) || is_national_holiday_date(date) {
        return true;
    }
    if date != Local::now().date_naive() {
        return false;
    }
    if !can_check_market_holiday() {
        return false;
    }

    let today = today_key_suffix();
    if let Some(holiday) = cached_market_holiday(&today).await {
        return holiday;
    }

    match fetch_bbca_volume().await {
        Ok(volume) => {
            let holiday = volume == 0;
            store_market_holiday(&today, holiday).await;
            if holiday {
                println!(
                    "\x1b[33mMarket libur: Yahoo {BENCHMARK_EMITEN} volume=0 (tanggal {today})\x1b[0m"
                );
            } else {
                println!(
                    "Market buka: Yahoo {BENCHMARK_EMITEN} volume={volume} (tanggal {today})"
                );
            }
            holiday
        }
        Err(e) => {
            eprintln!("Cek market libur Yahoo {BENCHMARK_EMITEN} gagal: {e} — anggap buka");
            false
        }
    }
}

/// `true` bila market libur hari ini (Sabtu/Minggu, ARRAY_HOLIDAY, atau BBCA volume=0 mulai 10:00).
pub async fn is_market_holiday() -> bool {
    is_market_holiday_on(Local::now().date_naive()).await
}
