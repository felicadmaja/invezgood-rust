//! Deteksi market libur untuk semua RPC.
//!
//! Sabtu/Minggu selalu hari libur (tanpa cek Yahoo).
//! Tanggal yang ada di Scylla `invezgood.hari_libur` (PK `date`) = libur nasional (tanpa cek Yahoo).
//! Senin–Jumat setelah jam **10:00** waktu lokal: GET Yahoo Finance v8 chart **BBCA**.JK (1d, hari ini).
//! Volume titik terakhir = 0 → hari libur.
//! Cache Redis `invezgood:market_holiday:{YYYY-MM-DD}` (`1`=libur, `0`=buka; TTL s/d 23:59:59).
//! Senin–Jumat sebelum 10:00 (bukan tanggal `invezgood.hari_libur`) selalu `false`. Error fetch → `false`.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use chrono::{Datelike, Local, NaiveDate, TimeZone, Timelike, Weekday};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use serde_json::Value;
use tokio::sync::{Mutex, OnceCell};
use tokio::time::sleep;

const BENCHMARK_EMITEN: &str = "BBCA";
const KEY_PREFIX: &str = "invezgood:market_holiday:";
const YAHOO_CHART_URL: &str = "https://query2.finance.yahoo.com/v8/finance/chart";
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
const RATE_LIMIT_RETRY_DELAY: Duration = Duration::from_millis(300);
const RATE_LIMIT_MAX_RETRIES: u32 = 20;
const KEYSPACE: &str = "invezgood";
const TABLE: &str = "hari_libur";
const NATIONAL_HOLIDAY_QUERY: &str = "SELECT date FROM invezgood.hari_libur WHERE date = ?";

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

static SCYLLA: OnceCell<Arc<Session>> = OnceCell::const_new();
static NATIONAL_CACHE: OnceLock<Mutex<NationalCache>> = OnceLock::new();

/// Cache hasil cek `invezgood.hari_libur` yang berlaku satu hari kalender.
/// `cached_on` = tanggal lokal saat isi cache dibuat; begitu tanggal lokal berganti
/// seluruh isi dibuang supaya kalender dibaca ulang dari Scylla.
struct NationalCache {
    cached_on: NaiveDate,
    holidays: HashMap<NaiveDate, bool>,
}

/// Pakai session Scylla milik app (dipanggil sekali saat startup) supaya crate ini tidak
/// membuka koneksi kedua. Tanpa ini: connect sendiri dari `SCYLLA_URI`/`SCYLLA_USER`/`SCYLLA_PASSWORD`.
pub fn init_scylla_session(session: Arc<Session>) {
    if SCYLLA.set(session).is_err() {
        eprintln!("market_holiday: session Scylla sudah terpasang — init_scylla_session diabaikan");
    }
}

async fn scylla_session() -> Result<Arc<Session>, String> {
    SCYLLA
        .get_or_try_init(|| async {
            let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".into());
            let user = std::env::var("SCYLLA_USER").unwrap_or_else(|_| "cassandra".into());
            let password = std::env::var("SCYLLA_PASSWORD").unwrap_or_default();
            SessionBuilder::new()
                .known_node(uri)
                .user(user, password)
                .build()
                .await
                .map(Arc::new)
                .map_err(|e| format!("connect Scylla {KEYSPACE}.{TABLE}: {e}"))
        })
        .await
        .map(Arc::clone)
}

fn national_cache() -> &'static Mutex<NationalCache> {
    NATIONAL_CACHE.get_or_init(|| {
        Mutex::new(NationalCache {
            cached_on: Local::now().date_naive(),
            holidays: HashMap::new(),
        })
    })
}

/// Buang seluruh isi cache bila hari lokal sudah berganti sejak cache diisi.
fn drop_if_stale(cache: &mut NationalCache) {
    let today = Local::now().date_naive();
    if cache.cached_on != today {
        cache.holidays.clear();
        cache.cached_on = today;
    }
}

async fn national_cache_get(date: NaiveDate) -> Option<bool> {
    let mut guard = national_cache().lock().await;
    drop_if_stale(&mut guard);
    guard.holidays.get(&date).copied()
}

async fn national_cache_set(date: NaiveDate, holiday: bool) {
    let mut guard = national_cache().lock().await;
    drop_if_stale(&mut guard);
    guard.holidays.insert(date, holiday);
}

/// Buang cache satu tanggal — dipakai setelah Insert/Update/DeleteHariLibur agar
/// perubahan kalender langsung terpakai tanpa menunggu pergantian hari.
pub async fn invalidate_national_holiday(date: NaiveDate) {
    national_cache().lock().await.holidays.remove(&date);
}

async fn national_holiday_from_scylla(date: NaiveDate) -> Result<bool, String> {
    let session = scylla_session().await?;
    let rows = session
        .query_unpaged(NATIONAL_HOLIDAY_QUERY, (date,))
        .await
        .map_err(|e| format!("query {KEYSPACE}.{TABLE} date={date}: {e}"))?
        .into_rows_result()
        .map_err(|e| format!("rows {KEYSPACE}.{TABLE} date={date}: {e}"))?;

    Ok(rows
        .maybe_first_row::<(NaiveDate,)>()
        .map_err(|e| format!("row {KEYSPACE}.{TABLE} date={date}: {e}"))?
        .is_some())
}

/// True bila `date` ada di `invezgood.hari_libur` (query by PK `date`) = libur nasional.
/// Hasil di-cache di memori sepanjang hari itu dan dibuang saat tanggal lokal berganti;
/// error koneksi/query → `false` (dianggap bukan libur).
pub async fn is_national_holiday_date(date: NaiveDate) -> bool {
    if let Some(holiday) = national_cache_get(date).await {
        return holiday;
    }

    // Future query Scylla di-box: tanpa ini tipe future handler gRPC (chart, haka_haki)
    // melewati batas kedalaman tipe rustc saat menghitung layout.
    let query: Pin<Box<dyn Future<Output = Result<bool, String>> + Send>> =
        Box::pin(national_holiday_from_scylla(date));

    match query.await {
        Ok(holiday) => {
            national_cache_set(date, holiday).await;
            holiday
        }
        Err(e) => {
            eprintln!("Cek libur nasional {KEYSPACE}.{TABLE} gagal: {e} — anggap bukan libur");
            false
        }
    }
}

/// True bila hari ini ada di `invezgood.hari_libur` (libur nasional Indonesia).
pub async fn is_national_holiday() -> bool {
    is_national_holiday_date(Local::now().date_naive()).await
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

/// `true` bila `date` hari libur: Sabtu/Minggu, ada di `invezgood.hari_libur`,
/// atau (bila hari ini) BBCA volume=0 mulai 10:00.
pub async fn is_market_holiday_on(date: NaiveDate) -> bool {
    if is_weekend_date(date) || is_national_holiday_date(date).await {
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

/// `true` bila market libur hari ini (Sabtu/Minggu, `invezgood.hari_libur`, atau BBCA volume=0 mulai 10:00).
pub async fn is_market_holiday() -> bool {
    is_market_holiday_on(Local::now().date_naive()).await
}
