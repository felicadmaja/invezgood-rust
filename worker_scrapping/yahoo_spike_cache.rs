//! Cache Redis: hasil spike Yahoo yang dikirim ke client.
//!
//! Env: `REDIS_URL` (default `redis://localhost:6379`).
//! Key JSON: `invezgood:yahoo_atr:spike_today:{YYYY-MM-DD}` —
//!   `[{spike_at, emiten_name, jenis_spike, value_spike_percentage}, ...]`
//! Key SET: `invezgood:yahoo_atr:spike_reported:{YYYY-MM-DD}` — member = emiten_name (skip GET Yahoo).
//! TTL: sampai 23:59:59 hari lokal; key bertanggal → ganti hari otomatis invalid.
//! Redis down → treat sebagai cache kosong (tetap cek Yahoo).

use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

use chrono::{Local, TimeZone};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use tokio::sync::Mutex;

fn reported_key() -> String {
    format!(
        "invezgood:yahoo_atr:spike_reported:{}",
        Local::now().format("%Y-%m-%d")
    )
}

fn today_key() -> String {
    format!(
        "invezgood:yahoo_atr:spike_today:{}",
        Local::now().format("%Y-%m-%d")
    )
}

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

/// Emiten yang sudah di-stream ke client hari ini (jangan GET Yahoo lagi).
pub async fn already_reported() -> HashSet<String> {
    let mut conn = match connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Redis yahoo_atr spike get: koneksi gagal ({e}) — cache miss");
            return HashSet::new();
        }
    };
    let members: Vec<String> = match conn.smembers(reported_key()).await {
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

/// Tandai emiten sudah di-stream ke client; TTL di-refresh sampai 23:59:59.
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
    let key = reported_key();
    let added: i64 = match conn.sadd(&key, &codes).await {
        Ok(n) => n,
        Err(e) => {
            eprintln!("Redis yahoo_atr spike SADD: {e}");
            return;
        }
    };
    let secs = ttl_until_end_of_day_secs();
    if let Err(e) = conn.expire::<_, ()>(&key, secs as i64).await {
        eprintln!("Redis yahoo_atr spike EXPIRE: {e}");
    }
    if added > 0 {
        println!(
            "Yahoo spike cache: +{added} emiten di-Redis (skip Yahoo s/d 23:59:59) {}",
            codes.join(",")
        );
    }
}

/// Detail spike yang sudah di-output hari ini (untuk stream reconnect).
pub async fn today_details() -> Vec<crate::yahoo_atr::SpikeEmiten> {
    let mut conn = match connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Redis yahoo_atr spike_today get: koneksi gagal ({e})");
            return Vec::new();
        }
    };
    let raw: Option<String> = match conn.get(today_key()).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Redis yahoo_atr spike_today GET: {e}");
            return Vec::new();
        }
    };
    let Some(raw) = raw else {
        return Vec::new();
    };
    serde_json::from_str(&raw).unwrap_or_else(|e| {
        eprintln!("Redis yahoo_atr spike_today JSON: {e}");
        Vec::new()
    })
}

pub async fn set_today_details(items: &[crate::yahoo_atr::SpikeEmiten]) {
    let mut conn = match connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Redis yahoo_atr spike_today set: koneksi gagal ({e})");
            return;
        }
    };
    let raw = match serde_json::to_string(items) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Redis yahoo_atr spike_today encode: {e}");
            return;
        }
    };
    let key = today_key();
    if let Err(e) = conn.set::<_, _, ()>(&key, raw).await {
        eprintln!("Redis yahoo_atr spike_today SET: {e}");
        return;
    }
    let secs = ttl_until_end_of_day_secs();
    if let Err(e) = conn.expire::<_, ()>(&key, secs as i64).await {
        eprintln!("Redis yahoo_atr spike_today EXPIRE: {e}");
    }
}

fn merge_first_wins(
    existing: Vec<crate::yahoo_atr::SpikeEmiten>,
    incoming: Vec<crate::yahoo_atr::SpikeEmiten>,
) -> Vec<crate::yahoo_atr::SpikeEmiten> {
    let mut by_code: HashMap<String, crate::yahoo_atr::SpikeEmiten> = existing
        .into_iter()
        .map(|p| (p.emiten_name.clone(), p))
        .collect();
    for p in incoming {
        by_code.entry(p.emiten_name.clone()).or_insert(p);
    }
    let mut out: Vec<crate::yahoo_atr::SpikeEmiten> = by_code.into_values().collect();
    out.sort_by(|a, b| a.emiten_name.cmp(&b.emiten_name));
    out
}

/// Kode emiten yang sudah ada di cache hari ini (JSON + SET).
pub async fn cached_emiten_names() -> HashSet<String> {
    let mut names = already_reported().await;
    for row in today_details().await {
        let code = row.emiten_name.trim().to_ascii_uppercase();
        if !code.is_empty() {
            names.insert(code);
        }
    }
    names
}

/// Setelah GET Yahoo: upsert spike baru ke Redis (first-write wins), return isi cache lengkap.
pub async fn upsert_spikes(
    incoming: &[crate::yahoo_atr::SpikeEmiten],
) -> Vec<crate::yahoo_atr::SpikeEmiten> {
    let existing = today_details().await;
    if incoming.is_empty() {
        return existing;
    }
    let acc = merge_first_wins(existing, incoming.to_vec());
    set_today_details(&acc).await;
    let names: Vec<String> = incoming.iter().map(|s| s.emiten_name.clone()).collect();
    mark_reported(&names).await;
    acc
}
