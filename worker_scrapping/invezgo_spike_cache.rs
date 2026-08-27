//! Cache Redis hasil spike Invezgo untuk stream client.
//! Key terpisah opening (09:00–09:05) vs intraday (di luar jam itu):
//! - Opening JSON: `invezgood:invezgo:spike_opening_today:{YYYY-MM-DD}`
//! - Opening SET:  `invezgood:invezgo:spike_opening_reported:{YYYY-MM-DD}`
//! - Intraday JSON: `invezgood:invezgo:spike_intraday_today:{YYYY-MM-DD}`
//! - Intraday SET:  `invezgood:invezgo:spike_intraday_reported:{YYYY-MM-DD}`
//! Stream client: merge kedua JSON (first-write wins per emiten). Skip GET Invezgo hanya per mode aktif.

use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

use chrono::{Local, TimeZone};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use tokio::sync::Mutex;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SpikeCacheKind {
    Opening,
    Intraday,
}

fn current_kind() -> SpikeCacheKind {
    if crate::yahoo_atr::in_opening_spike_window() {
        SpikeCacheKind::Opening
    } else {
        SpikeCacheKind::Intraday
    }
}

fn today_date_suffix() -> String {
    Local::now().format("%Y-%m-%d").to_string()
}

fn reported_key(kind: SpikeCacheKind) -> String {
    let date = today_date_suffix();
    match kind {
        SpikeCacheKind::Opening => format!("invezgood:invezgo:spike_opening_reported:{date}"),
        SpikeCacheKind::Intraday => format!("invezgood:invezgo:spike_intraday_reported:{date}"),
    }
}

fn today_key(kind: SpikeCacheKind) -> String {
    let date = today_date_suffix();
    match kind {
        SpikeCacheKind::Opening => format!("invezgood:invezgo:spike_opening_today:{date}"),
        SpikeCacheKind::Intraday => format!("invezgood:invezgo:spike_intraday_today:{date}"),
    }
}

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string())
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

async fn already_reported_for(kind: SpikeCacheKind) -> HashSet<String> {
    let mut conn = match connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Redis invezgo spike get: koneksi gagal ({e}) — cache miss");
            return HashSet::new();
        }
    };
    let members: Vec<String> = match conn.smembers(reported_key(kind)).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Redis invezgo spike SMEMBERS: {e} — cache miss");
            return HashSet::new();
        }
    };
    members
        .into_iter()
        .map(|s| s.trim().to_ascii_uppercase())
        .filter(|s| !s.is_empty())
        .collect()
}

async fn mark_reported_for(kind: SpikeCacheKind, emitens: &[String]) {
    if emitens.is_empty() {
        return;
    }
    let mut conn = match connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Redis invezgo spike set: koneksi gagal ({e})");
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
    let key = reported_key(kind);
    let added: i64 = match conn.sadd(&key, &codes).await {
        Ok(n) => n,
        Err(e) => {
            eprintln!("Redis invezgo spike SADD: {e}");
            return;
        }
    };
    let secs = ttl_until_end_of_day_secs();
    if let Err(e) = conn.expire::<_, ()>(&key, secs as i64).await {
        eprintln!("Redis invezgo spike EXPIRE: {e}");
    }
    if added > 0 {
        let mode = match kind {
            SpikeCacheKind::Opening => "opening",
            SpikeCacheKind::Intraday => "intraday",
        };
        println!(
            "Invezgo spike cache ({mode}): +{added} emiten di-Redis (skip Invezgo s/d 23:59:59) {}",
            codes.join(",")
        );
    }
}

async fn today_details_for(kind: SpikeCacheKind) -> Vec<crate::yahoo_atr::SpikeEmiten> {
    let mut conn = match connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Redis invezgo spike_today get: koneksi gagal ({e})");
            return Vec::new();
        }
    };
    let raw: Option<String> = match conn.get(today_key(kind)).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Redis invezgo spike_today GET: {e}");
            return Vec::new();
        }
    };
    let Some(raw) = raw else {
        return Vec::new();
    };
    serde_json::from_str(&raw).unwrap_or_else(|e| {
        eprintln!("Redis invezgo spike_today JSON: {e}");
        Vec::new()
    })
}

async fn set_today_details_for(kind: SpikeCacheKind, items: &[crate::yahoo_atr::SpikeEmiten]) {
    let mut conn = match connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Redis invezgo spike_today set: koneksi gagal ({e})");
            return;
        }
    };
    let raw = match serde_json::to_string(items) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Redis invezgo spike_today encode: {e}");
            return;
        }
    };
    let key = today_key(kind);
    if let Err(e) = conn.set::<_, _, ()>(&key, raw).await {
        eprintln!("Redis invezgo spike_today SET: {e}");
        return;
    }
    let secs = ttl_until_end_of_day_secs();
    if let Err(e) = conn.expire::<_, ()>(&key, secs as i64).await {
        eprintln!("Redis invezgo spike_today EXPIRE: {e}");
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

/// Gabungan spike opening + intraday hari ini (untuk stream client).
pub async fn today_details() -> Vec<crate::yahoo_atr::SpikeEmiten> {
    let opening = today_details_for(SpikeCacheKind::Opening).await;
    let intraday = today_details_for(SpikeCacheKind::Intraday).await;
    merge_first_wins(opening, intraday)
}

/// Emiten yang sudah dicek/di-cache untuk **mode aktif** (opening atau intraday).
pub async fn cached_emiten_names() -> HashSet<String> {
    let kind = current_kind();
    let mut names = already_reported_for(kind).await;
    for row in today_details_for(kind).await {
        let code = row.emiten_name.trim().to_ascii_uppercase();
        if !code.is_empty() {
            names.insert(code);
        }
    }
    names
}

/// Upsert spike ke cache mode aktif; return gabungan opening + intraday untuk stream.
pub async fn upsert_spikes(
    incoming: &[crate::yahoo_atr::SpikeEmiten],
) -> Vec<crate::yahoo_atr::SpikeEmiten> {
    let kind = current_kind();
    let existing = today_details_for(kind).await;
    if incoming.is_empty() {
        return today_details().await;
    }
    let acc = merge_first_wins(existing, incoming.to_vec());
    set_today_details_for(kind, &acc).await;
    let names: Vec<String> = incoming.iter().map(|s| s.emiten_name.clone()).collect();
    mark_reported_for(kind, &names).await;
    today_details().await
}
