//! Cache Moka untuk `GetAllStocks` — key global per tanggal (daftar saham sama untuk semua user).
//! TTL: sampai 23:59:59 waktu lokal hari ini.

use std::time::Duration;

use chrono::{Local, TimeZone};
use moka::future::Cache;
use moka::Expiry;

use crate::pb::StockListRow;

#[derive(Clone)]
pub struct CachedAllStocks {
    pub message: String,
    pub items: Vec<StockListRow>,
}

struct EndOfDayExpiry;

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

fn cache_key() -> String {
    format!("all:{}", Local::now().format("%Y-%m-%d"))
}

impl Expiry<String, CachedAllStocks> for EndOfDayExpiry {
    fn expire_after_create(
        &self,
        _key: &String,
        _value: &CachedAllStocks,
        _current_time: std::time::Instant,
    ) -> Option<Duration> {
        Some(Duration::from_secs(ttl_until_end_of_day_secs()))
    }
}

#[derive(Clone)]
pub struct AllStocksCache {
    moka: Cache<String, CachedAllStocks>,
}

impl AllStocksCache {
    pub fn new() -> Self {
        let moka = Cache::builder()
            .max_capacity(2)
            .expire_after(EndOfDayExpiry)
            .build();
        Self { moka }
    }

    pub async fn get(&self) -> Option<CachedAllStocks> {
        self.moka.get(&cache_key()).await
    }

    pub async fn set(&self, cached: CachedAllStocks) {
        self.moka.insert(cache_key(), cached).await;
    }
}
