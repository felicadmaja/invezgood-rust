//! Moka TTL per emiten: bila scrape Stockbit sukses ≤15 menit lalu, skip API → baca Scylla saja.

use std::sync::OnceLock;
use std::time::Duration;

use moka::future::Cache;

const STOCKBIT_SCRAPE_TTL_SECS: u64 = 15 * 60;
const DEFAULT_MOKA_MAX_ENTRIES: u64 = 10_000;

static STOCKBIT_SCRAPE_CACHE: OnceLock<Cache<String, ()>> = OnceLock::new();

fn cache() -> &'static Cache<String, ()> {
    STOCKBIT_SCRAPE_CACHE.get_or_init(|| {
        let max_entries = std::env::var("PORTOFOLIO_HISTORY_MOKA_MAX_ENTRIES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MOKA_MAX_ENTRIES);

        Cache::builder()
            .max_capacity(max_entries)
            .time_to_live(Duration::from_secs(STOCKBIT_SCRAPE_TTL_SECS))
            .build()
    })
}

/// `true` bila emiten pernah scrape sukses dalam window TTL (15 menit).
pub async fn is_fresh(emiten: &str) -> bool {
    cache().get(emiten).await.is_some()
}

/// Tandai scrape Stockbit sukses untuk emiten (reset TTL 15 menit).
pub async fn mark_scraped(emiten: &str) {
    cache().insert(emiten.to_string(), ()).await;
}
