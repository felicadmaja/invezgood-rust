use std::sync::Arc;
use std::time::Duration;

use chrono::Local;
use moka::future::Cache;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};

use crate::invezgo;
use crate::pb::ChartBar;

const REDIS_KEY_PREFIX: &str = "chart:";
const REDIS_HOLIDAY_PREFIX: &str = "chart:intraday-holiday:";
const DEFAULT_MOKA_MAX_ENTRIES: u64 = 10_000;
const DEFAULT_CACHE_TTL_SECS: u64 = 24 * 60 * 60;
const HOLIDAY_TTL_SECS: u64 = 24 * 60 * 60;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedChartBar {
    date: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: String,
}

impl From<&ChartBar> for CachedChartBar {
    fn from(bar: &ChartBar) -> Self {
        Self {
            date: bar.date.clone(),
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            volume: bar.volume.clone(),
        }
    }
}

impl From<CachedChartBar> for ChartBar {
    fn from(bar: CachedChartBar) -> Self {
        Self {
            date: bar.date,
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            volume: bar.volume,
        }
    }
}

#[derive(Clone)]
pub struct ChartCache {
    moka: Cache<String, Vec<CachedChartBar>>,
    holiday_moka: Cache<String, bool>,
    redis: redis::Client,
    ttl: Duration,
}

impl ChartCache {
    pub fn new() -> Result<Self, String> {
        let ttl_secs = std::env::var("CHART_CACHE_TTL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_CACHE_TTL_SECS);
        let ttl = Duration::from_secs(ttl_secs);

        let max_entries = std::env::var("CHART_MOKA_MAX_ENTRIES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MOKA_MAX_ENTRIES);

        let moka = Cache::builder()
            .max_capacity(max_entries)
            .time_to_live(ttl)
            .build();

        let holiday_moka = Cache::builder()
            .max_capacity(max_entries)
            .time_to_live(Duration::from_secs(HOLIDAY_TTL_SECS))
            .build();

        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
        let redis = redis::Client::open(redis_url).map_err(|e| format!("redis client: {e}"))?;

        Ok(Self {
            moka,
            holiday_moka,
            redis,
            ttl,
        })
    }

    fn holiday_key(code: &str) -> String {
        let today = Local::now().format("%Y-%m-%d");
        format!("{REDIS_HOLIDAY_PREFIX}{today}:{code}")
    }

    /// True bila code sudah ditandai market libur untuk hari ini.
    pub async fn is_intraday_holiday(&self, code: &str) -> Result<bool, String> {
        let key = Self::holiday_key(code);

        if self.holiday_moka.get(&key).await.unwrap_or(false) {
            return Ok(true);
        }

        let mut conn = self
            .redis
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| format!("redis connect GET holiday {key}: {e}"))?;

        let raw: Option<String> = conn
            .get(&key)
            .await
            .map_err(|e| format!("redis GET holiday {key}: {e}"))?;

        if raw.as_deref() == Some("1") {
            self.holiday_moka.insert(key, true).await;
            return Ok(true);
        }
        Ok(false)
    }

    /// Tandai code sebagai market libur hari ini (skip API berikutnya).
    pub async fn mark_intraday_holiday(&self, code: &str) -> Result<(), String> {
        let key = Self::holiday_key(code);
        self.holiday_moka.insert(key.clone(), true).await;

        let mut conn = self
            .redis
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| format!("redis connect SET holiday {key}: {e}"))?;

        conn.set_ex::<_, _, ()>(&key, "1", HOLIDAY_TTL_SECS)
            .await
            .map_err(|e| format!("redis SETEX holiday {key}: {e}"))?;
        Ok(())
    }

    pub fn cache_key(code: &str, from_date: &str, to_date: &str) -> String {
        format!("{REDIS_KEY_PREFIX}{code}:{from_date}:{to_date}")
    }

    pub async fn get_chart(
        &self,
        code: &str,
        from_date: &str,
        to_date: &str,
    ) -> Result<(Vec<ChartBar>, String), String> {
        let key = Self::cache_key(code, from_date, to_date);

        if let Some(cached) = self.moka.get(&key).await {
            return Ok((
                cached.into_iter().map(ChartBar::from).collect(),
                format!("chart cache HIT moka {key}"),
            ));
        }

        if let Some(cached) = self.redis_get(&key).await? {
            self.moka.insert(key.clone(), cached.clone()).await;
            return Ok((
                cached.into_iter().map(ChartBar::from).collect(),
                format!("chart cache HIT redis {key}"),
            ));
        }

        let items = invezgo::fetch_from_api(code, from_date, to_date).await?;
        let cached: Vec<CachedChartBar> = items.iter().map(CachedChartBar::from).collect();
        self.moka.insert(key.clone(), cached.clone()).await;
        self.redis_set(&key, &cached).await?;
        Ok((
            items,
            format!("chart cache MISS {key} — GET Invezgo"),
        ))
    }

    async fn redis_get(&self, key: &str) -> Result<Option<Vec<CachedChartBar>>, String> {
        let mut conn = self
            .redis
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| format!("redis connect GET {key}: {e}"))?;

        let raw: Option<String> = conn
            .get(key)
            .await
            .map_err(|e| format!("redis GET {key}: {e}"))?;

        let Some(raw) = raw else {
            return Ok(None);
        };

        serde_json::from_str(&raw).map(Some).map_err(|e| format!("redis JSON {key}: {e}"))
    }

    async fn redis_set(&self, key: &str, value: &[CachedChartBar]) -> Result<(), String> {
        let mut conn = self
            .redis
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| format!("redis connect SET {key}: {e}"))?;

        let payload =
            serde_json::to_string(value).map_err(|e| format!("redis serialize {key}: {e}"))?;

        conn.set_ex(key, payload, self.ttl.as_secs())
            .await
            .map_err(|e| format!("redis SETEX {key}: {e}"))
    }
}

pub fn new_shared_cache() -> Result<Arc<ChartCache>, String> {
    Ok(Arc::new(ChartCache::new()?))
}
