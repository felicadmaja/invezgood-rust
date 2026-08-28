use std::sync::Arc;
use std::time::Duration;

use chrono::Local;
use moka::future::Cache;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};

use crate::invezgo;
use crate::pb::{ChartBar, GetCurrentDayChartFromInvezgoResponse};

const REDIS_KEY_PREFIX: &str = "chart:";
const REDIS_INTRADAY_EOD_PREFIX: &str = "chart:intraday-eod:";
const DEFAULT_MOKA_MAX_ENTRIES: u64 = 10_000;
const DEFAULT_CACHE_TTL_SECS: u64 = 24 * 60 * 60;
const INTRADAY_EOD_TTL_SECS: u64 = 24 * 60 * 60;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedIntradayData {
    code: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    avg: f64,
    volume: i64,
    freq: i64,
    value: i64,
    prev: f64,
    bid_price: f64,
    bid_lot: i64,
    bid_freq: i64,
    offer_price: f64,
    offer_lot: i64,
    offer_freq: i64,
    iep: f64,
    iev: i64,
}

impl From<&GetCurrentDayChartFromInvezgoResponse> for CachedIntradayData {
    fn from(r: &GetCurrentDayChartFromInvezgoResponse) -> Self {
        Self {
            code: r.code.clone(),
            open: r.open,
            high: r.high,
            low: r.low,
            close: r.close,
            avg: r.avg,
            volume: r.volume,
            freq: r.freq,
            value: r.value,
            prev: r.prev,
            bid_price: r.bid_price,
            bid_lot: r.bid_lot,
            bid_freq: r.bid_freq,
            offer_price: r.offer_price,
            offer_lot: r.offer_lot,
            offer_freq: r.offer_freq,
            iep: r.iep,
            iev: r.iev,
        }
    }
}

impl From<CachedIntradayData> for GetCurrentDayChartFromInvezgoResponse {
    fn from(c: CachedIntradayData) -> Self {
        let mut resp = Self {
            code: c.code,
            open: c.open,
            high: c.high,
            low: c.low,
            close: c.close,
            avg: c.avg,
            volume: c.volume,
            freq: c.freq,
            value: c.value,
            prev: c.prev,
            bid_price: c.bid_price,
            bid_lot: c.bid_lot,
            bid_freq: c.bid_freq,
            offer_price: c.offer_price,
            offer_lot: c.offer_lot,
            offer_freq: c.offer_freq,
            iep: c.iep,
            iev: c.iev,
            success: true,
            message: "ok".to_string(),
        };
        resp.normalize_intraday_prices();
        resp
    }
}

#[derive(Clone)]
pub struct ChartCache {
    moka: Cache<String, Vec<CachedChartBar>>,
    intraday_eod_moka: Cache<String, CachedIntradayData>,
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

        let intraday_eod_moka = Cache::builder()
            .max_capacity(max_entries)
            .time_to_live(Duration::from_secs(INTRADAY_EOD_TTL_SECS))
            .build();

        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
        let redis = redis::Client::open(redis_url).map_err(|e| format!("redis client: {e}"))?;

        Ok(Self {
            moka,
            intraday_eod_moka,
            redis,
            ttl,
        })
    }

    fn intraday_eod_key(code: &str) -> String {
        let today = Local::now().format("%Y-%m-%d");
        format!("{REDIS_INTRADAY_EOD_PREFIX}{today}:{code}")
    }

    /// Snapshot intraday hari ini: Moka → Redis → None (key per tanggal+code).
    pub async fn get_intraday_eod(
        &self,
        code: &str,
    ) -> Result<Option<(GetCurrentDayChartFromInvezgoResponse, String)>, String> {
        let key = Self::intraday_eod_key(code);

        if let Some(cached) = self.intraday_eod_moka.get(&key).await {
            return Ok(Some((
                cached.into(),
                format!("intraday eod HIT moka {key}"),
            )));
        }

        let mut conn = self
            .redis
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| format!("redis connect GET eod {key}: {e}"))?;

        let raw: Option<String> = conn
            .get(&key)
            .await
            .map_err(|e| format!("redis GET eod {key}: {e}"))?;

        let Some(raw) = raw else {
            return Ok(None);
        };

        let cached: CachedIntradayData =
            serde_json::from_str(&raw).map_err(|e| format!("redis JSON eod {key}: {e}"))?;
        self.intraday_eod_moka
            .insert(key.clone(), cached.clone())
            .await;
        Ok(Some((
            cached.into(),
            format!("intraday eod HIT redis {key}"),
        )))
    }

    /// Simpan snapshot intraday hari ini (TTL 24 jam, key per tanggal+code).
    pub async fn set_intraday_eod(
        &self,
        code: &str,
        data: &GetCurrentDayChartFromInvezgoResponse,
    ) -> Result<(), String> {
        let key = Self::intraday_eod_key(code);
        let cached = CachedIntradayData::from(data);
        self.intraday_eod_moka
            .insert(key.clone(), cached.clone())
            .await;

        let mut conn = self
            .redis
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| format!("redis connect SET eod {key}: {e}"))?;

        let payload = serde_json::to_string(&cached)
            .map_err(|e| format!("redis serialize eod {key}: {e}"))?;

        conn.set_ex::<_, _, ()>(&key, payload, INTRADAY_EOD_TTL_SECS)
            .await
            .map_err(|e| format!("redis SETEX eod {key}: {e}"))?;
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
        self.get_chart_cached(
            code,
            from_date,
            to_date,
            invezgo::fetch_from_api(code, from_date, to_date),
        )
        .await
    }

    /// Cache history IHSG (index COMPOSITE) — pola sama `get_chart`.
    pub async fn get_ihsg_chart(
        &self,
        from_date: &str,
        to_date: &str,
    ) -> Result<(Vec<ChartBar>, String), String> {
        self.get_chart_cached(
            "COMPOSITE",
            from_date,
            to_date,
            invezgo::fetch_ihsg_from_api(from_date, to_date),
        )
        .await
    }

    async fn get_chart_cached<F>(
        &self,
        code: &str,
        from_date: &str,
        to_date: &str,
        fetch: F,
    ) -> Result<(Vec<ChartBar>, String), String>
    where
        F: std::future::Future<Output = Result<Vec<ChartBar>, String>>,
    {
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

        let items = fetch.await?;
        let cached: Vec<CachedChartBar> = items.iter().map(CachedChartBar::from).collect();
        self.moka.insert(key.clone(), cached.clone()).await;
        self.redis_set(&key, &cached).await?;
        Ok((items, format!("chart cache MISS {key} — GET Invezgo")))
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

        serde_json::from_str(&raw)
            .map(Some)
            .map_err(|e| format!("redis JSON {key}: {e}"))
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
