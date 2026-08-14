//! Cache Moka → Redis untuk `GetHakaHakiFromInvezgo` hari ini (EOD).

use std::sync::Arc;
use std::time::Duration;

use chrono::Local;
use moka::future::Cache;
use prost::Message;
use redis::AsyncCommands;

use crate::pb::GetHakaHakiFromInvezgoResponse;

const REDIS_EOD_PREFIX: &str = "invezgood:haka_haki:intraday-eod:";
const DEFAULT_MOKA_MAX_ENTRIES: u64 = 10_000;
const EOD_TTL_SECS: u64 = 24 * 60 * 60;

#[derive(Clone)]
pub struct IntradayCache {
    eod_moka: Cache<String, GetHakaHakiFromInvezgoResponse>,
    redis: redis::Client,
}

impl IntradayCache {
    pub fn new() -> Result<Self, String> {
        let max_entries = std::env::var("HAKA_HAKI_MOKA_MAX_ENTRIES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MOKA_MAX_ENTRIES);

        let eod_moka = Cache::builder()
            .max_capacity(max_entries)
            .time_to_live(Duration::from_secs(EOD_TTL_SECS))
            .build();

        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
        let redis = redis::Client::open(redis_url).map_err(|e| format!("redis client: {e}"))?;

        Ok(Self {
            eod_moka,
            redis,
        })
    }

    fn eod_key(code: &str, range: i32) -> String {
        let today = Local::now().format("%Y-%m-%d");
        format!(
            "{REDIS_EOD_PREFIX}{today}:{}:{range}",
            code.trim().to_ascii_uppercase()
        )
    }

    pub async fn get_intraday_eod(
        &self,
        code: &str,
        range: i32,
    ) -> Result<Option<(GetHakaHakiFromInvezgoResponse, String)>, String> {
        let key = Self::eod_key(code, range);

        if let Some(cached) = self.eod_moka.get(&key).await {
            if !cached.items.is_empty() {
                return Ok(Some((
                    cached,
                    format!("haka_haki eod HIT moka {key}"),
                )));
            }
        }

        let mut conn = self
            .redis
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| format!("redis connect GET eod {key}: {e}"))?;

        let raw: Option<Vec<u8>> = conn
            .get(&key)
            .await
            .map_err(|e| format!("redis GET eod {key}: {e}"))?;

        let Some(raw) = raw else {
            return Ok(None);
        };

        let cached = GetHakaHakiFromInvezgoResponse::decode(raw.as_slice())
            .map_err(|e| format!("redis decode eod {key}: {e}"))?;
        if cached.items.is_empty() {
            return Ok(None);
        }
        self.eod_moka.insert(key.clone(), cached.clone()).await;
        Ok(Some((
            cached,
            format!("haka_haki eod HIT redis {key}"),
        )))
    }

    pub async fn set_intraday_eod(
        &self,
        code: &str,
        range: i32,
        resp: &GetHakaHakiFromInvezgoResponse,
    ) -> Result<(), String> {
        if !resp.success || resp.items.is_empty() {
            return Ok(());
        }
        let key = Self::eod_key(code, range);
        self.eod_moka.insert(key.clone(), resp.clone()).await;

        let mut conn = self
            .redis
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| format!("redis connect SET eod {key}: {e}"))?;

        let payload = resp.encode_to_vec();
        conn.set_ex::<_, _, ()>(&key, payload, EOD_TTL_SECS)
            .await
            .map_err(|e| format!("redis SETEX eod {key}: {e}"))?;
        Ok(())
    }
}

pub fn new_shared_intraday_cache() -> Result<Arc<IntradayCache>, String> {
    Ok(Arc::new(IntradayCache::new()?))
}
