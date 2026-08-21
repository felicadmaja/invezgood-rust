//! Cache Moka 1 menit untuk `GetAllStocks` — key tunggal global (daftar saham sama untuk semua user).

use std::time::Duration;

use moka::future::Cache;

use crate::pb::GetAllStocksResponse;

const CACHE_KEY: &str = "all";
const TTL_SECS: u64 = 60;

#[derive(Clone)]
pub struct AllStocksCache {
    moka: Cache<String, GetAllStocksResponse>,
}

impl AllStocksCache {
    pub fn new() -> Self {
        let moka = Cache::builder()
            .max_capacity(1)
            .time_to_live(Duration::from_secs(TTL_SECS))
            .build();
        Self { moka }
    }

    pub async fn get(&self) -> Option<GetAllStocksResponse> {
        self.moka.get(CACHE_KEY).await
    }

    pub async fn set(&self, response: GetAllStocksResponse) {
        self.moka.insert(CACHE_KEY.to_string(), response).await;
    }
}
