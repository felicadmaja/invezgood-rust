use std::sync::Arc;

use moka::future::Cache;
use scylla::client::session::Session;

use crate::compute::{cache_ttl, compute_median};
use crate::pb::GetMedianEvToEbitdaResponse;
use crate::yahoo::YahooClient;

const CACHE_KEY: &str = "median";

pub struct MedianCache {
    inner: Cache<String, Arc<GetMedianEvToEbitdaResponse>>,
    yahoo: Arc<YahooClient>,
}

impl MedianCache {
    pub fn new(yahoo: Arc<YahooClient>) -> Self {
        Self {
            inner: Cache::builder().time_to_live(cache_ttl()).build(),
            yahoo,
        }
    }

    pub async fn get_or_compute(
        &self,
        session: Arc<Session>,
    ) -> Result<Arc<GetMedianEvToEbitdaResponse>, String> {
        let yahoo = Arc::clone(&self.yahoo);
        self.inner
            .try_get_with(CACHE_KEY.to_string(), async move {
                compute_median(session, yahoo)
                    .await
                    .map(Arc::new)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
            })
            .await
            .map_err(|e| e.to_string())
    }
}

pub fn new_shared_median_cache(yahoo: Arc<YahooClient>) -> Arc<MedianCache> {
    Arc::new(MedianCache::new(yahoo))
}
