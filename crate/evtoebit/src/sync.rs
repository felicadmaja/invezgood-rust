//! Logic sync Yahoo Finance → Scylla — dipakai scheduler bulanan dan example seed.

use std::sync::Arc;

use chrono::Utc;
use scylla::client::session::Session;

use crate::cache::MedianCache;
use crate::compute::compute_median;
use crate::repository;
use crate::yahoo::YahooClient;

/// Compute median dari Yahoo Finance, truncate `invezgood.evtoebit`, upsert baris sektor.
/// Bila `cache` diberikan, invalidate + isi ulang cache in-memory agar RPC tidak stale.
pub async fn sync_median_from_yahoo_to_scylla(
    session: Arc<Session>,
    yahoo: Arc<YahooClient>,
    cache: Option<Arc<MedianCache>>,
) -> Result<(usize, String), String> {
    let resp = compute_median(session.clone(), yahoo).await?;
    let message = resp.message.clone();
    repository::truncate_all(session.as_ref()).await?;
    let updated_at = Utc::now();
    let n = repository::upsert_all(session.as_ref(), &resp.rows, updated_at).await?;
    if let Some(cache) = cache {
        cache.invalidate().await;
        cache.store(resp).await;
    }
    Ok((n, message))
}
