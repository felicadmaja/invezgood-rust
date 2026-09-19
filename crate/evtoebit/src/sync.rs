//! Logic sync Yahoo Finance → Scylla — dipakai scheduler harian dan example seed.

use std::sync::Arc;

use chrono::Utc;
use scylla::client::session::Session;

use crate::compute::compute_median;
use crate::pb::GetMedianEvToEbitdaFromYahooFinanceResponse;
use crate::repository;
use crate::yahoo::YahooClient;

/// Truncate + upsert baris agregat ke `invezgood.evtoebit`.
pub async fn persist_median_response(
    session: &Session,
    resp: &GetMedianEvToEbitdaFromYahooFinanceResponse,
) -> Result<usize, String> {
    repository::truncate_all(session).await?;
    let updated_at = Utc::now();
    repository::upsert_all(session, &resp.rows, updated_at).await
}

/// Compute median dari Yahoo Finance, truncate `invezgood.evtoebit`, upsert baris sektor.
pub async fn sync_median_from_yahoo_to_scylla(
    session: Arc<Session>,
    yahoo: Arc<YahooClient>,
) -> Result<(usize, String), String> {
    let resp = compute_median(session.clone(), yahoo, None).await?;
    let message = resp.message.clone();
    let n = persist_median_response(session.as_ref(), &resp).await?;
    Ok((n, message))
}
