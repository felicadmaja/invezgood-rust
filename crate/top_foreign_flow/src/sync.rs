//! Logic sync satu tanggal — dipakai RPC `GetTopForeignFlowByTanggal`.

use std::sync::Arc;

use scylla::client::session::Session;

pub struct SyncOutcome {
    pub saved: usize,
    pub cached: bool,
    pub rows: Vec<crate::model::TopForeignFlowRow>,
}

pub async fn sync_trade_date(
    session: Arc<Session>,
    trade_date: chrono::NaiveDate,
) -> Result<SyncOutcome, String> {
    crate::invezgo::ensure_not_today(trade_date)?;
    crate::invezgo::ensure_not_weekend(trade_date)?;

    let cached = crate::repository::exists_by_date_mv(session.as_ref(), trade_date).await?;

    let saved = if cached {
        0
    } else {
        crate::invezgo::fetch_and_save(session.clone(), trade_date).await?
    };

    let rows = crate::repository::find_by_date(session.as_ref(), trade_date).await?;

    Ok(SyncOutcome {
        saved,
        cached,
        rows,
    })
}
