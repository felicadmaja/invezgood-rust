use futures::stream::{self, StreamExt, TryStreamExt};
use scylla::client::session::Session;

use crate::model::{PortofolioEquityRow, PortofolioRow, EQUITY_TABLE, KEYSPACE, TABLE};

const TOKEN_SEGMENTS: usize = 16;
const SCAN_CONCURRENCY: usize = 8;

const PORTOFOLIO_COLUMNS: &str = "emiten_name, long_name, emiten_icon, balance_lot, available_lot, \
    average_price, current_price, invested, market_value, potential_p_l, percentage";

const FIND_BY_EMITEN: &str = "SELECT emiten_name, long_name, emiten_icon, balance_lot, available_lot, \
    average_price, current_price, invested, market_value, potential_p_l, percentage \
    FROM invezgood.portofolio WHERE emiten_name = ?";

const FIND_ALL_EQUITY: &str = "SELECT nama, value FROM invezgood.portofolio_equity";

pub async fn find_all(session: &Session) -> Result<Vec<PortofolioRow>, String> {
    let table = format!("{KEYSPACE}.{TABLE}");
    let scan_q = format!(
        "SELECT {PORTOFOLIO_COLUMNS} FROM {table} \
         WHERE token(emiten_name) >= ? AND token(emiten_name) <= ?"
    );

    let segment_rows: Vec<Vec<PortofolioRow>> = stream::iter(0..TOKEN_SEGMENTS)
        .map(|seg| {
            let scan_q = scan_q.clone();
            let start = token_segment_start(seg, TOKEN_SEGMENTS);
            let end = token_segment_end(seg, TOKEN_SEGMENTS);
            async move {
                let mut rows = session
                    .query_iter(scan_q.as_str(), (start, end))
                    .await
                    .map_err(|e| format!("find_all scan {KEYSPACE}.{TABLE}: {e}"))?
                    .rows_stream::<PortofolioRow>()
                    .map_err(|e| format!("find_all stream {KEYSPACE}.{TABLE}: {e}"))?;

                let mut out = Vec::new();
                while let Some(row) = rows
                    .try_next()
                    .await
                    .map_err(|e| format!("find_all row {KEYSPACE}.{TABLE}: {e}"))?
                {
                    out.push(row);
                }
                Ok(out)
            }
        })
        .buffer_unordered(SCAN_CONCURRENCY)
        .try_collect()
        .await
        .map_err(|e: String| e)?;

    Ok(segment_rows.into_iter().flatten().collect())
}

pub async fn find_by_emiten_name(
    session: &Session,
    emiten_name: &str,
) -> Result<Option<PortofolioRow>, String> {
    let mut rows = session
        .query_iter(FIND_BY_EMITEN, (emiten_name,))
        .await
        .map_err(|e| format!("find_by_emiten_name {KEYSPACE}.{TABLE} code={emiten_name}: {e}"))?
        .rows_stream::<PortofolioRow>()
        .map_err(|e| format!("find_by_emiten_name stream {KEYSPACE}.{TABLE}: {e}"))?;

    rows.try_next()
        .await
        .map_err(|e| format!("find_by_emiten_name row {KEYSPACE}.{TABLE}: {e}"))
}

#[allow(dead_code)]
pub async fn find_all_equity(session: &Session) -> Result<Vec<PortofolioEquityRow>, String> {
    let mut rows = session
        .query_iter(FIND_ALL_EQUITY, &[])
        .await
        .map_err(|e| format!("find_all_equity {KEYSPACE}.{EQUITY_TABLE}: {e}"))?
        .rows_stream::<PortofolioEquityRow>()
        .map_err(|e| format!("find_all_equity stream {KEYSPACE}.{EQUITY_TABLE}: {e}"))?;

    let mut items = Vec::new();
    while let Some(row) = rows
        .try_next()
        .await
        .map_err(|e| format!("find_all_equity row {KEYSPACE}.{EQUITY_TABLE}: {e}"))?
    {
        items.push(row);
    }
    Ok(items)
}

fn token_segment_start(seg: usize, num_seg: usize) -> i64 {
    if seg == 0 {
        i64::MIN
    } else {
        let span = (i64::MAX as i128) - (i64::MIN as i128);
        (i64::MIN as i128 + (span * seg as i128) / num_seg as i128) as i64
    }
}

fn token_segment_end(seg: usize, num_seg: usize) -> i64 {
    if seg + 1 == num_seg {
        i64::MAX
    } else {
        token_segment_start(seg + 1, num_seg).saturating_sub(1)
    }
}
