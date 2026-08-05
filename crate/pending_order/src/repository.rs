use futures::stream::{self, StreamExt, TryStreamExt};
use scylla::client::session::Session;

use crate::model::{PendingOrderRow, KEYSPACE, MV_BY_EMITEN, TABLE};

const TOKEN_SEGMENTS: usize = 16;
const SCAN_CONCURRENCY: usize = 8;

const COLUMNS: &str = "order_id, tahun_bulan_tanggal, emiten_name, status, message, side, \
    time_open, lot_open, lot_done, price_order, amount_open, amount_match, amount_match_total, \
    is_gtc, updated_at";

pub async fn find_all(session: &Session) -> Result<Vec<PendingOrderRow>, String> {
    let table = format!("{KEYSPACE}.{TABLE}");
    let scan_q = format!(
        "SELECT {COLUMNS} FROM {table} WHERE token(order_id) >= ? AND token(order_id) <= ?"
    );

    let segment_rows: Vec<Vec<PendingOrderRow>> = stream::iter(0..TOKEN_SEGMENTS)
        .map(|seg| {
            let scan_q = scan_q.clone();
            let start = token_segment_start(seg, TOKEN_SEGMENTS);
            let end = token_segment_end(seg, TOKEN_SEGMENTS);
            async move {
                let mut rows = session
                    .query_iter(scan_q.as_str(), (start, end))
                    .await
                    .map_err(|e| format!("find_all scan {KEYSPACE}.{TABLE}: {e}"))?
                    .rows_stream::<PendingOrderRow>()
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
) -> Result<Vec<PendingOrderRow>, String> {
    let q = format!(
        "SELECT {COLUMNS} FROM {KEYSPACE}.{MV_BY_EMITEN} WHERE emiten_name = ?"
    );

    let mut rows = session
        .query_iter(q.as_str(), (emiten_name,))
        .await
        .map_err(|e| format!("find_by_emiten_name {KEYSPACE}.{MV_BY_EMITEN} code={emiten_name}: {e}"))?
        .rows_stream::<PendingOrderRow>()
        .map_err(|e| format!("find_by_emiten_name stream {KEYSPACE}.{MV_BY_EMITEN}: {e}"))?;

    let mut out = Vec::new();
    while let Some(row) = rows
        .try_next()
        .await
        .map_err(|e| format!("find_by_emiten_name row {KEYSPACE}.{MV_BY_EMITEN}: {e}"))?
    {
        out.push(row);
    }
    Ok(out)
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
