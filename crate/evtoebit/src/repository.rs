use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use scylla::client::session::Session;

use crate::model::{EvToEbitRow, KEYSPACE, TABLE};
use crate::pb::MedianEvMultipleRow;

const FIND_ALL: &str =
    "SELECT sektor, sub_sektor, n, median_ev_ebit, p25_ev_ebit, p75_ev_ebit, median_ev_ebitda, flag, updated_at FROM invezgood.evtoebit";

const UPSERT: &str = "INSERT INTO invezgood.evtoebit (sektor, sub_sektor, n, median_ev_ebit, p25_ev_ebit, p75_ev_ebit, median_ev_ebitda, flag, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

pub fn row_from_pb(row: &MedianEvMultipleRow, updated_at: DateTime<Utc>) -> EvToEbitRow {
    EvToEbitRow {
        sektor: row.sektor.clone(),
        sub_sektor: row.sub_sektor.clone(),
        n: row.n,
        median_ev_ebit: row.median_ev_ebit,
        p25_ev_ebit: row.p25_ev_ebit,
        p75_ev_ebit: row.p75_ev_ebit,
        median_ev_ebitda: row.median_ev_ebitda,
        flag: if row.flag.is_empty() {
            None
        } else {
            Some(row.flag.clone())
        },
        updated_at,
    }
}

pub async fn find_all(session: &Session) -> Result<Vec<EvToEbitRow>, String> {
    let rows = session
        .query_iter(FIND_ALL, &[])
        .await
        .map_err(|e| format!("find_all {KEYSPACE}.{TABLE}: {e}"))?
        .rows_stream::<EvToEbitRow>()
        .map_err(|e| format!("find_all stream {KEYSPACE}.{TABLE}: {e}"))?;

    rows.try_collect()
        .await
        .map_err(|e| format!("find_all rows {KEYSPACE}.{TABLE}: {e}"))
}

/// Upsert semua baris agregat; `updated_at` sama untuk seluruh batch.
pub async fn upsert_all(
    session: &Session,
    rows: &[MedianEvMultipleRow],
    updated_at: DateTime<Utc>,
) -> Result<usize, String> {
    let mut count = 0usize;
    for row in rows {
        let db_row = row_from_pb(row, updated_at);
        session
            .query_unpaged(
                UPSERT,
                (
                    &db_row.sektor,
                    &db_row.sub_sektor,
                    db_row.n,
                    db_row.median_ev_ebit,
                    db_row.p25_ev_ebit,
                    db_row.p75_ev_ebit,
                    db_row.median_ev_ebitda,
                    db_row.flag.as_deref(),
                    db_row.updated_at,
                ),
            )
            .await
            .map_err(|e| {
                format!(
                    "upsert {KEYSPACE}.{TABLE} sektor={} sub_sektor={}: {e}",
                    db_row.sektor, db_row.sub_sektor
                )
            })?;
        count += 1;
    }
    Ok(count)
}
