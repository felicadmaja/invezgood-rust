use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use scylla::client::session::Session;

use crate::model::{EvToEbitRow, KEYSPACE, TABLE};
use crate::pb::MedianEvMultipleRow;

const FIND_ALL: &str =
    "SELECT sektor, n, median_ev_ebit, p25_ev_ebit, p75_ev_ebit, median_ev_ebitda, flag, updated_at FROM invezgood.evtoebit";

const UPSERT: &str = "INSERT INTO invezgood.evtoebit (sektor, n, median_ev_ebit, p25_ev_ebit, p75_ev_ebit, median_ev_ebitda, flag, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

const TRUNCATE: &str = "TRUNCATE invezgood.evtoebit";

const DROP_TABLE: &str = "DROP TABLE IF EXISTS invezgood.evtoebit";

const CREATE_TABLE: &str = r#"
CREATE TABLE invezgood.evtoebit (
    sektor           text PRIMARY KEY,
    n                int,
    median_ev_ebit   double,
    p25_ev_ebit      double,
    p75_ev_ebit      double,
    median_ev_ebitda double,
    flag             text,
    updated_at       timestamp
)
"#;

pub fn row_to_pb(row: &EvToEbitRow) -> MedianEvMultipleRow {
    MedianEvMultipleRow {
        sektor: row.sektor.clone(),
        n: row.n,
        median_ev_ebit: row.median_ev_ebit,
        p25_ev_ebit: row.p25_ev_ebit,
        p75_ev_ebit: row.p75_ev_ebit,
        median_ev_ebitda: row.median_ev_ebitda,
        flag: row.flag.clone().unwrap_or_default(),
        updated_at: Some(row.updated_at.timestamp()),
    }
}

pub fn row_from_pb(row: &MedianEvMultipleRow, updated_at: DateTime<Utc>) -> EvToEbitRow {
    EvToEbitRow {
        sektor: row.sektor.clone(),
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

pub async fn truncate_all(session: &Session) -> Result<(), String> {
    session
        .query_unpaged(TRUNCATE, &[])
        .await
        .map_err(|e| format!("truncate {KEYSPACE}.{TABLE}: {e}"))?;
    Ok(())
}

pub async fn recreate_table(session: &Session) -> Result<(), String> {
    session
        .query_unpaged(DROP_TABLE, &[])
        .await
        .map_err(|e| format!("drop {KEYSPACE}.{TABLE}: {e}"))?;
    session
        .query_unpaged(CREATE_TABLE, &[])
        .await
        .map_err(|e| format!("create {KEYSPACE}.{TABLE}: {e}"))?;
    Ok(())
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
            .map_err(|e| format!("upsert {KEYSPACE}.{TABLE} sektor={}: {e}", db_row.sektor))?;
        count += 1;
    }
    Ok(count)
}
