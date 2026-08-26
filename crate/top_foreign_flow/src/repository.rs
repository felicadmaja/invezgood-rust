use futures::TryStreamExt;
use scylla::client::session::Session;

use crate::model::{TopForeignFlowRow, KEYSPACE, TABLE};

const UPSERT: &str = "INSERT INTO invezgood.top_foreign_flow \
    (tahun_bulan_tanggal, value, code, name, price, change, volume, accum_or_dist) \
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

const DELETE_BY_DATE: &str =
    "DELETE FROM invezgood.top_foreign_flow WHERE tahun_bulan_tanggal = ?";

const FIND_BY_DATE: &str = "SELECT tahun_bulan_tanggal, value, code, name, price, change, volume, accum_or_dist \
    FROM invezgood.top_foreign_flow WHERE tahun_bulan_tanggal = ?";

pub async fn delete_by_date(
    session: &Session,
    trade_date: chrono::NaiveDate,
) -> Result<(), String> {
    session
        .query_unpaged(DELETE_BY_DATE, (trade_date,))
        .await
        .map_err(|e| format!("delete_by_date {KEYSPACE}.{TABLE} date={trade_date}: {e}"))?;
    Ok(())
}

pub async fn upsert(session: &Session, row: &TopForeignFlowRow) -> Result<(), String> {
    session
        .query_unpaged(
            UPSERT,
            (
                row.tahun_bulan_tanggal,
                row.value,
                &row.code,
                row.name.as_deref(),
                row.price,
                row.change,
                row.volume,
                row.accum_or_dist.as_deref(),
            ),
        )
        .await
        .map_err(|e| {
            format!(
                "upsert {KEYSPACE}.{TABLE} date={} code={} value={}: {e}",
                row.tahun_bulan_tanggal, row.code, row.value
            )
        })?;
    Ok(())
}

pub async fn find_by_date(
    session: &Session,
    trade_date: chrono::NaiveDate,
) -> Result<Vec<TopForeignFlowRow>, String> {
    let mut rows = session
        .query_iter(FIND_BY_DATE, (trade_date,))
        .await
        .map_err(|e| format!("find_by_date {KEYSPACE}.{TABLE} date={trade_date}: {e}"))?
        .rows_stream::<TopForeignFlowRow>()
        .map_err(|e| format!("find_by_date stream {KEYSPACE}.{TABLE}: {e}"))?;

    let mut items = Vec::new();
    while let Some(row) = rows
        .try_next()
        .await
        .map_err(|e| format!("find_by_date row {KEYSPACE}.{TABLE}: {e}"))?
    {
        items.push(row);
    }

    Ok(items)
}
