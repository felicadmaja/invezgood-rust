use futures::TryStreamExt;
use scylla::client::session::Session;

use crate::model::{TopGainerLoserRow, KEYSPACE, TABLE};

const UPSERT: &str = "INSERT INTO invezgood.top_gainer_loser \
    (tahun_bulan_tanggal, code, name, price, change_pct, value, volume, logo, calculated_value, tipe, graph) \
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

const FIND_BY_DATE: &str = "SELECT tahun_bulan_tanggal, code, name, price, change_pct, value, volume, logo, calculated_value, tipe, graph \
    FROM invezgood.top_gainer_loser WHERE tahun_bulan_tanggal = ?";

pub async fn upsert(session: &Session, row: &TopGainerLoserRow) -> Result<(), String> {
    session
        .query_unpaged(
            UPSERT,
            (
                row.tahun_bulan_tanggal,
                &row.code,
                row.name.as_deref(),
                row.price,
                row.change_pct,
                row.value.as_deref(),
                row.volume.as_deref(),
                row.logo.as_deref(),
                row.calculated_value,
                row.tipe.as_deref(),
                row.graph.as_ref(),
            ),
        )
        .await
        .map_err(|e| {
            format!(
                "upsert {KEYSPACE}.{TABLE} date={} code={}: {e}",
                row.tahun_bulan_tanggal, row.code
            )
        })?;
    Ok(())
}

pub async fn find_by_date(
    session: &Session,
    trade_date: chrono::NaiveDate,
) -> Result<Vec<TopGainerLoserRow>, String> {
    let mut rows = session
        .query_iter(FIND_BY_DATE, (trade_date,))
        .await
        .map_err(|e| format!("find_by_date {KEYSPACE}.{TABLE} date={trade_date}: {e}"))?
        .rows_stream::<TopGainerLoserRow>()
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
