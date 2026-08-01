use scylla::client::session::Session;

use crate::model::{TopGainerLoserRow, KEYSPACE, TABLE};

const UPSERT: &str = "INSERT INTO invezgood.top_gainer_loser \
    (tahun_bulan_tanggal, code, name, price, change_pct, value, volume, logo, calculated_value, tipe, graph) \
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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
