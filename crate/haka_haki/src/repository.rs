use futures::TryStreamExt;
use scylla::client::session::Session;

use crate::model::{HakaHakiRow, KEYSPACE, MV_BY_AGG_CODE_TAHUN_BULAN_TANGGAL, TABLE};

const COLUMNS: &str = "code, tahun_bulan_tanggal, jam_menit, agg_code_tahun_bulan_tanggal, volume, buy, sell";

const UPSERT: &str = "INSERT INTO invezgood.haka_haki \
    (code, tahun_bulan_tanggal, jam_menit, agg_code_tahun_bulan_tanggal, volume, buy, sell) \
    VALUES (?, ?, ?, ?, ?, ?, ?)";

pub async fn upsert(session: &Session, row: &HakaHakiRow) -> Result<(), String> {
    session
        .query_unpaged(
            UPSERT,
            (
                &row.code,
                row.tahun_bulan_tanggal,
                &row.jam_menit,
                &row.agg_code_tahun_bulan_tanggal,
                row.volume,
                row.buy,
                row.sell,
            ),
        )
        .await
        .map_err(|e| {
            format!(
                "upsert {KEYSPACE}.{TABLE} code={} date={} jam={}: {e}",
                row.code, row.tahun_bulan_tanggal, row.jam_menit
            )
        })?;
    Ok(())
}

pub async fn upsert_many(session: &Session, rows: &[HakaHakiRow]) -> Result<usize, String> {
    let mut n = 0usize;
    for row in rows {
        upsert(session, row).await?;
        n += 1;
    }
    Ok(n)
}

pub async fn find_by_agg_code_tahun_bulan_tanggal(
    session: &Session,
    agg: &str,
) -> Result<Vec<HakaHakiRow>, String> {
    let q = format!(
        "SELECT {COLUMNS} FROM {KEYSPACE}.{MV_BY_AGG_CODE_TAHUN_BULAN_TANGGAL} \
         WHERE agg_code_tahun_bulan_tanggal = ?"
    );

    let mut rows = session
        .query_iter(q.as_str(), (agg,))
        .await
        .map_err(|e| {
            format!(
                "find_by_agg {KEYSPACE}.{MV_BY_AGG_CODE_TAHUN_BULAN_TANGGAL} agg={agg}: {e}"
            )
        })?
        .rows_stream::<HakaHakiRow>()
        .map_err(|e| format!("find_by_agg stream {KEYSPACE}.{MV_BY_AGG_CODE_TAHUN_BULAN_TANGGAL}: {e}"))?;

    let mut out = Vec::new();
    while let Some(row) = rows
        .try_next()
        .await
        .map_err(|e| format!("find_by_agg row {KEYSPACE}.{MV_BY_AGG_CODE_TAHUN_BULAN_TANGGAL}: {e}"))?
    {
        out.push(row);
    }
    Ok(out)
}
