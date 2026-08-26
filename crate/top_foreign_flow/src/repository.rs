use futures::TryStreamExt;
use scylla::client::session::Session;

use crate::model::{TopForeignFlowPkRow, TopForeignFlowRow, KEYSPACE, MV_BY_CODE, MV_BY_TAHUN_BULAN_TANGGAL, TABLE};

const UPSERT: &str = "INSERT INTO invezgood.top_foreign_flow \
    (tahun_bulan_tanggal, value, code, name, price, change, volume, accum_or_dist) \
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

const DELETE_BY_DATE: &str =
    "DELETE FROM invezgood.top_foreign_flow WHERE tahun_bulan_tanggal = ?";

const FIND_BY_DATE: &str = "SELECT tahun_bulan_tanggal, value, code, name, price, change, volume, accum_or_dist \
    FROM invezgood.top_foreign_flow WHERE tahun_bulan_tanggal = ?";

const FIND_BY_PK: &str = "SELECT tahun_bulan_tanggal, value, code, name, price, change, volume, accum_or_dist \
    FROM invezgood.top_foreign_flow WHERE tahun_bulan_tanggal = ? AND value = ? AND code = ?";

pub async fn exists_by_date_mv(
    session: &Session,
    trade_date: chrono::NaiveDate,
) -> Result<bool, String> {
    let query = format!(
        "SELECT tahun_bulan_tanggal, value, code \
         FROM {KEYSPACE}.{MV_BY_TAHUN_BULAN_TANGGAL} \
         WHERE tahun_bulan_tanggal = ? LIMIT 1"
    );
    let mut rows = session
        .query_iter(query, (trade_date,))
        .await
        .map_err(|e| {
            format!(
                "exists_by_date_mv {KEYSPACE}.{MV_BY_TAHUN_BULAN_TANGGAL} date={trade_date}: {e}"
            )
        })?
        .rows_stream::<TopForeignFlowPkRow>()
        .map_err(|e| {
            format!(
                "exists_by_date_mv stream {KEYSPACE}.{MV_BY_TAHUN_BULAN_TANGGAL}: {e}"
            )
        })?;

    Ok(rows
        .try_next()
        .await
        .map_err(|e| {
            format!(
                "exists_by_date_mv row {KEYSPACE}.{MV_BY_TAHUN_BULAN_TANGGAL} date={trade_date}: {e}"
            )
        })?
        .is_some())
}

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

pub async fn find_by_code(
    session: &Session,
    code: &str,
) -> Result<Vec<TopForeignFlowRow>, String> {
    let query = format!(
        "SELECT code, tahun_bulan_tanggal, value \
         FROM {KEYSPACE}.{MV_BY_CODE} WHERE code = ?"
    );
    let mut pk_rows = session
        .query_iter(query, (code,))
        .await
        .map_err(|e| format!("find_by_code {KEYSPACE}.{MV_BY_CODE} code={code}: {e}"))?
        .rows_stream::<TopForeignFlowPkRow>()
        .map_err(|e| format!("find_by_code stream {KEYSPACE}.{MV_BY_CODE}: {e}"))?;

    let mut items = Vec::new();
    while let Some(pk) = pk_rows
        .try_next()
        .await
        .map_err(|e| format!("find_by_code pk row {KEYSPACE}.{MV_BY_CODE} code={code}: {e}"))?
    {
        let mut rows = session
            .query_iter(
                FIND_BY_PK,
                (pk.tahun_bulan_tanggal, pk.value, pk.code.as_str()),
            )
            .await
            .map_err(|e| {
                format!(
                    "find_by_pk {KEYSPACE}.{TABLE} date={} code={} value={}: {e}",
                    pk.tahun_bulan_tanggal, pk.code, pk.value
                )
            })?
            .rows_stream::<TopForeignFlowRow>()
            .map_err(|e| format!("find_by_pk stream {KEYSPACE}.{TABLE}: {e}"))?;

        if let Some(row) = rows
            .try_next()
            .await
            .map_err(|e| format!("find_by_pk row {KEYSPACE}.{TABLE}: {e}"))?
        {
            items.push(row);
        }
    }

    Ok(items)
}
