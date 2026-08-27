use std::collections::HashMap;

use futures::TryStreamExt;
use scylla::client::session::Session;

use crate::model::{
    ShareholderCompositionPkRow, ShareholderCompositionRow, KEYSPACE, MV_BY_CODE, TABLE,
};

const UPSERT: &str = "INSERT INTO invezgood.shareholder_composition \
    (code, tahun_bulan, detail) VALUES (?, ?, ?)";

const FIND_BY_PK: &str = "SELECT code, tahun_bulan, detail \
    FROM invezgood.shareholder_composition WHERE code = ? AND tahun_bulan = ?";

pub async fn upsert(session: &Session, row: &ShareholderCompositionRow) -> Result<(), String> {
    let detail = row.detail.clone().unwrap_or_default();
    session
        .query_unpaged(
            UPSERT,
            (row.code.as_str(), row.tahun_bulan.as_str(), detail),
        )
        .await
        .map_err(|e| {
            format!(
                "upsert {KEYSPACE}.{TABLE} code={} tahun_bulan={}: {e}",
                row.code, row.tahun_bulan
            )
        })?;
    Ok(())
}

/// Baca MV `shareholder_composition_by_code` → baris penuh dari tabel dasar per PK.
pub async fn find_by_code(
    session: &Session,
    code: &str,
) -> Result<Vec<ShareholderCompositionRow>, String> {
    let query = format!(
        "SELECT code, tahun_bulan FROM {KEYSPACE}.{MV_BY_CODE} WHERE code = ?"
    );
    let mut pk_rows = session
        .query_iter(query, (code,))
        .await
        .map_err(|e| format!("find_by_code {KEYSPACE}.{MV_BY_CODE} code={code}: {e}"))?
        .rows_stream::<ShareholderCompositionPkRow>()
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
                (pk.code.as_str(), pk.tahun_bulan.as_str()),
            )
            .await
            .map_err(|e| {
                format!(
                    "find_by_pk {KEYSPACE}.{TABLE} code={} tahun_bulan={}: {e}",
                    pk.code, pk.tahun_bulan
                )
            })?
            .rows_stream::<ShareholderCompositionRow>()
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

pub fn row_to_proto_detail(detail: Option<HashMap<String, String>>) -> HashMap<String, String> {
    detail.unwrap_or_default()
}
