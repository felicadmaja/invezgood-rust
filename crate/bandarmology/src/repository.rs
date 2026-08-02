use futures::TryStreamExt;
use scylla::client::session::Session;

use crate::model::{BandarmologyRow, KEYSPACE, TABLE};

const FIND_BY_CODE_DATE: &str = "SELECT code, tahun_bulan_tanggal, bandarmology, updated_at \
    FROM invezgood.bandarmology WHERE code = ? AND tahun_bulan_tanggal = ?";

const UPSERT: &str = "INSERT INTO invezgood.bandarmology \
    (code, tahun_bulan_tanggal, bandarmology, updated_at) VALUES (?, ?, ?, ?)";

pub fn has_bandarmology_data(row: &BandarmologyRow) -> bool {
    row.bandarmology
        .as_ref()
        .is_some_and(|entries| !entries.is_empty())
}

pub async fn find_by_code_and_date(
    session: &Session,
    code: &str,
    trade_date: chrono::NaiveDate,
) -> Result<Option<BandarmologyRow>, String> {
    let mut rows = session
        .query_iter(FIND_BY_CODE_DATE, (code, trade_date))
        .await
        .map_err(|e| {
            format!("find_by_code_and_date {KEYSPACE}.{TABLE} code={code} date={trade_date}: {e}")
        })?
        .rows_stream::<BandarmologyRow>()
        .map_err(|e| format!("find_by_code_and_date stream {KEYSPACE}.{TABLE}: {e}"))?;

    rows.try_next()
        .await
        .map_err(|e| format!("find_by_code_and_date row {KEYSPACE}.{TABLE}: {e}"))
}

pub async fn upsert(session: &Session, row: &BandarmologyRow) -> Result<(), String> {
    session
        .query_unpaged(
            UPSERT,
            (
                &row.code,
                row.tahun_bulan_tanggal,
                row.bandarmology.as_ref(),
                row.updated_at,
            ),
        )
        .await
        .map_err(|e| {
            format!(
                "upsert {KEYSPACE}.{TABLE} code={} date={}: {e}",
                row.code, row.tahun_bulan_tanggal
            )
        })?;
    Ok(())
}
