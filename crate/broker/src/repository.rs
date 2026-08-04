use futures::TryStreamExt;
use scylla::client::session::Session;

use crate::model::{
    BrokerRow, BrokerStalkerRow, KEYSPACE, TABLE, TABLE_BROKER_STALKER,
};

const FIND_ALL: &str = "SELECT broker_code, name, tipe, asosiasi, catatan, is_huge, is_top, updated_at \
    FROM invezgood.broker";

const FIND_BY_CODE: &str = "SELECT broker_code, name, tipe, asosiasi, catatan, is_huge, is_top, updated_at \
    FROM invezgood.broker WHERE broker_code = ?";

const UPSERT: &str = "INSERT INTO invezgood.broker \
    (broker_code, name, tipe, asosiasi, catatan, is_huge, is_top, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

const DELETE_BY_CODE: &str = "DELETE FROM invezgood.broker WHERE broker_code = ?";

const UPDATE_IS_HUGE: &str = "UPDATE invezgood.broker SET is_huge = ?, updated_at = ? WHERE broker_code = ?";

const UPDATE_IS_TOP: &str = "UPDATE invezgood.broker SET is_top = ?, updated_at = ? WHERE broker_code = ?";

const FIND_STALKER: &str = "SELECT broker_code, tahun_bulan, summary, list \
    FROM invezgood.broker_stalker WHERE broker_code = ? AND tahun_bulan = ?";

const UPSERT_STALKER: &str = "INSERT INTO invezgood.broker_stalker \
    (broker_code, tahun_bulan, summary, list) VALUES (?, ?, ?, ?)";

pub async fn find_all(session: &Session) -> Result<Vec<BrokerRow>, String> {
    let mut rows = session
        .query_iter(FIND_ALL, &[])
        .await
        .map_err(|e| format!("find_all {KEYSPACE}.{TABLE}: {e}"))?
        .rows_stream::<BrokerRow>()
        .map_err(|e| format!("find_all stream {KEYSPACE}.{TABLE}: {e}"))?;

    let mut items = Vec::new();
    while let Some(row) = rows
        .try_next()
        .await
        .map_err(|e| format!("find_all row {KEYSPACE}.{TABLE}: {e}"))?
    {
        items.push(row);
    }

    Ok(items)
}

pub async fn find_by_code(
    session: &Session,
    broker_code: &str,
) -> Result<Option<BrokerRow>, String> {
    let mut rows = session
        .query_iter(FIND_BY_CODE, (broker_code,))
        .await
        .map_err(|e| format!("find_by_code {KEYSPACE}.{TABLE} code={broker_code}: {e}"))?
        .rows_stream::<BrokerRow>()
        .map_err(|e| format!("find_by_code stream {KEYSPACE}.{TABLE}: {e}"))?;

    rows.try_next()
        .await
        .map_err(|e| format!("find_by_code row {KEYSPACE}.{TABLE}: {e}"))
}

pub async fn upsert(session: &Session, row: &BrokerRow) -> Result<(), String> {
    session
        .query_unpaged(
            UPSERT,
            (
                &row.broker_code,
                row.name.as_deref(),
                row.tipe,
                row.asosiasi.as_deref(),
                row.catatan.as_deref(),
                row.is_huge,
                row.is_top,
                row.updated_at,
            ),
        )
        .await
        .map_err(|e| {
            format!(
                "upsert {KEYSPACE}.{TABLE} code={}: {e}",
                row.broker_code
            )
        })?;
    Ok(())
}

pub async fn update_by_code(
    session: &Session,
    row: &BrokerRow,
) -> Result<(), String> {
    upsert(session, row).await
}

pub async fn delete_by_code(session: &Session, broker_code: &str) -> Result<(), String> {
    session
        .query_unpaged(DELETE_BY_CODE, (broker_code,))
        .await
        .map_err(|e| format!("delete_by_code {KEYSPACE}.{TABLE} code={broker_code}: {e}"))?;
    Ok(())
}

pub async fn update_is_huge(
    session: &Session,
    broker_code: &str,
    is_huge: bool,
    updated_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), String> {
    session
        .query_unpaged(UPDATE_IS_HUGE, (is_huge, updated_at, broker_code))
        .await
        .map_err(|e| {
            format!("update_is_huge {KEYSPACE}.{TABLE} code={broker_code}: {e}")
        })?;
    Ok(())
}

pub async fn update_is_top(
    session: &Session,
    broker_code: &str,
    is_top: bool,
    updated_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), String> {
    session
        .query_unpaged(UPDATE_IS_TOP, (is_top, updated_at, broker_code))
        .await
        .map_err(|e| {
            format!("update_is_top {KEYSPACE}.{TABLE} code={broker_code}: {e}")
        })?;
    Ok(())
}

pub async fn find_stalker(
    session: &Session,
    broker_code: &str,
    tahun_bulan: &str,
) -> Result<Option<BrokerStalkerRow>, String> {
    let mut rows = session
        .query_iter(FIND_STALKER, (broker_code, tahun_bulan))
        .await
        .map_err(|e| {
            format!(
                "find_stalker {KEYSPACE}.{TABLE_BROKER_STALKER} code={broker_code} bulan={tahun_bulan}: {e}"
            )
        })?
        .rows_stream::<BrokerStalkerRow>()
        .map_err(|e| {
            format!("find_stalker stream {KEYSPACE}.{TABLE_BROKER_STALKER}: {e}")
        })?;

    rows.try_next().await.map_err(|e| {
        format!("find_stalker row {KEYSPACE}.{TABLE_BROKER_STALKER}: {e}")
    })
}

pub async fn upsert_stalker(session: &Session, row: &BrokerStalkerRow) -> Result<(), String> {
    session
        .query_unpaged(
            UPSERT_STALKER,
            (
                &row.broker_code,
                &row.tahun_bulan,
                row.summary.as_ref(),
                row.list.as_ref(),
            ),
        )
        .await
        .map_err(|e| {
            format!(
                "upsert_stalker {KEYSPACE}.{TABLE_BROKER_STALKER} code={} bulan={}: {e}",
                row.broker_code, row.tahun_bulan
            )
        })?;
    Ok(())
}
