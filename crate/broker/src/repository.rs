use futures::TryStreamExt;
use scylla::client::session::Session;

use crate::model::{BrokerRow, KEYSPACE, TABLE};

const FIND_ALL: &str = "SELECT broker_code, name, tipe, asosiasi, catatan, updated_at \
    FROM invezgood.broker";

const FIND_BY_CODE: &str = "SELECT broker_code, name, tipe, asosiasi, catatan, updated_at \
    FROM invezgood.broker WHERE broker_code = ?";

const UPSERT: &str = "INSERT INTO invezgood.broker \
    (broker_code, name, tipe, asosiasi, catatan, updated_at) VALUES (?, ?, ?, ?, ?, ?)";

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
                row.tipe.as_deref(),
                row.asosiasi.as_deref(),
                row.catatan.as_deref(),
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
