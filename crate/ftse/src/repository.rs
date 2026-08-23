use futures::TryStreamExt;
use scylla::client::session::Session;

use crate::model::{FtseRow, KEYSPACE, TABLE};

const FIND_ALL: &str = "SELECT code, grade, status, updated_at FROM invezgood.ftse";

const FIND_BY_CODE: &str =
    "SELECT code, grade, status, updated_at FROM invezgood.ftse WHERE code = ?";

const UPSERT: &str = "INSERT INTO invezgood.ftse (code, grade, status, updated_at) \
    VALUES (?, ?, ?, ?)";

const UPDATE: &str =
    "UPDATE invezgood.ftse SET grade = ?, status = ?, updated_at = ? WHERE code = ?";

const DELETE_BY_CODE: &str = "DELETE FROM invezgood.ftse WHERE code = ?";

pub async fn find_all(session: &Session) -> Result<Vec<FtseRow>, String> {
    let mut rows = session
        .query_iter(FIND_ALL, &[])
        .await
        .map_err(|e| format!("find_all {KEYSPACE}.{TABLE}: {e}"))?
        .rows_stream::<FtseRow>()
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

pub async fn find_by_code(session: &Session, code: &str) -> Result<Option<FtseRow>, String> {
    let mut rows = session
        .query_iter(FIND_BY_CODE, (code,))
        .await
        .map_err(|e| format!("find_by_code {KEYSPACE}.{TABLE} code={code}: {e}"))?
        .rows_stream::<FtseRow>()
        .map_err(|e| format!("find_by_code stream {KEYSPACE}.{TABLE}: {e}"))?;

    rows.try_next()
        .await
        .map_err(|e| format!("find_by_code row {KEYSPACE}.{TABLE}: {e}"))
}

pub async fn upsert(session: &Session, row: &FtseRow) -> Result<(), String> {
    session
        .query_unpaged(
            UPSERT,
            (
                &row.code,
                row.grade.as_deref(),
                row.status.as_deref(),
                row.updated_at,
            ),
        )
        .await
        .map_err(|e| format!("upsert {KEYSPACE}.{TABLE} code={}: {e}", row.code))?;
    Ok(())
}

pub async fn update(session: &Session, row: &FtseRow) -> Result<(), String> {
    session
        .query_unpaged(
            UPDATE,
            (
                row.grade.as_deref(),
                row.status.as_deref(),
                row.updated_at,
                &row.code,
            ),
        )
        .await
        .map_err(|e| format!("update {KEYSPACE}.{TABLE} code={}: {e}", row.code))?;
    Ok(())
}

pub async fn delete_by_code(session: &Session, code: &str) -> Result<(), String> {
    session
        .query_unpaged(DELETE_BY_CODE, (code,))
        .await
        .map_err(|e| format!("delete_by_code {KEYSPACE}.{TABLE} code={code}: {e}"))?;
    Ok(())
}
