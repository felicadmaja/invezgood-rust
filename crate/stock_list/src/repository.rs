use futures::TryStreamExt;
use scylla::client::session::Session;

use crate::model::{StockListRow, KEYSPACE, TABLE};

const UPSERT: &str =
    "INSERT INTO invezgood.stock_list (code, name, sector, logo) VALUES (?, ?, ?, ?)";

const LIST: &str = "SELECT code, name, sector, logo FROM invezgood.stock_list LIMIT ?";

pub async fn upsert(
    session: &Session,
    code: &str,
    name: Option<&str>,
    sector: Option<&str>,
    logo: Option<&str>,
) -> Result<(), String> {
    session
        .query_unpaged(UPSERT, (code, name, sector, logo))
        .await
        .map_err(|e| format!("upsert {KEYSPACE}.{TABLE} code={code}: {e}"))?;
    Ok(())
}

pub async fn list(session: &Session, limit: i32) -> Result<Vec<StockListRow>, String> {
    let rows = session
        .query_iter(LIST, (limit,))
        .await
        .map_err(|e| format!("list {KEYSPACE}.{TABLE}: {e}"))?
        .rows_stream::<StockListRow>()
        .map_err(|e| format!("list {KEYSPACE}.{TABLE} stream: {e}"))?;

    let mut items = Vec::new();
    let mut rows = rows;
    while let Some(row) = rows
        .try_next()
        .await
        .map_err(|e| format!("list {KEYSPACE}.{TABLE} row: {e}"))?
    {
        items.push(row);
    }

    Ok(items)
}
