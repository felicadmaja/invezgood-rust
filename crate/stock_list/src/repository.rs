use scylla::client::session::Session;

use crate::model::{StockListKeystatsDb, KEYSPACE, TABLE};

const UPSERT: &str =
    "INSERT INTO invezgood.stock_list (code, name, sector, logo, keystats) VALUES (?, ?, ?, ?, ?)";

pub async fn upsert(
    session: &Session,
    code: &str,
    name: Option<&str>,
    sector: Option<&str>,
    logo: Option<&str>,
    keystats: Option<StockListKeystatsDb>,
) -> Result<(), String> {
    session
        .query_unpaged(UPSERT, (code, name, sector, logo, keystats))
        .await
        .map_err(|e| format!("upsert {KEYSPACE}.{TABLE} code={code}: {e}"))?;
    Ok(())
}
