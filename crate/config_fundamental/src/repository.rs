use futures::TryStreamExt;
use scylla::client::session::Session;

use crate::model::{ConfigFundamentalRow, KEYSPACE, TABLE};

const FIND_ALL: &str = "SELECT key, value, description FROM invezgood.config_fundamental";

const FIND_BY_KEY: &str =
    "SELECT key, value, description FROM invezgood.config_fundamental WHERE key = ?";

const UPDATE: &str = "UPDATE invezgood.config_fundamental SET value = ?, description = ? WHERE key = ?";

pub async fn find_all(session: &Session) -> Result<Vec<ConfigFundamentalRow>, String> {
    let rows = session
        .query_iter(FIND_ALL, &[])
        .await
        .map_err(|e| format!("find_all {KEYSPACE}.{TABLE}: {e}"))?
        .rows_stream::<ConfigFundamentalRow>()
        .map_err(|e| format!("find_all stream {KEYSPACE}.{TABLE}: {e}"))?;

    rows.try_collect()
        .await
        .map_err(|e| format!("find_all rows {KEYSPACE}.{TABLE}: {e}"))
}

pub async fn find_by_key(
    session: &Session,
    key: &str,
) -> Result<Option<ConfigFundamentalRow>, String> {
    let mut rows = session
        .query_iter(FIND_BY_KEY, (key,))
        .await
        .map_err(|e| format!("find_by_key {KEYSPACE}.{TABLE} key={key}: {e}"))?
        .rows_stream::<ConfigFundamentalRow>()
        .map_err(|e| format!("find_by_key stream {KEYSPACE}.{TABLE}: {e}"))?;

    rows.try_next()
        .await
        .map_err(|e| format!("find_by_key row {KEYSPACE}.{TABLE} key={key}: {e}"))
}

/// Update value/description. `Ok(false)` bila key belum ada (cegah upsert tidak disengaja).
pub async fn update(
    session: &Session,
    key: &str,
    value: f64,
    description: &str,
) -> Result<bool, String> {
    if find_by_key(session, key).await?.is_none() {
        return Ok(false);
    }

    session
        .query_unpaged(UPDATE, (value, description, key))
        .await
        .map_err(|e| format!("update {KEYSPACE}.{TABLE} key={key}: {e}"))?;
    Ok(true)
}
