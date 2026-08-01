use std::collections::HashSet;

use futures::TryStreamExt;
use scylla::client::session::Session;
use scylla::DeserializeRow;
use scylla::statement::prepared::PreparedStatement;

use crate::model::{UserRow, KEYSPACE, TABLE};

const SCAN_RANGE: &str =
    "SELECT email, nama, password, role FROM invezgood.user WHERE token(email) > ? AND token(email) <= ?";

const SCAN_WRAP: &str =
    "SELECT email, nama, password, role FROM invezgood.user WHERE token(email) > ? OR token(email) <= ?";

const FIND_BY_EMAIL: &str =
    "SELECT email, nama, password, role FROM invezgood.user WHERE email = ?";

const LOCAL_TOKENS: &str = "SELECT tokens FROM system.local";
const PEERS_TOKENS: &str = "SELECT tokens FROM system.peers";

#[derive(Debug, DeserializeRow)]
struct TokensRow {
    tokens: HashSet<i64>,
}

/// Lookup satu user by partition key `email`.
pub async fn find_by_email(session: &Session, email: &str) -> Result<Option<UserRow>, String> {
    let mut rows = session
        .query_iter(FIND_BY_EMAIL, (email,))
        .await
        .map_err(|e| format!("find_by_email {KEYSPACE}.{TABLE} email={email}: {e}"))?
        .rows_stream::<UserRow>()
        .map_err(|e| format!("find_by_email stream {KEYSPACE}.{TABLE}: {e}"))?;

    rows.try_next()
        .await
        .map_err(|e| format!("find_by_email row {KEYSPACE}.{TABLE}: {e}"))
}

/// Full read via token ring — satu query per range vnode di cluster.
pub async fn token_ring_scan(session: &Session) -> Result<Vec<UserRow>, String> {
    let mut tokens = fetch_cluster_tokens(session).await?;
    if tokens.is_empty() {
        return Ok(Vec::new());
    }

    tokens.sort_unstable();
    tokens.dedup();

    let scan_range = session
        .prepare(SCAN_RANGE)
        .await
        .map_err(|e| format!("prepare token scan range {KEYSPACE}.{TABLE}: {e}"))?;
    let scan_wrap = session
        .prepare(SCAN_WRAP)
        .await
        .map_err(|e| format!("prepare token scan wrap {KEYSPACE}.{TABLE}: {e}"))?;

    let mut items = Vec::new();
    let n = tokens.len();

    for i in 0..n {
        let end = tokens[i];
        let start = tokens[(i + n - 1) % n];

        let range_rows = if start < end {
            fetch_range(session, scan_range.clone(), (start, end)).await?
        } else {
            fetch_range(session, scan_wrap.clone(), (start, end)).await?
        };

        items.extend(range_rows);
    }

    Ok(items)
}

async fn fetch_cluster_tokens(session: &Session) -> Result<Vec<i64>, String> {
    let mut tokens = HashSet::new();

    for query in [LOCAL_TOKENS, PEERS_TOKENS] {
        let rows = session
            .query_iter(query, &[])
            .await
            .map_err(|e| format!("query cluster tokens: {e}"))?
            .rows_stream::<TokensRow>()
            .map_err(|e| format!("cluster tokens stream: {e}"))?;

        let mut rows = rows;
        while let Some(row) = rows
            .try_next()
            .await
            .map_err(|e| format!("cluster tokens row: {e}"))?
        {
            tokens.extend(row.tokens);
        }
    }

    Ok(tokens.into_iter().collect())
}

async fn fetch_range(
    session: &Session,
    prepared: PreparedStatement,
    bounds: (i64, i64),
) -> Result<Vec<UserRow>, String> {
    let mut rows = session
        .execute_iter(prepared, bounds)
        .await
        .map_err(|e| format!("token scan {KEYSPACE}.{TABLE} ({}, {}]: {e}", bounds.0, bounds.1))?
        .rows_stream::<UserRow>()
        .map_err(|e| format!("token scan stream {KEYSPACE}.{TABLE}: {e}"))?;

    let mut items = Vec::new();
    while let Some(row) = rows
        .try_next()
        .await
        .map_err(|e| format!("token scan row {KEYSPACE}.{TABLE}: {e}"))?
    {
        items.push(row);
    }

    Ok(items)
}
