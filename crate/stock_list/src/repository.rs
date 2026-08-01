use std::collections::HashSet;

use futures::TryStreamExt;
use scylla::client::session::Session;
use scylla::DeserializeRow;
use scylla::statement::prepared::PreparedStatement;

use crate::model::{
    ShareHolder1Db, ShareHolder5Db, StockListBalanceStatementDb, StockListCashFlowDb,
    StockListIncomeStatementDb, StockListKeystatsDb, StockListRow, KEYSPACE, TABLE,
};

const ROW_SELECT: &str = "code, name, sector, logo, keystats, keystats_updated_at, \
    balance_statement, balance_statement_updated_at, income_statement, income_statement_updated_at, \
    cash_flow, cash_flow_updated_at, share_holder_5, share_holder_5_updated_at, \
    share_holder_1, share_holder_1_updated_at";

const UPSERT: &str =
    "INSERT INTO invezgood.stock_list (code, name, sector, logo) VALUES (?, ?, ?, ?)";

const SCAN_RANGE: &str =
    "SELECT ROW_SELECT FROM invezgood.stock_list WHERE token(code) > ? AND token(code) <= ?";

const SCAN_WRAP: &str =
    "SELECT ROW_SELECT FROM invezgood.stock_list WHERE token(code) > ? OR token(code) <= ?";

const LOCAL_TOKENS: &str = "SELECT tokens FROM system.local";
const PEERS_TOKENS: &str = "SELECT tokens FROM system.peers";

const SELECT_BY_CODE: &str = "SELECT ROW_SELECT FROM invezgood.stock_list WHERE code = ?";

const UPDATE_KEYSTATS: &str =
    "UPDATE invezgood.stock_list SET keystats = ?, keystats_updated_at = ? WHERE code = ?";

const UPDATE_BALANCE_STATEMENT: &str =
    "UPDATE invezgood.stock_list SET balance_statement = ?, balance_statement_updated_at = ? WHERE code = ?";

const UPDATE_INCOME_STATEMENT: &str =
    "UPDATE invezgood.stock_list SET income_statement = ?, income_statement_updated_at = ? WHERE code = ?";

const UPDATE_CASH_FLOW: &str =
    "UPDATE invezgood.stock_list SET cash_flow = ?, cash_flow_updated_at = ? WHERE code = ?";

const UPDATE_SHARE_HOLDER_5: &str =
    "UPDATE invezgood.stock_list SET share_holder_5 = ?, share_holder_5_updated_at = ? WHERE code = ?";

const UPDATE_SHARE_HOLDER_1: &str =
    "UPDATE invezgood.stock_list SET share_holder_1 = ?, share_holder_1_updated_at = ? WHERE code = ?";

#[derive(Debug, DeserializeRow)]
struct TokensRow {
    tokens: HashSet<i64>,
}

fn with_row_select(query: &str) -> String {
    query.replace("ROW_SELECT", ROW_SELECT)
}

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

pub async fn get_by_code(session: &Session, code: &str) -> Result<Option<StockListRow>, String> {
    let query = with_row_select(SELECT_BY_CODE);
    let mut rows = session
        .query_iter(query, (code,))
        .await
        .map_err(|e| format!("select {KEYSPACE}.{TABLE} code={code}: {e}"))?
        .rows_stream::<StockListRow>()
        .map_err(|e| format!("select stream {KEYSPACE}.{TABLE} code={code}: {e}"))?;

    rows.try_next()
        .await
        .map_err(|e| format!("select row {KEYSPACE}.{TABLE} code={code}: {e}"))
}

pub async fn update_keystats(
    session: &Session,
    code: &str,
    keystats: StockListKeystatsDb,
    updated_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), String> {
    session
        .query_unpaged(UPDATE_KEYSTATS, (keystats, updated_at, code))
        .await
        .map_err(|e| format!("update keystats {KEYSPACE}.{TABLE} code={code}: {e}"))?;
    Ok(())
}

pub async fn update_balance_statement(
    session: &Session,
    code: &str,
    balance_statement: StockListBalanceStatementDb,
    updated_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), String> {
    session
        .query_unpaged(
            UPDATE_BALANCE_STATEMENT,
            (balance_statement, updated_at, code),
        )
        .await
        .map_err(|e| format!("update balance_statement {KEYSPACE}.{TABLE} code={code}: {e}"))?;
    Ok(())
}

pub async fn update_income_statement(
    session: &Session,
    code: &str,
    income_statement: StockListIncomeStatementDb,
    updated_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), String> {
    session
        .query_unpaged(UPDATE_INCOME_STATEMENT, (income_statement, updated_at, code))
        .await
        .map_err(|e| format!("update income_statement {KEYSPACE}.{TABLE} code={code}: {e}"))?;
    Ok(())
}

pub async fn update_cash_flow(
    session: &Session,
    code: &str,
    cash_flow: StockListCashFlowDb,
    updated_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), String> {
    session
        .query_unpaged(UPDATE_CASH_FLOW, (cash_flow, updated_at, code))
        .await
        .map_err(|e| format!("update cash_flow {KEYSPACE}.{TABLE} code={code}: {e}"))?;
    Ok(())
}

pub async fn update_share_holder_5(
    session: &Session,
    code: &str,
    share_holder_5: ShareHolder5Db,
    updated_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), String> {
    session
        .query_unpaged(UPDATE_SHARE_HOLDER_5, (share_holder_5, updated_at, code))
        .await
        .map_err(|e| format!("update share_holder_5 {KEYSPACE}.{TABLE} code={code}: {e}"))?;
    Ok(())
}

pub async fn update_share_holder_1(
    session: &Session,
    code: &str,
    share_holder_1: ShareHolder1Db,
    updated_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), String> {
    session
        .query_unpaged(UPDATE_SHARE_HOLDER_1, (share_holder_1, updated_at, code))
        .await
        .map_err(|e| format!("update share_holder_1 {KEYSPACE}.{TABLE} code={code}: {e}"))?;
    Ok(())
}

/// Full read via token ring — satu query per range vnode di cluster.
pub async fn token_ring_scan(session: &Session) -> Result<Vec<StockListRow>, String> {
    let mut tokens = fetch_cluster_tokens(session).await?;
    if tokens.is_empty() {
        return Ok(Vec::new());
    }

    tokens.sort_unstable();
    tokens.dedup();

    let scan_range = session
        .prepare(with_row_select(SCAN_RANGE))
        .await
        .map_err(|e| format!("prepare token scan range {KEYSPACE}.{TABLE}: {e}"))?;
    let scan_wrap = session
        .prepare(with_row_select(SCAN_WRAP))
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
) -> Result<Vec<StockListRow>, String> {
    let mut rows = session
        .execute_iter(prepared, bounds)
        .await
        .map_err(|e| format!("token scan {KEYSPACE}.{TABLE} ({}, {}]: {e}", bounds.0, bounds.1))?
        .rows_stream::<StockListRow>()
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
