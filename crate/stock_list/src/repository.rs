use futures::TryStreamExt;
use scylla::client::session::Session;
use scylla::DeserializeRow;

use crate::model::{
    CompanyInformationDb, CorporateActionDb, ShareHolder1Db, ShareHolder5Db, ShareHolderCompositionDb,
    StockListBalanceStatementDb, StockListCashFlowDb, StockListIncomeStatementDb,
    StockListKeystatsDb, StockListRow, StockListSummaryRow, KEYSPACE, TABLE,
};

const ROW_SELECT: &str = "code, name, sector, logo, keystats, keystats_updated_at, \
    balance_statement, balance_statement_updated_at, income_statement, income_statement_updated_at, \
    cash_flow, cash_flow_updated_at, share_holder_5, share_holder_5_updated_at, \
    share_holder_1, share_holder_1_updated_at, share_holder_composition, share_holder_composition_updated_at, \
    company_information, company_information_updated_at, corporate_action, corporate_action_updated_at, \
    catatan_owner, catatan_pribadi, is_plan_to_trade, is_konglomerasi";

const UPSERT: &str =
    "INSERT INTO invezgood.stock_list (code, name, sector, logo) VALUES (?, ?, ?, ?)";

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

const UPDATE_SHARE_HOLDER_COMPOSITION: &str =
    "UPDATE invezgood.stock_list SET share_holder_composition = ?, share_holder_composition_updated_at = ? WHERE code = ?";

const UPDATE_COMPANY_INFORMATION: &str =
    "UPDATE invezgood.stock_list SET company_information = ?, company_information_updated_at = ? WHERE code = ?";

const UPDATE_CORPORATE_ACTION: &str =
    "UPDATE invezgood.stock_list SET corporate_action = ?, corporate_action_updated_at = ? WHERE code = ?";

const UPDATE_IS_KONGLOMERASI: &str =
    "UPDATE invezgood.stock_list SET is_konglomerasi = ? WHERE code = ?";

const UPDATE_IS_PLAN_TO_TRADE: &str =
    "UPDATE invezgood.stock_list SET is_plan_to_trade = ? WHERE code = ?";

const UPDATE_CATATAN_OWNER: &str =
    "UPDATE invezgood.stock_list SET catatan_owner = ? WHERE code = ?";

const UPDATE_CATATAN_PRIBADI: &str =
    "UPDATE invezgood.stock_list SET catatan_pribadi = ? WHERE code = ?";

const SELECT_CODE: &str = "SELECT code FROM invezgood.stock_list WHERE code = ?";

const LIST_ALL: &str = "SELECT code, name, sector, logo, keystats_updated_at, \
    catatan_owner, catatan_pribadi, is_plan_to_trade, is_konglomerasi FROM invezgood.stock_list";

fn with_row_select(query: &str) -> String {
    query.replace("ROW_SELECT", ROW_SELECT)
}

#[derive(Debug, DeserializeRow)]
struct CodeOnlyRow {
    #[allow(dead_code)]
    code: String,
}

async fn code_exists(session: &Session, code: &str) -> Result<bool, String> {
    let mut rows = session
        .query_iter(SELECT_CODE, (code,))
        .await
        .map_err(|e| format!("select code {KEYSPACE}.{TABLE} code={code}: {e}"))?
        .rows_stream::<CodeOnlyRow>()
        .map_err(|e| format!("select code stream {KEYSPACE}.{TABLE} code={code}: {e}"))?;

    Ok(rows
        .try_next()
        .await
        .map_err(|e| format!("select code row {KEYSPACE}.{TABLE} code={code}: {e}"))?
        .is_some())
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

pub async fn update_share_holder_composition(
    session: &Session,
    code: &str,
    share_holder_composition: ShareHolderCompositionDb,
    updated_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), String> {
    session
        .query_unpaged(
            UPDATE_SHARE_HOLDER_COMPOSITION,
            (share_holder_composition, updated_at, code),
        )
        .await
        .map_err(|e| {
            format!("update share_holder_composition {KEYSPACE}.{TABLE} code={code}: {e}")
        })?;
    Ok(())
}

pub async fn update_company_information(
    session: &Session,
    code: &str,
    company_information: CompanyInformationDb,
    updated_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), String> {
    session
        .query_unpaged(UPDATE_COMPANY_INFORMATION, (company_information, updated_at, code))
        .await
        .map_err(|e| format!("update company_information {KEYSPACE}.{TABLE} code={code}: {e}"))?;
    Ok(())
}

pub async fn update_corporate_action(
    session: &Session,
    code: &str,
    corporate_action: CorporateActionDb,
    updated_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), String> {
    session
        .query_unpaged(UPDATE_CORPORATE_ACTION, (corporate_action, updated_at, code))
        .await
        .map_err(|e| format!("update corporate_action {KEYSPACE}.{TABLE} code={code}: {e}"))?;
    Ok(())
}

pub async fn update_is_konglomerasi(
    session: &Session,
    code: &str,
    is_konglomerasi: bool,
) -> Result<(), String> {
    if !code_exists(session, code).await? {
        return Err(format!("stock_list code={code} tidak ditemukan"));
    }

    session
        .query_unpaged(UPDATE_IS_KONGLOMERASI, (is_konglomerasi, code))
        .await
        .map_err(|e| format!("update is_konglomerasi {KEYSPACE}.{TABLE} code={code}: {e}"))?;
    Ok(())
}

pub async fn update_is_plan_to_trade(
    session: &Session,
    code: &str,
    is_plan_to_trade: bool,
) -> Result<(), String> {
    if !code_exists(session, code).await? {
        return Err(format!("stock_list code={code} tidak ditemukan"));
    }

    session
        .query_unpaged(UPDATE_IS_PLAN_TO_TRADE, (is_plan_to_trade, code))
        .await
        .map_err(|e| format!("update is_plan_to_trade {KEYSPACE}.{TABLE} code={code}: {e}"))?;
    Ok(())
}

pub async fn update_catatan_owner(
    session: &Session,
    code: &str,
    catatan_owner: &str,
) -> Result<(), String> {
    if !code_exists(session, code).await? {
        return Err(format!("stock_list code={code} tidak ditemukan"));
    }

    session
        .query_unpaged(UPDATE_CATATAN_OWNER, (catatan_owner, code))
        .await
        .map_err(|e| format!("update catatan_owner {KEYSPACE}.{TABLE} code={code}: {e}"))?;
    Ok(())
}

pub async fn update_catatan_pribadi(
    session: &Session,
    code: &str,
    catatan_pribadi: &str,
) -> Result<(), String> {
    if !code_exists(session, code).await? {
        return Err(format!("stock_list code={code} tidak ditemukan"));
    }

    session
        .query_unpaged(UPDATE_CATATAN_PRIBADI, (catatan_pribadi, code))
        .await
        .map_err(|e| format!("update catatan_pribadi {KEYSPACE}.{TABLE} code={code}: {e}"))?;
    Ok(())
}

/// Daftar semua saham — kolom ringan saja (untuk GetAllStocks).
pub async fn list_all(session: &Session) -> Result<Vec<StockListSummaryRow>, String> {
    let mut rows = session
        .query_iter(LIST_ALL, &[])
        .await
        .map_err(|e| format!("list all {KEYSPACE}.{TABLE}: {e}"))?
        .rows_stream::<StockListSummaryRow>()
        .map_err(|e| format!("list all stream {KEYSPACE}.{TABLE}: {e}"))?;

    let mut items = Vec::new();
    while let Some(row) = rows
        .try_next()
        .await
        .map_err(|e| format!("list all row {KEYSPACE}.{TABLE}: {e}"))?
    {
        items.push(row);
    }

    Ok(items)
}
