use std::collections::HashMap;

use futures::TryStreamExt;
use scylla::client::session::Session;
use scylla::DeserializeRow;

use crate::model::{
    CompanyInformationDb, CorporateActionDb, KeyStatsFromStockbitDb, KeyStatsFromStockbitRow,
    ShareHolder1Db, ShareHolder5Db, ShareHolderCompositionDb, StockbitProfileByCodeRow,
    StockbitProfileColDb, StockbitReportsByCodeRow, StockbitReportsDb,
    StockListBalanceStatementDb, StockListCashFlowDb, StockListIncomeStatementDb,
    StockListKeystatsDb, StockListKeystatsRow, StockListRow, StockListSummaryRow,
    HorizontalLineByCodeRow, WyckoffChartByCodeRow, TakeProfitWyckoffByCodeRow, WyckoffChartDb,
    NotationDb, KEYSPACE, TABLE,
};

const ROW_SELECT: &str = "code, name, sector, sub_sector, logo, keystats, keystats_updated_at, \
    balance_statement, balance_statement_updated_at, income_statement, income_statement_updated_at, \
    cash_flow, cash_flow_updated_at, share_holder_5, share_holder_5_updated_at, \
    share_holder_1, share_holder_1_updated_at, share_holder_composition, share_holder_composition_updated_at, \
    company_information, company_information_updated_at, corporate_action, corporate_action_updated_at, \
    catatan_owner, catatan_pribadi, is_plan_to_trade, is_konglomerasi, wyckoff_chart, horizontal_line, takeprofit_wyckoff, is_bad_fundamental, notation, is_idx_30, is_lq_45, is_idx_80";

const UPSERT: &str =
    "INSERT INTO invezgood.stock_list (code, name, sector, logo) VALUES (?, ?, ?, ?)";

const SELECT_BY_CODE: &str = "SELECT ROW_SELECT FROM invezgood.stock_list WHERE code = ?";

const SELECT_WYCKOFF_CHART_BY_CODE: &str =
    "SELECT code, wyckoff_chart FROM invezgood.stock_list WHERE code = ?";

const SELECT_HORIZONTAL_LINE_BY_CODE: &str =
    "SELECT code, horizontal_line FROM invezgood.stock_list WHERE code = ?";

const SELECT_TAKEPROFIT_WYCKOFF_BY_CODE: &str =
    "SELECT code, takeprofit_wyckoff FROM invezgood.stock_list WHERE code = ?";

const SELECT_KEYSTATS_FROM_STOCKBIT_BY_CODE: &str = "SELECT code, \
    closure_fin_items_results_stockbit, closure_fin_items_results_stockbit_updated_at, \
    financial_year_parent_stockbit, financial_year_parent_stockbit_updated_at, \
    stats_stockbit, stats_stockbit_updated_at, \
    dividend_group_stockbit, dividend_group_stockbit_updated_at \
    FROM invezgood.stock_list WHERE code = ?";

const SELECT_STOCKBIT_REPORTS_BY_CODE: &str =
    "SELECT code, stockbit_reports, stockbit_reports_updated_at FROM invezgood.stock_list WHERE code = ?";

const SELECT_STOCKBIT_PROFILE_BY_CODE: &str =
    "SELECT code, stockbit_profile, stockbit_profile_updated_at FROM invezgood.stock_list WHERE code = ?";

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

const UPDATE_KEYSTATS_FROM_STOCKBIT: &str = "UPDATE invezgood.stock_list SET \
    closure_fin_items_results_stockbit = ?, closure_fin_items_results_stockbit_updated_at = ?, \
    financial_year_parent_stockbit = ?, financial_year_parent_stockbit_updated_at = ?, \
    stats_stockbit = ?, stats_stockbit_updated_at = ?, \
    dividend_group_stockbit = ?, dividend_group_stockbit_updated_at = ? WHERE code = ?";

const UPDATE_STOCKBIT_REPORTS: &str =
    "UPDATE invezgood.stock_list SET stockbit_reports = ?, stockbit_reports_updated_at = ? WHERE code = ?";

const UPDATE_STOCKBIT_PROFILE: &str =
    "UPDATE invezgood.stock_list SET stockbit_profile = ?, stockbit_profile_updated_at = ? WHERE code = ?";

const UPDATE_IS_KONGLOMERASI: &str =
    "UPDATE invezgood.stock_list SET is_konglomerasi = ? WHERE code = ?";

const UPDATE_IS_PLAN_TO_TRADE: &str =
    "UPDATE invezgood.stock_list SET is_plan_to_trade = ? WHERE code = ?";

const UPDATE_IS_BAD_FUNDAMENTAL: &str =
    "UPDATE invezgood.stock_list SET is_bad_fundamental = ? WHERE code = ?";

const UPDATE_NOTATION: &str =
    "UPDATE invezgood.stock_list SET notation = ? WHERE code = ?";

const UPDATE_CATATAN_OWNER: &str =
    "UPDATE invezgood.stock_list SET catatan_owner = ? WHERE code = ?";

const UPDATE_CATATAN_PRIBADI: &str =
    "UPDATE invezgood.stock_list SET catatan_pribadi = ? WHERE code = ?";

const UPDATE_WYCKOFF_CHART: &str =
    "UPDATE invezgood.stock_list SET wyckoff_chart = ? WHERE code = ?";

const UPDATE_HORIZONTAL_LINE: &str =
    "UPDATE invezgood.stock_list SET horizontal_line = ? WHERE code = ?";

const UPDATE_TAKEPROFIT_WYCKOFF: &str =
    "UPDATE invezgood.stock_list SET takeprofit_wyckoff = ? WHERE code = ?";

const DELETE_TAKEPROFIT_WYCKOFF: &str =
    "UPDATE invezgood.stock_list SET takeprofit_wyckoff = null WHERE code = ?";

const UPDATE_SUB_SECTOR: &str =
    "UPDATE invezgood.stock_list SET sub_sector = ? WHERE code = ?";

const SELECT_CODE: &str = "SELECT code FROM invezgood.stock_list WHERE code = ?";

const LIST_ALL: &str = "SELECT code, name, sector, sub_sector, logo, keystats_updated_at, \
    catatan_owner, catatan_pribadi, is_plan_to_trade, is_konglomerasi, takeprofit_wyckoff, is_bad_fundamental, notation, is_idx_30, is_lq_45, is_idx_80 \
    FROM invezgood.stock_list";

const LIST_ALL_KEYSTATS: &str =
    "SELECT code, keystats, keystats_updated_at FROM invezgood.stock_list";

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

pub async fn get_wyckoff_chart_by_code(
    session: &Session,
    code: &str,
) -> Result<Option<WyckoffChartByCodeRow>, String> {
    let mut rows = session
        .query_iter(SELECT_WYCKOFF_CHART_BY_CODE, (code,))
        .await
        .map_err(|e| format!("select wyckoff_chart {KEYSPACE}.{TABLE} code={code}: {e}"))?
        .rows_stream::<WyckoffChartByCodeRow>()
        .map_err(|e| format!("select wyckoff_chart stream {KEYSPACE}.{TABLE} code={code}: {e}"))?;

    rows.try_next()
        .await
        .map_err(|e| format!("select wyckoff_chart row {KEYSPACE}.{TABLE} code={code}: {e}"))
}

pub async fn get_horizontal_line_by_code(
    session: &Session,
    code: &str,
) -> Result<Option<HorizontalLineByCodeRow>, String> {
    let mut rows = session
        .query_iter(SELECT_HORIZONTAL_LINE_BY_CODE, (code,))
        .await
        .map_err(|e| format!("select horizontal_line {KEYSPACE}.{TABLE} code={code}: {e}"))?
        .rows_stream::<HorizontalLineByCodeRow>()
        .map_err(|e| {
            format!("select horizontal_line stream {KEYSPACE}.{TABLE} code={code}: {e}")
        })?;

    rows.try_next()
        .await
        .map_err(|e| format!("select horizontal_line row {KEYSPACE}.{TABLE} code={code}: {e}"))
}

pub async fn get_takeprofit_wyckoff_by_code(
    session: &Session,
    code: &str,
) -> Result<Option<TakeProfitWyckoffByCodeRow>, String> {
    let mut rows = session
        .query_iter(SELECT_TAKEPROFIT_WYCKOFF_BY_CODE, (code,))
        .await
        .map_err(|e| format!("select takeprofit_wyckoff {KEYSPACE}.{TABLE} code={code}: {e}"))?
        .rows_stream::<TakeProfitWyckoffByCodeRow>()
        .map_err(|e| {
            format!("select takeprofit_wyckoff stream {KEYSPACE}.{TABLE} code={code}: {e}")
        })?;

    rows.try_next()
        .await
        .map_err(|e| format!("select takeprofit_wyckoff row {KEYSPACE}.{TABLE} code={code}: {e}"))
}

pub async fn get_keystats_from_stockbit_by_code(
    session: &Session,
    code: &str,
) -> Result<Option<KeyStatsFromStockbitRow>, String> {
    let mut rows = session
        .query_iter(SELECT_KEYSTATS_FROM_STOCKBIT_BY_CODE, (code,))
        .await
        .map_err(|e| format!("select keystats stockbit {KEYSPACE}.{TABLE} code={code}: {e}"))?
        .rows_stream::<KeyStatsFromStockbitRow>()
        .map_err(|e| format!("select keystats stockbit stream {KEYSPACE}.{TABLE} code={code}: {e}"))?;

    rows.try_next()
        .await
        .map_err(|e| format!("select keystats stockbit row {KEYSPACE}.{TABLE} code={code}: {e}"))
}

pub async fn get_stockbit_reports_by_code(
    session: &Session,
    code: &str,
) -> Result<Option<StockbitReportsByCodeRow>, String> {
    let mut rows = session
        .query_iter(SELECT_STOCKBIT_REPORTS_BY_CODE, (code,))
        .await
        .map_err(|e| format!("select stockbit reports {KEYSPACE}.{TABLE} code={code}: {e}"))?
        .rows_stream::<StockbitReportsByCodeRow>()
        .map_err(|e| format!("select stockbit reports stream {KEYSPACE}.{TABLE} code={code}: {e}"))?;

    rows.try_next()
        .await
        .map_err(|e| format!("select stockbit reports row {KEYSPACE}.{TABLE} code={code}: {e}"))
}

pub async fn update_stockbit_reports(
    session: &Session,
    code: &str,
    reports: StockbitReportsDb,
    updated_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), String> {
    session
        .query_unpaged(UPDATE_STOCKBIT_REPORTS, (reports, updated_at, code))
        .await
        .map_err(|e| format!("update stockbit reports {KEYSPACE}.{TABLE} code={code}: {e}"))?;
    Ok(())
}

pub async fn get_stockbit_profile_by_code(
    session: &Session,
    code: &str,
) -> Result<Option<StockbitProfileByCodeRow>, String> {
    let mut rows = session
        .query_iter(SELECT_STOCKBIT_PROFILE_BY_CODE, (code,))
        .await
        .map_err(|e| format!("select stockbit profile {KEYSPACE}.{TABLE} code={code}: {e}"))?
        .rows_stream::<StockbitProfileByCodeRow>()
        .map_err(|e| format!("select stockbit profile stream {KEYSPACE}.{TABLE} code={code}: {e}"))?;

    rows.try_next()
        .await
        .map_err(|e| format!("select stockbit profile row {KEYSPACE}.{TABLE} code={code}: {e}"))
}

pub async fn update_stockbit_profile(
    session: &Session,
    code: &str,
    profile: StockbitProfileColDb,
    updated_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), String> {
    session
        .query_unpaged(UPDATE_STOCKBIT_PROFILE, (profile, updated_at, code))
        .await
        .map_err(|e| format!("update stockbit profile {KEYSPACE}.{TABLE} code={code}: {e}"))?;
    Ok(())
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

pub async fn update_keystats_from_stockbit(
    session: &Session,
    code: &str,
    payload: &KeyStatsFromStockbitDb,
    updated_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), String> {
    session
        .query_unpaged(
            UPDATE_KEYSTATS_FROM_STOCKBIT,
            (
                payload.closure_fin_items_results.clone(),
                updated_at,
                payload.financial_year_parent.clone(),
                updated_at,
                payload.stats.clone(),
                updated_at,
                payload.dividend_group.clone(),
                updated_at,
                code,
            ),
        )
        .await
        .map_err(|e| format!("update keystats stockbit {KEYSPACE}.{TABLE} code={code}: {e}"))?;
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

pub async fn update_is_bad_fundamental(
    session: &Session,
    code: &str,
    is_bad_fundamental: bool,
) -> Result<(), String> {
    if !code_exists(session, code).await? {
        return Err(format!("stock_list code={code} tidak ditemukan"));
    }

    session
        .query_unpaged(UPDATE_IS_BAD_FUNDAMENTAL, (is_bad_fundamental, code))
        .await
        .map_err(|e| format!("update is_bad_fundamental {KEYSPACE}.{TABLE} code={code}: {e}"))?;
    Ok(())
}

pub async fn update_notation(
    session: &Session,
    code: &str,
    notation: NotationDb,
) -> Result<(), String> {
    if !code_exists(session, code).await? {
        return Err(format!("stock_list code={code} tidak ditemukan"));
    }

    session
        .query_unpaged(UPDATE_NOTATION, (notation, code))
        .await
        .map_err(|e| format!("update notation {KEYSPACE}.{TABLE} code={code}: {e}"))?;
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

pub async fn update_sub_sector(
    session: &Session,
    code: &str,
    sub_sector: &str,
) -> Result<(), String> {
    if !code_exists(session, code).await? {
        return Err(format!("stock_list code={code} tidak ditemukan"));
    }

    session
        .query_unpaged(UPDATE_SUB_SECTOR, (sub_sector, code))
        .await
        .map_err(|e| format!("update sub_sector {KEYSPACE}.{TABLE} code={code}: {e}"))?;
    Ok(())
}

pub async fn update_wyckoff_chart(
    session: &Session,
    code: &str,
    wyckoff_chart: WyckoffChartDb,
) -> Result<(), String> {
    if !code_exists(session, code).await? {
        return Err(format!("stock_list code={code} tidak ditemukan"));
    }

    session
        .query_unpaged(UPDATE_WYCKOFF_CHART, (wyckoff_chart, code))
        .await
        .map_err(|e| format!("update wyckoff_chart {KEYSPACE}.{TABLE} code={code}: {e}"))?;
    Ok(())
}

pub async fn update_horizontal_line(
    session: &Session,
    code: &str,
    horizontal_line: &[i32],
) -> Result<(), String> {
    if !code_exists(session, code).await? {
        return Err(format!("stock_list code={code} tidak ditemukan"));
    }

    session
        .query_unpaged(UPDATE_HORIZONTAL_LINE, (horizontal_line, code))
        .await
        .map_err(|e| format!("update horizontal_line {KEYSPACE}.{TABLE} code={code}: {e}"))?;
    Ok(())
}

pub async fn upsert_takeprofit_wyckoff(
    session: &Session,
    code: &str,
    takeprofit_wyckoff: &HashMap<String, f64>,
) -> Result<(), String> {
    if !code_exists(session, code).await? {
        return Err(format!("stock_list code={code} tidak ditemukan"));
    }

    session
        .query_unpaged(UPDATE_TAKEPROFIT_WYCKOFF, (takeprofit_wyckoff, code))
        .await
        .map_err(|e| format!("upsert takeprofit_wyckoff {KEYSPACE}.{TABLE} code={code}: {e}"))?;
    Ok(())
}

pub async fn delete_takeprofit_wyckoff(session: &Session, code: &str) -> Result<(), String> {
    if !code_exists(session, code).await? {
        return Err(format!("stock_list code={code} tidak ditemukan"));
    }

    session
        .query_unpaged(DELETE_TAKEPROFIT_WYCKOFF, (code,))
        .await
        .map_err(|e| format!("delete takeprofit_wyckoff {KEYSPACE}.{TABLE} code={code}: {e}"))?;
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

/// Daftar semua keystats (untuk GetAllKeyStats).
pub async fn list_all_keystats(session: &Session) -> Result<Vec<StockListKeystatsRow>, String> {
    let mut rows = session
        .query_iter(LIST_ALL_KEYSTATS, &[])
        .await
        .map_err(|e| format!("list all keystats {KEYSPACE}.{TABLE}: {e}"))?
        .rows_stream::<StockListKeystatsRow>()
        .map_err(|e| format!("list all keystats stream {KEYSPACE}.{TABLE}: {e}"))?;

    let mut items = Vec::new();
    while let Some(row) = rows
        .try_next()
        .await
        .map_err(|e| format!("list all keystats row {KEYSPACE}.{TABLE}: {e}"))?
    {
        items.push(row);
    }

    items.sort_by(|a, b| a.code.cmp(&b.code));

    Ok(items)
}
