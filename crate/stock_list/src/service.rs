use std::sync::Arc;

use chrono::{DateTime, Utc};
use scylla::client::session::Session;
use tonic::{Request, Response, Status};

use crate::model::{BalanceStatement, Keystats, ShareHolder5, StockListRow as DbStockListRow};
use crate::pb::stock_list_server::StockList;
use crate::pb::{
    BalanceStatementRow, CashFlowStatementRow, FinancialStatementRowItem, GetBalanceStatementRequest,
    GetCashFlowStatementRequest, GetIncomeStatementRequest, GetKeyStatsRequest,
    GetShareHolder5Request, GetStockListRequest, GetStockListResponse, IncomeStatementRow,
    KeyStatsColumn, KeyStatsRow, KeyStatsRowItem, KeyStatsValue, ShareHolder5Entry, ShareHolder5Row,
    StockListRow,
};

const CACHE_MAX_AGE_SECS: i64 = 30 * 24 * 60 * 60;

pub struct StockListService {
    session: Arc<Session>,
    redis: redis::Client,
}

impl StockListService {
    pub fn new(session: Arc<Session>) -> Result<Self, String> {
        let redis = crate::redis_cache::client_from_env()?;
        Ok(Self { session, redis })
    }

    fn should_refresh(updated_at: Option<DateTime<Utc>>) -> bool {
        let Some(updated_at) = updated_at else {
            return true;
        };
        Utc::now().timestamp() - updated_at.timestamp() > CACHE_MAX_AGE_SECS
    }

    fn db_row_to_proto(row: DbStockListRow) -> StockListRow {
        StockListRow {
            code: row.code,
            name: row.name.unwrap_or_default(),
            sector: row.sector.unwrap_or_default(),
            logo: row.logo.unwrap_or_default(),
            keystats_updated_at: row.keystats_updated_at.map(|dt| dt.timestamp()),
        }
    }

    fn financial_rows_to_proto(rows: Vec<crate::model::BalanceStatementRow>) -> Vec<FinancialStatementRowItem> {
        rows.into_iter()
            .map(|row| FinancialStatementRowItem {
                id: row.id,
                name: row.name,
                level: row.level,
                values: row
                    .values
                    .into_iter()
                    .map(|v| KeyStatsValue {
                        col: v.col,
                        year: v.year,
                        amount: v.amount,
                        period: v.period,
                    })
                    .collect(),
                parent_id: row.parent_id,
                is_abstract: row.is_abstract,
                display_order: row.display_order,
            })
            .collect()
    }

    fn columns_to_proto(columns: Vec<crate::model::KeystatsColumn>) -> Vec<KeyStatsColumn> {
        columns
            .into_iter()
            .map(|c| KeyStatsColumn {
                year: c.year,
                label: c.label,
                period: c.period,
            })
            .collect()
    }

    fn balance_to_proto(
        code: String,
        statement: BalanceStatement,
        updated_at: Option<DateTime<Utc>>,
    ) -> BalanceStatementRow {
        BalanceStatementRow {
            code,
            rows: Self::financial_rows_to_proto(statement.rows),
            columns: Self::columns_to_proto(statement.columns),
            balance_statement_updated_at: updated_at.map(|dt| dt.timestamp()),
        }
    }

    fn income_to_proto(
        code: String,
        statement: BalanceStatement,
        updated_at: Option<DateTime<Utc>>,
    ) -> IncomeStatementRow {
        IncomeStatementRow {
            code,
            rows: Self::financial_rows_to_proto(statement.rows),
            columns: Self::columns_to_proto(statement.columns),
            income_statement_updated_at: updated_at.map(|dt| dt.timestamp()),
        }
    }

    fn cash_flow_to_proto(
        code: String,
        statement: BalanceStatement,
        updated_at: Option<DateTime<Utc>>,
    ) -> CashFlowStatementRow {
        CashFlowStatementRow {
            code,
            rows: Self::financial_rows_to_proto(statement.rows),
            columns: Self::columns_to_proto(statement.columns),
            cash_flow_updated_at: updated_at.map(|dt| dt.timestamp()),
        }
    }

    fn share_holder_5_to_proto(
        code: String,
        entries: ShareHolder5,
        updated_at: Option<DateTime<Utc>>,
    ) -> ShareHolder5Row {
        ShareHolder5Row {
            code: code.clone(),
            items: entries
                .items
                .into_iter()
                .map(|entry| ShareHolder5Entry {
                    code: code.clone(),
                    name: entry.name,
                    date: entry.date.timestamp(),
                    val: entry.val,
                    percent: entry.percent,
                })
                .collect(),
            share_holder_5_updated_at: updated_at.map(|dt| dt.timestamp()),
        }
    }

    fn keystats_to_proto(code: String, keystats: Keystats, updated_at: Option<DateTime<Utc>>) -> KeyStatsRow {
        KeyStatsRow {
            code,
            rows: keystats
                .rows
                .into_iter()
                .map(|row| KeyStatsRowItem {
                    id: row.id,
                    name: row.name,
                    values: row
                        .values
                        .into_iter()
                        .map(|v| KeyStatsValue {
                            col: v.col,
                            year: v.year,
                            amount: v.amount,
                            period: v.period,
                        })
                        .collect(),
                })
                .collect(),
            columns: Self::columns_to_proto(keystats.columns),
            keystats_updated_at: updated_at.map(|dt| dt.timestamp()),
        }
    }

    fn keystats_from_db_row(row: &DbStockListRow) -> Result<(Keystats, Option<DateTime<Utc>>), Status> {
        let Some(keystats_db) = row.keystats.clone() else {
            return Err(Status::not_found(format!(
                "keystats belum tersedia untuk code={}",
                row.code
            )));
        };

        Ok((Keystats::from(keystats_db), row.keystats_updated_at))
    }

    fn balance_from_db_row(row: &DbStockListRow) -> Result<(BalanceStatement, Option<DateTime<Utc>>), Status> {
        let Some(balance_db) = row.balance_statement.clone() else {
            return Err(Status::not_found(format!(
                "balance_statement belum tersedia untuk code={}",
                row.code
            )));
        };

        Ok((BalanceStatement::from(balance_db), row.balance_statement_updated_at))
    }

    fn income_from_db_row(row: &DbStockListRow) -> Result<(BalanceStatement, Option<DateTime<Utc>>), Status> {
        let Some(income_db) = row.income_statement.clone() else {
            return Err(Status::not_found(format!(
                "income_statement belum tersedia untuk code={}",
                row.code
            )));
        };

        Ok((BalanceStatement::from(income_db), row.income_statement_updated_at))
    }

    fn cash_flow_from_db_row(row: &DbStockListRow) -> Result<(BalanceStatement, Option<DateTime<Utc>>), Status> {
        let Some(cash_flow_db) = row.cash_flow.clone() else {
            return Err(Status::not_found(format!(
                "cash_flow belum tersedia untuk code={}",
                row.code
            )));
        };

        Ok((BalanceStatement::from(cash_flow_db), row.cash_flow_updated_at))
    }

    fn share_holder_5_from_db_row(
        row: &DbStockListRow,
    ) -> Result<(ShareHolder5, Option<DateTime<Utc>>), Status> {
        let Some(entries_db) = row.share_holder_5.clone() else {
            return Err(Status::not_found(format!(
                "share_holder_5 belum tersedia untuk code={}",
                row.code
            )));
        };

        Ok((ShareHolder5::from(Some(entries_db)), row.share_holder_5_updated_at))
    }
}

#[tonic::async_trait]
impl StockList for StockListService {
    async fn get_stock_list(
        &self,
        _request: Request<GetStockListRequest>,
    ) -> Result<Response<GetStockListResponse>, Status> {
        let mut redis_conn = self
            .redis
            .get_multiplexed_tokio_connection()
            .await
            .map_err(|e| Status::internal(format!("redis connect: {e}")))?;

        let refresh = crate::redis_cache::should_refresh_from_api(&mut redis_conn)
            .await
            .map_err(Status::internal)?;

        let message = if refresh {
            let count = crate::invezgo::fetch_and_save(self.session.clone())
                .await
                .map_err(Status::internal)?;
            crate::redis_cache::set_updated_at(&mut redis_conn)
                .await
                .map_err(Status::internal)?;
            format!("refresh Invezgo: {count} saham disimpan ke stock_list")
        } else {
            "cache valid (<30 hari): baca dari Scylla".into()
        };

        let rows = crate::repository::token_ring_scan(self.session.as_ref())
            .await
            .map_err(Status::internal)?;

        let items = rows.into_iter().map(Self::db_row_to_proto).collect();

        Ok(Response::new(GetStockListResponse {
            success: true,
            message,
            items,
        }))
    }

    async fn get_key_stats(
        &self,
        request: Request<GetKeyStatsRequest>,
    ) -> Result<Response<KeyStatsRow>, Status> {
        let code = request.into_inner().code.trim().to_ascii_uppercase();
        if code.is_empty() {
            return Err(Status::invalid_argument("code wajib diisi"));
        }

        let existing = crate::repository::get_by_code(self.session.as_ref(), &code)
            .await
            .map_err(Status::internal)?;

        let refresh = existing
            .as_ref()
            .map(|row| Self::should_refresh(row.keystats_updated_at))
            .unwrap_or(true);

        if refresh {
            let (keystats, updated_at) = crate::invezgo::fetch_and_save_keystats(
                self.session.clone(),
                &code,
            )
            .await
            .map_err(Status::internal)?;

            return Ok(Response::new(Self::keystats_to_proto(
                code,
                keystats,
                Some(updated_at),
            )));
        }

        let row = existing.ok_or_else(|| {
            Status::not_found(format!("stock_list code={code} tidak ditemukan"))
        })?;

        let (keystats, updated_at) = Self::keystats_from_db_row(&row)?;
        Ok(Response::new(Self::keystats_to_proto(
            code,
            keystats,
            updated_at,
        )))
    }

    async fn get_balance_statement(
        &self,
        request: Request<GetBalanceStatementRequest>,
    ) -> Result<Response<BalanceStatementRow>, Status> {
        let code = request.into_inner().code.trim().to_ascii_uppercase();
        if code.is_empty() {
            return Err(Status::invalid_argument("code wajib diisi"));
        }

        let existing = crate::repository::get_by_code(self.session.as_ref(), &code)
            .await
            .map_err(Status::internal)?;

        let refresh = existing
            .as_ref()
            .map(|row| Self::should_refresh(row.balance_statement_updated_at))
            .unwrap_or(true);

        if refresh {
            let (statement, updated_at) =
                crate::invezgo::fetch_and_save_balance_statement(self.session.clone(), &code)
                    .await
                    .map_err(Status::internal)?;

            return Ok(Response::new(Self::balance_to_proto(
                code,
                statement,
                Some(updated_at),
            )));
        }

        let row = existing.ok_or_else(|| {
            Status::not_found(format!("stock_list code={code} tidak ditemukan"))
        })?;

        let (statement, updated_at) = Self::balance_from_db_row(&row)?;
        Ok(Response::new(Self::balance_to_proto(
            code,
            statement,
            updated_at,
        )))
    }

    async fn get_income_statement(
        &self,
        request: Request<GetIncomeStatementRequest>,
    ) -> Result<Response<IncomeStatementRow>, Status> {
        let code = request.into_inner().code.trim().to_ascii_uppercase();
        if code.is_empty() {
            return Err(Status::invalid_argument("code wajib diisi"));
        }

        let existing = crate::repository::get_by_code(self.session.as_ref(), &code)
            .await
            .map_err(Status::internal)?;

        let refresh = existing
            .as_ref()
            .map(|row| Self::should_refresh(row.income_statement_updated_at))
            .unwrap_or(true);

        if refresh {
            let (statement, updated_at) =
                crate::invezgo::fetch_and_save_income_statement(self.session.clone(), &code)
                    .await
                    .map_err(Status::internal)?;

            return Ok(Response::new(Self::income_to_proto(
                code,
                statement,
                Some(updated_at),
            )));
        }

        let row = existing.ok_or_else(|| {
            Status::not_found(format!("stock_list code={code} tidak ditemukan"))
        })?;

        let (statement, updated_at) = Self::income_from_db_row(&row)?;
        Ok(Response::new(Self::income_to_proto(
            code,
            statement,
            updated_at,
        )))
    }

    async fn get_cash_flow_statement(
        &self,
        request: Request<GetCashFlowStatementRequest>,
    ) -> Result<Response<CashFlowStatementRow>, Status> {
        let code = request.into_inner().code.trim().to_ascii_uppercase();
        if code.is_empty() {
            return Err(Status::invalid_argument("code wajib diisi"));
        }

        let existing = crate::repository::get_by_code(self.session.as_ref(), &code)
            .await
            .map_err(Status::internal)?;

        let refresh = existing
            .as_ref()
            .map(|row| Self::should_refresh(row.cash_flow_updated_at))
            .unwrap_or(true);

        if refresh {
            let (statement, updated_at) =
                crate::invezgo::fetch_and_save_cash_flow(self.session.clone(), &code)
                    .await
                    .map_err(Status::internal)?;

            return Ok(Response::new(Self::cash_flow_to_proto(
                code,
                statement,
                Some(updated_at),
            )));
        }

        let row = existing.ok_or_else(|| {
            Status::not_found(format!("stock_list code={code} tidak ditemukan"))
        })?;

        let (statement, updated_at) = Self::cash_flow_from_db_row(&row)?;
        Ok(Response::new(Self::cash_flow_to_proto(
            code,
            statement,
            updated_at,
        )))
    }

    async fn get_share_holder5(
        &self,
        request: Request<GetShareHolder5Request>,
    ) -> Result<Response<ShareHolder5Row>, Status> {
        let code = request.into_inner().code.trim().to_ascii_uppercase();
        if code.is_empty() {
            return Err(Status::invalid_argument("code wajib diisi"));
        }

        let existing = crate::repository::get_by_code(self.session.as_ref(), &code)
            .await
            .map_err(Status::internal)?;

        let refresh = existing
            .as_ref()
            .map(|row| Self::should_refresh(row.share_holder_5_updated_at))
            .unwrap_or(true);

        if refresh {
            let (entries, updated_at) =
                crate::invezgo::fetch_and_save_share_holder_5(self.session.clone(), &code)
                    .await
                    .map_err(Status::internal)?;

            return Ok(Response::new(Self::share_holder_5_to_proto(
                code,
                entries,
                Some(updated_at),
            )));
        }

        let row = existing.ok_or_else(|| {
            Status::not_found(format!("stock_list code={code} tidak ditemukan"))
        })?;

        let (entries, updated_at) = Self::share_holder_5_from_db_row(&row)?;
        Ok(Response::new(Self::share_holder_5_to_proto(
            code,
            entries,
            updated_at,
        )))
    }
}
