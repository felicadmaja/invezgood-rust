use std::sync::Arc;

use chrono::{DateTime, Utc};
use scylla::client::session::Session;
use tonic::{Request, Response, Status};

use crate::model::{BalanceStatement, Keystats, ShareHolder1, ShareHolder5, StockListRow as DbStockListRow};
use crate::pb::stock_list_server::StockList;
use crate::pb::{
    FinancialStatementResponse, FinancialStatementRowItem, GetAllStocksRequest, GetAllStocksResponse,
    GetFinancialStatementRequest, GetShareHolder1Request, GetShareHolder5Request, KeyStatsColumn,
    KeyStatsRowItem, KeyStatsValue, ShareHolder1Entry, ShareHolder1Row, ShareHolder5Entry,
    ShareHolder5Row, StockListRow,
};

const CACHE_MAX_AGE_SECS: i64 = 30 * 24 * 60 * 60;

#[derive(Clone, Copy)]
enum StatementKind {
    Keystats,
    Bs,
    Is,
    Cf,
}

impl StatementKind {
    fn parse(raw: &str) -> Result<Self, Status> {
        match raw.trim().to_ascii_uppercase().as_str() {
            "KEYSTATS" | "KS" => Ok(Self::Keystats),
            "BS" => Ok(Self::Bs),
            "IS" => Ok(Self::Is),
            "CF" => Ok(Self::Cf),
            other => Err(Status::invalid_argument(format!(
                "statement tidak dikenal: {other} (gunakan KEYSTATS, BS, IS, atau CF)"
            ))),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Keystats => "KEYSTATS",
            Self::Bs => "BS",
            Self::Is => "IS",
            Self::Cf => "CF",
        }
    }

    fn updated_at(self, row: &DbStockListRow) -> Option<DateTime<Utc>> {
        match self {
            Self::Keystats => row.keystats_updated_at,
            Self::Bs => row.balance_statement_updated_at,
            Self::Is => row.income_statement_updated_at,
            Self::Cf => row.cash_flow_updated_at,
        }
    }
}

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

    fn panel_to_proto(
        code: String,
        kind: StatementKind,
        statement: BalanceStatement,
        updated_at: Option<DateTime<Utc>>,
    ) -> FinancialStatementResponse {
        FinancialStatementResponse {
            code,
            statement: kind.as_str().into(),
            keystats_rows: Vec::new(),
            rows: Self::financial_rows_to_proto(statement.rows),
            columns: Self::columns_to_proto(statement.columns),
            updated_at: updated_at.map(|dt| dt.timestamp()),
        }
    }

    fn keystats_to_proto(
        code: String,
        keystats: Keystats,
        updated_at: Option<DateTime<Utc>>,
    ) -> FinancialStatementResponse {
        FinancialStatementResponse {
            code,
            statement: StatementKind::Keystats.as_str().into(),
            keystats_rows: keystats
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
            rows: Vec::new(),
            columns: Self::columns_to_proto(keystats.columns),
            updated_at: updated_at.map(|dt| dt.timestamp()),
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

    fn share_holder_1_to_proto(
        code: String,
        entries: ShareHolder1,
        updated_at: Option<DateTime<Utc>>,
    ) -> ShareHolder1Row {
        ShareHolder1Row {
            code: code.clone(),
            items: entries
                .items
                .into_iter()
                .map(|entry| ShareHolder1Entry {
                    code: code.clone(),
                    name: entry.name,
                    r#type: entry.holder_type,
                    status: entry.status,
                    nationality: entry.nationality,
                    domicile: entry.domicile,
                    scripless: entry.scripless,
                    scrip: entry.scrip,
                    total: entry.total,
                    percentage: entry.percentage,
                })
                .collect(),
            share_holder_1_updated_at: updated_at.map(|dt| dt.timestamp()),
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

    fn share_holder_1_from_db_row(
        row: &DbStockListRow,
    ) -> Result<(ShareHolder1, Option<DateTime<Utc>>), Status> {
        let Some(entries_db) = row.share_holder_1.clone() else {
            return Err(Status::not_found(format!(
                "share_holder_1 belum tersedia untuk code={}",
                row.code
            )));
        };

        Ok((ShareHolder1::from(Some(entries_db)), row.share_holder_1_updated_at))
    }

    async fn fetch_and_save_statement(
        session: Arc<Session>,
        code: &str,
        kind: StatementKind,
    ) -> Result<FinancialStatementResponse, Status> {
        match kind {
            StatementKind::Keystats => {
                let (keystats, updated_at) =
                    crate::invezgo::fetch_and_save_keystats(session, code)
                        .await
                        .map_err(Status::internal)?;
                Ok(Self::keystats_to_proto(code.to_string(), keystats, Some(updated_at)))
            }
            StatementKind::Bs => {
                let (statement, updated_at) =
                    crate::invezgo::fetch_and_save_balance_statement(session, code)
                        .await
                        .map_err(Status::internal)?;
                Ok(Self::panel_to_proto(
                    code.to_string(),
                    kind,
                    statement,
                    Some(updated_at),
                ))
            }
            StatementKind::Is => {
                let (statement, updated_at) =
                    crate::invezgo::fetch_and_save_income_statement(session, code)
                        .await
                        .map_err(Status::internal)?;
                Ok(Self::panel_to_proto(
                    code.to_string(),
                    kind,
                    statement,
                    Some(updated_at),
                ))
            }
            StatementKind::Cf => {
                let (statement, updated_at) =
                    crate::invezgo::fetch_and_save_cash_flow(session, code)
                        .await
                        .map_err(Status::internal)?;
                Ok(Self::panel_to_proto(
                    code.to_string(),
                    kind,
                    statement,
                    Some(updated_at),
                ))
            }
        }
    }

    fn statement_from_db_row(
        row: &DbStockListRow,
        kind: StatementKind,
    ) -> Result<FinancialStatementResponse, Status> {
        let code = row.code.clone();
        match kind {
            StatementKind::Keystats => {
                let (keystats, updated_at) = Self::keystats_from_db_row(row)?;
                Ok(Self::keystats_to_proto(code, keystats, updated_at))
            }
            StatementKind::Bs => {
                let (statement, updated_at) = Self::balance_from_db_row(row)?;
                Ok(Self::panel_to_proto(code, kind, statement, updated_at))
            }
            StatementKind::Is => {
                let (statement, updated_at) = Self::income_from_db_row(row)?;
                Ok(Self::panel_to_proto(code, kind, statement, updated_at))
            }
            StatementKind::Cf => {
                let (statement, updated_at) = Self::cash_flow_from_db_row(row)?;
                Ok(Self::panel_to_proto(code, kind, statement, updated_at))
            }
        }
    }
}

#[tonic::async_trait]
impl StockList for StockListService {
    async fn get_all_stocks(
        &self,
        _request: Request<GetAllStocksRequest>,
    ) -> Result<Response<GetAllStocksResponse>, Status> {
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

        Ok(Response::new(GetAllStocksResponse {
            success: true,
            message,
            items,
        }))
    }

    async fn get_financial_statement(
        &self,
        request: Request<GetFinancialStatementRequest>,
    ) -> Result<Response<FinancialStatementResponse>, Status> {
        let req = request.into_inner();
        let code = req.code.trim().to_ascii_uppercase();
        if code.is_empty() {
            return Err(Status::invalid_argument("code wajib diisi"));
        }

        let kind = StatementKind::parse(&req.statement)?;

        let existing = crate::repository::get_by_code(self.session.as_ref(), &code)
            .await
            .map_err(Status::internal)?;

        let refresh = existing
            .as_ref()
            .map(|row| Self::should_refresh(kind.updated_at(row)))
            .unwrap_or(true);

        if refresh {
            let response =
                Self::fetch_and_save_statement(self.session.clone(), &code, kind).await?;
            return Ok(Response::new(response));
        }

        let row = existing.ok_or_else(|| {
            Status::not_found(format!("stock_list code={code} tidak ditemukan"))
        })?;

        Ok(Response::new(Self::statement_from_db_row(&row, kind)?))
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

    async fn get_share_holder1(
        &self,
        request: Request<GetShareHolder1Request>,
    ) -> Result<Response<ShareHolder1Row>, Status> {
        let code = request.into_inner().code.trim().to_ascii_uppercase();
        if code.is_empty() {
            return Err(Status::invalid_argument("code wajib diisi"));
        }

        let existing = crate::repository::get_by_code(self.session.as_ref(), &code)
            .await
            .map_err(Status::internal)?;

        let refresh = existing
            .as_ref()
            .map(|row| Self::should_refresh(row.share_holder_1_updated_at))
            .unwrap_or(true);

        if refresh {
            let (entries, updated_at) =
                crate::invezgo::fetch_and_save_share_holder_1(self.session.clone(), &code)
                    .await
                    .map_err(Status::internal)?;

            return Ok(Response::new(Self::share_holder_1_to_proto(
                code,
                entries,
                Some(updated_at),
            )));
        }

        let row = existing.ok_or_else(|| {
            Status::not_found(format!("stock_list code={code} tidak ditemukan"))
        })?;

        let (entries, updated_at) = Self::share_holder_1_from_db_row(&row)?;
        Ok(Response::new(Self::share_holder_1_to_proto(
            code,
            entries,
            updated_at,
        )))
    }
}
