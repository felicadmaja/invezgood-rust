use std::sync::Arc;

use chrono::{DateTime, Utc};
use scylla::client::session::Session;
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, AuthSession, SessionStore};

use crate::model::{
    BalanceStatement, CompanyInformation, CorporateAction, Keystats, ShareHolder1, ShareHolder5,
    ShareHolderComposition, StockListKeystatsRow, StockListRow as DbStockListRow,
    StockListSummaryRow,
};
use crate::pb::stock_list_server::StockList;
use crate::pb::{
    CompanyInformationData, CompanyPersonEntry, CompanySubsidiaryEntry,
    CorporateActionByCodeResponse, CorporateActionData, CorporateActionEntry,
    FinancialStatementResponse, FinancialStatementRowItem, GetAllKeyStatsRequest,
    GetAllKeyStatsResponse, GetAllStocksRequest, GetAllStocksResponse,
    GetCorporateActionByCodeRequest, GetFinancialStatementByCodeRequest,
    GetHorizontalLineByCodeRequest, GetHorizontalLineByCodeResponse, GetStockByCodeRequest,
    GetStockByRepeatedCodeRequest, GetStockByRepeatedCodeResponse,
    GetShareHolderAndCompanyInformationByCodeRequest, GetWyckoffChartByCodeRequest,
    GetWyckoffChartByCodeResponse,
    KeystatsData, KeystatsResponse, KeyStatsColumn, KeyStatsRowItem,
    KeyStatsValue, ShareHolder1Data, ShareHolder1Entry, ShareHolder5Data, ShareHolder5Entry,
    ShareHolderAndCompanyInformationByCodeResponse, ShareHolderCompositionData,
    ShareHolderCompositionEntry, StatementPanelData, StockByCodeResponse, StockListRow,
    UpdateHorizontalLineByCodeRequest, UpdateHorizontalLineByCodeResponse,
    UpdateIsKonglomerasiRequest, UpdateIsKonglomerasiResponse, UpdateIsPlanToTradeRequest,
    UpdateIsPlanToTradeResponse, UpdateCatatanOwnerRequest, UpdateCatatanOwnerResponse,
    UpdateCatatanPribadiRequest, UpdateCatatanPribadiResponse, UpdateWyckoffChartByCodeRequest,
    UpdateWyckoffChartByCodeResponse, WyckoffChartData,
};

const CACHE_MAX_AGE_SECS: i64 = 30 * 24 * 60 * 60;

const ALL_STATEMENT_KINDS: [StatementKind; 4] = [
    StatementKind::Keystats,
    StatementKind::Bs,
    StatementKind::Is,
    StatementKind::Cf,
];

const ALL_SHARE_HOLDER_KINDS: [ShareHolderKind; 3] = [
    ShareHolderKind::Holder5,
    ShareHolderKind::Holder1,
    ShareHolderKind::Composition,
];

#[derive(Clone, Copy)]
enum StatementKind {
    Keystats,
    Bs,
    Is,
    Cf,
}

impl StatementKind {
    fn updated_at(self, row: &DbStockListRow) -> Option<DateTime<Utc>> {
        match self {
            Self::Keystats => row.keystats_updated_at,
            Self::Bs => row.balance_statement_updated_at,
            Self::Is => row.income_statement_updated_at,
            Self::Cf => row.cash_flow_updated_at,
        }
    }
}

#[derive(Clone, Copy)]
enum ShareHolderKind {
    Holder5,
    Holder1,
    Composition,
}

impl ShareHolderKind {
    fn updated_at(self, row: &DbStockListRow) -> Option<DateTime<Utc>> {
        match self {
            Self::Holder5 => row.share_holder_5_updated_at,
            Self::Holder1 => row.share_holder_1_updated_at,
            Self::Composition => row.share_holder_composition_updated_at,
        }
    }
}

pub struct StockListService {
    session: Arc<Session>,
    redis: redis::Client,
    auth_sessions: SessionStore,
}

impl StockListService {
    pub fn new(session: Arc<Session>, auth_sessions: SessionStore) -> Result<Self, String> {
        let redis = crate::redis_cache::client_from_env()?;
        Ok(Self {
            session,
            redis,
            auth_sessions,
        })
    }

    async fn require_auth<T>(&self, request: &Request<T>) -> Result<AuthSession, Status> {
        let token = extract_bearer_token(request)?;
        validate_session(&self.auth_sessions, &token)
            .await
            .map_err(|_| Status::unauthenticated("login diperlukan"))
    }

    fn log_rpc_debug(rpc_name: &str, user_name: &str, started: std::time::Instant) {
        eprintln!(
            "{rpc_name} {user_name} {}ms",
            started.elapsed().as_millis()
        );
    }

    fn should_refresh(updated_at: Option<DateTime<Utc>>) -> bool {
        let Some(updated_at) = updated_at else {
            return true;
        };
        Utc::now().timestamp() - updated_at.timestamp() > CACHE_MAX_AGE_SECS
    }

    fn stock_by_code_from_db_row(row: &DbStockListRow) -> StockByCodeResponse {
        let code = row.code.as_str();

        StockByCodeResponse {
            code: row.code.clone(),
            name: row.name.clone().unwrap_or_default(),
            sector: row.sector.clone().unwrap_or_default(),
            logo: row.logo.clone().unwrap_or_default(),
            keystats: row.keystats.clone().map(|db| {
                Self::keystats_data_from_model(Keystats::from(db), row.keystats_updated_at)
            }),
            balance_statement: row.balance_statement.clone().map(|db| {
                Self::panel_data_from_model(
                    BalanceStatement::from(db),
                    row.balance_statement_updated_at,
                )
            }),
            income_statement: row.income_statement.clone().map(|db| {
                Self::panel_data_from_model(
                    BalanceStatement::from(db),
                    row.income_statement_updated_at,
                )
            }),
            cash_flow: row.cash_flow.clone().map(|db| {
                Self::panel_data_from_model(BalanceStatement::from(db), row.cash_flow_updated_at)
            }),
            share_holder_5: row.share_holder_5.clone().map(|db| {
                Self::share_holder_5_data(
                    code,
                    ShareHolder5::from(Some(db)),
                    row.share_holder_5_updated_at,
                )
            }),
            share_holder_1: row.share_holder_1.clone().map(|db| {
                Self::share_holder_1_data(
                    code,
                    ShareHolder1::from(Some(db)),
                    row.share_holder_1_updated_at,
                )
            }),
            share_holder_composition: row.share_holder_composition.clone().map(|db| {
                Self::share_holder_composition_data(
                    code,
                    ShareHolderComposition::from(Some(db)),
                    row.share_holder_composition_updated_at,
                )
            }),
            company_information: row.company_information.clone().map(|db| {
                Self::company_information_data(
                    CompanyInformation::from(db),
                    row.company_information_updated_at,
                )
            }),
            corporate_action: row.corporate_action.clone().map(|db| {
                Self::corporate_action_data(
                    CorporateAction::from(db),
                    row.corporate_action_updated_at,
                )
            }),
            catatan_owner: row.catatan_owner.clone().unwrap_or_default(),
            catatan_pribadi: row.catatan_pribadi.clone().unwrap_or_default(),
            is_plan_to_trade: row.is_plan_to_trade.unwrap_or(false),
            is_konglomerasi: row.is_konglomerasi.unwrap_or(false),
            wyckoff_chart: row.wyckoff_chart.as_ref().map(Self::wyckoff_chart_from_db),
            horizontal_line: row.horizontal_line.clone().unwrap_or_default(),
        }
    }

    fn wyckoff_chart_from_db(db: &crate::model::WyckoffChartDb) -> WyckoffChartData {
        fn strings(v: &Option<Vec<String>>) -> Vec<String> {
            v.clone().unwrap_or_default()
        }

        WyckoffChartData {
            accumulation_trading_range: db.accumulation_trading_range.clone().unwrap_or_default(),
            distribution_trading_range: db.distribution_trading_range.clone().unwrap_or_default(),
            sc: strings(&db.sc),
            ar: strings(&db.ar),
            st: strings(&db.st),
            ps: strings(&db.ps),
            spr: strings(&db.spr),
            ut: strings(&db.ut),
            sos: strings(&db.sos),
            lps: strings(&db.lps),
            buec: strings(&db.buec),
            mup: strings(&db.mup),
            psy: strings(&db.psy),
            bc: strings(&db.bc),
            utad: strings(&db.utad),
            sow: strings(&db.sow),
            lpsy: strings(&db.lpsy),
            mdw: strings(&db.mdw),
        }
    }

    fn wyckoff_chart_to_db(data: WyckoffChartData) -> crate::model::WyckoffChartDb {
        fn opt_strings(values: Vec<String>) -> Option<Vec<String>> {
            if values.is_empty() {
                None
            } else {
                Some(values)
            }
        }

        fn opt_i32s(values: Vec<i32>) -> Option<Vec<i32>> {
            if values.is_empty() {
                None
            } else {
                Some(values)
            }
        }

        crate::model::WyckoffChartDb {
            accumulation_trading_range: opt_i32s(data.accumulation_trading_range),
            distribution_trading_range: opt_i32s(data.distribution_trading_range),
            sc: opt_strings(data.sc),
            ar: opt_strings(data.ar),
            st: opt_strings(data.st),
            ps: opt_strings(data.ps),
            spr: opt_strings(data.spr),
            ut: opt_strings(data.ut),
            sos: opt_strings(data.sos),
            lps: opt_strings(data.lps),
            buec: opt_strings(data.buec),
            mup: opt_strings(data.mup),
            psy: opt_strings(data.psy),
            bc: opt_strings(data.bc),
            utad: opt_strings(data.utad),
            sow: opt_strings(data.sow),
            lpsy: opt_strings(data.lpsy),
            mdw: opt_strings(data.mdw),
        }
    }

    fn summary_row_to_proto(row: StockListSummaryRow) -> StockListRow {
        StockListRow {
            code: row.code,
            name: row.name.unwrap_or_default(),
            sector: row.sector.unwrap_or_default(),
            logo: row.logo.unwrap_or_default(),
            keystats_updated_at: row.keystats_updated_at.map(|dt| dt.timestamp()),
            catatan_owner: row.catatan_owner.unwrap_or_default(),
            catatan_pribadi: row.catatan_pribadi.unwrap_or_default(),
            is_plan_to_trade: row.is_plan_to_trade.unwrap_or(false),
            is_konglomerasi: row.is_konglomerasi.unwrap_or(false),
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

    fn keystats_data_from_model(
        keystats: Keystats,
        updated_at: Option<DateTime<Utc>>,
    ) -> KeystatsData {
        KeystatsData {
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
            updated_at: updated_at.map(|dt| dt.timestamp()),
        }
    }

    fn panel_data_from_model(
        statement: BalanceStatement,
        updated_at: Option<DateTime<Utc>>,
    ) -> StatementPanelData {
        StatementPanelData {
            rows: Self::financial_rows_to_proto(statement.rows),
            columns: Self::columns_to_proto(statement.columns),
            updated_at: updated_at.map(|dt| dt.timestamp()),
        }
    }

    fn financial_statement_from_parts(
        code: &str,
        keystats: Option<crate::model::StockListKeystatsDb>,
        keystats_updated_at: Option<DateTime<Utc>>,
        balance_statement: Option<crate::model::StockListBalanceStatementDb>,
        balance_statement_updated_at: Option<DateTime<Utc>>,
        income_statement: Option<crate::model::StockListIncomeStatementDb>,
        income_statement_updated_at: Option<DateTime<Utc>>,
        cash_flow: Option<crate::model::StockListCashFlowDb>,
        cash_flow_updated_at: Option<DateTime<Utc>>,
    ) -> FinancialStatementResponse {
        FinancialStatementResponse {
            code: code.to_string(),
            keystats: keystats.map(|db| {
                Self::keystats_data_from_model(Keystats::from(db), keystats_updated_at)
            }),
            balance_statement: balance_statement.map(|db| {
                Self::panel_data_from_model(BalanceStatement::from(db), balance_statement_updated_at)
            }),
            income_statement: income_statement.map(|db| {
                Self::panel_data_from_model(BalanceStatement::from(db), income_statement_updated_at)
            }),
            cash_flow: cash_flow.map(|db| {
                Self::panel_data_from_model(BalanceStatement::from(db), cash_flow_updated_at)
            }),
        }
    }

    fn financial_statement_from_db_row(row: &DbStockListRow) -> FinancialStatementResponse {
        Self::financial_statement_from_parts(
            &row.code,
            row.keystats.clone(),
            row.keystats_updated_at,
            row.balance_statement.clone(),
            row.balance_statement_updated_at,
            row.income_statement.clone(),
            row.income_statement_updated_at,
            row.cash_flow.clone(),
            row.cash_flow_updated_at,
        )
    }

    fn keystats_response_from_row(row: StockListKeystatsRow) -> KeystatsResponse {
        KeystatsResponse {
            code: row.code,
            keystats: row.keystats.map(|db| {
                Self::keystats_data_from_model(Keystats::from(db), row.keystats_updated_at)
            }),
        }
    }

    fn share_holder_5_data(
        code: &str,
        entries: ShareHolder5,
        updated_at: Option<DateTime<Utc>>,
    ) -> ShareHolder5Data {
        ShareHolder5Data {
            items: entries
                .items
                .into_iter()
                .map(|entry| ShareHolder5Entry {
                    code: code.to_string(),
                    name: entry.name,
                    date: entry.date.timestamp(),
                    val: entry.val,
                    percent: entry.percent,
                })
                .collect(),
            updated_at: updated_at.map(|dt| dt.timestamp()),
        }
    }

    fn share_holder_1_data(
        code: &str,
        entries: ShareHolder1,
        updated_at: Option<DateTime<Utc>>,
    ) -> ShareHolder1Data {
        ShareHolder1Data {
            items: entries
                .items
                .into_iter()
                .map(|entry| ShareHolder1Entry {
                    code: code.to_string(),
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
            updated_at: updated_at.map(|dt| dt.timestamp()),
        }
    }

    fn share_holder_composition_data(
        code: &str,
        entries: ShareHolderComposition,
        updated_at: Option<DateTime<Utc>>,
    ) -> ShareHolderCompositionData {
        ShareHolderCompositionData {
            items: entries
                .items
                .into_iter()
                .map(|entry| ShareHolderCompositionEntry {
                    code: code.to_string(),
                    name: entry.name,
                    percentage: entry.percentage,
                    badge: entry.badge,
                })
                .collect(),
            updated_at: updated_at.map(|dt| dt.timestamp()),
        }
    }

    fn share_holder_and_company_information_from_db_row(
        row: &DbStockListRow,
    ) -> ShareHolderAndCompanyInformationByCodeResponse {
        let code = row.code.as_str();

        let share_holder_5 = row.share_holder_5.clone().map(|db| {
            Self::share_holder_5_data(
                code,
                ShareHolder5::from(Some(db)),
                row.share_holder_5_updated_at,
            )
        });

        let share_holder_1 = row.share_holder_1.clone().map(|db| {
            Self::share_holder_1_data(
                code,
                ShareHolder1::from(Some(db)),
                row.share_holder_1_updated_at,
            )
        });

        let share_holder_composition = row.share_holder_composition.clone().map(|db| {
            Self::share_holder_composition_data(
                code,
                ShareHolderComposition::from(Some(db)),
                row.share_holder_composition_updated_at,
            )
        });

        let company_information = row.company_information.clone().map(|db| {
            Self::company_information_data(
                CompanyInformation::from(db),
                row.company_information_updated_at,
            )
        });

        ShareHolderAndCompanyInformationByCodeResponse {
            code: row.code.clone(),
            share_holder_5,
            share_holder_1,
            share_holder_composition,
            company_information,
        }
    }

    fn company_information_data(
        info: CompanyInformation,
        updated_at: Option<DateTime<Utc>>,
    ) -> CompanyInformationData {
        CompanyInformationData {
            address: info.address,
            industry: info.industry,
            subsindustry: info.subsindustry,
            activity: info.activity,
            name: info.name,
            npwp: info.npwp,
            board: info.board,
            sector: info.sector,
            subsector: info.subsector,
            listing_date: info.listing_date.map(|dt| dt.timestamp()),
            website: info.website,
            logo: info.logo,
            additional_info: info.additional_info,
            people: info.people,
            report_type: info.report_type,
            administration: info.administration,
            description: info.description,
            ipo_pct: info.ipo_pct,
            ipo_price: info.ipo_price,
            ipo_share: info.ipo_share,
            ipo_underwriter: info.ipo_underwriter,
            nominal_price: info.nominal_price,
            category: info.category,
            active: info.active,
            commissioner: info
                .commissioner
                .into_iter()
                .map(|e| CompanyPersonEntry {
                    name: e.name,
                    position: e.position,
                })
                .collect(),
            director: info
                .director
                .into_iter()
                .map(|e| CompanyPersonEntry {
                    name: e.name,
                    position: e.position,
                })
                .collect(),
            subsidiary: info
                .subsidiary
                .into_iter()
                .map(|e| CompanySubsidiaryEntry {
                    name: e.name,
                    percentage: e.percentage,
                })
                .collect(),
            updated_at: updated_at.map(|dt| dt.timestamp()),
        }
    }

    fn corporate_action_data(
        action: CorporateAction,
        updated_at: Option<DateTime<Utc>>,
    ) -> CorporateActionData {
        CorporateActionData {
            total_page: action.total_page,
            page: action.page,
            next_page: action.next_page,
            data: action
                .data
                .into_iter()
                .map(|entry| CorporateActionEntry {
                    code: entry.code,
                    r#type: entry.action_type,
                    payload: entry.payload,
                })
                .collect(),
            updated_at: updated_at.map(|dt| dt.timestamp()),
        }
    }

    fn corporate_action_by_code_from_db_row(row: &DbStockListRow) -> CorporateActionByCodeResponse {
        let corporate_action = row.corporate_action.clone().map(|db| {
            Self::corporate_action_data(
                CorporateAction::from(db),
                row.corporate_action_updated_at,
            )
        });

        CorporateActionByCodeResponse {
            code: row.code.clone(),
            corporate_action,
        }
    }

    async fn refresh_statement_if_stale(
        session: Arc<Session>,
        code: &str,
        kind: StatementKind,
    ) -> Result<(), Status> {
        match kind {
            StatementKind::Keystats => {
                crate::invezgo::fetch_and_save_keystats(session, code)
                    .await
                    .map_err(Status::internal)?;
            }
            StatementKind::Bs => {
                crate::invezgo::fetch_and_save_balance_statement(session, code)
                    .await
                    .map_err(Status::internal)?;
            }
            StatementKind::Is => {
                crate::invezgo::fetch_and_save_income_statement(session, code)
                    .await
                    .map_err(Status::internal)?;
            }
            StatementKind::Cf => {
                crate::invezgo::fetch_and_save_cash_flow(session, code)
                    .await
                    .map_err(Status::internal)?;
            }
        }
        Ok(())
    }

    async fn refresh_share_holder_if_stale(
        session: Arc<Session>,
        code: &str,
        kind: ShareHolderKind,
    ) -> Result<(), Status> {
        match kind {
            ShareHolderKind::Holder5 => {
                crate::invezgo::fetch_and_save_share_holder_5(session, code)
                    .await
                    .map_err(Status::internal)?;
            }
            ShareHolderKind::Holder1 => {
                crate::invezgo::fetch_and_save_share_holder_1(session, code)
                    .await
                    .map_err(Status::internal)?;
            }
            ShareHolderKind::Composition => {
                crate::invezgo::fetch_and_save_share_holder_composition(session, code)
                    .await
                    .map_err(Status::internal)?;
            }
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl StockList for StockListService {
    async fn get_all_stocks(
        &self,
        request: Request<GetAllStocksRequest>,
    ) -> Result<Response<GetAllStocksResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<GetAllStocksResponse>, Status> = async {
            let _inner = request.into_inner();
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

            let rows = crate::repository::list_all(self.session.as_ref())
                .await
                .map_err(Status::internal)?;

            let items = rows.into_iter().map(Self::summary_row_to_proto).collect();

            Ok(Response::new(GetAllStocksResponse {
                success: true,
                message,
                items,
            }))
        }
        .await;

        Self::log_rpc_debug("GetAllStocks", &user_name, started);
        result
    }

    async fn get_stock_by_code(
        &self,
        request: Request<GetStockByCodeRequest>,
    ) -> Result<Response<StockByCodeResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<StockByCodeResponse>, Status> = async {
            let code = request.into_inner().code.trim().to_ascii_uppercase();
            if code.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi"));
            }

            let row = crate::repository::get_by_code(self.session.as_ref(), &code)
                .await
                .map_err(Status::internal)?
                .ok_or_else(|| Status::not_found(format!("stock_list code={code} tidak ditemukan")))?;

            Ok(Response::new(Self::stock_by_code_from_db_row(&row)))
        }
        .await;

        Self::log_rpc_debug("GetStockByCode", &user_name, started);
        result
    }

    async fn get_stock_by_repeated_code(
        &self,
        request: Request<GetStockByRepeatedCodeRequest>,
    ) -> Result<Response<GetStockByRepeatedCodeResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<GetStockByRepeatedCodeResponse>, Status> = async {
            let codes = request.into_inner().code;
            if codes.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi (minimal 1)"));
            }

            let mut items = Vec::new();
            let mut seen = std::collections::HashSet::new();
            for raw in codes {
                let code = raw.trim().to_ascii_uppercase();
                if code.is_empty() || !seen.insert(code.clone()) {
                    continue;
                }
                if let Some(row) = crate::repository::get_by_code(self.session.as_ref(), &code)
                    .await
                    .map_err(Status::internal)?
                {
                    items.push(Self::stock_by_code_from_db_row(&row));
                }
            }

            Ok(Response::new(GetStockByRepeatedCodeResponse { items }))
        }
        .await;

        Self::log_rpc_debug("GetStockByRepeatedCode", &user_name, started);
        result
    }

    async fn get_financial_statement_by_code(
        &self,
        request: Request<GetFinancialStatementByCodeRequest>,
    ) -> Result<Response<FinancialStatementResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<FinancialStatementResponse>, Status> = async {
            let code = request.into_inner().code.trim().to_ascii_uppercase();
            if code.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi"));
            }

            let mut existing = crate::repository::get_by_code(self.session.as_ref(), &code)
                .await
                .map_err(Status::internal)?;

            for kind in ALL_STATEMENT_KINDS {
                let refresh = existing
                    .as_ref()
                    .map(|row| Self::should_refresh(kind.updated_at(row)))
                    .unwrap_or(true);

                if refresh {
                    Self::refresh_statement_if_stale(self.session.clone(), &code, kind).await?;
                    existing = crate::repository::get_by_code(self.session.as_ref(), &code)
                        .await
                        .map_err(Status::internal)?;
                }
            }

            let row = existing.ok_or_else(|| {
                Status::not_found(format!("stock_list code={code} tidak ditemukan"))
            })?;

            Ok(Response::new(Self::financial_statement_from_db_row(&row)))
        }
        .await;

        Self::log_rpc_debug("GetFinancialStatementByCode", &user_name, started);
        result
    }

    async fn get_all_key_stats(
        &self,
        request: Request<GetAllKeyStatsRequest>,
    ) -> Result<Response<GetAllKeyStatsResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<GetAllKeyStatsResponse>, Status> = async {
            let _inner = request.into_inner();
            let rows = crate::repository::list_all_keystats(self.session.as_ref())
                .await
                .map_err(Status::internal)?;

            let items = rows
                .into_iter()
                .map(Self::keystats_response_from_row)
                .collect();

            Ok(Response::new(GetAllKeyStatsResponse { items }))
        }
        .await;

        Self::log_rpc_debug("GetAllKeyStats", &user_name, started);
        result
    }

    async fn get_share_holder_and_company_information_by_code(
        &self,
        request: Request<GetShareHolderAndCompanyInformationByCodeRequest>,
    ) -> Result<Response<ShareHolderAndCompanyInformationByCodeResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<ShareHolderAndCompanyInformationByCodeResponse>, Status> =
            async {
                let code = request.into_inner().code.trim().to_ascii_uppercase();
                if code.is_empty() {
                    return Err(Status::invalid_argument("code wajib diisi"));
                }

                let mut existing = crate::repository::get_by_code(self.session.as_ref(), &code)
                    .await
                    .map_err(Status::internal)?;

                for kind in ALL_SHARE_HOLDER_KINDS {
                    let refresh = existing
                        .as_ref()
                        .map(|row| Self::should_refresh(kind.updated_at(row)))
                        .unwrap_or(true);

                    if refresh {
                        Self::refresh_share_holder_if_stale(self.session.clone(), &code, kind)
                            .await?;
                        existing = crate::repository::get_by_code(self.session.as_ref(), &code)
                            .await
                            .map_err(Status::internal)?;
                    }
                }

                let refresh_company_information = existing
                    .as_ref()
                    .map(|row| Self::should_refresh(row.company_information_updated_at))
                    .unwrap_or(true);

                if refresh_company_information {
                    crate::invezgo::fetch_and_save_company_information(self.session.clone(), &code)
                        .await
                        .map_err(Status::internal)?;
                    existing = crate::repository::get_by_code(self.session.as_ref(), &code)
                        .await
                        .map_err(Status::internal)?;
                }

                let row = existing.ok_or_else(|| {
                    Status::not_found(format!("stock_list code={code} tidak ditemukan"))
                })?;

                Ok(Response::new(
                    Self::share_holder_and_company_information_from_db_row(&row),
                ))
            }
            .await;

        Self::log_rpc_debug("GetShareHolderAndCompanyInformationByCode", &user_name, started);
        result
    }

    async fn get_corporate_action_by_code(
        &self,
        request: Request<GetCorporateActionByCodeRequest>,
    ) -> Result<Response<CorporateActionByCodeResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<CorporateActionByCodeResponse>, Status> = async {
            let code = request.into_inner().code.trim().to_ascii_uppercase();
            if code.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi"));
            }

            let mut existing = crate::repository::get_by_code(self.session.as_ref(), &code)
                .await
                .map_err(Status::internal)?;

            let refresh = existing
                .as_ref()
                .map(|row| Self::should_refresh(row.corporate_action_updated_at))
                .unwrap_or(true);

            if refresh {
                crate::invezgo::fetch_and_save_corporate_action(self.session.clone(), &code)
                    .await
                    .map_err(Status::internal)?;
                existing = crate::repository::get_by_code(self.session.as_ref(), &code)
                    .await
                    .map_err(Status::internal)?;
            }

            let row = existing.ok_or_else(|| {
                Status::not_found(format!("stock_list code={code} tidak ditemukan"))
            })?;

            Ok(Response::new(Self::corporate_action_by_code_from_db_row(&row)))
        }
        .await;

        Self::log_rpc_debug("GetCorporateActionByCode", &user_name, started);
        result
    }

    async fn get_wyckoff_chart_by_code(
        &self,
        request: Request<GetWyckoffChartByCodeRequest>,
    ) -> Result<Response<GetWyckoffChartByCodeResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<GetWyckoffChartByCodeResponse>, Status> = async {
            let code = request.into_inner().code.trim().to_ascii_uppercase();
            if code.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi"));
            }

            let row = crate::repository::get_wyckoff_chart_by_code(self.session.as_ref(), &code)
                .await
                .map_err(Status::internal)?
                .ok_or_else(|| Status::not_found(format!("stock_list code={code} tidak ditemukan")))?;

            Ok(Response::new(GetWyckoffChartByCodeResponse {
                code: row.code,
                wyckoff_chart: row
                    .wyckoff_chart
                    .as_ref()
                    .map(Self::wyckoff_chart_from_db),
            }))
        }
        .await;

        Self::log_rpc_debug("GetWyckoffChartByCode", &user_name, started);
        result
    }

    async fn update_is_konglomerasi(
        &self,
        request: Request<UpdateIsKonglomerasiRequest>,
    ) -> Result<Response<UpdateIsKonglomerasiResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<UpdateIsKonglomerasiResponse>, Status> = async {
            let inner = request.into_inner();
            let code = inner.code.trim().to_ascii_uppercase();
            if code.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi"));
            }

            crate::repository::update_is_konglomerasi(
                self.session.as_ref(),
                &code,
                inner.is_konglomerasi,
            )
            .await
            .map_err(|e| {
                if e.contains("tidak ditemukan") {
                    Status::not_found(e)
                } else {
                    Status::internal(e)
                }
            })?;

            Ok(Response::new(UpdateIsKonglomerasiResponse {
                code,
                is_konglomerasi: inner.is_konglomerasi,
            }))
        }
        .await;

        Self::log_rpc_debug("UpdateIsKonglomerasi", &user_name, started);
        result
    }

    async fn update_is_plan_to_trade(
        &self,
        request: Request<UpdateIsPlanToTradeRequest>,
    ) -> Result<Response<UpdateIsPlanToTradeResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<UpdateIsPlanToTradeResponse>, Status> = async {
            let inner = request.into_inner();
            let code = inner.code.trim().to_ascii_uppercase();
            if code.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi"));
            }

            crate::repository::update_is_plan_to_trade(
                self.session.as_ref(),
                &code,
                inner.is_plan_to_trade,
            )
            .await
            .map_err(|e| {
                if e.contains("tidak ditemukan") {
                    Status::not_found(e)
                } else {
                    Status::internal(e)
                }
            })?;

            Ok(Response::new(UpdateIsPlanToTradeResponse {
                code,
                is_plan_to_trade: inner.is_plan_to_trade,
            }))
        }
        .await;

        Self::log_rpc_debug("UpdateIsPlanToTrade", &user_name, started);
        result
    }

    async fn update_catatan_owner(
        &self,
        request: Request<UpdateCatatanOwnerRequest>,
    ) -> Result<Response<UpdateCatatanOwnerResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<UpdateCatatanOwnerResponse>, Status> = async {
            let inner = request.into_inner();
            let code = inner.code.trim().to_ascii_uppercase();
            if code.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi"));
            }

            crate::repository::update_catatan_owner(
                self.session.as_ref(),
                &code,
                inner.catatan_owner.as_str(),
            )
            .await
            .map_err(|e| {
                if e.contains("tidak ditemukan") {
                    Status::not_found(e)
                } else {
                    Status::internal(e)
                }
            })?;

            Ok(Response::new(UpdateCatatanOwnerResponse {
                code,
                catatan_owner: inner.catatan_owner,
            }))
        }
        .await;

        Self::log_rpc_debug("UpdateCatatanOwner", &user_name, started);
        result
    }

    async fn update_catatan_pribadi(
        &self,
        request: Request<UpdateCatatanPribadiRequest>,
    ) -> Result<Response<UpdateCatatanPribadiResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<UpdateCatatanPribadiResponse>, Status> = async {
            let inner = request.into_inner();
            let code = inner.code.trim().to_ascii_uppercase();
            if code.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi"));
            }

            crate::repository::update_catatan_pribadi(
                self.session.as_ref(),
                &code,
                inner.catatan_pribadi.as_str(),
            )
            .await
            .map_err(|e| {
                if e.contains("tidak ditemukan") {
                    Status::not_found(e)
                } else {
                    Status::internal(e)
                }
            })?;

            Ok(Response::new(UpdateCatatanPribadiResponse {
                code,
                catatan_pribadi: inner.catatan_pribadi,
            }))
        }
        .await;

        Self::log_rpc_debug("UpdateCatatanPribadi", &user_name, started);
        result
    }

    async fn update_wyckoff_chart_by_code(
        &self,
        request: Request<UpdateWyckoffChartByCodeRequest>,
    ) -> Result<Response<UpdateWyckoffChartByCodeResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<UpdateWyckoffChartByCodeResponse>, Status> = async {
            let inner = request.into_inner();
            let code = inner.code.trim().to_ascii_uppercase();
            if code.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi"));
            }

            let Some(wyckoff_chart) = inner.wyckoff_chart else {
                return Err(Status::invalid_argument("wyckoff_chart wajib diisi"));
            };

            crate::repository::update_wyckoff_chart(
                self.session.as_ref(),
                &code,
                Self::wyckoff_chart_to_db(wyckoff_chart),
            )
            .await
            .map_err(|e| {
                if e.contains("tidak ditemukan") {
                    Status::not_found(e)
                } else {
                    Status::internal(e)
                }
            })?;

            Ok(Response::new(UpdateWyckoffChartByCodeResponse {
                success: true,
                message: format!("wyckoff_chart code={code} berhasil diupdate"),
            }))
        }
        .await;

        Self::log_rpc_debug("UpdateWyckoffChartByCode", &user_name, started);
        result
    }

    async fn get_horizontal_line_by_code(
        &self,
        request: Request<GetHorizontalLineByCodeRequest>,
    ) -> Result<Response<GetHorizontalLineByCodeResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<GetHorizontalLineByCodeResponse>, Status> = async {
            let code = request.into_inner().code.trim().to_ascii_uppercase();
            if code.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi"));
            }

            let row = crate::repository::get_horizontal_line_by_code(self.session.as_ref(), &code)
                .await
                .map_err(Status::internal)?
                .ok_or_else(|| Status::not_found(format!("stock_list code={code} tidak ditemukan")))?;

            Ok(Response::new(GetHorizontalLineByCodeResponse {
                code: row.code,
                horizontal_line: row.horizontal_line.unwrap_or_default(),
            }))
        }
        .await;

        Self::log_rpc_debug("GetHorizontalLineByCode", &user_name, started);
        result
    }

    async fn update_horizontal_line_by_code(
        &self,
        request: Request<UpdateHorizontalLineByCodeRequest>,
    ) -> Result<Response<UpdateHorizontalLineByCodeResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<UpdateHorizontalLineByCodeResponse>, Status> = async {
            let inner = request.into_inner();
            let code = inner.code.trim().to_ascii_uppercase();
            if code.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi"));
            }

            crate::repository::update_horizontal_line(
                self.session.as_ref(),
                &code,
                &inner.horizontal_line,
            )
            .await
            .map_err(|e| {
                if e.contains("tidak ditemukan") {
                    Status::not_found(e)
                } else {
                    Status::internal(e)
                }
            })?;

            Ok(Response::new(UpdateHorizontalLineByCodeResponse {
                success: true,
                message: format!(
                    "horizontal_line code={code} berhasil diupdate ({} nilai)",
                    inner.horizontal_line.len()
                ),
            }))
        }
        .await;

        Self::log_rpc_debug("UpdateHorizontalLineByCode", &user_name, started);
        result
    }
}
