use std::pin::Pin;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::Stream;
use grpc_stream::send_or_break;
use scylla::client::session::Session;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use user::{extract_bearer_token, validate_session, AuthSession, SessionStore};

use crate::model::{
    BalanceStatement, CompanyInformation, CorporateAction, Keystats, KeyStatsFromStockbitRow,
    ShareHolder1, ShareHolder5, ShareHolderComposition,
    StockbitProfileByCodeRow, StockbitReportsByCodeRow,
    StockListKeystatsRow, StockListRow as DbStockListRow, StockListSummaryRow,
    StockbitClosureFinItemsGroupDb, StockbitDividendGroupDb, StockbitDividendYearValueDb,
    StockbitFinancialYearGroupDb, StockbitFinancialYearValueDb,
    StockbitFinNameResultDb, StockbitFitemDb, StockbitMostRecentQuarterDb, StockbitPeriodValueDb,
    StockbitProfileAddressDb, StockbitProfileAssetAllocationEntryDb, StockbitProfileBeneficiaryDb,
    StockbitProfileDb, StockbitProfileExecutiveEntryDb, StockbitProfileFeeEntryDb,
    StockbitProfileFundProfileDb, StockbitProfileHistoryDb, StockbitProfileKeyExecutiveDb,
    StockbitProfileListingInformationDb, StockbitProfilePercentageDb, StockbitProfileProspectusDb,
    StockbitProfileShareholderEntryDb, StockbitProfileShareholderNumberDb,
    StockbitProfileShareholderOnePercentDb, StockbitProfileSubsidiaryDb,
    StockbitProfileTopHoldingEntryDb, StockbitProfileValueInfoDb,
    StockbitReportFollowingActivityDb, StockbitReportItemDb, StockbitReportNewsFeedDb,
    StockbitReportReactionDb, StockbitReportReactionEntryDb, StockbitReportStatusDb,
    StockbitReportStreamDb, StockbitReportSummaryDb, StockbitReportUserDb, StockbitStatsDb,
};
use crate::pb::stock_list_server::StockList;
use crate::pb::{
    CompanyInformationData, CompanyPersonEntry, CompanySubsidiaryEntry,
    CorporateActionByCodeResponse, CorporateActionData, CorporateActionEntry,
    FinancialStatementResponse, FinancialStatementRowItem,     GetAllKeyStatsRequest, GetAllKeyStatsResponse, GetAllKeyStatsStreamPart, GetAllStocksRequest, GetAllStocksResponse,
    GetCorporateActionByCodeRequest, GetFinancialStatementByCodeRequest,
    GetHorizontalLineByCodeRequest, GetHorizontalLineByCodeResponse,
    GetTakeProfitWyckoffByCodeRequest, GetTakeProfitWyckoffByCodeResponse,
    GetKeyStatsFromStockbitRequest, GetStockbitProfileByCodeRequest,
    GetStockbitReportsByCodeRequest, GetStockByCodeRequest,
    GetStockByRepeatedCodeRequest, GetStockByRepeatedCodeResponse,
    GetStockByRepeatedCodeStreamPart, GetShareHolderAndCompanyInformationByCodeRequest, GetWyckoffChartByCodeRequest,
    GetWyckoffChartByCodeResponse,
    KeystatsData, KeystatsResponse, KeyStatsColumn, KeyStatsFromStockbitResponse,
    KeyStatsFromStockbitStreamPart, KeyStatsRowItem, KeyStatsValue, ShareHolder1Data, ShareHolder1Entry,
    ShareHolder5Data,
    ShareHolder5Entry, ShareHolderAndCompanyInformationByCodeResponse,
    ShareHolderCompositionData, ShareHolderCompositionEntry, StatementPanelData,
    StockbitClosureFinItemsGroup, StockbitDividendGroup, StockbitDividendYearValue,
    StockbitFinancialYearGroup, StockbitFinancialYearParent, StockbitFinancialYearValue,
    StockbitFinNameResult, StockbitFitem, StockbitMostRecentQuarter, StockbitPeriodValue,
    StockbitProfileAddress, StockbitProfileAssetAllocationEntry, StockbitProfileBeneficiary,
    StockbitProfileData, StockbitProfileExecutiveEntry, StockbitProfileFeeEntry,
    StockbitProfileFundProfile, StockbitProfileHistory, StockbitProfileKeyExecutive,
    StockbitProfileListingInformation, StockbitProfilePercentage, StockbitProfileProspectus,
    StockbitProfileResponse, StockbitProfileShareholderEntry, StockbitProfileShareholderNumber,
    StockbitProfileShareholderOnePercent, StockbitProfileStreamPart, StockbitProfileSubsidiary,
    StockbitProfileTopHoldingEntry, StockbitProfileValueInfo,
    StockbitReportFollowingActivity, StockbitReportItem, StockbitReportNewsFeed, StockbitReportReaction,
    StockbitReportReactionEntry, StockbitReportsRow, StockbitReportsStreamPart, StockbitReportStatus, StockbitReportStream,
    StockbitReportSummary, StockbitReportUser, StockbitStats, StockByCodeResponse, StockListRow,
    NotationEntry,
    UpdateHorizontalLineByCodeRequest, UpdateHorizontalLineByCodeResponse,
    UpsertTakeProfitWyckoffByCodeRequest, UpsertTakeProfitWyckoffByCodeResponse,
    DeleteTakeProfitWyckoffByCodeRequest, DeleteTakeProfitWyckoffByCodeResponse,
    UpdateIsKonglomerasiRequest, UpdateIsKonglomerasiResponse, UpdateIsPlanToTradeRequest,
    UpdateIsPlanToTradeResponse, UpdateIsBadFundamentalByCodeRequest,
    UpdateIsBadFundamentalByCodeResponse, UpdateNotationInvezgoRequest,
    UpdateNotationInvezgoResponse, UpdateCatatanOwnerRequest, UpdateCatatanOwnerResponse,
    UpdateCatatanPribadiRequest, UpdateCatatanPribadiResponse, UpdateSubSectorRequest,
    UpdateSubSectorResponse, UpdateWyckoffChartByCodeRequest,
    UpdateWyckoffChartByCodeResponse, WyckoffChartData,
};

const CACHE_MAX_AGE_SECS: i64 = 30 * 24 * 60 * 60;
const KEYSTATS_FROM_STOCKBIT_COOLDOWN: Duration = Duration::from_secs(5);

type GetStockbitProfileByCodeStream =
    Pin<Box<dyn Stream<Item = Result<StockbitProfileResponse, Status>> + Send>>;

type GetKeyStatsFromStockbitStream =
    Pin<Box<dyn Stream<Item = Result<KeyStatsFromStockbitResponse, Status>> + Send>>;

type GetStockbitReportsByCodeStream =
    Pin<Box<dyn Stream<Item = Result<StockbitReportsRow, Status>> + Send>>;

type GetStockByRepeatedCodeStream =
    Pin<Box<dyn Stream<Item = Result<GetStockByRepeatedCodeResponse, Status>> + Send>>;

type GetAllKeyStatsStream =
    Pin<Box<dyn Stream<Item = Result<GetAllKeyStatsResponse, Status>> + Send>>;

static LAST_KEYSTATS_FROM_STOCKBIT: OnceLock<Mutex<Option<std::time::Instant>>> = OnceLock::new();

fn keystats_from_stockbit_gate() -> &'static Mutex<Option<std::time::Instant>> {
    LAST_KEYSTATS_FROM_STOCKBIT.get_or_init(|| Mutex::new(None))
}

async fn acquire_keystats_from_stockbit_slot(user_name: &str) -> Result<(), Status> {
    let mut last = keystats_from_stockbit_gate().lock().await;
    if let Some(at) = *last {
        let elapsed = at.elapsed();
        if elapsed < KEYSTATS_FROM_STOCKBIT_COOLDOWN {
            let remaining_secs = (KEYSTATS_FROM_STOCKBIT_COOLDOWN - elapsed).as_secs().max(1);
            eprintln!(
                "GetKeyStatsFromStockbit {user_name} rate-limit ditolak: sisa {remaining_secs}s"
            );
            return Err(Status::failed_precondition(format!(
                "Rate limit: maksimal 1× / 5 detik untuk semua user. Tunggu {remaining_secs} detik lagi"
            )));
        }
    }
    *last = Some(std::time::Instant::now());
    Ok(())
}

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
    all_stocks_cache: crate::all_stocks_cache::AllStocksCache,
}

impl StockListService {
    pub fn new(session: Arc<Session>, auth_sessions: SessionStore) -> Result<Self, String> {
        let redis = crate::redis_cache::client_from_env()?;
        Ok(Self {
            session,
            redis,
            auth_sessions,
            all_stocks_cache: crate::all_stocks_cache::AllStocksCache::new(),
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
            sub_sector: row.sub_sector.clone().unwrap_or_default(),
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
            takeprofit_wyckoff: row.takeprofit_wyckoff.clone().unwrap_or_default(),
            is_bad_fundamental: row.is_bad_fundamental.unwrap_or(false),
            notation: row
                .notation
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(|e| NotationEntry {
                    notation: e.notation,
                    description: e.description,
                })
                .collect(),
            is_idx_30: row.is_idx_30.unwrap_or(false),
            is_lq_45: row.is_lq_45.unwrap_or(false),
            is_idx_80: row.is_idx_80.unwrap_or(false),
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
            sub_sector: row.sub_sector.unwrap_or_default(),
            logo: row.logo.unwrap_or_default(),
            keystats_updated_at: row.keystats_updated_at.map(|dt| dt.timestamp()),
            catatan_owner: row.catatan_owner.unwrap_or_default(),
            catatan_pribadi: row.catatan_pribadi.unwrap_or_default(),
            is_plan_to_trade: row.is_plan_to_trade.unwrap_or(false),
            is_konglomerasi: row.is_konglomerasi.unwrap_or(false),
            takeprofit_wyckoff: row.takeprofit_wyckoff.unwrap_or_default(),
            is_bad_fundamental: row.is_bad_fundamental.unwrap_or(false),
            notation: row
                .notation
                .unwrap_or_default()
                .into_iter()
                .map(|e| NotationEntry {
                    notation: e.notation,
                    description: e.description,
                })
                .collect(),
            is_idx_30: row.is_idx_30.unwrap_or(false),
            is_lq_45: row.is_lq_45.unwrap_or(false),
            is_idx_80: row.is_idx_80.unwrap_or(false),
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

    fn stockbit_fitem_to_proto(item: StockbitFitemDb) -> StockbitFitem {
        StockbitFitem {
            id: item.id,
            name: item.name,
            value: item.value,
        }
    }

    fn stockbit_fin_name_result_to_proto(item: StockbitFinNameResultDb) -> StockbitFinNameResult {
        StockbitFinNameResult {
            fitem: Some(Self::stockbit_fitem_to_proto(item.fitem)),
            hidden_graph_ico: item.hidden_graph_ico,
            is_new_update: item.is_new_update,
        }
    }

    fn stockbit_closure_group_to_proto(
        group: StockbitClosureFinItemsGroupDb,
    ) -> StockbitClosureFinItemsGroup {
        StockbitClosureFinItemsGroup {
            keystats_name: group.keystats_name,
            fin_name_results: group
                .fin_name_results
                .unwrap_or_default()
                .into_iter()
                .map(Self::stockbit_fin_name_result_to_proto)
                .collect(),
        }
    }

    fn stockbit_period_value_to_proto(item: StockbitPeriodValueDb) -> StockbitPeriodValue {
        StockbitPeriodValue {
            period: item.period,
            quarter_value: item.quarter_value,
            year: item.year,
            is_new_update: item.is_new_update,
        }
    }

    fn stockbit_financial_year_value_to_proto(
        item: StockbitFinancialYearValueDb,
    ) -> StockbitFinancialYearValue {
        StockbitFinancialYearValue {
            year: item.year,
            period_values: item
                .period_values
                .unwrap_or_default()
                .into_iter()
                .map(Self::stockbit_period_value_to_proto)
                .collect(),
            annualised_value: item.annualised_value,
            ttm_value: item.ttm_value,
            is_new_update: item.is_new_update,
            dividend: item.dividend,
            payout_ratio: item.payout_ratio,
            dividend_yield: item.dividend_yield,
        }
    }

    fn stockbit_most_recent_quarter_to_proto(
        item: StockbitMostRecentQuarterDb,
    ) -> StockbitMostRecentQuarter {
        StockbitMostRecentQuarter {
            date: item.date,
            quarter: item.quarter,
            is_new_update: item.is_new_update,
        }
    }

    fn stockbit_financial_year_group_to_proto(
        group: StockbitFinancialYearGroupDb,
    ) -> StockbitFinancialYearGroup {
        StockbitFinancialYearGroup {
            financial_year_values: group
                .financial_year_values
                .unwrap_or_default()
                .into_iter()
                .map(Self::stockbit_financial_year_value_to_proto)
                .collect(),
            fitem_name: group.fitem_name,
            most_recent_quarter: Some(Self::stockbit_most_recent_quarter_to_proto(
                group.most_recent_quarter,
            )),
        }
    }

    fn stockbit_stats_to_proto(stats: Option<StockbitStatsDb>) -> StockbitStats {
        let Some(stats) = stats else {
            return StockbitStats::default();
        };
        StockbitStats {
            current_share_outstanding: stats.current_share_outstanding,
            market_cap: stats.market_cap,
            enterprise_value: stats.enterprise_value,
            free_float: stats.free_float,
        }
    }

    fn stockbit_dividend_year_value_to_proto(
        item: StockbitDividendYearValueDb,
    ) -> StockbitDividendYearValue {
        StockbitDividendYearValue {
            period: item.period,
            dividend: item.dividend,
            ex_date: item.ex_date,
            payment_date: item.payment_date,
        }
    }

    fn stockbit_dividend_group_to_proto(group: Option<StockbitDividendGroupDb>) -> StockbitDividendGroup {
        let Some(group) = group else {
            return StockbitDividendGroup::default();
        };
        StockbitDividendGroup {
            fitem_id: group.fitem_id.unwrap_or_default(),
            dividend_year_values: group
                .dividend_year_values
                .unwrap_or_default()
                .into_iter()
                .map(Self::stockbit_dividend_year_value_to_proto)
                .collect(),
        }
    }

    fn keystats_from_stockbit_stream_chunk(
        code: &str,
        message: &str,
        part: KeyStatsFromStockbitStreamPart,
        mut response: KeyStatsFromStockbitResponse,
    ) -> KeyStatsFromStockbitResponse {
        response.success = true;
        response.message = message.to_string();
        response.code = code.to_string();
        response.part = part.into();
        response
    }

    fn keystats_from_stockbit_row_stream(
        row: KeyStatsFromStockbitRow,
        message: &str,
    ) -> Vec<KeyStatsFromStockbitResponse> {
        let msg = message.to_string();
        let code = row.code.clone();
        let closure_updated_at = row
            .closure_fin_items_results_stockbit_updated_at
            .map(|dt| dt.timestamp());
        let financial_year_updated_at = row
            .financial_year_parent_stockbit_updated_at
            .map(|dt| dt.timestamp());
        let stats_updated_at = row.stats_stockbit_updated_at.map(|dt| dt.timestamp());
        let dividend_updated_at = row
            .dividend_group_stockbit_updated_at
            .map(|dt| dt.timestamp());

        let mut chunks = vec![Self::keystats_from_stockbit_stream_chunk(
            &code,
            &msg,
            KeyStatsFromStockbitStreamPart::Meta,
            KeyStatsFromStockbitResponse {
                stats: Some(Self::stockbit_stats_to_proto(row.stats_stockbit)),
                stats_updated_at,
                closure_fin_items_results_updated_at: closure_updated_at,
                financial_year_parent_updated_at: financial_year_updated_at,
                dividend_group_updated_at: dividend_updated_at,
                ..Default::default()
            },
        )];

        for group in row
            .closure_fin_items_results_stockbit
            .unwrap_or_default()
        {
            chunks.push(Self::keystats_from_stockbit_stream_chunk(
                &code,
                &msg,
                KeyStatsFromStockbitStreamPart::ClosureFinItemsGroup,
                KeyStatsFromStockbitResponse {
                    closure_fin_items_results: vec![Self::stockbit_closure_group_to_proto(group)],
                    closure_fin_items_results_updated_at: closure_updated_at,
                    ..Default::default()
                },
            ));
        }

        if let Some(parent) = row.financial_year_parent_stockbit {
            for group in parent.financial_year_groups.unwrap_or_default() {
                chunks.push(Self::keystats_from_stockbit_stream_chunk(
                    &code,
                    &msg,
                    KeyStatsFromStockbitStreamPart::FinancialYearGroup,
                    KeyStatsFromStockbitResponse {
                        financial_year_parent: Some(StockbitFinancialYearParent {
                            financial_year_groups: vec![
                                Self::stockbit_financial_year_group_to_proto(group),
                            ],
                            financial_year_groups_usd: vec![],
                        }),
                        financial_year_parent_updated_at: financial_year_updated_at,
                        ..Default::default()
                    },
                ));
            }
            for group in parent.financial_year_groups_usd.unwrap_or_default() {
                chunks.push(Self::keystats_from_stockbit_stream_chunk(
                    &code,
                    &msg,
                    KeyStatsFromStockbitStreamPart::FinancialYearGroupUsd,
                    KeyStatsFromStockbitResponse {
                        financial_year_parent: Some(StockbitFinancialYearParent {
                            financial_year_groups: vec![],
                            financial_year_groups_usd: vec![
                                Self::stockbit_financial_year_group_to_proto(group),
                            ],
                        }),
                        financial_year_parent_updated_at: financial_year_updated_at,
                        ..Default::default()
                    },
                ));
            }
        }

        chunks.push(Self::keystats_from_stockbit_stream_chunk(
            &code,
            &msg,
            KeyStatsFromStockbitStreamPart::DividendGroup,
            KeyStatsFromStockbitResponse {
                dividend_group: Some(Self::stockbit_dividend_group_to_proto(
                    row.dividend_group_stockbit,
                )),
                dividend_group_updated_at: dividend_updated_at,
                ..Default::default()
            },
        ));

        chunks.push(Self::keystats_from_stockbit_stream_chunk(
            &code,
            &msg,
            KeyStatsFromStockbitStreamPart::Done,
            KeyStatsFromStockbitResponse::default(),
        ));

        chunks
    }

    fn stockbit_report_user_to_proto(user: StockbitReportUserDb) -> StockbitReportUser {
        StockbitReportUser {
            user_id: user.user_id,
            is_author: user.is_author,
            username: user.username,
            fullname: user.fullname,
            avatar: user.avatar,
            is_verified: user.is_verified,
            user_privilege: user.user_privilege,
            is_pro: user.is_pro,
            country: user.country,
            verified_status: user.verified_status,
        }
    }

    fn stockbit_report_status_to_proto(status: StockbitReportStatusDb) -> StockbitReportStatus {
        StockbitReportStatus {
            is_pinned: status.is_pinned,
            is_trending: status.is_trending,
            is_reposted: status.is_reposted,
            is_liked: status.is_liked,
            is_saved: status.is_saved,
            is_followed: status.is_followed,
            is_unavailable: status.is_unavailable,
            is_junk: status.is_junk,
            is_spam: status.is_spam,
            is_violation: status.is_violation,
            is_deleted: status.is_deleted,
        }
    }

    fn stockbit_report_item_to_proto(item: StockbitReportItemDb) -> StockbitReportItem {
        StockbitReportItem {
            r#type: item.report_type,
        }
    }

    fn stockbit_report_news_feed_to_proto(feed: StockbitReportNewsFeedDb) -> StockbitReportNewsFeed {
        StockbitReportNewsFeed {
            source: feed.source,
            label: feed.label,
            img: feed.img,
        }
    }

    fn stockbit_report_following_activity_to_proto(
        activity: StockbitReportFollowingActivityDb,
    ) -> StockbitReportFollowingActivity {
        StockbitReportFollowingActivity {
            users: activity.users.unwrap_or_default(),
            info: activity.info,
        }
    }

    fn stockbit_report_summary_to_proto(summary: StockbitReportSummaryDb) -> StockbitReportSummary {
        StockbitReportSummary {
            title: summary.title,
            summary: summary.summary,
            key_points: summary.key_points.unwrap_or_default(),
            key_takeaway: summary.key_takeaway,
            model: summary.model,
            model_version: summary.model_version,
        }
    }

    fn stockbit_report_reaction_entry_to_proto(
        entry: StockbitReportReactionEntryDb,
    ) -> StockbitReportReactionEntry {
        StockbitReportReactionEntry {
            reaction: entry.reaction,
            total: entry.total,
        }
    }

    fn stockbit_report_reaction_to_proto(reaction: StockbitReportReactionDb) -> StockbitReportReaction {
        StockbitReportReaction {
            reactions: reaction
                .reactions
                .unwrap_or_default()
                .into_iter()
                .map(Self::stockbit_report_reaction_entry_to_proto)
                .collect(),
            total: reaction.total,
            my_reaction: reaction.my_reaction.unwrap_or_default(),
        }
    }

    fn stockbit_report_stream_to_proto(stream: StockbitReportStreamDb) -> StockbitReportStream {
        StockbitReportStream {
            stream_id: stream.stream_id,
            title_url: stream.title_url,
            title: stream.title,
            content: stream.content,
            content_original: stream.content_original,
            created_at: stream.created_at,
            created_display: stream.created_display,
            updated_at: stream.updated_at,
            user: Some(Self::stockbit_report_user_to_proto(stream.user)),
            status: Some(Self::stockbit_report_status_to_proto(stream.status)),
            total_replies: stream.total_replies,
            total_likes: stream.total_likes,
            likers: stream.likers,
            stream_type: stream.stream_type,
            images: stream.images.unwrap_or_default(),
            parent_stream_id: stream.parent_stream_id,
            reports: stream
                .reports
                .unwrap_or_default()
                .into_iter()
                .map(Self::stockbit_report_item_to_proto)
                .collect(),
            news_feed: Some(Self::stockbit_report_news_feed_to_proto(stream.news_feed)),
            last_reply_date: stream.last_reply_date,
            topics: stream.topics.unwrap_or_default(),
            image_frame_type: stream.image_frame_type,
            commenter_type: stream.commenter_type,
            following_activity: Some(Self::stockbit_report_following_activity_to_proto(
                stream.following_activity,
            )),
            reply_to: stream.reply_to,
            summary: stream
                .summary
                .map(Self::stockbit_report_summary_to_proto),
            reaction: Some(
                stream
                    .reaction
                    .map(Self::stockbit_report_reaction_to_proto)
                    .unwrap_or_default(),
            ),
        }
    }

    fn stockbit_profile_address_to_proto(addr: StockbitProfileAddressDb) -> StockbitProfileAddress {
        StockbitProfileAddress {
            id: addr.id,
            email: addr.email.unwrap_or_default(),
            fax: addr.fax,
            npwp: addr.npwp,
            phone: addr.phone,
            website: addr.website,
            key: addr.key,
            lastupdate: addr.lastupdate,
            value: addr.value,
            office: addr.office,
        }
    }

    fn stockbit_profile_history_to_proto(history: StockbitProfileHistoryDb) -> StockbitProfileHistory {
        StockbitProfileHistory {
            amount: history.amount,
            board: history.board,
            date: history.date,
            price: history.price,
            registrar: history.registrar,
            shares: history.shares,
            underwriters: history.underwriters.unwrap_or_default(),
            administrative_bureau: history.administrative_bureau,
            free_float: history.free_float,
        }
    }

    fn stockbit_profile_executive_entry_to_proto(
        entry: StockbitProfileExecutiveEntryDb,
    ) -> StockbitProfileExecutiveEntry {
        StockbitProfileExecutiveEntry {
            id: entry.id,
            key: entry.key_label,
            lastupdate: entry.lastupdate,
            value: entry.value,
        }
    }

    fn stockbit_profile_key_executive_to_proto(
        exec: StockbitProfileKeyExecutiveDb,
    ) -> StockbitProfileKeyExecutive {
        StockbitProfileKeyExecutive {
            commissioner: exec
                .commissioner
                .unwrap_or_default()
                .into_iter()
                .map(Self::stockbit_profile_executive_entry_to_proto)
                .collect(),
            director: exec
                .director
                .unwrap_or_default()
                .into_iter()
                .map(Self::stockbit_profile_executive_entry_to_proto)
                .collect(),
            independent_commissioner: exec
                .independent_commissioner
                .unwrap_or_default()
                .into_iter()
                .map(Self::stockbit_profile_executive_entry_to_proto)
                .collect(),
            president_commissioner: exec
                .president_commissioner
                .unwrap_or_default()
                .into_iter()
                .map(Self::stockbit_profile_executive_entry_to_proto)
                .collect(),
            president_director: exec
                .president_director
                .unwrap_or_default()
                .into_iter()
                .map(Self::stockbit_profile_executive_entry_to_proto)
                .collect(),
            vice_president: exec
                .vice_president
                .unwrap_or_default()
                .into_iter()
                .map(Self::stockbit_profile_executive_entry_to_proto)
                .collect(),
            vice_president_commissioner: exec
                .vice_president_commissioner
                .unwrap_or_default()
                .into_iter()
                .map(Self::stockbit_profile_executive_entry_to_proto)
                .collect(),
            independent_vice_president_commissioner: exec
                .independent_vice_president_commissioner
                .unwrap_or_default()
                .into_iter()
                .map(Self::stockbit_profile_executive_entry_to_proto)
                .collect(),
            independent_president_commissioner: exec
                .independent_president_commissioner
                .unwrap_or_default()
                .into_iter()
                .map(Self::stockbit_profile_executive_entry_to_proto)
                .collect(),
        }
    }

    fn stockbit_profile_shareholder_entry_to_proto(
        entry: StockbitProfileShareholderEntryDb,
    ) -> StockbitProfileShareholderEntry {
        StockbitProfileShareholderEntry {
            percentage: entry.percentage,
            name: entry.name,
            value: entry.value,
            badges: entry.badges.unwrap_or_default(),
            id: entry.id,
            r#type: entry.shareholder_type,
            location: entry.location,
            nationality: entry.nationality,
            domicile: entry.domicile,
            scripless: entry.scripless,
            scrip: entry.scrip,
            value_formatted: entry.value_formatted,
            classification: entry.classification,
        }
    }

    fn stockbit_profile_value_info_to_proto(info: StockbitProfileValueInfoDb) -> StockbitProfileValueInfo {
        StockbitProfileValueInfo {
            value: info.value,
            info: info.info,
        }
    }

    fn stockbit_profile_prospectus_to_proto(
        doc: StockbitProfileProspectusDb,
    ) -> StockbitProfileProspectus {
        StockbitProfileProspectus {
            name: doc.name,
            file: doc.file,
            dir: doc.dir,
            url: doc.url,
        }
    }

    fn stockbit_profile_fund_profile_to_proto(
        profile: StockbitProfileFundProfileDb,
    ) -> StockbitProfileFundProfile {
        StockbitProfileFundProfile {
            fund_type: Some(Self::stockbit_profile_value_info_to_proto(profile.fund_type)),
            inception_date: profile.inception_date,
            fund_manager: profile.fund_manager,
            fund_manager_ico: profile.fund_manager_ico,
            custodian_bank: profile.custodian_bank,
            custodian_ico: profile.custodian_ico,
            risk_level: Some(Self::stockbit_profile_value_info_to_proto(profile.risk_level)),
            aum: Some(Self::stockbit_profile_value_info_to_proto(profile.aum)),
            maxdrawdown: Some(Self::stockbit_profile_value_info_to_proto(profile.maxdrawdown)),
            cagr5year: Some(Self::stockbit_profile_value_info_to_proto(profile.cagr5year)),
            expense_ratio: Some(Self::stockbit_profile_value_info_to_proto(profile.expense_ratio)),
            average_yield: Some(Self::stockbit_profile_value_info_to_proto(profile.average_yield)),
            prospectus: Some(Self::stockbit_profile_prospectus_to_proto(profile.prospectus)),
            fund_fact_sheet: profile
                .fund_fact_sheet
                .unwrap_or_default()
                .into_iter()
                .map(Self::stockbit_profile_prospectus_to_proto)
                .collect(),
            redemption_bank_name: profile.redemption_bank_name,
            min_buy: profile.min_buy,
            buy_fee: profile.buy_fee,
            sell_fee: profile.sell_fee,
        }
    }

    fn stockbit_profile_shareholder_number_to_proto(
        row: StockbitProfileShareholderNumberDb,
    ) -> StockbitProfileShareholderNumber {
        StockbitProfileShareholderNumber {
            shareholder_date: row.shareholder_date,
            total_share: row.total_share,
            change: row.change,
            change_formatted: row.change_formatted,
            change_value: row.change_value,
        }
    }

    fn stockbit_profile_percentage_to_proto(pct: StockbitProfilePercentageDb) -> StockbitProfilePercentage {
        StockbitProfilePercentage {
            raw: pct.raw,
            formatted: pct.formatted,
        }
    }

    fn stockbit_profile_listing_information_to_proto(
        info: StockbitProfileListingInformationDb,
    ) -> StockbitProfileListingInformation {
        StockbitProfileListingInformation {
            exercise_start_date: info.exercise_start_date,
            exercise_end_date: info.exercise_end_date,
            exercise_price: info.exercise_price,
            expire_date: info.expire_date,
            listing_date: info.listing_date,
            foreign_percentage: Some(Self::stockbit_profile_percentage_to_proto(
                info.foreign_percentage,
            )),
            local_percentage: Some(Self::stockbit_profile_percentage_to_proto(info.local_percentage)),
            number_of_securities: info.number_of_securities,
            total_shares: info.total_shares,
        }
    }

    fn stockbit_profile_beneficiary_to_proto(
        beneficiary: StockbitProfileBeneficiaryDb,
    ) -> StockbitProfileBeneficiary {
        StockbitProfileBeneficiary {
            name: beneficiary.name,
        }
    }

    fn stockbit_profile_shareholder_one_percent_to_proto(
        data: StockbitProfileShareholderOnePercentDb,
    ) -> StockbitProfileShareholderOnePercent {
        StockbitProfileShareholderOnePercent {
            shareholder: data
                .shareholder
                .unwrap_or_default()
                .into_iter()
                .map(Self::stockbit_profile_shareholder_entry_to_proto)
                .collect(),
            last_updated: data.last_updated,
        }
    }

    fn stockbit_profile_subsidiary_to_proto(
        sub: StockbitProfileSubsidiaryDb,
    ) -> StockbitProfileSubsidiary {
        StockbitProfileSubsidiary {
            name: sub.name,
            percentage: sub.percentage,
        }
    }

    fn stockbit_profile_fee_entry_to_proto(entry: StockbitProfileFeeEntryDb) -> StockbitProfileFeeEntry {
        StockbitProfileFeeEntry {
            name: entry.name,
            value: entry.value,
        }
    }

    fn stockbit_profile_asset_allocation_entry_to_proto(
        entry: StockbitProfileAssetAllocationEntryDb,
    ) -> StockbitProfileAssetAllocationEntry {
        StockbitProfileAssetAllocationEntry {
            name: entry.name,
            percentage: entry.percentage,
            value: entry.value,
        }
    }

    fn stockbit_profile_top_holding_entry_to_proto(
        entry: StockbitProfileTopHoldingEntryDb,
    ) -> StockbitProfileTopHoldingEntry {
        StockbitProfileTopHoldingEntry {
            name: entry.name,
            percentage: entry.percentage,
            value: entry.value,
        }
    }

    fn stockbit_profile_meta_to_proto(profile: &StockbitProfileDb) -> StockbitProfileData {
        StockbitProfileData {
            address: vec![],
            background: profile.background.clone(),
            history: Some(Self::stockbit_profile_history_to_proto(profile.history.clone())),
            key_executive: Some(Self::stockbit_profile_key_executive_to_proto(
                profile.key_executive.clone(),
            )),
            secretary: profile
                .secretary
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(Self::stockbit_profile_executive_entry_to_proto)
                .collect(),
            shareholder: vec![],
            subsidiary: profile
                .subsidiary
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(Self::stockbit_profile_subsidiary_to_proto)
                .collect(),
            profile: Some(Self::stockbit_profile_fund_profile_to_proto(
                profile.fund_profile.clone(),
            )),
            fee: profile
                .fee
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(Self::stockbit_profile_fee_entry_to_proto)
                .collect(),
            asset_allocation: profile
                .asset_allocation
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(Self::stockbit_profile_asset_allocation_entry_to_proto)
                .collect(),
            shareholder_reksa: profile
                .shareholder_reksa
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(Self::stockbit_profile_shareholder_entry_to_proto)
                .collect(),
            pdf: profile
                .pdf
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(Self::stockbit_profile_prospectus_to_proto)
                .collect(),
            shareholder_numbers: vec![],
            badges: profile.badges.clone().unwrap_or_default(),
            top_holdings: profile
                .top_holdings
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(Self::stockbit_profile_top_holding_entry_to_proto)
                .collect(),
            shareholder_director_commissioner: vec![],
            listing_information: Some(Self::stockbit_profile_listing_information_to_proto(
                profile.listing_information.clone(),
            )),
            beneficiary: vec![],
            shareholder_one_percent: Some(Self::stockbit_profile_shareholder_one_percent_to_proto(
                profile.shareholder_one_percent.clone(),
            )),
            classification: profile.classification.clone().unwrap_or_default(),
        }
    }

    fn stockbit_profile_stream_chunk(
        code: &str,
        message: &str,
        updated_at: DateTime<Utc>,
        part: StockbitProfileStreamPart,
        data: StockbitProfileData,
    ) -> StockbitProfileResponse {
        StockbitProfileResponse {
            success: true,
            message: message.to_string(),
            code: code.to_string(),
            data: Some(data),
            updated_at: Some(updated_at.timestamp()),
            part: part.into(),
        }
    }

    fn stockbit_profile_row_stream(
        row: StockbitProfileByCodeRow,
        message: &str,
    ) -> Result<Vec<StockbitProfileResponse>, Status> {
        let profile = row.stockbit_profile.ok_or_else(|| {
            Status::internal(format!("stockbit_profile code={} kosong", row.code))
        })?;
        let updated_at = row.stockbit_profile_updated_at.unwrap_or_else(Utc::now);
        let code = row.code;
        let msg = message.to_string();
        let mut chunks = Vec::new();

        chunks.push(Self::stockbit_profile_stream_chunk(
            &code,
            &msg,
            updated_at,
            StockbitProfileStreamPart::Meta,
            Self::stockbit_profile_meta_to_proto(&profile),
        ));

        for address in profile.address.unwrap_or_default() {
            chunks.push(Self::stockbit_profile_stream_chunk(
                &code,
                &msg,
                updated_at,
                StockbitProfileStreamPart::Address,
                StockbitProfileData {
                    address: vec![Self::stockbit_profile_address_to_proto(address)],
                    ..Default::default()
                },
            ));
        }

        for shareholder in profile.shareholder.unwrap_or_default() {
            chunks.push(Self::stockbit_profile_stream_chunk(
                &code,
                &msg,
                updated_at,
                StockbitProfileStreamPart::Shareholder,
                StockbitProfileData {
                    shareholder: vec![Self::stockbit_profile_shareholder_entry_to_proto(shareholder)],
                    ..Default::default()
                },
            ));
        }

        for entry in profile.shareholder_director_commissioner.unwrap_or_default() {
            chunks.push(Self::stockbit_profile_stream_chunk(
                &code,
                &msg,
                updated_at,
                StockbitProfileStreamPart::ShareholderDirectorCommissioner,
                StockbitProfileData {
                    shareholder_director_commissioner: vec![
                        Self::stockbit_profile_shareholder_entry_to_proto(entry),
                    ],
                    ..Default::default()
                },
            ));
        }

        for row_item in profile.shareholder_numbers.unwrap_or_default() {
            chunks.push(Self::stockbit_profile_stream_chunk(
                &code,
                &msg,
                updated_at,
                StockbitProfileStreamPart::ShareholderNumber,
                StockbitProfileData {
                    shareholder_numbers: vec![Self::stockbit_profile_shareholder_number_to_proto(
                        row_item,
                    )],
                    ..Default::default()
                },
            ));
        }

        for beneficiary in profile.beneficiary.unwrap_or_default() {
            chunks.push(Self::stockbit_profile_stream_chunk(
                &code,
                &msg,
                updated_at,
                StockbitProfileStreamPart::Beneficiary,
                StockbitProfileData {
                    beneficiary: vec![Self::stockbit_profile_beneficiary_to_proto(beneficiary)],
                    ..Default::default()
                },
            ));
        }

        chunks.push(Self::stockbit_profile_stream_chunk(
            &code,
            &msg,
            updated_at,
            StockbitProfileStreamPart::Done,
            StockbitProfileData::default(),
        ));

        Ok(chunks)
    }

    fn stockbit_reports_stream_chunk(
        code: &str,
        message: &str,
        updated_at: Option<i64>,
        part: StockbitReportsStreamPart,
        stream: Vec<StockbitReportStream>,
    ) -> StockbitReportsRow {
        StockbitReportsRow {
            success: true,
            message: message.to_string(),
            code: code.to_string(),
            stream,
            updated_at,
            part: part.into(),
        }
    }

    fn stockbit_reports_row_stream(
        row: StockbitReportsByCodeRow,
        message: &str,
    ) -> Vec<StockbitReportsRow> {
        let updated_at = row.stockbit_reports_updated_at.map(|dt| dt.timestamp());
        let code = row.code;
        let msg = message.to_string();
        let mut chunks = vec![Self::stockbit_reports_stream_chunk(
            &code,
            &msg,
            updated_at,
            StockbitReportsStreamPart::Meta,
            vec![],
        )];

        for item in row.stockbit_reports.unwrap_or_default() {
            chunks.push(Self::stockbit_reports_stream_chunk(
                &code,
                &msg,
                updated_at,
                StockbitReportsStreamPart::Stream,
                vec![Self::stockbit_report_stream_to_proto(item)],
            ));
        }

        chunks.push(Self::stockbit_reports_stream_chunk(
            &code,
            &msg,
            updated_at,
            StockbitReportsStreamPart::Done,
            vec![],
        ));

        chunks
    }
}

#[tonic::async_trait]
impl StockList for StockListService {
    type GetStockbitProfileByCodeStream = GetStockbitProfileByCodeStream;
    type GetKeyStatsFromStockbitStream = GetKeyStatsFromStockbitStream;
    type GetStockbitReportsByCodeStream = GetStockbitReportsByCodeStream;
    type GetStockByRepeatedCodeStream = GetStockByRepeatedCodeStream;
    type GetAllKeyStatsStream = GetAllKeyStatsStream;

    async fn get_all_stocks(
        &self,
        request: Request<GetAllStocksRequest>,
    ) -> Result<Response<GetAllStocksResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        if let Some(cached) = self.all_stocks_cache.get().await {
            eprintln!(
                "GetAllStocks {user_name} {}ms - HIT moka",
                started.elapsed().as_millis()
            );
            return Ok(Response::new(cached));
        }

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

            let response = GetAllStocksResponse {
                success: true,
                message,
                items,
            };
            self.all_stocks_cache.set(response.clone()).await;

            Ok(Response::new(response))
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
    ) -> Result<Response<Self::GetStockByRepeatedCodeStream>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let codes = request.into_inner().code;
        if codes.is_empty() {
            return Err(Status::invalid_argument("code wajib diisi (minimal 1)"));
        }

        let mut normalized = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for raw in codes {
            let code = raw.trim().to_ascii_uppercase();
            if code.is_empty() || !seen.insert(code.clone()) {
                continue;
            }
            normalized.push(code);
        }
        if normalized.is_empty() {
            return Err(Status::invalid_argument("code wajib diisi (minimal 1 valid)"));
        }

        let session = Arc::clone(&self.session);
        let user_name_spawn = user_name.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(32);

        tokio::spawn(async move {
            if !send_or_break(
                &tx,
                Ok(GetStockByRepeatedCodeResponse {
                    part: GetStockByRepeatedCodeStreamPart::Meta.into(),
                    ..Default::default()
                }),
            )
            .await
            {
                Self::log_rpc_debug("GetStockByRepeatedCode", &user_name_spawn, started);
                return;
            }

            for code in normalized {
                match crate::repository::get_by_code(session.as_ref(), &code).await {
                    Ok(Some(row)) => {
                        if !send_or_break(
                            &tx,
                            Ok(GetStockByRepeatedCodeResponse {
                                item: Some(Self::stock_by_code_from_db_row(&row)),
                                part: GetStockByRepeatedCodeStreamPart::Item.into(),
                            }),
                        )
                        .await
                        {
                            break;
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        let _ = send_or_break(&tx, Err(Status::internal(e))).await;
                        break;
                    }
                }
            }

            let _ = send_or_break(
                &tx,
                Ok(GetStockByRepeatedCodeResponse {
                    part: GetStockByRepeatedCodeStreamPart::Done.into(),
                    ..Default::default()
                }),
            )
            .await;

            Self::log_rpc_debug("GetStockByRepeatedCode", &user_name_spawn, started);
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as GetStockByRepeatedCodeStream,
        ))
    }

    async fn get_key_stats_from_stockbit(
        &self,
        request: Request<GetKeyStatsFromStockbitRequest>,
    ) -> Result<Response<Self::GetKeyStatsFromStockbitStream>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let code = request.into_inner().code.trim().to_ascii_uppercase();
        if code.is_empty() {
            return Err(Status::invalid_argument("code wajib diisi"));
        }
        if code.len() != 4 || !code.chars().all(|c| c.is_ascii_alphabetic()) {
            return Err(Status::invalid_argument(format!(
                "code tidak valid ({code}); wajib tepat 4 huruf alphabet"
            )));
        }

        crate::repository::get_keystats_from_stockbit_by_code(self.session.as_ref(), &code)
            .await
            .map_err(Status::internal)?
            .ok_or_else(|| Status::not_found(format!("stock_list code={code} tidak ditemukan")))?;

        let session = Arc::clone(&self.session);
        let user_name_spawn = user_name.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(32);

        tokio::spawn(async move {
            let stream_result: Result<Vec<KeyStatsFromStockbitResponse>, Status> =
                match tokio::select! {
                    biased;
                    () = tx.closed() => None,
                    result = async {
                        let mut row = crate::repository::get_keystats_from_stockbit_by_code(
                            session.as_ref(),
                            &code,
                        )
                        .await
                        .map_err(Status::internal)?
                        .ok_or_else(|| {
                            Status::not_found(format!("stock_list code={code} tidak ditemukan"))
                        })?;

                        let message = if crate::stockbit::needs_stockbit_keystats_refresh(&row) {
                            acquire_keystats_from_stockbit_slot(&user_name_spawn).await?;
                            eprintln!(
                                "GetKeyStatsFromStockbit {user_name_spawn} GET Stockbit API keystats/ratio/v1/{code}"
                            );
                            crate::stockbit::fetch_and_save_keystats_from_stockbit(
                                Arc::clone(&session),
                                &code,
                            )
                            .await
                            .map_err(Status::internal)?;
                            row = crate::repository::get_keystats_from_stockbit_by_code(
                                session.as_ref(),
                                &code,
                            )
                            .await
                            .map_err(Status::internal)?
                            .ok_or_else(|| {
                                Status::not_found(format!("stock_list code={code} tidak ditemukan"))
                            })?;
                            "Key stats Stockbit di-upsert ke stock_list"
                        } else {
                            "Key stats Stockbit dari Scylla"
                        };

                        Ok(Self::keystats_from_stockbit_row_stream(row, message))
                    } => Some(result),
                } {
                    None => {
                        Self::log_rpc_debug("GetKeyStatsFromStockbit", &user_name_spawn, started);
                        return;
                    }
                    Some(result) => result,
                };

            match stream_result {
                Ok(chunks) => {
                    for chunk in chunks {
                        if !send_or_break(&tx, Ok(chunk)).await {
                            break;
                        }
                    }
                }
                Err(status) => {
                    let _ = send_or_break(&tx, Err(status)).await;
                }
            }

            Self::log_rpc_debug("GetKeyStatsFromStockbit", &user_name_spawn, started);
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as GetKeyStatsFromStockbitStream,
        ))
    }

    async fn get_stockbit_profile_by_code(
        &self,
        request: Request<GetStockbitProfileByCodeRequest>,
    ) -> Result<Response<Self::GetStockbitProfileByCodeStream>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let code = request.into_inner().code.trim().to_ascii_uppercase();
        if code.is_empty() {
            return Err(Status::invalid_argument("code wajib diisi"));
        }
        if code.len() != 4 || !code.chars().all(|c| c.is_ascii_alphabetic()) {
            return Err(Status::invalid_argument(format!(
                "code tidak valid ({code}); wajib tepat 4 huruf alphabet"
            )));
        }

        crate::repository::get_stockbit_profile_by_code(self.session.as_ref(), &code)
            .await
            .map_err(Status::internal)?
            .ok_or_else(|| Status::not_found(format!("stock_list code={code} tidak ditemukan")))?;

        let session = Arc::clone(&self.session);
        let user_name_spawn = user_name.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(32);

        tokio::spawn(async move {
            let stream_result: Result<Vec<StockbitProfileResponse>, Status> =
                match tokio::select! {
                    biased;
                    () = tx.closed() => None,
                    result = async {
                        let mut row =
                            crate::repository::get_stockbit_profile_by_code(session.as_ref(), &code)
                                .await
                                .map_err(Status::internal)?
                                .ok_or_else(|| {
                                    Status::not_found(format!(
                                        "stock_list code={code} tidak ditemukan"
                                    ))
                                })?;

                        let message = if crate::stockbit_profile::needs_stockbit_profile_refresh(
                            row.stockbit_profile.as_ref(),
                            row.stockbit_profile_updated_at,
                        ) {
                            acquire_keystats_from_stockbit_slot(&user_name_spawn).await?;
                            eprintln!(
                                "\x1b[32mGetStockbitProfileByCode {user_name_spawn} GET Stockbit API emitten/{code}/profile\x1b[0m"
                            );
                            crate::stockbit_profile::fetch_and_save_stockbit_profile(
                                Arc::clone(&session),
                                &code,
                            )
                            .await
                            .map_err(Status::internal)?;
                            row = crate::repository::get_stockbit_profile_by_code(
                                session.as_ref(),
                                &code,
                            )
                            .await
                            .map_err(Status::internal)?
                            .ok_or_else(|| {
                                Status::not_found(format!(
                                    "stock_list code={code} tidak ditemukan"
                                ))
                            })?;
                            "Stockbit profile di-upsert ke stock_list"
                        } else {
                            eprintln!(
                                "GetStockbitProfileByCode {user_name_spawn} cache Scylla emitten/{code}/profile (<30 hari, data ada)"
                            );
                            "Stockbit profile dari Scylla"
                        };

                        Self::stockbit_profile_row_stream(row, message)
                    } => Some(result),
                } {
                    None => {
                        Self::log_rpc_debug("GetStockbitProfileByCode", &user_name_spawn, started);
                        return;
                    }
                    Some(result) => result,
                };

            match stream_result {
                Ok(chunks) => {
                    for chunk in chunks {
                        if !send_or_break(&tx, Ok(chunk)).await {
                            break;
                        }
                    }
                }
                Err(status) => {
                    let _ = send_or_break(&tx, Err(status)).await;
                }
            }

            Self::log_rpc_debug("GetStockbitProfileByCode", &user_name_spawn, started);
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as GetStockbitProfileByCodeStream,
        ))
    }

    async fn get_stockbit_reports_by_code(
        &self,
        request: Request<GetStockbitReportsByCodeRequest>,
    ) -> Result<Response<Self::GetStockbitReportsByCodeStream>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let code = request.into_inner().code.trim().to_ascii_uppercase();
        if code.is_empty() {
            return Err(Status::invalid_argument("code wajib diisi"));
        }
        if code.len() != 4 || !code.chars().all(|c| c.is_ascii_alphabetic()) {
            return Err(Status::invalid_argument(format!(
                "code tidak valid ({code}); wajib tepat 4 huruf alphabet"
            )));
        }

        crate::repository::get_stockbit_reports_by_code(self.session.as_ref(), &code)
            .await
            .map_err(Status::internal)?
            .ok_or_else(|| Status::not_found(format!("stock_list code={code} tidak ditemukan")))?;

        let session = Arc::clone(&self.session);
        let user_name_spawn = user_name.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(32);

        tokio::spawn(async move {
            let stream_result: Result<Vec<StockbitReportsRow>, Status> =
                match tokio::select! {
                    biased;
                    () = tx.closed() => None,
                    result = async {
                        let mut row =
                            crate::repository::get_stockbit_reports_by_code(session.as_ref(), &code)
                                .await
                                .map_err(Status::internal)?
                                .ok_or_else(|| {
                                    Status::not_found(format!(
                                        "stock_list code={code} tidak ditemukan"
                                    ))
                                })?;

                        let message = if crate::stockbit_reports::needs_stockbit_reports_refresh(
                            row.stockbit_reports_updated_at,
                        ) {
                            eprintln!(
                                "GetStockbitReportsByCode {user_name_spawn} GET Stockbit API stream/v3/symbol/{code}"
                            );
                            crate::stockbit_reports::fetch_and_save_stockbit_reports(
                                Arc::clone(&session),
                                &code,
                            )
                            .await
                            .map_err(Status::internal)?;
                            row = crate::repository::get_stockbit_reports_by_code(
                                session.as_ref(),
                                &code,
                            )
                            .await
                            .map_err(Status::internal)?
                            .ok_or_else(|| {
                                Status::not_found(format!(
                                    "stock_list code={code} tidak ditemukan"
                                ))
                            })?;
                            "Stockbit reports di-upsert ke stock_list"
                        } else {
                            "Stockbit reports dari Scylla"
                        };

                        Ok(Self::stockbit_reports_row_stream(row, message))
                    } => Some(result),
                } {
                    None => return,
                    Some(result) => result,
                };

            match stream_result {
                Ok(chunks) => {
                    for chunk in chunks {
                        if !send_or_break(&tx, Ok(chunk)).await {
                            break;
                        }
                    }
                }
                Err(status) => {
                    let _ = send_or_break(&tx, Err(status)).await;
                }
            }

            eprintln!(
                "GetStockbitReportsByCode {user_name_spawn} {code} {}ms",
                started.elapsed().as_millis()
            );
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as GetStockbitReportsByCodeStream,
        ))
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
    ) -> Result<Response<Self::GetAllKeyStatsStream>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;
        let _ = request.into_inner();

        let session = Arc::clone(&self.session);
        let user_name_spawn = user_name.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(32);

        tokio::spawn(async move {
            if !send_or_break(
                &tx,
                Ok(GetAllKeyStatsResponse {
                    part: GetAllKeyStatsStreamPart::Meta.into(),
                    ..Default::default()
                }),
            )
            .await
            {
                Self::log_rpc_debug("GetAllKeyStats", &user_name_spawn, started);
                return;
            }

            match crate::repository::list_all_keystats(session.as_ref()).await {
                Ok(rows) => {
                    for row in rows {
                        if !send_or_break(
                            &tx,
                            Ok(GetAllKeyStatsResponse {
                                item: Some(Self::keystats_response_from_row(row)),
                                part: GetAllKeyStatsStreamPart::Item.into(),
                            }),
                        )
                        .await
                        {
                            break;
                        }
                    }
                }
                Err(e) => {
                    let _ = send_or_break(&tx, Err(Status::internal(e))).await;
                    Self::log_rpc_debug("GetAllKeyStats", &user_name_spawn, started);
                    return;
                }
            }

            let _ = send_or_break(
                &tx,
                Ok(GetAllKeyStatsResponse {
                    part: GetAllKeyStatsStreamPart::Done.into(),
                    ..Default::default()
                }),
            )
            .await;

            Self::log_rpc_debug("GetAllKeyStats", &user_name_spawn, started);
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as GetAllKeyStatsStream,
        ))
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
        let code = request.into_inner().code.trim().to_ascii_uppercase();

        let result: Result<Response<CorporateActionByCodeResponse>, Status> = async {
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

        eprintln!(
            "GetCorporateActionByCode {user_name} {code} {}ms",
            started.elapsed().as_millis()
        );
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

    async fn update_sub_sector(
        &self,
        request: Request<UpdateSubSectorRequest>,
    ) -> Result<Response<UpdateSubSectorResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;
        let inner = request.into_inner();
        let code = inner.code.trim().to_ascii_uppercase();
        let sub_sector = inner.sub_sector.trim().to_string();

        let result: Result<Response<UpdateSubSectorResponse>, Status> = async {
            if code.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi"));
            }

            crate::repository::update_sub_sector(self.session.as_ref(), &code, &sub_sector)
                .await
                .map_err(|e| {
                    if e.contains("tidak ditemukan") {
                        Status::not_found(e)
                    } else {
                        Status::internal(e)
                    }
                })?;

            Ok(Response::new(UpdateSubSectorResponse {
                success: true,
                message: format!("sub_sector code={code} berhasil diupdate"),
            }))
        }
        .await;

        eprintln!(
            "UpdateSubSector {user_name} {code} {}ms",
            started.elapsed().as_millis()
        );
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

    async fn update_is_bad_fundamental_by_code(
        &self,
        request: Request<UpdateIsBadFundamentalByCodeRequest>,
    ) -> Result<Response<UpdateIsBadFundamentalByCodeResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;
        let inner = request.into_inner();
        let code = inner.code.trim().to_ascii_uppercase();

        let result: Result<Response<UpdateIsBadFundamentalByCodeResponse>, Status> = async {
            if code.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi"));
            }

            crate::repository::update_is_bad_fundamental(
                self.session.as_ref(),
                &code,
                inner.is_bad_fundamental,
            )
            .await
            .map_err(|e| {
                if e.contains("tidak ditemukan") {
                    Status::not_found(e)
                } else {
                    Status::internal(e)
                }
            })?;

            Ok(Response::new(UpdateIsBadFundamentalByCodeResponse {
                success: true,
                message: format!(
                    "is_bad_fundamental code={code}={}",
                    inner.is_bad_fundamental
                ),
            }))
        }
        .await;

        eprintln!(
            "UpdateIsBadFundamentalByCode {user_name} {code} {}ms",
            started.elapsed().as_millis()
        );
        result
    }

    async fn update_notation_invezgo(
        &self,
        request: Request<UpdateNotationInvezgoRequest>,
    ) -> Result<Response<UpdateNotationInvezgoResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;

        let result: Result<Response<UpdateNotationInvezgoResponse>, Status> = async {
            let _inner = request.into_inner();

            let (updated, skipped) = crate::invezgo::fetch_and_save_notation(self.session.clone())
                .await
                .map_err(Status::internal)?;

            Ok(Response::new(UpdateNotationInvezgoResponse {
                success: true,
                message: format!(
                    "notation Invezgo: {updated} code diupdate, {skipped} dilewati (tidak ada di stock_list)"
                ),
                updated_count: updated as i32,
                skipped_count: skipped as i32,
            }))
        }
        .await;

        Self::log_rpc_debug("UpdateNotationInvezgo", &user_name, started);
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

    async fn get_take_profit_wyckoff_by_code(
        &self,
        request: Request<GetTakeProfitWyckoffByCodeRequest>,
    ) -> Result<Response<GetTakeProfitWyckoffByCodeResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;
        let code = request.into_inner().code.trim().to_ascii_uppercase();

        let result: Result<Response<GetTakeProfitWyckoffByCodeResponse>, Status> = async {
            if code.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi"));
            }

            let row =
                crate::repository::get_takeprofit_wyckoff_by_code(self.session.as_ref(), &code)
                    .await
                    .map_err(Status::internal)?
                    .ok_or_else(|| {
                        Status::not_found(format!("stock_list code={code} tidak ditemukan"))
                    })?;

            let takeprofit_wyckoff = row.takeprofit_wyckoff.unwrap_or_default();
            let entries = takeprofit_wyckoff.len();

            Ok(Response::new(GetTakeProfitWyckoffByCodeResponse {
                success: true,
                message: format!("takeprofit_wyckoff code={} ({entries} entri)", row.code),
                takeprofit_wyckoff,
            }))
        }
        .await;

        eprintln!(
            "GetTakeProfitWyckoffByCode {user_name} {code} {}ms",
            started.elapsed().as_millis()
        );
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

    async fn upsert_take_profit_wyckoff_by_code(
        &self,
        request: Request<UpsertTakeProfitWyckoffByCodeRequest>,
    ) -> Result<Response<UpsertTakeProfitWyckoffByCodeResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;
        let inner = request.into_inner();
        let code = inner.code.trim().to_ascii_uppercase();
        let entries = inner.takeprofit_wyckoff.len();

        let result: Result<Response<UpsertTakeProfitWyckoffByCodeResponse>, Status> = async {
            if code.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi"));
            }

            crate::repository::upsert_takeprofit_wyckoff(
                self.session.as_ref(),
                &code,
                &inner.takeprofit_wyckoff,
            )
            .await
            .map_err(|e| {
                if e.contains("tidak ditemukan") {
                    Status::not_found(e)
                } else {
                    Status::internal(e)
                }
            })?;

            Ok(Response::new(UpsertTakeProfitWyckoffByCodeResponse {
                success: true,
                message: format!(
                    "takeprofit_wyckoff code={code} berhasil diupsert ({entries} entri)"
                ),
            }))
        }
        .await;

        eprintln!(
            "UpsertTakeProfitWyckoffByCode {user_name} {code} {}ms",
            started.elapsed().as_millis()
        );
        result
    }

    async fn delete_take_profit_wyckoff_by_code(
        &self,
        request: Request<DeleteTakeProfitWyckoffByCodeRequest>,
    ) -> Result<Response<DeleteTakeProfitWyckoffByCodeResponse>, Status> {
        let started = std::time::Instant::now();
        let auth = self.require_auth(&request).await?;
        let user_name = auth.nama;
        let code = request.into_inner().code.trim().to_ascii_uppercase();

        let result: Result<Response<DeleteTakeProfitWyckoffByCodeResponse>, Status> = async {
            if code.is_empty() {
                return Err(Status::invalid_argument("code wajib diisi"));
            }

            crate::repository::delete_takeprofit_wyckoff(self.session.as_ref(), &code)
                .await
                .map_err(|e| {
                    if e.contains("tidak ditemukan") {
                        Status::not_found(e)
                    } else {
                        Status::internal(e)
                    }
                })?;

            Ok(Response::new(DeleteTakeProfitWyckoffByCodeResponse {
                success: true,
                message: format!("takeprofit_wyckoff code={code} berhasil dihapus"),
            }))
        }
        .await;

        eprintln!(
            "DeleteTakeProfitWyckoffByCode {user_name} {code} {}ms",
            started.elapsed().as_millis()
        );
        result
    }
}
