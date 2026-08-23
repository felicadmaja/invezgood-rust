//! Model baris tabel `invezgood.stock_list`.

use std::collections::HashMap;

use scylla::DeserializeRow;
use scylla::DeserializeValue;
use scylla::SerializeRow;
use scylla::SerializeValue;

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "stock_list";
pub const MV_BY_IS_PLAN_TO_TRADE: &str = "stock_list_by_is_plan_to_trade";

/// UDT `keystats_value`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct KeystatsValueDb {
    pub col: String,
    pub year: i32,
    pub amount: f64,
    pub period: String,
}

/// UDT `keystats_column`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct KeystatsColumnDb {
    pub year: i32,
    pub label: String,
    pub period: String,
}

/// UDT `keystats_row`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct KeystatsRowDb {
    pub id: String,
    pub name: String,
    #[scylla(default_when_null)]
    pub values: Option<Vec<KeystatsValueDb>>,
}

/// UDT `balance_statement_row`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct BalanceStatementRowDb {
    pub id: String,
    pub name: String,
    pub level: i32,
    #[scylla(default_when_null)]
    pub values: Option<Vec<KeystatsValueDb>>,
    #[scylla(default_when_null)]
    pub parent_id: Option<String>,
    pub is_abstract: bool,
    pub display_order: i32,
}

/// UDT `stock_list_keystats` — field = rows, columns.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct StockListKeystatsDb {
    #[scylla(default_when_null)]
    pub rows: Option<Vec<KeystatsRowDb>>,
    #[scylla(default_when_null)]
    pub columns: Option<Vec<KeystatsColumnDb>>,
}

/// UDT `stock_list_balance_statement` — field = rows, columns.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct StockListBalanceStatementDb {
    #[scylla(default_when_null)]
    pub rows: Option<Vec<BalanceStatementRowDb>>,
    #[scylla(default_when_null)]
    pub columns: Option<Vec<KeystatsColumnDb>>,
}

/// UDT `stock_list_income_statement` — struktur sama dengan balance_statement.
pub type StockListIncomeStatementDb = StockListBalanceStatementDb;

/// UDT `stock_list_cash_flow` — struktur sama dengan balance_statement.
pub type StockListCashFlowDb = StockListBalanceStatementDb;

/// UDT `share_holder_5_entry`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct ShareHolder5EntryDb {
    pub name: String,
    pub date: chrono::DateTime<chrono::Utc>,
    pub val: String,
    pub percent: f64,
}

/// Kolom `share_holder_5` — list entri pemegang saham >1%.
pub type ShareHolder5Db = Option<Vec<ShareHolder5EntryDb>>;

/// UDT `share_holder_1_entry`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct ShareHolder1EntryDb {
    pub name: String,
    pub holder_type: String,
    pub status: String,
    pub nationality: String,
    pub domicile: String,
    pub scripless: String,
    pub scrip: String,
    pub total: String,
    pub percentage: f64,
}

/// Kolom `share_holder_1` — list entri pemegang saham detail >1%.
pub type ShareHolder1Db = Option<Vec<ShareHolder1EntryDb>>;

/// UDT `share_holder_composition_entry`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct ShareHolderCompositionEntryDb {
    pub name: String,
    pub percentage: f64,
    pub badge: String,
}

/// Kolom `share_holder_composition` — komposisi kepemilikan (pengendali, direksi, dll.).
pub type ShareHolderCompositionDb = Option<Vec<ShareHolderCompositionEntryDb>>;

/// UDT `company_person_entry`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct CompanyPersonEntryDb {
    pub name: String,
    pub position: String,
}

/// UDT `company_subsidiary_entry`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct CompanySubsidiaryEntryDb {
    pub name: String,
    pub percentage: f64,
}

/// UDT `company_information` — profil perusahaan dari API /analysis/information/{code}.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct CompanyInformationDb {
    #[scylla(default_when_null)]
    pub address: Option<String>,
    #[scylla(default_when_null)]
    pub industry: Option<String>,
    #[scylla(default_when_null)]
    pub subsindustry: Option<String>,
    #[scylla(default_when_null)]
    pub activity: Option<String>,
    #[scylla(default_when_null)]
    pub name: Option<String>,
    #[scylla(default_when_null)]
    pub npwp: Option<String>,
    #[scylla(default_when_null)]
    pub board: Option<String>,
    #[scylla(default_when_null)]
    pub sector: Option<String>,
    #[scylla(default_when_null)]
    pub subsector: Option<String>,
    #[scylla(default_when_null)]
    pub listing_date: Option<chrono::DateTime<chrono::Utc>>,
    #[scylla(default_when_null)]
    pub website: Option<String>,
    #[scylla(default_when_null)]
    pub logo: Option<String>,
    #[scylla(default_when_null)]
    pub additional_info: Option<String>,
    #[scylla(default_when_null)]
    pub people: Option<String>,
    #[scylla(default_when_null)]
    pub report_type: Option<String>,
    #[scylla(default_when_null)]
    pub administration: Option<String>,
    #[scylla(default_when_null)]
    pub description: Option<String>,
    #[scylla(default_when_null)]
    pub ipo_pct: Option<f64>,
    #[scylla(default_when_null)]
    pub ipo_price: Option<f64>,
    #[scylla(default_when_null)]
    pub ipo_share: Option<String>,
    #[scylla(default_when_null)]
    pub ipo_underwriter: Option<String>,
    #[scylla(default_when_null)]
    pub nominal_price: Option<f64>,
    #[scylla(default_when_null)]
    pub category: Option<Vec<String>>,
    #[scylla(default_when_null)]
    pub active: Option<bool>,
    #[scylla(default_when_null)]
    pub commissioner: Option<Vec<CompanyPersonEntryDb>>,
    #[scylla(default_when_null)]
    pub director: Option<Vec<CompanyPersonEntryDb>>,
    #[scylla(default_when_null)]
    pub subsidiary: Option<Vec<CompanySubsidiaryEntryDb>>,
}

/// UDT `corporate_action_entry`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct CorporateActionEntryDb {
    pub code: String,
    #[scylla(rename = "type")]
    pub action_type: String,
    #[scylla(default_when_null)]
    pub payload: Option<HashMap<String, String>>,
}

/// UDT `corporate_action` — kalender corporate action dari API /analysis/calendar.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct CorporateActionDb {
    pub total_page: i32,
    pub page: i32,
    #[scylla(default_when_null)]
    pub next_page: Option<i32>,
    #[scylla(default_when_null)]
    pub data: Option<Vec<CorporateActionEntryDb>>,
}

/// UDT `wyckoff_chart` — data chart Wyckoff per saham.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct WyckoffChartDb {
    #[scylla(default_when_null)]
    pub accumulation_trading_range: Option<Vec<i32>>,
    #[scylla(default_when_null)]
    pub distribution_trading_range: Option<Vec<i32>>,
    #[scylla(default_when_null)]
    pub sc: Option<Vec<String>>,
    #[scylla(default_when_null)]
    pub ar: Option<Vec<String>>,
    #[scylla(default_when_null)]
    pub st: Option<Vec<String>>,
    #[scylla(default_when_null)]
    pub ps: Option<Vec<String>>,
    #[scylla(default_when_null)]
    pub spr: Option<Vec<String>>,
    #[scylla(default_when_null)]
    pub ut: Option<Vec<String>>,
    #[scylla(default_when_null)]
    pub sos: Option<Vec<String>>,
    #[scylla(default_when_null)]
    pub lps: Option<Vec<String>>,
    #[scylla(default_when_null)]
    pub buec: Option<Vec<String>>,
    #[scylla(default_when_null)]
    pub mup: Option<Vec<String>>,
    #[scylla(default_when_null)]
    pub psy: Option<Vec<String>>,
    #[scylla(default_when_null)]
    pub bc: Option<Vec<String>>,
    #[scylla(default_when_null)]
    pub utad: Option<Vec<String>>,
    #[scylla(default_when_null)]
    pub sow: Option<Vec<String>>,
    #[scylla(default_when_null)]
    pub lpsy: Option<Vec<String>>,
    #[scylla(default_when_null)]
    pub mdw: Option<Vec<String>>,
}

/// UDT `stockbit_fitem`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct StockbitFitemDb {
    pub id: String,
    pub name: String,
    pub value: String,
}

/// UDT `stockbit_fin_name_result`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct StockbitFinNameResultDb {
    pub fitem: StockbitFitemDb,
    pub hidden_graph_ico: bool,
    pub is_new_update: bool,
}

/// UDT `stockbit_closure_fin_items_group`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct StockbitClosureFinItemsGroupDb {
    pub keystats_name: String,
    #[scylla(default_when_null)]
    pub fin_name_results: Option<Vec<StockbitFinNameResultDb>>,
}

/// Kolom `closure_fin_items_results_stockbit`.
pub type ClosureFinItemsResultsStockbitDb = Option<Vec<StockbitClosureFinItemsGroupDb>>;

/// UDT `stockbit_period_value`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct StockbitPeriodValueDb {
    pub period: String,
    pub quarter_value: String,
    pub year: String,
    pub is_new_update: bool,
}

/// UDT `stockbit_financial_year_value`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct StockbitFinancialYearValueDb {
    pub year: String,
    #[scylla(default_when_null)]
    pub period_values: Option<Vec<StockbitPeriodValueDb>>,
    pub annualised_value: String,
    pub ttm_value: String,
    pub is_new_update: bool,
    pub dividend: String,
    pub payout_ratio: String,
    pub dividend_yield: String,
}

/// UDT `stockbit_most_recent_quarter`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct StockbitMostRecentQuarterDb {
    pub date: String,
    pub quarter: String,
    pub is_new_update: bool,
}

/// UDT `stockbit_financial_year_group`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct StockbitFinancialYearGroupDb {
    #[scylla(default_when_null)]
    pub financial_year_values: Option<Vec<StockbitFinancialYearValueDb>>,
    pub fitem_name: String,
    pub most_recent_quarter: StockbitMostRecentQuarterDb,
}

/// UDT `stockbit_financial_year_parent`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct StockbitFinancialYearParentDb {
    #[scylla(default_when_null)]
    pub financial_year_groups: Option<Vec<StockbitFinancialYearGroupDb>>,
    #[scylla(default_when_null)]
    pub financial_year_groups_usd: Option<Vec<StockbitFinancialYearGroupDb>>,
}

/// UDT `stockbit_stats`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct StockbitStatsDb {
    pub current_share_outstanding: String,
    pub market_cap: String,
    pub enterprise_value: String,
    pub free_float: String,
}

/// UDT `stockbit_dividend_year_value`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct StockbitDividendYearValueDb {
    pub period: i32,
    pub dividend: String,
    pub ex_date: String,
    pub payment_date: String,
}

/// UDT `stockbit_dividend_group`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue)]
pub struct StockbitDividendGroupDb {
    #[scylla(default_when_null)]
    pub fitem_id: Option<Vec<String>>,
    #[scylla(default_when_null)]
    pub dividend_year_values: Option<Vec<StockbitDividendYearValueDb>>,
}

/// Payload Stockbit keystats/ratio yang di-upsert ke Scylla.
#[derive(Debug, Clone)]
pub struct KeyStatsFromStockbitDb {
    pub closure_fin_items_results: ClosureFinItemsResultsStockbitDb,
    pub financial_year_parent: Option<StockbitFinancialYearParentDb>,
    pub stats: Option<StockbitStatsDb>,
    pub dividend_group: Option<StockbitDividendGroupDb>,
}

/// UDT `stockbit_report_user`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitReportUserDb {
    pub user_id: i32,
    pub is_author: bool,
    pub username: String,
    pub fullname: String,
    pub avatar: String,
    pub is_verified: bool,
    pub user_privilege: String,
    pub is_pro: bool,
    pub country: String,
    pub verified_status: String,
}

/// UDT `stockbit_report_status`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitReportStatusDb {
    pub is_pinned: bool,
    pub is_trending: bool,
    pub is_reposted: bool,
    pub is_liked: bool,
    pub is_saved: bool,
    pub is_followed: bool,
    pub is_unavailable: bool,
    pub is_junk: bool,
    pub is_spam: bool,
    pub is_violation: bool,
    pub is_deleted: bool,
}

/// UDT `stockbit_report_item`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitReportItemDb {
    #[serde(rename = "type")]
    #[scylla(rename = "type")]
    pub report_type: String,
}

/// UDT `stockbit_report_news_feed`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitReportNewsFeedDb {
    pub source: String,
    pub label: String,
    pub img: String,
}

/// UDT `stockbit_report_following_activity`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitReportFollowingActivityDb {
    #[serde(default)]
    #[scylla(default_when_null)]
    pub users: Option<Vec<String>>,
    pub info: String,
}

/// UDT `stockbit_report_summary`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitReportSummaryDb {
    pub title: String,
    pub summary: String,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub key_points: Option<Vec<String>>,
    pub key_takeaway: String,
    pub model: String,
    pub model_version: String,
}

/// UDT `stockbit_report_reaction_entry`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitReportReactionEntryDb {
    pub reaction: String,
    pub total: i32,
}

/// UDT `stockbit_report_reaction`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitReportReactionDb {
    #[serde(default)]
    #[scylla(default_when_null)]
    pub reactions: Option<Vec<StockbitReportReactionEntryDb>>,
    pub total: i32,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub my_reaction: Option<String>,
}

/// UDT `stockbit_report_stream`.
#[derive(Debug, Clone, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitReportStreamDb {
    pub stream_id: i64,
    pub title_url: String,
    pub title: String,
    pub content: String,
    pub content_original: String,
    pub created_at: String,
    pub created_display: String,
    pub updated_at: String,
    pub user: StockbitReportUserDb,
    pub status: StockbitReportStatusDb,
    pub total_replies: i32,
    pub total_likes: i32,
    pub likers: String,
    #[serde(rename = "type")]
    #[scylla(rename = "type")]
    pub stream_type: String,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub images: Option<Vec<String>>,
    pub parent_stream_id: i64,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub reports: Option<Vec<StockbitReportItemDb>>,
    pub news_feed: StockbitReportNewsFeedDb,
    pub last_reply_date: i64,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub topics: Option<Vec<String>>,
    pub image_frame_type: String,
    pub commenter_type: String,
    pub following_activity: StockbitReportFollowingActivityDb,
    pub reply_to: i32,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub summary: Option<StockbitReportSummaryDb>,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub reaction: Option<StockbitReportReactionDb>,
}

/// Kolom `stockbit_reports`.
pub type StockbitReportsDb = Option<Vec<StockbitReportStreamDb>>;

/// Subset kolom untuk `GetStockbitReportsByCode`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct StockbitReportsByCodeRow {
    pub code: String,
    #[scylla(default_when_null)]
    pub stockbit_reports: StockbitReportsDb,
    #[scylla(default_when_null)]
    pub stockbit_reports_updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// UDT `stockbit_profile_address`.
#[derive(Debug, Clone, Default, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitProfileAddressDb {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub email: Option<Vec<String>>,
    #[serde(default)]
    pub fax: String,
    #[serde(default)]
    pub npwp: String,
    #[serde(default)]
    pub phone: String,
    #[serde(default)]
    pub website: String,
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub lastupdate: String,
    #[serde(default)]
    pub value: String,
    #[serde(default)]
    pub office: String,
}

/// UDT `stockbit_profile_history`.
#[derive(Debug, Clone, Default, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitProfileHistoryDb {
    #[serde(default)]
    pub amount: String,
    #[serde(default)]
    pub board: String,
    #[serde(default)]
    pub date: String,
    #[serde(default)]
    pub price: String,
    #[serde(default)]
    pub registrar: String,
    #[serde(default)]
    pub shares: String,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub underwriters: Option<Vec<String>>,
    #[serde(default)]
    pub administrative_bureau: String,
    #[serde(default)]
    pub free_float: String,
}

/// UDT `stockbit_profile_executive_entry`.
#[derive(Debug, Clone, Default, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitProfileExecutiveEntryDb {
    #[serde(default)]
    pub id: String,
    #[serde(default, rename = "key")]
    #[scylla(rename = "key")]
    pub key_label: String,
    #[serde(default)]
    pub lastupdate: String,
    #[serde(default)]
    pub value: String,
}

/// UDT `stockbit_profile_key_executive`.
#[derive(Debug, Clone, Default, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitProfileKeyExecutiveDb {
    #[serde(default)]
    #[scylla(default_when_null)]
    pub commissioner: Option<Vec<StockbitProfileExecutiveEntryDb>>,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub director: Option<Vec<StockbitProfileExecutiveEntryDb>>,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub independent_commissioner: Option<Vec<StockbitProfileExecutiveEntryDb>>,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub president_commissioner: Option<Vec<StockbitProfileExecutiveEntryDb>>,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub president_director: Option<Vec<StockbitProfileExecutiveEntryDb>>,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub vice_president: Option<Vec<StockbitProfileExecutiveEntryDb>>,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub vice_president_commissioner: Option<Vec<StockbitProfileExecutiveEntryDb>>,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub independent_vice_president_commissioner: Option<Vec<StockbitProfileExecutiveEntryDb>>,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub independent_president_commissioner: Option<Vec<StockbitProfileExecutiveEntryDb>>,
}

/// UDT `stockbit_profile_shareholder_entry`.
#[derive(Debug, Clone, Default, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitProfileShareholderEntryDb {
    #[serde(default)]
    pub percentage: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub value: String,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub badges: Option<Vec<String>>,
    #[serde(default)]
    pub id: String,
    #[serde(default, rename = "type")]
    #[scylla(rename = "type")]
    pub shareholder_type: String,
    #[serde(default)]
    pub location: String,
    #[serde(default)]
    pub nationality: String,
    #[serde(default)]
    pub domicile: String,
    #[serde(default)]
    pub scripless: String,
    #[serde(default)]
    pub scrip: String,
    #[serde(default)]
    pub value_formatted: String,
    #[serde(default)]
    pub classification: String,
}

/// UDT `stockbit_profile_value_info`.
#[derive(Debug, Clone, Default, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitProfileValueInfoDb {
    #[serde(default)]
    pub value: String,
    #[serde(default)]
    pub info: String,
}

/// UDT `stockbit_profile_prospectus`.
#[derive(Debug, Clone, Default, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitProfileProspectusDb {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub file: String,
    #[serde(default)]
    pub dir: String,
    #[serde(default)]
    pub url: String,
}

/// UDT `stockbit_profile_fund_profile`.
#[derive(Debug, Clone, Default, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitProfileFundProfileDb {
    #[serde(default)]
    pub fund_type: StockbitProfileValueInfoDb,
    #[serde(default)]
    pub inception_date: String,
    #[serde(default)]
    pub fund_manager: String,
    #[serde(default)]
    pub fund_manager_ico: String,
    #[serde(default)]
    pub custodian_bank: String,
    #[serde(default)]
    pub custodian_ico: String,
    #[serde(default)]
    pub risk_level: StockbitProfileValueInfoDb,
    #[serde(default)]
    pub aum: StockbitProfileValueInfoDb,
    #[serde(default)]
    pub maxdrawdown: StockbitProfileValueInfoDb,
    #[serde(default)]
    pub cagr5year: StockbitProfileValueInfoDb,
    #[serde(default)]
    pub expense_ratio: StockbitProfileValueInfoDb,
    #[serde(default)]
    pub average_yield: StockbitProfileValueInfoDb,
    #[serde(default)]
    pub prospectus: StockbitProfileProspectusDb,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub fund_fact_sheet: Option<Vec<StockbitProfileProspectusDb>>,
    #[serde(default)]
    pub redemption_bank_name: String,
    #[serde(default)]
    pub min_buy: String,
    #[serde(default)]
    pub buy_fee: String,
    #[serde(default)]
    pub sell_fee: String,
}

/// UDT `stockbit_profile_shareholder_number`.
#[derive(Debug, Clone, Default, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitProfileShareholderNumberDb {
    #[serde(default)]
    pub shareholder_date: String,
    #[serde(default)]
    pub total_share: String,
    #[serde(default, rename = "change")]
    #[scylla(rename = "change")]
    pub change: i32,
    #[serde(default)]
    pub change_formatted: String,
    #[serde(default)]
    pub change_value: String,
}

/// UDT `stockbit_profile_percentage`.
#[derive(Debug, Clone, Default, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitProfilePercentageDb {
    #[serde(default)]
    pub raw: i32,
    #[serde(default)]
    pub formatted: String,
}

/// UDT `stockbit_profile_listing_information`.
#[derive(Debug, Clone, Default, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitProfileListingInformationDb {
    #[serde(default)]
    pub exercise_start_date: String,
    #[serde(default)]
    pub exercise_end_date: String,
    #[serde(default)]
    pub exercise_price: i32,
    #[serde(default)]
    pub expire_date: String,
    #[serde(default)]
    pub listing_date: String,
    #[serde(default)]
    pub foreign_percentage: StockbitProfilePercentageDb,
    #[serde(default)]
    pub local_percentage: StockbitProfilePercentageDb,
    #[serde(default)]
    pub number_of_securities: i32,
    #[serde(default)]
    pub total_shares: i32,
}

/// UDT `stockbit_profile_beneficiary`.
#[derive(Debug, Clone, Default, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitProfileBeneficiaryDb {
    #[serde(default)]
    pub name: String,
}

/// UDT `stockbit_profile_shareholder_one_percent`.
#[derive(Debug, Clone, Default, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitProfileShareholderOnePercentDb {
    #[serde(default)]
    #[scylla(default_when_null)]
    pub shareholder: Option<Vec<StockbitProfileShareholderEntryDb>>,
    #[serde(default)]
    pub last_updated: String,
}

/// UDT `stockbit_profile_subsidiary`.
#[derive(Debug, Clone, Default, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitProfileSubsidiaryDb {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub percentage: String,
}

/// UDT `stockbit_profile_fee_entry`.
#[derive(Debug, Clone, Default, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitProfileFeeEntryDb {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub value: String,
}

/// UDT `stockbit_profile_asset_allocation_entry`.
#[derive(Debug, Clone, Default, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitProfileAssetAllocationEntryDb {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub percentage: String,
    #[serde(default)]
    pub value: String,
}

/// UDT `stockbit_profile_top_holding_entry`.
#[derive(Debug, Clone, Default, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitProfileTopHoldingEntryDb {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub percentage: String,
    #[serde(default)]
    pub value: String,
}

/// UDT `stockbit_profile` — profil emiten dari API /emitten/{code}/profile.
#[derive(Debug, Clone, Default, SerializeValue, DeserializeValue, serde::Deserialize)]
pub struct StockbitProfileDb {
    #[serde(default)]
    #[scylla(default_when_null)]
    pub address: Option<Vec<StockbitProfileAddressDb>>,
    #[serde(default)]
    pub background: String,
    #[serde(default)]
    pub history: StockbitProfileHistoryDb,
    #[serde(default)]
    pub key_executive: StockbitProfileKeyExecutiveDb,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub secretary: Option<Vec<StockbitProfileExecutiveEntryDb>>,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub shareholder: Option<Vec<StockbitProfileShareholderEntryDb>>,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub subsidiary: Option<Vec<StockbitProfileSubsidiaryDb>>,
    #[serde(default, rename = "profile")]
    #[scylla(rename = "profile")]
    pub fund_profile: StockbitProfileFundProfileDb,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub fee: Option<Vec<StockbitProfileFeeEntryDb>>,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub asset_allocation: Option<Vec<StockbitProfileAssetAllocationEntryDb>>,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub shareholder_reksa: Option<Vec<StockbitProfileShareholderEntryDb>>,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub pdf: Option<Vec<StockbitProfileProspectusDb>>,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub shareholder_numbers: Option<Vec<StockbitProfileShareholderNumberDb>>,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub badges: Option<Vec<String>>,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub top_holdings: Option<Vec<StockbitProfileTopHoldingEntryDb>>,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub shareholder_director_commissioner: Option<Vec<StockbitProfileShareholderEntryDb>>,
    #[serde(default)]
    pub listing_information: StockbitProfileListingInformationDb,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub beneficiary: Option<Vec<StockbitProfileBeneficiaryDb>>,
    #[serde(default)]
    pub shareholder_one_percent: StockbitProfileShareholderOnePercentDb,
    #[serde(default)]
    #[scylla(default_when_null)]
    pub classification: Option<String>,
}

/// Kolom `stockbit_profile`.
pub type StockbitProfileColDb = Option<StockbitProfileDb>;

/// Subset kolom untuk `GetStockbitProfileByCode`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct StockbitProfileByCodeRow {
    pub code: String,
    #[scylla(default_when_null)]
    pub stockbit_profile: StockbitProfileColDb,
    #[scylla(default_when_null)]
    pub stockbit_profile_updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Subset kolom Stockbit keystats untuk `GetKeyStatsFromStockbit`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct KeyStatsFromStockbitRow {
    pub code: String,
    #[scylla(default_when_null)]
    pub closure_fin_items_results_stockbit: ClosureFinItemsResultsStockbitDb,
    #[scylla(default_when_null)]
    pub closure_fin_items_results_stockbit_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    #[scylla(default_when_null)]
    pub financial_year_parent_stockbit: Option<StockbitFinancialYearParentDb>,
    #[scylla(default_when_null)]
    pub financial_year_parent_stockbit_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    #[scylla(default_when_null)]
    pub stats_stockbit: Option<StockbitStatsDb>,
    #[scylla(default_when_null)]
    pub stats_stockbit_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    #[scylla(default_when_null)]
    pub dividend_group_stockbit: Option<StockbitDividendGroupDb>,
    #[scylla(default_when_null)]
    pub dividend_group_stockbit_updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Satu baris `invezgood.stock_list`.
#[derive(Debug, Clone, DeserializeRow, SerializeRow)]
pub struct StockListRow {
    pub code: String,
    #[scylla(default_when_null)]
    pub name: Option<String>,
    #[scylla(default_when_null)]
    pub sector: Option<String>,
    #[scylla(default_when_null)]
    pub sub_sector: Option<String>,
    #[scylla(default_when_null)]
    pub logo: Option<String>,
    #[scylla(default_when_null)]
    pub keystats: Option<StockListKeystatsDb>,
    #[scylla(default_when_null)]
    pub keystats_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    #[scylla(default_when_null)]
    pub balance_statement: Option<StockListBalanceStatementDb>,
    #[scylla(default_when_null)]
    pub balance_statement_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    #[scylla(default_when_null)]
    pub income_statement: Option<StockListIncomeStatementDb>,
    #[scylla(default_when_null)]
    pub income_statement_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    #[scylla(default_when_null)]
    pub cash_flow: Option<StockListCashFlowDb>,
    #[scylla(default_when_null)]
    pub cash_flow_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    #[scylla(default_when_null)]
    pub share_holder_5: ShareHolder5Db,
    #[scylla(default_when_null)]
    pub share_holder_5_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    #[scylla(default_when_null)]
    pub share_holder_1: ShareHolder1Db,
    #[scylla(default_when_null)]
    pub share_holder_1_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    #[scylla(default_when_null)]
    pub share_holder_composition: ShareHolderCompositionDb,
    #[scylla(default_when_null)]
    pub share_holder_composition_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    #[scylla(default_when_null)]
    pub company_information: Option<CompanyInformationDb>,
    #[scylla(default_when_null)]
    pub company_information_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    #[scylla(default_when_null)]
    pub corporate_action: Option<CorporateActionDb>,
    #[scylla(default_when_null)]
    pub corporate_action_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    #[scylla(default_when_null)]
    pub catatan_owner: Option<String>,
    #[scylla(default_when_null)]
    pub catatan_pribadi: Option<String>,
    #[scylla(default_when_null)]
    pub is_plan_to_trade: Option<bool>,
    #[scylla(default_when_null)]
    pub is_konglomerasi: Option<bool>,
    #[scylla(default_when_null)]
    pub wyckoff_chart: Option<WyckoffChartDb>,
    #[scylla(default_when_null)]
    pub horizontal_line: Option<Vec<i32>>,
    #[scylla(default_when_null)]
    pub takeprofit_wyckoff: Option<HashMap<String, f64>>,
    #[scylla(default_when_null)]
    pub is_bad_fundamental: Option<bool>,
}

/// Subset kolom untuk `GetWyckoffChartByCode`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct WyckoffChartByCodeRow {
    pub code: String,
    #[scylla(default_when_null)]
    pub wyckoff_chart: Option<WyckoffChartDb>,
}

/// Subset kolom untuk `GetHorizontalLineByCode`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct HorizontalLineByCodeRow {
    pub code: String,
    #[scylla(default_when_null)]
    pub horizontal_line: Option<Vec<i32>>,
}

/// Subset kolom untuk `GetTakeProfitWyckoffByCode`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct TakeProfitWyckoffByCodeRow {
    pub code: String,
    #[scylla(default_when_null)]
    pub takeprofit_wyckoff: Option<HashMap<String, f64>>,
}

/// Subset kolom untuk `GetAllKeyStats`.
#[derive(Debug, Clone, DeserializeRow)]
pub struct StockListKeystatsRow {
    pub code: String,
    #[scylla(default_when_null)]
    pub keystats: Option<StockListKeystatsDb>,
    #[scylla(default_when_null)]
    pub keystats_updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Subset kolom untuk `GetAllStocks` (list ringan).
#[derive(Debug, Clone, DeserializeRow)]
pub struct StockListSummaryRow {
    pub code: String,
    #[scylla(default_when_null)]
    pub name: Option<String>,
    #[scylla(default_when_null)]
    pub sector: Option<String>,
    #[scylla(default_when_null)]
    pub sub_sector: Option<String>,
    #[scylla(default_when_null)]
    pub logo: Option<String>,
    #[scylla(default_when_null)]
    pub keystats_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    #[scylla(default_when_null)]
    pub catatan_owner: Option<String>,
    #[scylla(default_when_null)]
    pub catatan_pribadi: Option<String>,
    #[scylla(default_when_null)]
    pub is_plan_to_trade: Option<bool>,
    #[scylla(default_when_null)]
    pub is_konglomerasi: Option<bool>,
    #[scylla(default_when_null)]
    pub takeprofit_wyckoff: Option<HashMap<String, f64>>,
    #[scylla(default_when_null)]
    pub is_bad_fundamental: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct CompanyPersonEntry {
    pub name: String,
    pub position: String,
}

#[derive(Debug, Clone)]
pub struct CompanySubsidiaryEntry {
    pub name: String,
    pub percentage: f64,
}

#[derive(Debug, Clone, Default)]
pub struct CompanyInformation {
    pub address: String,
    pub industry: String,
    pub subsindustry: String,
    pub activity: String,
    pub name: String,
    pub npwp: String,
    pub board: String,
    pub sector: String,
    pub subsector: String,
    pub listing_date: Option<chrono::DateTime<chrono::Utc>>,
    pub website: String,
    pub logo: String,
    pub additional_info: Option<String>,
    pub people: Option<String>,
    pub report_type: Option<String>,
    pub administration: Option<String>,
    pub description: Option<String>,
    pub ipo_pct: Option<f64>,
    pub ipo_price: Option<f64>,
    pub ipo_share: Option<String>,
    pub ipo_underwriter: Option<String>,
    pub nominal_price: Option<f64>,
    pub category: Vec<String>,
    pub active: bool,
    pub commissioner: Vec<CompanyPersonEntry>,
    pub director: Vec<CompanyPersonEntry>,
    pub subsidiary: Vec<CompanySubsidiaryEntry>,
}

#[derive(Debug, Clone, Default)]
pub struct CorporateActionEntry {
    pub code: String,
    pub action_type: String,
    pub payload: HashMap<String, String>,
}

#[derive(Debug, Clone, Default)]
pub struct CorporateAction {
    pub total_page: i32,
    pub page: i32,
    pub next_page: Option<i32>,
    pub data: Vec<CorporateActionEntry>,
}

#[derive(Debug, Clone, Default)]
pub struct ShareHolderComposition {
    pub items: Vec<ShareHolderCompositionEntry>,
}

#[derive(Debug, Clone)]
pub struct ShareHolderCompositionEntry {
    pub name: String,
    pub percentage: f64,
    pub badge: String,
}

#[derive(Debug, Clone, Default)]
pub struct ShareHolder1 {
    pub items: Vec<ShareHolder1Entry>,
}

#[derive(Debug, Clone)]
pub struct ShareHolder1Entry {
    pub name: String,
    pub holder_type: String,
    pub status: String,
    pub nationality: String,
    pub domicile: String,
    pub scripless: String,
    pub scrip: String,
    pub total: String,
    pub percentage: f64,
}

#[derive(Debug, Clone, Default)]
pub struct ShareHolder5 {
    pub items: Vec<ShareHolder5Entry>,
}

#[derive(Debug, Clone)]
pub struct ShareHolder5Entry {
    pub name: String,
    pub date: chrono::DateTime<chrono::Utc>,
    pub val: String,
    pub percent: f64,
}

#[derive(Debug, Clone)]
pub struct KeystatsValue {
    pub col: String,
    pub year: i32,
    pub amount: f64,
    pub period: String,
}

#[derive(Debug, Clone)]
pub struct KeystatsRow {
    pub id: String,
    pub name: String,
    pub values: Vec<KeystatsValue>,
}

#[derive(Debug, Clone)]
pub struct KeystatsColumn {
    pub year: i32,
    pub label: String,
    pub period: String,
}

#[derive(Debug, Clone)]
pub struct Keystats {
    pub rows: Vec<KeystatsRow>,
    pub columns: Vec<KeystatsColumn>,
}

#[derive(Debug, Clone)]
pub struct BalanceStatementRow {
    pub id: String,
    pub name: String,
    pub level: i32,
    pub values: Vec<KeystatsValue>,
    pub parent_id: Option<String>,
    pub is_abstract: bool,
    pub display_order: i32,
}

#[derive(Debug, Clone)]
pub struct BalanceStatement {
    pub rows: Vec<BalanceStatementRow>,
    pub columns: Vec<KeystatsColumn>,
}

impl From<KeystatsValueDb> for KeystatsValue {
    fn from(db: KeystatsValueDb) -> Self {
        Self {
            col: db.col,
            year: db.year,
            amount: db.amount,
            period: db.period,
        }
    }
}

impl From<KeystatsValue> for KeystatsValueDb {
    fn from(v: KeystatsValue) -> Self {
        Self {
            col: v.col,
            year: v.year,
            amount: v.amount,
            period: v.period,
        }
    }
}

impl From<KeystatsColumnDb> for KeystatsColumn {
    fn from(db: KeystatsColumnDb) -> Self {
        Self {
            year: db.year,
            label: db.label,
            period: db.period,
        }
    }
}

impl From<KeystatsColumn> for KeystatsColumnDb {
    fn from(c: KeystatsColumn) -> Self {
        Self {
            year: c.year,
            label: c.label,
            period: c.period,
        }
    }
}

impl From<KeystatsRowDb> for KeystatsRow {
    fn from(db: KeystatsRowDb) -> Self {
        Self {
            id: db.id,
            name: db.name,
            values: db
                .values
                .unwrap_or_default()
                .into_iter()
                .map(KeystatsValue::from)
                .collect(),
        }
    }
}

impl From<KeystatsRow> for KeystatsRowDb {
    fn from(r: KeystatsRow) -> Self {
        Self {
            id: r.id,
            name: r.name,
            values: Some(r.values.into_iter().map(KeystatsValueDb::from).collect()),
        }
    }
}

impl From<StockListKeystatsDb> for Keystats {
    fn from(db: StockListKeystatsDb) -> Self {
        Self {
            rows: db
                .rows
                .unwrap_or_default()
                .into_iter()
                .map(KeystatsRow::from)
                .collect(),
            columns: db
                .columns
                .unwrap_or_default()
                .into_iter()
                .map(KeystatsColumn::from)
                .collect(),
        }
    }
}

impl From<Keystats> for StockListKeystatsDb {
    fn from(k: Keystats) -> Self {
        Self {
            rows: Some(k.rows.into_iter().map(KeystatsRowDb::from).collect()),
            columns: Some(k.columns.into_iter().map(KeystatsColumnDb::from).collect()),
        }
    }
}

impl From<BalanceStatementRowDb> for BalanceStatementRow {
    fn from(db: BalanceStatementRowDb) -> Self {
        Self {
            id: db.id,
            name: db.name,
            level: db.level,
            values: db
                .values
                .unwrap_or_default()
                .into_iter()
                .map(KeystatsValue::from)
                .collect(),
            parent_id: db.parent_id,
            is_abstract: db.is_abstract,
            display_order: db.display_order,
        }
    }
}

impl From<BalanceStatementRow> for BalanceStatementRowDb {
    fn from(r: BalanceStatementRow) -> Self {
        Self {
            id: r.id,
            name: r.name,
            level: r.level,
            values: Some(r.values.into_iter().map(KeystatsValueDb::from).collect()),
            parent_id: r.parent_id,
            is_abstract: r.is_abstract,
            display_order: r.display_order,
        }
    }
}

impl From<StockListBalanceStatementDb> for BalanceStatement {
    fn from(db: StockListBalanceStatementDb) -> Self {
        Self {
            rows: db
                .rows
                .unwrap_or_default()
                .into_iter()
                .map(BalanceStatementRow::from)
                .collect(),
            columns: db
                .columns
                .unwrap_or_default()
                .into_iter()
                .map(KeystatsColumn::from)
                .collect(),
        }
    }
}

impl From<BalanceStatement> for StockListBalanceStatementDb {
    fn from(b: BalanceStatement) -> Self {
        Self {
            rows: Some(
                b.rows
                    .into_iter()
                    .map(BalanceStatementRowDb::from)
                    .collect(),
            ),
            columns: Some(b.columns.into_iter().map(KeystatsColumnDb::from).collect()),
        }
    }
}

impl From<ShareHolder5EntryDb> for ShareHolder5Entry {
    fn from(db: ShareHolder5EntryDb) -> Self {
        Self {
            name: db.name,
            date: db.date,
            val: db.val,
            percent: db.percent,
        }
    }
}

impl From<ShareHolder5Entry> for ShareHolder5EntryDb {
    fn from(e: ShareHolder5Entry) -> Self {
        Self {
            name: e.name,
            date: e.date,
            val: e.val,
            percent: e.percent,
        }
    }
}

impl From<ShareHolder5Db> for ShareHolder5 {
    fn from(entries: ShareHolder5Db) -> Self {
        Self {
            items: entries
                .unwrap_or_default()
                .into_iter()
                .map(ShareHolder5Entry::from)
                .collect(),
        }
    }
}

impl From<ShareHolder5> for ShareHolder5Db {
    fn from(entries: ShareHolder5) -> Self {
        Some(
            entries
                .items
                .into_iter()
                .map(ShareHolder5EntryDb::from)
                .collect(),
        )
    }
}

impl From<ShareHolder1EntryDb> for ShareHolder1Entry {
    fn from(db: ShareHolder1EntryDb) -> Self {
        Self {
            name: db.name,
            holder_type: db.holder_type,
            status: db.status,
            nationality: db.nationality,
            domicile: db.domicile,
            scripless: db.scripless,
            scrip: db.scrip,
            total: db.total,
            percentage: db.percentage,
        }
    }
}

impl From<ShareHolder1Entry> for ShareHolder1EntryDb {
    fn from(e: ShareHolder1Entry) -> Self {
        Self {
            name: e.name,
            holder_type: e.holder_type,
            status: e.status,
            nationality: e.nationality,
            domicile: e.domicile,
            scripless: e.scripless,
            scrip: e.scrip,
            total: e.total,
            percentage: e.percentage,
        }
    }
}

impl From<ShareHolder1Db> for ShareHolder1 {
    fn from(entries: ShareHolder1Db) -> Self {
        Self {
            items: entries
                .unwrap_or_default()
                .into_iter()
                .map(ShareHolder1Entry::from)
                .collect(),
        }
    }
}

impl From<ShareHolder1> for ShareHolder1Db {
    fn from(entries: ShareHolder1) -> Self {
        Some(
            entries
                .items
                .into_iter()
                .map(ShareHolder1EntryDb::from)
                .collect(),
        )
    }
}

impl From<ShareHolderCompositionEntryDb> for ShareHolderCompositionEntry {
    fn from(db: ShareHolderCompositionEntryDb) -> Self {
        Self {
            name: db.name,
            percentage: db.percentage,
            badge: db.badge,
        }
    }
}

impl From<ShareHolderCompositionEntry> for ShareHolderCompositionEntryDb {
    fn from(e: ShareHolderCompositionEntry) -> Self {
        Self {
            name: e.name,
            percentage: e.percentage,
            badge: e.badge,
        }
    }
}

impl From<ShareHolderCompositionDb> for ShareHolderComposition {
    fn from(entries: ShareHolderCompositionDb) -> Self {
        Self {
            items: entries
                .unwrap_or_default()
                .into_iter()
                .map(ShareHolderCompositionEntry::from)
                .collect(),
        }
    }
}

impl From<ShareHolderComposition> for ShareHolderCompositionDb {
    fn from(entries: ShareHolderComposition) -> Self {
        Some(
            entries
                .items
                .into_iter()
                .map(ShareHolderCompositionEntryDb::from)
                .collect(),
        )
    }
}

impl From<CompanyPersonEntryDb> for CompanyPersonEntry {
    fn from(db: CompanyPersonEntryDb) -> Self {
        Self {
            name: db.name,
            position: db.position,
        }
    }
}

impl From<CompanyPersonEntry> for CompanyPersonEntryDb {
    fn from(e: CompanyPersonEntry) -> Self {
        Self {
            name: e.name,
            position: e.position,
        }
    }
}

impl From<CompanySubsidiaryEntryDb> for CompanySubsidiaryEntry {
    fn from(db: CompanySubsidiaryEntryDb) -> Self {
        Self {
            name: db.name,
            percentage: db.percentage,
        }
    }
}

impl From<CompanySubsidiaryEntry> for CompanySubsidiaryEntryDb {
    fn from(e: CompanySubsidiaryEntry) -> Self {
        Self {
            name: e.name,
            percentage: e.percentage,
        }
    }
}

impl From<CompanyInformationDb> for CompanyInformation {
    fn from(db: CompanyInformationDb) -> Self {
        Self {
            address: db.address.unwrap_or_default(),
            industry: db.industry.unwrap_or_default(),
            subsindustry: db.subsindustry.unwrap_or_default(),
            activity: db.activity.unwrap_or_default(),
            name: db.name.unwrap_or_default(),
            npwp: db.npwp.unwrap_or_default(),
            board: db.board.unwrap_or_default(),
            sector: db.sector.unwrap_or_default(),
            subsector: db.subsector.unwrap_or_default(),
            listing_date: db.listing_date,
            website: db.website.unwrap_or_default(),
            logo: db.logo.unwrap_or_default(),
            additional_info: db.additional_info,
            people: db.people,
            report_type: db.report_type,
            administration: db.administration,
            description: db.description,
            ipo_pct: db.ipo_pct,
            ipo_price: db.ipo_price,
            ipo_share: db.ipo_share,
            ipo_underwriter: db.ipo_underwriter,
            nominal_price: db.nominal_price,
            category: db.category.unwrap_or_default(),
            active: db.active.unwrap_or(false),
            commissioner: db
                .commissioner
                .unwrap_or_default()
                .into_iter()
                .map(CompanyPersonEntry::from)
                .collect(),
            director: db
                .director
                .unwrap_or_default()
                .into_iter()
                .map(CompanyPersonEntry::from)
                .collect(),
            subsidiary: db
                .subsidiary
                .unwrap_or_default()
                .into_iter()
                .map(CompanySubsidiaryEntry::from)
                .collect(),
        }
    }
}

impl From<CompanyInformation> for CompanyInformationDb {
    fn from(info: CompanyInformation) -> Self {
        Self {
            address: Some(info.address),
            industry: Some(info.industry),
            subsindustry: Some(info.subsindustry),
            activity: Some(info.activity),
            name: Some(info.name),
            npwp: Some(info.npwp),
            board: Some(info.board),
            sector: Some(info.sector),
            subsector: Some(info.subsector),
            listing_date: info.listing_date,
            website: Some(info.website),
            logo: Some(info.logo),
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
            category: Some(info.category),
            active: Some(info.active),
            commissioner: Some(
                info.commissioner
                    .into_iter()
                    .map(CompanyPersonEntryDb::from)
                    .collect(),
            ),
            director: Some(
                info.director
                    .into_iter()
                    .map(CompanyPersonEntryDb::from)
                    .collect(),
            ),
            subsidiary: Some(
                info.subsidiary
                    .into_iter()
                    .map(CompanySubsidiaryEntryDb::from)
                    .collect(),
            ),
        }
    }
}

impl From<CorporateActionEntryDb> for CorporateActionEntry {
    fn from(db: CorporateActionEntryDb) -> Self {
        Self {
            code: db.code,
            action_type: db.action_type,
            payload: db.payload.unwrap_or_default(),
        }
    }
}

impl From<CorporateActionEntry> for CorporateActionEntryDb {
    fn from(entry: CorporateActionEntry) -> Self {
        Self {
            code: entry.code,
            action_type: entry.action_type,
            payload: Some(entry.payload).filter(|m| !m.is_empty()),
        }
    }
}

impl From<CorporateActionDb> for CorporateAction {
    fn from(db: CorporateActionDb) -> Self {
        Self {
            total_page: db.total_page,
            page: db.page,
            next_page: db.next_page,
            data: db
                .data
                .unwrap_or_default()
                .into_iter()
                .map(CorporateActionEntry::from)
                .collect(),
        }
    }
}

impl From<CorporateAction> for CorporateActionDb {
    fn from(action: CorporateAction) -> Self {
        Self {
            total_page: action.total_page,
            page: action.page,
            next_page: action.next_page,
            data: Some(
                action
                    .data
                    .into_iter()
                    .map(CorporateActionEntryDb::from)
                    .collect(),
            ),
        }
    }
}
