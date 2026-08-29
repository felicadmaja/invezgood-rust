//! Fetch & parse Stockbit keystats/ratio API → upsert kolom *_stockbit di Scylla.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use scylla::client::session::Session;
use serde::Deserialize;
use stockbit_browser::ensure_stockbit_bearer;

use crate::model::{
    ClosureFinItemsResultsStockbitDb, KeyStatsFromStockbitDb, KeyStatsFromStockbitRow,
    StockbitClosureFinItemsGroupDb, StockbitDividendGroupDb, StockbitDividendYearValueDb,
    StockbitFinancialYearGroupDb, StockbitFinancialYearParentDb, StockbitFinancialYearValueDb,
    StockbitFinNameResultDb, StockbitFitemDb, StockbitMostRecentQuarterDb, StockbitPeriodValueDb,
    StockbitStatsDb,
};

const KEYSTATS_RATIO_URL: &str = "https://exodus.stockbit.com/keystats/ratio/v1";
const KEYSTATS_YEAR_LIMIT: u32 = 10;
const STOCKBIT_KEYSTATS_MAX_AGE_SECS: i64 = 7 * 24 * 60 * 60;

#[derive(Debug, Deserialize)]
struct ApiResponse {
    data: ApiData,
}

#[derive(Debug, Deserialize, Default)]
struct ApiData {
    #[serde(default)]
    closure_fin_items_results: Vec<ApiClosureGroup>,
    financial_year_parent: Option<ApiFinancialYearParent>,
    stats: Option<ApiStats>,
    dividend_group: Option<ApiDividendGroup>,
}

#[derive(Debug, Deserialize)]
struct ApiClosureGroup {
    keystats_name: String,
    #[serde(default)]
    fin_name_results: Vec<ApiFinNameResult>,
}

#[derive(Debug, Deserialize)]
struct ApiFinNameResult {
    fitem: ApiFitem,
    #[serde(default)]
    hidden_graph_ico: bool,
    #[serde(default)]
    is_new_update: bool,
}

#[derive(Debug, Deserialize)]
struct ApiFitem {
    id: String,
    name: String,
    value: String,
}

#[derive(Debug, Deserialize)]
struct ApiFinancialYearParent {
    #[serde(default)]
    financial_year_groups: Vec<ApiFinancialYearGroup>,
    #[serde(default)]
    financial_year_groups_usd: Vec<ApiFinancialYearGroup>,
}

#[derive(Debug, Deserialize)]
struct ApiFinancialYearGroup {
    #[serde(default)]
    financial_year_values: Vec<ApiFinancialYearValue>,
    fitem_name: String,
    most_recent_quarter: ApiMostRecentQuarter,
}

#[derive(Debug, Deserialize)]
struct ApiFinancialYearValue {
    year: YearField,
    #[serde(default)]
    period_values: Vec<ApiPeriodValue>,
    annualised_value: String,
    ttm_value: String,
    #[serde(default)]
    is_new_update: bool,
    dividend: String,
    payout_ratio: String,
    dividend_yield: String,
}

#[derive(Debug, Deserialize)]
struct ApiPeriodValue {
    period: String,
    quarter_value: String,
    year: YearField,
    #[serde(default)]
    is_new_update: bool,
}

#[derive(Debug, Deserialize)]
struct ApiMostRecentQuarter {
    date: String,
    quarter: String,
    #[serde(default)]
    is_new_update: bool,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum YearField {
    Int(i32),
    Text(String),
}

impl YearField {
    fn into_string(self) -> String {
        match self {
            Self::Int(v) => v.to_string(),
            Self::Text(v) => v,
        }
    }
}

#[derive(Debug, Deserialize)]
struct ApiStats {
    current_share_outstanding: String,
    market_cap: String,
    enterprise_value: String,
    free_float: String,
}

#[derive(Debug, Deserialize)]
struct ApiDividendGroup {
    #[serde(default)]
    fitem_id: Vec<String>,
    #[serde(default)]
    dividend_year_values: Vec<ApiDividendYearValue>,
}

#[derive(Debug, Deserialize)]
struct ApiDividendYearValue {
    period: YearField,
    dividend: String,
    ex_date: String,
    payment_date: String,
}

fn is_list_empty<T>(value: &Option<Vec<T>>) -> bool {
    value.as_ref().map(|items| items.is_empty()).unwrap_or(true)
}

fn is_closure_empty(value: &ClosureFinItemsResultsStockbitDb) -> bool {
    is_list_empty(value)
}

fn is_financial_year_parent_empty(value: &Option<StockbitFinancialYearParentDb>) -> bool {
    match value {
        None => true,
        Some(parent) => {
            is_list_empty(&parent.financial_year_groups)
                && is_list_empty(&parent.financial_year_groups_usd)
        }
    }
}

fn is_stats_empty(value: &Option<StockbitStatsDb>) -> bool {
    value.is_none()
}

fn is_dividend_group_empty(value: &Option<StockbitDividendGroupDb>) -> bool {
    match value {
        None => true,
        Some(group) => {
            is_list_empty(&group.fitem_id) && is_list_empty(&group.dividend_year_values)
        }
    }
}

fn is_updated_at_stale(updated_at: Option<DateTime<Utc>>) -> bool {
    let Some(updated_at) = updated_at else {
        return true;
    };
    Utc::now()
        .signed_duration_since(updated_at)
        .num_seconds()
        > STOCKBIT_KEYSTATS_MAX_AGE_SECS
}

/// Perlu GET Stockbit API bila salah satu kolom null/[] atau *_updated_at >7 hari.
pub fn needs_stockbit_keystats_refresh(row: &KeyStatsFromStockbitRow) -> bool {
    is_closure_empty(&row.closure_fin_items_results_stockbit)
        || is_updated_at_stale(row.closure_fin_items_results_stockbit_updated_at)
        || is_financial_year_parent_empty(&row.financial_year_parent_stockbit)
        || is_updated_at_stale(row.financial_year_parent_stockbit_updated_at)
        || is_stats_empty(&row.stats_stockbit)
        || is_updated_at_stale(row.stats_stockbit_updated_at)
        || is_dividend_group_empty(&row.dividend_group_stockbit)
        || is_updated_at_stale(row.dividend_group_stockbit_updated_at)
}

fn map_closure_groups(groups: Vec<ApiClosureGroup>) -> ClosureFinItemsResultsStockbitDb {
    if groups.is_empty() {
        return None;
    }
    Some(
        groups
            .into_iter()
            .map(|g| StockbitClosureFinItemsGroupDb {
                keystats_name: g.keystats_name,
                fin_name_results: Some(
                    g.fin_name_results
                        .into_iter()
                        .map(|item| StockbitFinNameResultDb {
                            fitem: StockbitFitemDb {
                                id: item.fitem.id,
                                name: item.fitem.name,
                                value: item.fitem.value,
                            },
                            hidden_graph_ico: item.hidden_graph_ico,
                            is_new_update: item.is_new_update,
                        })
                        .collect(),
                ),
            })
            .collect(),
    )
}

fn map_financial_year_groups(groups: Vec<ApiFinancialYearGroup>) -> Option<Vec<StockbitFinancialYearGroupDb>> {
    if groups.is_empty() {
        return None;
    }
    Some(
        groups
            .into_iter()
            .map(|g| StockbitFinancialYearGroupDb {
                financial_year_values: Some(
                    g.financial_year_values
                        .into_iter()
                        .map(|yv| StockbitFinancialYearValueDb {
                            year: yv.year.into_string(),
                            period_values: Some(
                                yv.period_values
                                    .into_iter()
                                    .map(|pv| StockbitPeriodValueDb {
                                        period: pv.period,
                                        quarter_value: pv.quarter_value,
                                        year: pv.year.into_string(),
                                        is_new_update: pv.is_new_update,
                                    })
                                    .collect(),
                            ),
                            annualised_value: yv.annualised_value,
                            ttm_value: yv.ttm_value,
                            is_new_update: yv.is_new_update,
                            dividend: yv.dividend,
                            payout_ratio: yv.payout_ratio,
                            dividend_yield: yv.dividend_yield,
                        })
                        .collect(),
                ),
                fitem_name: g.fitem_name,
                most_recent_quarter: StockbitMostRecentQuarterDb {
                    date: g.most_recent_quarter.date,
                    quarter: g.most_recent_quarter.quarter,
                    is_new_update: g.most_recent_quarter.is_new_update,
                },
            })
            .collect(),
    )
}

fn map_financial_year_parent(
    parent: Option<ApiFinancialYearParent>,
) -> Option<StockbitFinancialYearParentDb> {
    let parent = parent?;
    Some(StockbitFinancialYearParentDb {
        financial_year_groups: map_financial_year_groups(parent.financial_year_groups),
        financial_year_groups_usd: map_financial_year_groups(parent.financial_year_groups_usd),
    })
}

fn map_stats(stats: Option<ApiStats>) -> Option<StockbitStatsDb> {
    stats.map(|s| StockbitStatsDb {
        current_share_outstanding: s.current_share_outstanding,
        market_cap: s.market_cap,
        enterprise_value: s.enterprise_value,
        free_float: s.free_float,
    })
}

fn map_dividend_group(group: Option<ApiDividendGroup>) -> Option<StockbitDividendGroupDb> {
    let group = group?;
    if group.fitem_id.is_empty() && group.dividend_year_values.is_empty() {
        return None;
    }
    Some(StockbitDividendGroupDb {
        fitem_id: if group.fitem_id.is_empty() {
            None
        } else {
            Some(group.fitem_id)
        },
        dividend_year_values: if group.dividend_year_values.is_empty() {
            None
        } else {
            Some(
                group
                    .dividend_year_values
                    .into_iter()
                    .map(|item| StockbitDividendYearValueDb {
                        period: match item.period {
                            YearField::Int(v) => v,
                            YearField::Text(v) => v.parse().unwrap_or(0),
                        },
                        dividend: item.dividend,
                        ex_date: item.ex_date,
                        payment_date: item.payment_date,
                    })
                    .collect(),
            )
        },
    })
}

fn parse_response(body: &str, code: &str) -> Result<KeyStatsFromStockbitDb, String> {
    let parsed: ApiResponse =
        serde_json::from_str(body).map_err(|e| format!("keystats/ratio {code} JSON: {e}"))?;
    let data = parsed.data;
    Ok(KeyStatsFromStockbitDb {
        closure_fin_items_results: map_closure_groups(data.closure_fin_items_results),
        financial_year_parent: map_financial_year_parent(data.financial_year_parent),
        stats: map_stats(data.stats),
        dividend_group: map_dividend_group(data.dividend_group),
    })
}

pub async fn fetch_keystats_ratio(code: &str) -> Result<KeyStatsFromStockbitDb, String> {
    let code = code.trim().to_ascii_uppercase();
    let bearer = ensure_stockbit_bearer()
        .await
        .map_err(|e| format!("Stockbit bearer gagal: {e}"))?;

    let url = format!("{KEYSTATS_RATIO_URL}/{code}?year_limit={KEYSTATS_YEAR_LIMIT}");
    let http = reqwest::Client::new();
    let resp = http
        .get(&url)
        .header("Authorization", format!("Bearer {bearer}"))
        .header("Accept", "application/json")
        .header("Origin", "https://stockbit.com")
        .header("Referer", "https://stockbit.com/")
        .header("x-platform", "web")
        .send()
        .await
        .map_err(|e| format!("keystats/ratio {code} request: {e}"))?;

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if status == reqwest::StatusCode::NOT_FOUND || status == reqwest::StatusCode::BAD_REQUEST {
        return Err(format!("Emiten {code} tidak ditemukan di Stockbit"));
    }
    if !status.is_success() {
        let preview: String = body.chars().take(280).collect();
        return Err(format!("keystats/ratio {code} HTTP {status}: {preview}"));
    }

    parse_response(&body, &code)
}

pub async fn fetch_and_save_keystats_from_stockbit(
    session: Arc<Session>,
    code: &str,
) -> Result<(KeyStatsFromStockbitDb, DateTime<Utc>), String> {
    let payload = fetch_keystats_ratio(code).await?;
    let updated_at = Utc::now();
    crate::repository::update_keystats_from_stockbit(
        session.as_ref(),
        code,
        &payload,
        updated_at,
    )
    .await?;
    Ok((payload, updated_at))
}
