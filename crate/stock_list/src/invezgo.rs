use std::collections::HashMap;
use std::sync::Arc;

use crate::model::{
    BalanceStatement, CompanyInformation, CompanyPersonEntry, CompanySubsidiaryEntry, CorporateAction,
    CorporateActionEntry, Keystats, NotationEntryDb, ShareHolder1, ShareHolder1Entry, ShareHolder5,
    ShareHolder5Entry, ShareHolderComposition, ShareHolderCompositionEntry,
};
use scylla::client::session::Session;
use serde::Deserialize;

const INVEZGO_STOCK_LIST_URL: &str = "https://api.invezgo.com/analysis/list/stock";
const INVEZGO_NOTATION_URL: &str = "https://api.invezgo.com/analysis/notation";

fn keystat_url(code: &str) -> String {
    format!("https://api.invezgo.com/analysis/keystat/{code}?type=Q")
}

fn financial_statement_url(code: &str, statement: &str) -> String {
    format!(
        "https://api.invezgo.com/analysis/financial-statement/{code}?statement={statement}&type=Q&limit=8"
    )
}

fn shareholder_detail_url(code: &str) -> String {
    format!("https://api.invezgo.com/analysis/shareholder-detail/{code}")
}

fn shareholder_detail_one_url(code: &str) -> String {
    format!("https://api.invezgo.com/analysis/shareholder-detail-one?code={code}")
}

fn shareholder_composition_url(code: &str) -> String {
    format!("https://api.invezgo.com/analysis/shareholder/{code}")
}

fn company_information_url(code: &str) -> String {
    format!("https://api.invezgo.com/analysis/information/{code}")
}

fn corporate_action_url(code: &str, page: i32) -> String {
    format!("https://api.invezgo.com/analysis/calendar?code={code}&page={page}")
}

async fn invezgo_get(url: &str) -> Result<String, String> {
    invezgo_http::get(url).await
}

#[derive(Debug, Deserialize)]
struct InvezgoStockItem {
    code: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    sector: Option<String>,
    #[serde(default)]
    logo: Option<String>,
}

#[derive(Debug, Deserialize)]
struct InvezgoStockListWrapped {
    data: Vec<InvezgoStockItem>,
}

#[derive(Debug, Deserialize)]
struct InvezgoNotationItem {
    code: String,
    #[serde(default)]
    list: Vec<InvezgoNotationEntry>,
}

#[derive(Debug, Deserialize)]
struct InvezgoNotationEntry {
    notation: String,
    description: String,
}

#[derive(Debug, Deserialize)]
struct InvezgoKeystatsResponse {
    rows: Vec<InvezgoKeystatsRow>,
    columns: Vec<InvezgoKeystatsColumn>,
}

#[derive(Debug, Deserialize)]
struct InvezgoKeystatsRow {
    id: String,
    name: String,
    #[serde(default)]
    values: Vec<InvezgoKeystatsValue>,
}

#[derive(Debug, Deserialize)]
struct InvezgoKeystatsValue {
    col: String,
    year: i32,
    amount: f64,
    period: String,
}

#[derive(Debug, Deserialize)]
struct InvezgoKeystatsColumn {
    year: i32,
    label: String,
    period: String,
}

#[derive(Debug, Deserialize)]
struct InvezgoFinancialStatementRow {
    id: String,
    name: String,
    level: i32,
    #[serde(default)]
    values: Vec<InvezgoKeystatsValue>,
    #[serde(default)]
    parent_id: Option<String>,
    #[serde(default)]
    is_abstract: bool,
    #[serde(default)]
    display_order: i32,
}

#[derive(Debug, Deserialize)]
struct InvezgoFinancialStatementResponse {
    rows: Vec<InvezgoFinancialStatementRow>,
    columns: Vec<InvezgoKeystatsColumn>,
}

#[derive(Debug, Deserialize)]
struct InvezgoShareHolderEntry {
    name: String,
    date: String,
    val: String,
    percent: f64,
}

#[derive(Debug, Deserialize)]
struct InvezgoShareHolder1Entry {
    #[serde(default)]
    name: Option<String>,
    #[serde(rename = "type", default)]
    holder_type: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    nationality: Option<String>,
    #[serde(default)]
    domicile: Option<String>,
    #[serde(default)]
    scripless: Option<String>,
    #[serde(default)]
    scrip: Option<String>,
    #[serde(default)]
    total: Option<String>,
    #[serde(default)]
    percentage: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct InvezgoShareHolderCompositionEntry {
    name: String,
    percentage: f64,
    #[serde(default)]
    badge: String,
}

#[derive(Debug, Deserialize)]
struct InvezgoCompanyPersonEntry {
    name: String,
    position: String,
}

#[derive(Debug, Deserialize)]
struct InvezgoCompanySubsidiaryEntry {
    name: String,
    percentage: f64,
}

#[derive(Debug, Deserialize)]
struct InvezgoCompanyInformationResponse {
    #[serde(default)]
    address: Option<String>,
    #[serde(default)]
    industry: Option<String>,
    #[serde(default)]
    subsindustry: Option<String>,
    #[serde(default)]
    activity: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    npwp: Option<String>,
    #[serde(default)]
    board: Option<String>,
    #[serde(default)]
    sector: Option<String>,
    #[serde(default)]
    subsector: Option<String>,
    #[serde(default)]
    listing_date: Option<String>,
    #[serde(default)]
    website: Option<String>,
    #[serde(default)]
    logo: Option<String>,
    #[serde(default)]
    additional_info: Option<String>,
    #[serde(default)]
    people: Option<String>,
    #[serde(default)]
    report_type: Option<String>,
    #[serde(default)]
    administration: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    ipo_pct: Option<f64>,
    #[serde(default)]
    ipo_price: Option<f64>,
    #[serde(default)]
    ipo_share: Option<String>,
    #[serde(default)]
    ipo_underwriter: Option<String>,
    #[serde(default)]
    nominal_price: Option<f64>,
    #[serde(default)]
    category: Vec<String>,
    #[serde(default)]
    active: bool,
    #[serde(default)]
    commissioner: Vec<InvezgoCompanyPersonEntry>,
    #[serde(default)]
    director: Vec<InvezgoCompanyPersonEntry>,
    #[serde(default)]
    subsidiary: Vec<InvezgoCompanySubsidiaryEntry>,
}

#[derive(Debug, Deserialize)]
struct InvezgoCorporateActionResponse {
    #[serde(rename = "totalPage")]
    total_page: i32,
    page: i32,
    #[serde(default, rename = "nextPage")]
    next_page: Option<i32>,
    #[serde(default)]
    data: Vec<InvezgoCorporateActionEntry>,
}

#[derive(Debug, Deserialize)]
struct InvezgoCorporateActionEntry {
    code: String,
    #[serde(rename = "type")]
    action_type: String,
    payload: serde_json::Value,
}

pub async fn fetch_balance_statement(code: &str) -> Result<BalanceStatement, String> {
    fetch_financial_statement(code, "BS").await
}

pub async fn fetch_income_statement(code: &str) -> Result<BalanceStatement, String> {
    fetch_financial_statement(code, "IS").await
}

pub async fn fetch_cash_flow(code: &str) -> Result<BalanceStatement, String> {
    fetch_financial_statement(code, "CF").await
}

async fn fetch_financial_statement(code: &str, statement: &str) -> Result<BalanceStatement, String> {
    let body = invezgo_get(&financial_statement_url(code, statement)).await?;
    parse_financial_statement(&body)
}

pub async fn fetch_and_save_balance_statement(
    session: Arc<Session>,
    code: &str,
) -> Result<(BalanceStatement, chrono::DateTime<chrono::Utc>), String> {
    let statement = fetch_balance_statement(code).await?;
    let updated_at = chrono::Utc::now();
    let db = crate::model::StockListBalanceStatementDb::from(statement.clone());
    crate::repository::update_balance_statement(session.as_ref(), code, db, updated_at).await?;
    Ok((statement, updated_at))
}

pub async fn fetch_and_save_income_statement(
    session: Arc<Session>,
    code: &str,
) -> Result<(BalanceStatement, chrono::DateTime<chrono::Utc>), String> {
    let statement = fetch_income_statement(code).await?;
    let updated_at = chrono::Utc::now();
    let db = crate::model::StockListIncomeStatementDb::from(statement.clone());
    crate::repository::update_income_statement(session.as_ref(), code, db, updated_at).await?;
    Ok((statement, updated_at))
}

pub async fn fetch_and_save_cash_flow(
    session: Arc<Session>,
    code: &str,
) -> Result<(BalanceStatement, chrono::DateTime<chrono::Utc>), String> {
    let statement = fetch_cash_flow(code).await?;
    let updated_at = chrono::Utc::now();
    let db = crate::model::StockListCashFlowDb::from(statement.clone());
    crate::repository::update_cash_flow(session.as_ref(), code, db, updated_at).await?;
    Ok((statement, updated_at))
}

pub async fn fetch_share_holder_5(code: &str) -> Result<ShareHolder5, String> {
    let body = invezgo_get(&shareholder_detail_url(code)).await?;
    parse_share_holder_5(&body)
}

pub async fn fetch_and_save_share_holder_5(
    session: Arc<Session>,
    code: &str,
) -> Result<(ShareHolder5, chrono::DateTime<chrono::Utc>), String> {
    let entries = fetch_share_holder_5(code).await?;
    let updated_at = chrono::Utc::now();
    let db = crate::model::ShareHolder5Db::from(entries.clone());
    crate::repository::update_share_holder_5(session.as_ref(), code, db, updated_at).await?;
    Ok((entries, updated_at))
}

fn parse_share_holder_5(body: &str) -> Result<ShareHolder5, String> {
    let parsed: Vec<InvezgoShareHolderEntry> = serde_json::from_str(body)
        .map_err(|e| format!("parse JSON Invezgo shareholder-detail gagal: {e}"))?;

    let items = parsed
        .into_iter()
        .map(|entry| {
            let date = chrono::DateTime::parse_from_rfc3339(&entry.date)
                .map_err(|e| {
                    format!(
                        "parse date shareholder-detail name={} date={}: {e}",
                        entry.name, entry.date
                    )
                })?
                .with_timezone(&chrono::Utc);

            Ok(ShareHolder5Entry {
                name: entry.name,
                date,
                val: entry.val,
                percent: entry.percent,
            })
        })
        .collect::<Result<Vec<ShareHolder5Entry>, String>>()?;

    Ok(ShareHolder5 { items })
}

pub async fn fetch_share_holder_1(code: &str) -> Result<ShareHolder1, String> {
    let body = invezgo_get(&shareholder_detail_one_url(code)).await?;
    parse_share_holder_1(&body)
}

pub async fn fetch_and_save_share_holder_1(
    session: Arc<Session>,
    code: &str,
) -> Result<(ShareHolder1, chrono::DateTime<chrono::Utc>), String> {
    let entries = fetch_share_holder_1(code).await?;
    let updated_at = chrono::Utc::now();
    let db = crate::model::ShareHolder1Db::from(entries.clone());
    crate::repository::update_share_holder_1(session.as_ref(), code, db, updated_at).await?;
    Ok((entries, updated_at))
}

fn parse_share_holder_1(body: &str) -> Result<ShareHolder1, String> {
    let parsed: Vec<InvezgoShareHolder1Entry> = serde_json::from_str(body)
        .map_err(|e| format!("parse JSON Invezgo shareholder-detail-one gagal: {e}"))?;

    let items = parsed
        .into_iter()
        .map(|entry| ShareHolder1Entry {
            name: entry.name.unwrap_or_default(),
            holder_type: entry.holder_type.unwrap_or_default(),
            status: entry.status.unwrap_or_default(),
            nationality: entry.nationality.unwrap_or_default(),
            domicile: entry.domicile.unwrap_or_default(),
            scripless: entry.scripless.unwrap_or_default(),
            scrip: entry.scrip.unwrap_or_default(),
            total: entry.total.unwrap_or_default(),
            percentage: entry.percentage.unwrap_or(0.0),
        })
        .collect();

    Ok(ShareHolder1 { items })
}

pub async fn fetch_share_holder_composition(code: &str) -> Result<ShareHolderComposition, String> {
    let body = invezgo_get(&shareholder_composition_url(code)).await?;
    parse_share_holder_composition(&body)
}

pub async fn fetch_and_save_share_holder_composition(
    session: Arc<Session>,
    code: &str,
) -> Result<(ShareHolderComposition, chrono::DateTime<chrono::Utc>), String> {
    let entries = fetch_share_holder_composition(code).await?;
    let updated_at = chrono::Utc::now();
    let db = crate::model::ShareHolderCompositionDb::from(entries.clone());
    crate::repository::update_share_holder_composition(session.as_ref(), code, db, updated_at)
        .await?;
    Ok((entries, updated_at))
}

fn parse_share_holder_composition(body: &str) -> Result<ShareHolderComposition, String> {
    let parsed: Vec<InvezgoShareHolderCompositionEntry> = serde_json::from_str(body)
        .map_err(|e| format!("parse JSON Invezgo shareholder gagal: {e}"))?;

    let items = parsed
        .into_iter()
        .map(|entry| ShareHolderCompositionEntry {
            name: entry.name,
            percentage: entry.percentage,
            badge: entry.badge,
        })
        .collect();

    Ok(ShareHolderComposition { items })
}

pub async fn fetch_company_information(code: &str) -> Result<CompanyInformation, String> {
    let body = invezgo_get(&company_information_url(code)).await?;
    parse_company_information(&body)
}

pub async fn fetch_and_save_company_information(
    session: Arc<Session>,
    code: &str,
) -> Result<(CompanyInformation, chrono::DateTime<chrono::Utc>), String> {
    let info = fetch_company_information(code).await?;
    let updated_at = chrono::Utc::now();
    let db = crate::model::CompanyInformationDb::from(info.clone());
    crate::repository::update_company_information(session.as_ref(), code, db, updated_at).await?;
    Ok((info, updated_at))
}

pub async fn fetch_corporate_action(code: &str) -> Result<CorporateAction, String> {
    let mut page = 1;
    let mut merged = CorporateAction::default();

    loop {
        let body = invezgo_get(&corporate_action_url(code, page)).await?;
        let parsed = parse_corporate_action_page(&body)?;
        if page == 1 {
            merged.total_page = parsed.total_page;
            merged.page = parsed.page;
        }
        merged.data.extend(parsed.data);

        match parsed.next_page {
            Some(next) if next > page => page = next,
            _ => {
                merged.next_page = parsed.next_page;
                break;
            }
        }
    }

    Ok(merged)
}

pub async fn fetch_and_save_corporate_action(
    session: Arc<Session>,
    code: &str,
) -> Result<(CorporateAction, chrono::DateTime<chrono::Utc>), String> {
    let action = fetch_corporate_action(code).await?;
    let updated_at = chrono::Utc::now();
    let db = crate::model::CorporateActionDb::from(action.clone());
    crate::repository::update_corporate_action(session.as_ref(), code, db, updated_at).await?;
    Ok((action, updated_at))
}

fn parse_corporate_action_page(body: &str) -> Result<CorporateAction, String> {
    let parsed: InvezgoCorporateActionResponse = serde_json::from_str(body)
        .map_err(|e| format!("parse JSON Invezgo calendar gagal: {e}"))?;

    Ok(CorporateAction {
        total_page: parsed.total_page,
        page: parsed.page,
        next_page: parsed.next_page,
        data: parsed
            .data
            .into_iter()
            .map(|entry| CorporateActionEntry {
                code: entry.code,
                action_type: entry.action_type,
                payload: json_object_to_string_map(entry.payload),
            })
            .collect(),
    })
}

fn json_object_to_string_map(value: serde_json::Value) -> HashMap<String, String> {
    let serde_json::Value::Object(map) = value else {
        return HashMap::new();
    };

    map.into_iter()
        .map(|(key, val)| (key, json_value_to_string(val)))
        .collect()
}

fn json_value_to_string(value: serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s,
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn parse_company_information(body: &str) -> Result<CompanyInformation, String> {
    let parsed: InvezgoCompanyInformationResponse = serde_json::from_str(body)
        .map_err(|e| format!("parse JSON Invezgo information gagal: {e}"))?;

    let listing_date = parsed
        .listing_date
        .as_deref()
        .map(parse_iso_timestamp)
        .transpose()?;

    Ok(CompanyInformation {
        address: parsed.address.unwrap_or_default(),
        industry: parsed.industry.unwrap_or_default(),
        subsindustry: parsed.subsindustry.unwrap_or_default(),
        activity: parsed.activity.unwrap_or_default(),
        name: parsed.name.unwrap_or_default(),
        npwp: parsed.npwp.unwrap_or_default(),
        board: parsed.board.unwrap_or_default(),
        sector: parsed.sector.unwrap_or_default(),
        subsector: parsed.subsector.unwrap_or_default(),
        listing_date,
        website: parsed.website.unwrap_or_default(),
        logo: parsed.logo.unwrap_or_default(),
        additional_info: parsed.additional_info,
        people: parsed.people,
        report_type: parsed.report_type,
        administration: parsed.administration,
        description: parsed.description,
        ipo_pct: parsed.ipo_pct,
        ipo_price: parsed.ipo_price,
        ipo_share: parsed.ipo_share,
        ipo_underwriter: parsed.ipo_underwriter,
        nominal_price: parsed.nominal_price,
        category: parsed.category,
        active: parsed.active,
        commissioner: parsed
            .commissioner
            .into_iter()
            .map(|e| CompanyPersonEntry {
                name: e.name,
                position: e.position,
            })
            .collect(),
        director: parsed
            .director
            .into_iter()
            .map(|e| CompanyPersonEntry {
                name: e.name,
                position: e.position,
            })
            .collect(),
        subsidiary: parsed
            .subsidiary
            .into_iter()
            .map(|e| CompanySubsidiaryEntry {
                name: e.name,
                percentage: e.percentage,
            })
            .collect(),
    })
}

fn parse_iso_timestamp(raw: &str) -> Result<chrono::DateTime<chrono::Utc>, String> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .map_err(|e| format!("parse timestamp {raw}: {e}"))
}

fn parse_financial_statement(body: &str) -> Result<BalanceStatement, String> {
    let parsed: InvezgoFinancialStatementResponse = serde_json::from_str(body)
        .map_err(|e| format!("parse JSON Invezgo financial-statement gagal: {e}"))?;

    Ok(BalanceStatement {
        rows: parsed
            .rows
            .into_iter()
            .map(|row| crate::model::BalanceStatementRow {
                id: row.id,
                name: row.name,
                level: row.level,
                values: row
                    .values
                    .into_iter()
                    .map(|v| crate::model::KeystatsValue {
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
            .collect(),
        columns: parsed
            .columns
            .into_iter()
            .map(|c| crate::model::KeystatsColumn {
                year: c.year,
                label: c.label,
                period: c.period,
            })
            .collect(),
    })
}

pub async fn fetch_keystats(code: &str) -> Result<Keystats, String> {
    let body = invezgo_get(&keystat_url(code)).await?;
    parse_keystats(&body)
}

pub async fn fetch_and_save_keystats(
    session: Arc<Session>,
    code: &str,
) -> Result<(Keystats, chrono::DateTime<chrono::Utc>), String> {
    let keystats = fetch_keystats(code).await?;
    let updated_at = chrono::Utc::now();
    let keystats_db = crate::model::StockListKeystatsDb::from(keystats.clone());

    crate::repository::update_keystats(session.as_ref(), code, keystats_db, updated_at).await?;

    Ok((keystats, updated_at))
}

fn parse_keystats(body: &str) -> Result<Keystats, String> {
    let parsed: InvezgoKeystatsResponse = serde_json::from_str(body)
        .map_err(|e| format!("parse JSON Invezgo keystat gagal: {e}"))?;

    Ok(Keystats {
        rows: parsed
            .rows
            .into_iter()
            .map(|row| crate::model::KeystatsRow {
                id: row.id,
                name: row.name,
                values: row
                    .values
                    .into_iter()
                    .map(|v| crate::model::KeystatsValue {
                        col: v.col,
                        year: v.year,
                        amount: v.amount,
                        period: v.period,
                    })
                    .collect(),
            })
            .collect(),
        columns: parsed
            .columns
            .into_iter()
            .map(|c| crate::model::KeystatsColumn {
                year: c.year,
                label: c.label,
                period: c.period,
            })
            .collect(),
    })
}

pub async fn fetch_and_save_notation(session: Arc<Session>) -> Result<(usize, usize), String> {
    let body = invezgo_get(INVEZGO_NOTATION_URL).await?;
    let items: Vec<InvezgoNotationItem> = serde_json::from_str(&body)
        .map_err(|e| format!("parse JSON Invezgo notation gagal: {e}"))?;

    let mut updated = 0usize;
    let mut skipped = 0usize;

    for item in items {
        let code = item.code.trim().to_ascii_uppercase();
        if code.is_empty() {
            continue;
        }

        let notation: Vec<NotationEntryDb> = item
            .list
            .into_iter()
            .map(|entry| NotationEntryDb {
                notation: entry.notation,
                description: entry.description,
            })
            .collect();
        let notation_db = if notation.is_empty() {
            None
        } else {
            Some(notation)
        };

        match crate::repository::update_notation(session.as_ref(), &code, notation_db).await {
            Ok(()) => updated += 1,
            Err(e) if e.contains("tidak ditemukan") => skipped += 1,
            Err(e) => return Err(e),
        }
    }

    Ok((updated, skipped))
}

pub async fn fetch_and_save(session: Arc<Session>) -> Result<usize, String> {
    let body = invezgo_get(INVEZGO_STOCK_LIST_URL).await?;
    let items = parse_stock_list(&body)?;
    let mut saved = 0usize;

    for item in items {
        crate::repository::upsert(
            session.as_ref(),
            &item.code,
            item.name.as_deref(),
            item.sector.as_deref(),
            item.logo.as_deref(),
        )
        .await?;
        saved += 1;
    }

    Ok(saved)
}

fn parse_stock_list(body: &str) -> Result<Vec<InvezgoStockItem>, String> {
    if let Ok(wrapped) = serde_json::from_str::<InvezgoStockListWrapped>(body) {
        return Ok(wrapped.data);
    }

    serde_json::from_str::<Vec<InvezgoStockItem>>(body)
        .map_err(|e| format!("parse JSON Invezgo gagal: {e}"))
}
