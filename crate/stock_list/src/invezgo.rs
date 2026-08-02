use std::sync::Arc;

use crate::model::{
    BalanceStatement, CompanyInformation, CompanyPersonEntry, CompanySubsidiaryEntry, Keystats,
    ShareHolder1, ShareHolder1Entry, ShareHolder5, ShareHolder5Entry, ShareHolderComposition,
    ShareHolderCompositionEntry,
};
use scylla::client::session::Session;
use serde::Deserialize;

const INVEZGO_STOCK_LIST_URL: &str = "https://api.invezgo.com/analysis/list/stock";

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
    name: String,
    #[serde(rename = "type")]
    holder_type: String,
    status: String,
    #[serde(default)]
    nationality: String,
    #[serde(default)]
    domicile: String,
    #[serde(default)]
    scripless: String,
    #[serde(default)]
    scrip: String,
    total: String,
    percentage: f64,
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
    let token = std::env::var("INVEZGO_BEARER_TOKEN")
        .map_err(|_| "INVEZGO_BEARER_TOKEN belum diset".to_string())?;

    let response = reqwest::Client::new()
        .get(financial_statement_url(code, statement))
        .header("Accept", "application/json")
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| {
            format!("request Invezgo financial-statement code={code} statement={statement} gagal: {e}")
        })?;

    let status = response.status();
    let body = response.text().await.map_err(|e| {
        format!("baca body Invezgo financial-statement code={code} statement={statement} gagal: {e}")
    })?;

    if !status.is_success() {
        return Err(format!(
            "Invezgo financial-statement HTTP {status} code={code} statement={statement}: {body}"
        ));
    }

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
    let token = std::env::var("INVEZGO_BEARER_TOKEN")
        .map_err(|_| "INVEZGO_BEARER_TOKEN belum diset".to_string())?;

    let response = reqwest::Client::new()
        .get(shareholder_detail_url(code))
        .header("Accept", "application/json")
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| format!("request Invezgo shareholder-detail code={code} gagal: {e}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| format!("baca body Invezgo shareholder-detail code={code} gagal: {e}"))?;

    if !status.is_success() {
        return Err(format!(
            "Invezgo shareholder-detail HTTP {status} code={code}: {body}"
        ));
    }

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
    let token = std::env::var("INVEZGO_BEARER_TOKEN")
        .map_err(|_| "INVEZGO_BEARER_TOKEN belum diset".to_string())?;

    let response = reqwest::Client::new()
        .get(shareholder_detail_one_url(code))
        .header("Accept", "application/json")
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| format!("request Invezgo shareholder-detail-one code={code} gagal: {e}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| format!("baca body Invezgo shareholder-detail-one code={code} gagal: {e}"))?;

    if !status.is_success() {
        return Err(format!(
            "Invezgo shareholder-detail-one HTTP {status} code={code}: {body}"
        ));
    }

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
            name: entry.name,
            holder_type: entry.holder_type,
            status: entry.status,
            nationality: entry.nationality,
            domicile: entry.domicile,
            scripless: entry.scripless,
            scrip: entry.scrip,
            total: entry.total,
            percentage: entry.percentage,
        })
        .collect();

    Ok(ShareHolder1 { items })
}

pub async fn fetch_share_holder_composition(code: &str) -> Result<ShareHolderComposition, String> {
    let token = std::env::var("INVEZGO_BEARER_TOKEN")
        .map_err(|_| "INVEZGO_BEARER_TOKEN belum diset".to_string())?;

    let response = reqwest::Client::new()
        .get(shareholder_composition_url(code))
        .header("Accept", "application/json")
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| format!("request Invezgo shareholder code={code} gagal: {e}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| format!("baca body Invezgo shareholder code={code} gagal: {e}"))?;

    if !status.is_success() {
        return Err(format!("Invezgo shareholder HTTP {status} code={code}: {body}"));
    }

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
    let token = std::env::var("INVEZGO_BEARER_TOKEN")
        .map_err(|_| "INVEZGO_BEARER_TOKEN belum diset".to_string())?;

    let response = reqwest::Client::new()
        .get(company_information_url(code))
        .header("Accept", "application/json")
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| format!("request Invezgo information code={code} gagal: {e}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| format!("baca body Invezgo information code={code} gagal: {e}"))?;

    if !status.is_success() {
        return Err(format!("Invezgo information HTTP {status} code={code}: {body}"));
    }

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
    let token = std::env::var("INVEZGO_BEARER_TOKEN")
        .map_err(|_| "INVEZGO_BEARER_TOKEN belum diset".to_string())?;

    let response = reqwest::Client::new()
        .get(keystat_url(code))
        .header("Accept", "application/json")
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| format!("request Invezgo keystat code={code} gagal: {e}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| format!("baca body Invezgo keystat code={code} gagal: {e}"))?;

    if !status.is_success() {
        return Err(format!("Invezgo keystat HTTP {status} code={code}: {body}"));
    }

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

pub async fn fetch_and_save(session: Arc<Session>) -> Result<usize, String> {
    let token = std::env::var("INVEZGO_BEARER_TOKEN")
        .map_err(|_| "INVEZGO_BEARER_TOKEN belum diset".to_string())?;

    let response = reqwest::Client::new()
        .get(INVEZGO_STOCK_LIST_URL)
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| format!("request Invezgo gagal: {e}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| format!("baca body Invezgo gagal: {e}"))?;

    if !status.is_success() {
        return Err(format!("Invezgo HTTP {status}: {body}"));
    }

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
