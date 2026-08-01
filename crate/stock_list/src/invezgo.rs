use std::sync::Arc;

use crate::model::{BalanceStatement, Keystats, ShareHolder5, ShareHolder5Entry};
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
