use chrono::Utc;
use futures::TryStreamExt;
use scylla::client::session::Session;
use std::collections::HashMap;

use crate::model::{
    normalize_quarter_label, quarter_index, required_prior_quarters, StandaloneMetrics,
    XlbrLaporanKeuanganRow, YtdMetrics, KEYSPACE, TABLE,
};

const SELECT_PRIOR_FOR_YEAR: &str =
    "SELECT code, fiscal_year, quarter, period_end, presentation_currency, unit_scale, \
    cash_from_operation, cash_from_investment, cash_from_financing, capital_expenditure, \
    free_cash_flow, net_income, interest_paid, tax_paid, uploaded_at, source_zip_hash, catatan \
    FROM invezgood.xlbr_laporan_keuangan WHERE code = ? AND fiscal_year = ?";

const SELECT_CHART: &str =
    "SELECT code, fiscal_year, quarter, period_end, presentation_currency, unit_scale, \
    cash_from_operation, cash_from_investment, cash_from_financing, capital_expenditure, \
    free_cash_flow, net_income, interest_paid, tax_paid, uploaded_at, source_zip_hash, catatan \
    FROM invezgood.xlbr_laporan_keuangan WHERE code = ?";

const UPSERT: &str =
    "INSERT INTO invezgood.xlbr_laporan_keuangan (code, fiscal_year, quarter, period_end, \
    presentation_currency, unit_scale, cash_from_operation, cash_from_investment, \
    cash_from_financing, capital_expenditure, free_cash_flow, net_income, interest_paid, \
    tax_paid, uploaded_at, source_zip_hash, catatan) \
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

pub async fn list_for_year(
    session: &Session,
    code: &str,
    fiscal_year: i32,
) -> Result<Vec<XlbrLaporanKeuanganRow>, String> {
    let mut stream = session
        .query_iter(SELECT_PRIOR_FOR_YEAR, (code, fiscal_year))
        .await
        .map_err(|e| format!("select {KEYSPACE}.{TABLE} year: {e}"))?
        .rows_stream::<XlbrLaporanKeuanganRow>()
        .map_err(|e| format!("stream {KEYSPACE}.{TABLE} year: {e}"))?;

    let mut rows = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(|e| format!("row year: {e}"))? {
        rows.push(row);
    }
    Ok(rows)
}

pub async fn list_chart(
    session: &Session,
    code: &str,
    limit: i32,
) -> Result<Vec<XlbrLaporanKeuanganRow>, String> {
    let mut stream = session
        .query_iter(SELECT_CHART, (code,))
        .await
        .map_err(|e| format!("select chart {KEYSPACE}.{TABLE}: {e}"))?
        .rows_stream::<XlbrLaporanKeuanganRow>()
        .map_err(|e| format!("stream chart: {e}"))?;

    let mut rows = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(|e| format!("row chart: {e}"))? {
        rows.push(row);
    }

    rows.sort_by(|a, b| {
        b.fiscal_year
            .cmp(&a.fiscal_year)
            .then_with(|| quarter_ord(&b.quarter).cmp(&quarter_ord(&a.quarter)))
    });
    rows.truncate(limit as usize);
    Ok(rows)
}

pub async fn list_by_code(
    session: &Session,
    code: &str,
) -> Result<Vec<XlbrLaporanKeuanganRow>, String> {
    let mut stream = session
        .query_iter(SELECT_CHART, (code,))
        .await
        .map_err(|e| format!("select {KEYSPACE}.{TABLE} code={code}: {e}"))?
        .rows_stream::<XlbrLaporanKeuanganRow>()
        .map_err(|e| format!("stream {KEYSPACE}.{TABLE} code={code}: {e}"))?;

    let mut rows = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(|e| format!("row code={code}: {e}"))? {
        rows.push(row);
    }
    Ok(rows)
}

pub async fn get_catatan_by_code(
    session: &Session,
    code: &str,
) -> Result<HashMap<String, String>, String> {
    let rows = list_by_code(session, code).await?;
    let Some(latest) = latest_row(&rows) else {
        return Err(format!("code {code} tidak ditemukan"));
    };
    Ok(latest.catatan.clone().unwrap_or_default())
}

pub async fn upsert_catatan_by_code(
    session: &Session,
    code: &str,
    catatan: HashMap<String, String>,
) -> Result<usize, String> {
    let rows = list_by_code(session, code).await?;
    if rows.is_empty() {
        return Err(format!("code {code} tidak ditemukan"));
    }

    let catatan = Some(catatan);
    let mut updated = 0usize;
    for mut row in rows {
        row.catatan = catatan.clone();
        upsert(session, &row).await?;
        updated += 1;
    }
    Ok(updated)
}

pub async fn upsert(
    session: &Session,
    row: &XlbrLaporanKeuanganRow,
) -> Result<(), String> {
    session
        .query_unpaged(UPSERT, row)
        .await
        .map_err(|e| format!("upsert {KEYSPACE}.{TABLE}: {e}"))?;
    Ok(())
}

pub fn standalone_sum_prior_to(rows: &[XlbrLaporanKeuanganRow], quarter: &str) -> YtdMetrics {
    let Ok(prior) = required_prior_quarters(quarter) else {
        return YtdMetrics::default();
    };
    let mut sum = YtdMetrics::default();
    for row in rows {
        if prior.iter().any(|q| {
            q.eq_ignore_ascii_case(&normalize_quarter_label(&row.quarter))
        }) {
            sum += YtdMetrics {
                cash_from_operation: row.cash_from_operation,
                cash_from_investment: row.cash_from_investment,
                cash_from_financing: row.cash_from_financing,
                capital_expenditure: row.capital_expenditure,
                net_income: row.net_income,
                interest_paid: row.interest_paid,
                tax_paid: row.tax_paid,
            };
        }
    }
    sum
}

pub fn row_from_standalone(
    code: &str,
    fiscal_year: i32,
    quarter: &str,
    period_end: chrono::DateTime<Utc>,
    presentation_currency: &str,
    unit_scale: i32,
    metrics: StandaloneMetrics,
    source_zip_hash: &str,
) -> XlbrLaporanKeuanganRow {
    XlbrLaporanKeuanganRow {
        code: code.to_string(),
        fiscal_year,
        quarter: quarter.to_string(),
        period_end,
        presentation_currency: presentation_currency.to_string(),
        unit_scale,
        cash_from_operation: metrics.cash_from_operation,
        cash_from_investment: metrics.cash_from_investment,
        cash_from_financing: metrics.cash_from_financing,
        capital_expenditure: metrics.capital_expenditure,
        free_cash_flow: metrics.free_cash_flow(),
        net_income: metrics.net_income,
        interest_paid: metrics.interest_paid,
        tax_paid: metrics.tax_paid,
        uploaded_at: Utc::now(),
        source_zip_hash: source_zip_hash.to_string(),
        catatan: None,
    }
}

fn quarter_ord(q: &str) -> i32 {
    quarter_index(q).map(|i| (i + 1) as i32).unwrap_or(0)
}

fn latest_row(rows: &[XlbrLaporanKeuanganRow]) -> Option<&XlbrLaporanKeuanganRow> {
    rows.iter().max_by(|a, b| {
        a.fiscal_year
            .cmp(&b.fiscal_year)
            .then_with(|| quarter_ord(&a.quarter).cmp(&quarter_ord(&b.quarter)))
    })
}
