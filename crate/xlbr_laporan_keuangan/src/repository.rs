use chrono::Utc;
use futures::TryStreamExt;
use scylla::client::session::Session;

use crate::model::{
    required_prior_quarters, StandaloneMetrics, XlbrLaporanKeuanganRow, YtdMetrics, KEYSPACE,
    TABLE,
};

const SELECT_PRIOR_FOR_YEAR: &str =
    "SELECT code, fiscal_year, quarter, period_end, presentation_currency, unit_scale, \
    cash_from_operation, cash_from_investment, cash_from_financing, capital_expenditure, \
    free_cash_flow, net_income, uploaded_at, source_zip_hash \
    FROM invezgood.xlbr_laporan_keuangan WHERE code = ? AND fiscal_year = ?";

const SELECT_CHART: &str =
    "SELECT code, fiscal_year, quarter, period_end, presentation_currency, unit_scale, \
    cash_from_operation, cash_from_investment, cash_from_financing, capital_expenditure, \
    free_cash_flow, net_income, uploaded_at, source_zip_hash \
    FROM invezgood.xlbr_laporan_keuangan WHERE code = ?";

const UPSERT: &str =
    "INSERT INTO invezgood.xlbr_laporan_keuangan (code, fiscal_year, quarter, period_end, \
    presentation_currency, unit_scale, cash_from_operation, cash_from_investment, \
    cash_from_financing, capital_expenditure, free_cash_flow, net_income, uploaded_at, \
    source_zip_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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

pub async fn upsert(
    session: &Session,
    row: &XlbrLaporanKeuanganRow,
) -> Result<(), String> {
    session
        .query_unpaged(
            UPSERT,
            (
                &row.code,
                row.fiscal_year,
                &row.quarter,
                row.period_end,
                &row.presentation_currency,
                row.unit_scale,
                row.cash_from_operation,
                row.cash_from_investment,
                row.cash_from_financing,
                row.capital_expenditure,
                row.free_cash_flow,
                row.net_income,
                row.uploaded_at,
                &row.source_zip_hash,
            ),
        )
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
        if prior
            .iter()
            .any(|q| q.eq_ignore_ascii_case(&row.quarter))
        {
            sum += YtdMetrics {
                cash_from_operation: row.cash_from_operation,
                cash_from_investment: row.cash_from_investment,
                cash_from_financing: row.cash_from_financing,
                capital_expenditure: row.capital_expenditure,
                net_income: row.net_income,
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
        uploaded_at: Utc::now(),
        source_zip_hash: source_zip_hash.to_string(),
    }
}

fn quarter_ord(q: &str) -> i32 {
    match q.to_ascii_uppercase().as_str() {
        "TW1" => 1,
        "TW2" => 2,
        "TW3" => 3,
        "TW4" => 4,
        _ => 0,
    }
}
