//! Model baris tabel `invezgood.xlbr_laporan_keuangan`.

use chrono::{DateTime, Utc};
use scylla::DeserializeRow;
use scylla::SerializeRow;

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "xlbr_laporan_keuangan";

pub const QUARTERS: [&str; 4] = ["TW1", "TW2", "TW3", "TW4"];

/// Metrik YTD mentah hasil parse ZIP (sebelum dekumulasi).
#[derive(Debug, Clone, Copy, Default)]
pub struct YtdMetrics {
    pub cash_from_operation: f64,
    pub cash_from_investment: f64,
    pub cash_from_financing: f64,
    pub capital_expenditure: f64,
    pub net_income: f64,
}

impl YtdMetrics {
    pub fn deaccumulate(&self, prior_standalone_sum: &YtdMetrics) -> StandaloneMetrics {
        StandaloneMetrics {
            cash_from_operation: self.cash_from_operation - prior_standalone_sum.cash_from_operation,
            cash_from_investment: self.cash_from_investment - prior_standalone_sum.cash_from_investment,
            cash_from_financing: self.cash_from_financing - prior_standalone_sum.cash_from_financing,
            capital_expenditure: self.capital_expenditure - prior_standalone_sum.capital_expenditure,
            net_income: self.net_income - prior_standalone_sum.net_income,
        }
    }
}

impl std::ops::AddAssign for YtdMetrics {
    fn add_assign(&mut self, rhs: Self) {
        self.cash_from_operation += rhs.cash_from_operation;
        self.cash_from_investment += rhs.cash_from_investment;
        self.cash_from_financing += rhs.cash_from_financing;
        self.capital_expenditure += rhs.capital_expenditure;
        self.net_income += rhs.net_income;
    }
}

/// Metrik standalone per kuartal (disimpan ke DB).
#[derive(Debug, Clone, Copy, Default)]
pub struct StandaloneMetrics {
    pub cash_from_operation: f64,
    pub cash_from_investment: f64,
    pub cash_from_financing: f64,
    pub capital_expenditure: f64,
    pub net_income: f64,
}

impl StandaloneMetrics {
    pub fn free_cash_flow(&self) -> f64 {
        self.cash_from_operation + self.capital_expenditure
    }
}

/// Metadata laporan hasil parse `1000000.html`.
#[derive(Debug, Clone)]
pub struct ParsedReportMeta {
    pub code: String,
    pub fiscal_year: i32,
    pub quarter: String,
    pub period_end: DateTime<Utc>,
    pub presentation_currency: String,
    pub unit_scale: i32,
}

/// Hasil parse penuh dari ZIP inline XBRL.
#[derive(Debug, Clone)]
pub struct ParsedXlbrZip {
    pub meta: ParsedReportMeta,
    pub ytd: YtdMetrics,
    pub source_zip_hash: String,
}

#[derive(Debug, Clone, DeserializeRow, SerializeRow)]
pub struct XlbrLaporanKeuanganRow {
    pub code: String,
    pub fiscal_year: i32,
    pub quarter: String,
    pub period_end: DateTime<Utc>,
    pub presentation_currency: String,
    pub unit_scale: i32,
    pub cash_from_operation: f64,
    pub cash_from_investment: f64,
    pub cash_from_financing: f64,
    pub capital_expenditure: f64,
    pub free_cash_flow: f64,
    pub net_income: f64,
    pub uploaded_at: DateTime<Utc>,
    pub source_zip_hash: String,
}

pub fn quarter_index(quarter: &str) -> Option<usize> {
    QUARTERS.iter().position(|q| q.eq_ignore_ascii_case(quarter))
}

pub fn required_prior_quarters(quarter: &str) -> Result<&'static [&'static str], String> {
    let idx = quarter_index(quarter).ok_or_else(|| format!("quarter tidak valid: {quarter}"))?;
    Ok(&QUARTERS[..idx])
}
