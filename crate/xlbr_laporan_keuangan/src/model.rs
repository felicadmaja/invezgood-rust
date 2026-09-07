//! Model baris tabel `invezgood.xlbr_laporan_keuangan`.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use scylla::DeserializeRow;
use scylla::SerializeRow;

pub const KEYSPACE: &str = "invezgood";
pub const TABLE: &str = "xlbr_laporan_keuangan";

pub const QUARTERS: [&str; 4] = ["Q1", "Q2", "Q3", "Q4"];

/// Normalisasi label kuartal ke `Q1`..`Q4` (legacy `TW1`..`TW4` tetap diterima saat baca).
pub fn normalize_quarter_label(raw: &str) -> String {
    let upper = raw.trim().to_ascii_uppercase();
    if let Some(suffix) = upper.strip_prefix("TW") {
        return format!("Q{suffix}");
    }
    upper
}

pub fn quarter_index(quarter: &str) -> Option<usize> {
    let q = normalize_quarter_label(quarter);
    QUARTERS.iter().position(|label| *label == q)
}

/// Metrik YTD mentah hasil parse ZIP (sebelum dekumulasi).
#[derive(Debug, Clone, Copy, Default)]
pub struct YtdMetrics {
    pub cash_from_operation: f64,
    pub cash_from_investment: f64,
    pub cash_from_financing: f64,
    pub capital_expenditure: f64,
    pub net_income: f64,
    pub interest_paid: f64,
    pub tax_paid: f64,
}

impl YtdMetrics {
    pub fn deaccumulate(&self, prior_standalone_sum: &YtdMetrics) -> StandaloneMetrics {
        StandaloneMetrics {
            cash_from_operation: self.cash_from_operation - prior_standalone_sum.cash_from_operation,
            cash_from_investment: self.cash_from_investment - prior_standalone_sum.cash_from_investment,
            cash_from_financing: self.cash_from_financing - prior_standalone_sum.cash_from_financing,
            capital_expenditure: self.capital_expenditure - prior_standalone_sum.capital_expenditure,
            net_income: self.net_income - prior_standalone_sum.net_income,
            interest_paid: self.interest_paid - prior_standalone_sum.interest_paid,
            tax_paid: self.tax_paid - prior_standalone_sum.tax_paid,
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
        self.interest_paid += rhs.interest_paid;
        self.tax_paid += rhs.tax_paid;
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
    pub interest_paid: f64,
    pub tax_paid: f64,
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

/// Snapshot utang berbunga dari laporan posisi keuangan (`CurrentYearInstant`).
#[derive(Debug, Clone, Copy, Default)]
pub struct BalanceSheetDebtMetrics {
    pub st_bank_loans: f64,
    pub current_maturities: f64,
    pub lt_loans: f64,
    pub bonds: f64,
    pub sukuk: f64,
    pub lease_liabilities: f64,
    pub hutang_berbunga: f64,
}

impl BalanceSheetDebtMetrics {
    pub fn compute_total(&mut self) {
        self.hutang_berbunga = self.st_bank_loans
            + self.current_maturities
            + self.lt_loans
            + self.bonds
            + self.sukuk
            + self.lease_liabilities;
    }
}

/// Hasil parse penuh dari ZIP inline XBRL.
#[derive(Debug, Clone)]
pub struct ParsedXlbrZip {
    pub meta: ParsedReportMeta,
    pub ytd: YtdMetrics,
    pub debt: BalanceSheetDebtMetrics,
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
    #[scylla(default_when_null)]
    pub interest_paid: f64,
    #[scylla(default_when_null)]
    pub tax_paid: f64,
    #[scylla(default_when_null)]
    pub st_bank_loans: f64,
    #[scylla(default_when_null)]
    pub current_maturities: f64,
    #[scylla(default_when_null)]
    pub lt_loans: f64,
    #[scylla(default_when_null)]
    pub bonds: f64,
    #[scylla(default_when_null)]
    pub sukuk: f64,
    #[scylla(default_when_null)]
    pub lease_liabilities: f64,
    #[scylla(default_when_null)]
    pub hutang_berbunga: f64,
    pub uploaded_at: DateTime<Utc>,
    pub source_zip_hash: String,
    #[scylla(default_when_null)]
    pub catatan: Option<HashMap<String, String>>,
}

pub fn required_prior_quarters(quarter: &str) -> Result<&'static [&'static str], String> {
    let idx = quarter_index(quarter).ok_or_else(|| format!("quarter tidak valid: {quarter}"))?;
    Ok(&QUARTERS[..idx])
}
