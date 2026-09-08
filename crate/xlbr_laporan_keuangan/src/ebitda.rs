//! Konstruksi EBIT/EBITDA dari komponen inline XBRL + perhitungan TTM.

use crate::download;
use crate::model::EbitdaComponents;
use crate::parser::{parse_zip_bytes, ZipHtmlCorpus, CONTEXT_PRIOR_YTD, CONTEXT_YTD};

const CONCEPT_PROFIT_LOSS: &str = "idx-cor:ProfitLoss";
const CONCEPT_TAX: &[&str] = &[
    "idx-cor:IncomeTaxExpenseBenefit",
    "idx-cor:TaxBenefitExpenses",
    "idx-cor:IncomeTaxExpense",
];
const CONCEPT_FIN_COST: &[&str] = &[
    "idx-cor:FinanceCosts",
    "idx-cor:InterestAndFinanceCosts",
];
const CONCEPT_FIN_INCOME: &[&str] = &["idx-cor:FinanceIncome"];
const CONCEPT_DNA: &[&str] = &[
    "idx-cor:AdjustmentsForDepreciationAndAmortisationExpense",
    "idx-cor:DepreciationAndAmortisation",
];

/// Ekstrak komponen EBIT/EBITDA untuk satu context duration (`CurrentYearDuration` / `PriorYearDuration`).
pub fn extract_ebitda_components(
    is_htmls: &[&str],
    cf_htmls: &[&str],
    all_htmls: &[&str],
    context: &str,
) -> Option<EbitdaComponents> {
    let profit_loss =
        crate::parser::extract_signed_optional(is_htmls, &[CONCEPT_PROFIT_LOSS], context)?;
    Some(EbitdaComponents {
        profit_loss,
        tax: crate::parser::extract_signed_optional(is_htmls, CONCEPT_TAX, context).unwrap_or(0.0),
        finance_cost: crate::parser::extract_signed_optional(is_htmls, CONCEPT_FIN_COST, context)
            .unwrap_or(0.0),
        finance_income: crate::parser::extract_signed_optional(is_htmls, CONCEPT_FIN_INCOME, context)
            .unwrap_or(0.0),
        depreciation_amortisation: extract_dna(cf_htmls, all_htmls, context),
    })
}

fn extract_dna(cf_htmls: &[&str], all_htmls: &[&str], context: &str) -> f64 {
    if let Some(v) = crate::parser::extract_signed_optional(cf_htmls, CONCEPT_DNA, context) {
        return v;
    }
    crate::parser::extract_signed_optional(all_htmls, CONCEPT_DNA, context).unwrap_or(0.0)
}

pub(crate) fn extract_from_corpus(corpus: &ZipHtmlCorpus) -> (EbitdaComponents, Option<EbitdaComponents>) {
    let is_order = corpus.search_order(crate::parser::PREFERRED_IS);
    let cf_order = corpus.search_order(crate::parser::PREFERRED_CF);
    let all_order = corpus.all_htmls();
    let current = extract_ebitda_components(&is_order, &cf_order, &all_order, CONTEXT_YTD)
        .unwrap_or_default();
    let prior =
        extract_ebitda_components(&is_order, &cf_order, &all_order, CONTEXT_PRIOR_YTD);
    (current, prior)
}

/// TTM posisi kuartal: FY(t−1) − YTD(t−1, Qn) + YTD(t, Qn). Q4: YTD = FY tahun berjalan.
pub fn compute_ebitda_ttm(
    quarter: &str,
    current_ytd: f64,
    prior_ytd: f64,
    prior_fy: f64,
) -> Option<f64> {
    if current_ytd == 0.0 {
        return None;
    }
    if quarter.eq_ignore_ascii_case("Q4") {
        return Some(current_ytd);
    }
    if prior_fy == 0.0 || prior_ytd == 0.0 {
        return None;
    }
    Some(prior_fy - prior_ytd + current_ytd)
}

fn zip_bytes_sync(code: &str, fiscal_year: i32, quarter: &str) -> Option<Vec<u8>> {
    let path = download::emiten_zip_path(code, fiscal_year, quarter);
    std::fs::read(path).ok()
}

fn ebitda_ytd_from_zip(bytes: &[u8]) -> Option<f64> {
    let parsed = parse_zip_bytes(bytes).ok()?;
    let v = parsed.ebitda_current.ebitda();
    if v == 0.0 { None } else { Some(v) }
}

/// Resolve EBITDA TTM saat upload: pakai `PriorYearDuration` di zip yang sama bila ada,
/// else baca zip kuartal/tahun sebelumnya dari `src/downloaded_xbrl/{CODE}/`.
pub fn resolve_ebitda_ttm(parsed: &crate::model::ParsedXlbrZip) -> f64 {
    let code = &parsed.meta.code;
    let year = parsed.meta.fiscal_year;
    let quarter = parsed.meta.quarter.as_str();
    let current = parsed.ebitda_current.ebitda();

    if quarter.eq_ignore_ascii_case("Q4") {
        return current;
    }

    let prior_ytd = parsed
        .ebitda_prior
        .as_ref()
        .map(EbitdaComponents::ebitda)
        .or_else(|| {
            zip_bytes_sync(code, year - 1, quarter).and_then(|b| ebitda_ytd_from_zip(&b))
        })
        .unwrap_or(0.0);

    let prior_fy = zip_bytes_sync(code, year - 1, "Q4")
        .and_then(|b| ebitda_ytd_from_zip(&b))
        .unwrap_or(0.0);

    compute_ebitda_ttm(quarter, current, prior_ytd, prior_fy).unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::fs;

    #[test]
    fn untr_q1_ebit_components() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/downloaded_xbrl/UNTR/inlineXBRL-UNTR-2025-Q1.zip"
        );
        if !Path::new(path).exists() {
            return;
        }
        let bytes = fs::read(path).expect("UNTR zip");
        let parsed = parse_zip_bytes(&bytes).expect("parse");
        let ebit = parsed.ebitda_current.ebit();
        // pl + tax + fin_cost - fin_inc = 3297724 + 1175839 + 640720 - 307963
        assert!((ebit - 4_806_320.0).abs() < 1.0);
    }

    #[test]
    fn untr_q1_ttm_when_prior_zips_exist() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/downloaded_xbrl/UNTR/inlineXBRL-UNTR-2025-Q1.zip"
        );
        if !Path::new(path).exists() {
            return;
        }
        let bytes = fs::read(path).expect("UNTR zip");
        let parsed = parse_zip_bytes(&bytes).expect("parse");
        let ttm = resolve_ebitda_ttm(&parsed);
        assert!(ttm > 0.0);
    }
}
