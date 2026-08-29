//! Parse inline XBRL ZIP IDX → metadata + metrik YTD.
//!
//! Tidak bergantung pada satu nama file HTML: baca semua `*.html` di zip, cari concept
//! XBRL `CurrentYearDuration` dengan urutan preferensi file + fallback concept.

use std::io::Read;

use chrono::{Datelike, NaiveDate, Utc};
use sha2::{Digest, Sha256};
use zip::ZipArchive;

use crate::model::{ParsedReportMeta, ParsedXlbrZip, YtdMetrics};

const FILE_DEI: &str = "1000000.html";

const PREFERRED_IS: &[&str] = &["1321000.html", "2311000.html", "2321000.html"];
const PREFERRED_CF: &[&str] = &["1510000.html", "2510000.html"];

const LABEL_PERIOD_SUBMISSION: &str = "Periode penyampaian laporan keuangan";

const CONCEPT_CFO: &str = "idx-cor:NetCashFlowsReceivedFromUsedInOperatingActivities";
const CONCEPT_CFI: &str = "idx-cor:NetCashFlowsReceivedFromUsedInInvestingActivities";
const CONCEPT_CFF: &str = "idx-cor:NetCashFlowsReceivedFromUsedInFinancingActivities";
const CONCEPT_NET_INCOME: &str = "idx-cor:ProfitLoss";
const CONCEPT_CAPEX_CANDIDATES: &[&str] = &[
    "idx-cor:PaymentsForAcquisitionOfPropertyPlantAndEquipment",
    "idx-cor:PaymentsForAcquisitionOfPropertyAndEquipment",
    "idx-cor:PaymentsForAcquisitionOfInvestmentProperties",
    "idx-cor:PaymentsForAcquisitionOfLandForDevelopment",
];

const CONTEXT_YTD: &str = "CurrentYearDuration";

struct ZipHtmlCorpus {
    entries: Vec<(String, String)>,
}

impl ZipHtmlCorpus {
    fn from_archive(archive: &mut ZipArchive<std::io::Cursor<&[u8]>>) -> Result<Self, String> {
        let mut entries = Vec::new();
        for i in 0..archive.len() {
            let mut file = archive
                .by_index(i)
                .map_err(|e| format!("baca entry zip index {i}: {e}"))?;
            let name = file.name().to_string();
            if !name.ends_with(".html") || file.is_dir() {
                continue;
            }
            let mut buf = String::new();
            file.read_to_string(&mut buf)
                .map_err(|e| format!("baca {name} gagal: {e}"))?;
            entries.push((name, buf));
        }
        if entries.is_empty() {
            return Err("zip tidak berisi file .html".into());
        }
        Ok(Self { entries })
    }

    fn html(&self, name: &str) -> Option<&str> {
        self.entries
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, h)| h.as_str())
    }

    fn search_order<'a>(&'a self, preferred: &[&str]) -> Vec<&'a str> {
        let mut order = Vec::new();
        for name in preferred {
            if self.html(name).is_some() {
                order.push(self.html(name).expect("checked"));
            }
        }
        for (_, html) in &self.entries {
            if !order.iter().any(|h| std::ptr::eq(*h, html.as_str())) {
                order.push(html.as_str());
            }
        }
        order
    }

    fn find_dei_html(&self) -> Result<&str, String> {
        if let Some(html) = self.html(FILE_DEI) {
            return Ok(html);
        }
        self.entries
            .iter()
            .find(|(_, html)| {
                html.contains("idx-dei:EntityCode") && html.contains(LABEL_PERIOD_SUBMISSION)
            })
            .map(|(_, html)| html.as_str())
            .ok_or_else(|| format!("file {FILE_DEI} atau DEI lain tidak ditemukan di zip"))
    }
}

pub fn parse_zip_bytes(bytes: &[u8]) -> Result<ParsedXlbrZip, String> {
    let source_zip_hash = hex_sha256(bytes);
    let mut archive =
        ZipArchive::new(std::io::Cursor::new(bytes)).map_err(|e| format!("buka zip gagal: {e}"))?;

    let corpus = ZipHtmlCorpus::from_archive(&mut archive)?;
    let dei_html = corpus.find_dei_html()?;
    let meta = parse_dei(dei_html)?;

    let is_order = corpus.search_order(PREFERRED_IS);
    let cf_order = corpus.search_order(PREFERRED_CF);

    let ytd = YtdMetrics {
        net_income: extract_required_from_htmls(
            &is_order,
            &[CONCEPT_NET_INCOME],
            "net income (ProfitLoss)",
        )?,
        cash_from_operation: extract_required_from_htmls(
            &cf_order,
            &[CONCEPT_CFO],
            "cash from operation",
        )?,
        cash_from_investment: extract_required_from_htmls(
            &cf_order,
            &[CONCEPT_CFI],
            "cash from investment",
        )?,
        cash_from_financing: extract_required_from_htmls(
            &cf_order,
            &[CONCEPT_CFF],
            "cash from financing",
        )?,
        capital_expenditure: extract_optional_from_htmls(&cf_order, CONCEPT_CAPEX_CANDIDATES),
    };

    Ok(ParsedXlbrZip {
        meta,
        ytd,
        source_zip_hash,
    })
}

fn extract_required_from_htmls(
    htmls: &[&str],
    concepts: &[&str],
    label: &str,
) -> Result<f64, String> {
    extract_from_htmls(htmls, concepts).ok_or_else(|| {
        let concepts = concepts
            .iter()
            .map(|c| format!("'{c}'"))
            .collect::<Vec<_>>()
            .join(" atau ");
        format!("{label} ({concepts} context={CONTEXT_YTD}) tidak ditemukan di zip")
    })
}

fn extract_optional_from_htmls(htmls: &[&str], concepts: &[&str]) -> f64 {
    extract_from_htmls(htmls, concepts).unwrap_or(0.0)
}

fn extract_from_htmls(htmls: &[&str], concepts: &[&str]) -> Option<f64> {
    for concept in concepts {
        for html in htmls {
            if let Some(value) = extract_non_fraction_if_present(html, concept, CONTEXT_YTD) {
                return Some(value);
            }
        }
    }
    None
}

fn parse_dei(html: &str) -> Result<ParsedReportMeta, String> {
    let code = extract_non_numeric(html, "idx-dei:EntityCode", "CurrentYearInstant")?
        .trim()
        .to_ascii_uppercase();
    if code.is_empty() {
        return Err("idx-dei:EntityCode kosong".into());
    }

    let quarter_raw = extract_non_numeric_by_row_label(html, LABEL_PERIOD_SUBMISSION)?;
    let quarter = normalize_quarter(&quarter_raw)?;

    let period_end_raw =
        extract_non_numeric(html, "idx-dei:CurrentPeriodEndDate", "CurrentYearInstant")?;
    let period_end = parse_dei_date(&period_end_raw)?;

    let fiscal_year = period_end.year();

    let presentation_currency =
        extract_non_numeric(html, "idx-dei:DescriptionOfPresentationCurrency", "CurrentYearInstant")?
            .split('/')
            .next()
            .unwrap_or("")
            .trim()
            .to_string();

    let rounding_raw =
        extract_non_numeric(html, "idx-dei:LevelOfRoundingUsedInFinancialStatements", "CurrentYearInstant")?;
    let unit_scale = parse_unit_scale(&rounding_raw)?;

    Ok(ParsedReportMeta {
        code,
        fiscal_year,
        quarter,
        period_end,
        presentation_currency,
        unit_scale,
    })
}

fn normalize_quarter(raw: &str) -> Result<String, String> {
    let lower = raw.to_ascii_lowercase();
    // Urut TW4→TW1: "kuartal ii/iii/iv" mengandung substring "kuartal i".
    if lower.contains("annual")
        || lower.contains("tahunan")
        || lower.contains("fourth")
        || lower.contains("kuartal iv")
        || lower.contains("quarter iv")
    {
        Ok("TW4".into())
    } else if lower.contains("third") || lower.contains("kuartal iii") || lower.contains("quarter iii") {
        Ok("TW3".into())
    } else if lower.contains("second") || lower.contains("kuartal ii") || lower.contains("quarter ii") {
        Ok("TW2".into())
    } else if lower.contains("first") || lower.contains("kuartal i") || lower.contains("quarter i") {
        Ok("TW1".into())
    } else {
        Err(format!(
            "quarter tidak dikenali dari '{LABEL_PERIOD_SUBMISSION}': {raw}"
        ))
    }
}

fn parse_unit_scale(raw: &str) -> Result<i32, String> {
    let lower = raw.to_ascii_lowercase();
    if lower.contains("million") || lower.contains("juta") {
        Ok(6)
    } else if lower.contains("thousand") || lower.contains("ribuan") {
        Ok(3)
    } else if lower.contains("whole") || lower.contains("penuh") {
        Ok(0)
    } else {
        Ok(3)
    }
}

fn parse_dei_date(raw: &str) -> Result<chrono::DateTime<Utc>, String> {
    let trimmed = raw.trim();
    for fmt in ["%B %d, %Y", "%B %d,%Y", "%Y-%m-%d"] {
        if let Ok(naive) = NaiveDate::parse_from_str(trimmed, fmt) {
            return Ok(naive.and_hms_opt(0, 0, 0).unwrap().and_utc());
        }
    }
    Err(format!("format tanggal DEI tidak dikenali: {raw}"))
}

fn extract_non_numeric_by_row_label(html: &str, label: &str) -> Result<String, String> {
    let label_pos = html
        .find(label)
        .ok_or_else(|| format!("label '{label}' tidak ditemukan di {FILE_DEI}"))?;
    let after_label = &html[label_pos..];
    let rel = after_label
        .find("<ix:nonNumeric")
        .ok_or_else(|| format!("ix:nonNumeric untuk '{label}' tidak ditemukan di {FILE_DEI}"))?;
    extract_ix_non_numeric_value(&after_label[rel..])
}

fn extract_ix_non_numeric_value(tag_and_inner: &str) -> Result<String, String> {
    let open_end = tag_and_inner
        .find('>')
        .ok_or_else(|| "tag ix:nonNumeric tidak lengkap".to_string())?;
    let tag = &tag_and_inner[..open_end + 1];
    if tag.contains("xsi:nil=\"true\"") {
        return Ok(String::new());
    }
    let inner = &tag_and_inner[open_end + 1..];
    if inner.starts_with("</") {
        return Ok(String::new());
    }
    let close = inner
        .find("</ix:nonNumeric>")
        .ok_or_else(|| "penutup ix:nonNumeric tidak ditemukan".to_string())?;
    Ok(decode_entities(inner[..close].trim()))
}

fn extract_non_numeric(html: &str, concept: &str, context: &str) -> Result<String, String> {
    let pattern = format!(r#"name="{concept}" contextRef="{context}""#);
    let start = html
        .find(&pattern)
        .ok_or_else(|| format!("{concept} context={context} tidak ditemukan"))?;
    extract_ix_non_numeric_value(&html[start..])
}

fn extract_non_fraction_if_present(html: &str, concept: &str, context: &str) -> Option<f64> {
    let pattern = format!(r#"name="{concept}" contextRef="{context}""#);
    let start = html.find(&pattern)?;
    let tag_end = html[start..].find('>')? + start + 1;
    let tag = &html[start..tag_end];
    if tag.contains("xsi:nil=\"true\"") {
        return Some(0.0);
    }
    let inner = &html[tag_end..];
    if inner.starts_with("</") {
        return Some(0.0);
    }
    let close = inner.find("</ix:nonFraction>")?;
    let display = inner[..close].trim();
    let scale = parse_attr_i32(tag, "scale").unwrap_or(0);
    let value = parse_display_amount(display).ok()?;
    let _ = scale;
    Some(value)
}

fn parse_attr_i32(tag: &str, name: &str) -> Option<i32> {
    let needle = format!("{name}=\"");
    let start = tag.find(&needle)? + needle.len();
    let rest = &tag[start..];
    let end = rest.find('"')?;
    rest[..end].parse().ok()
}

fn parse_display_amount(display: &str) -> Result<f64, String> {
    let cleaned: String = display
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '.' || *c == '-')
        .collect();
    cleaned
        .parse()
        .map_err(|e| format!("parse angka '{display}' gagal: {e}"))
}

fn decode_entities(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
}

fn hex_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn normalize_quarter_roman_substrings() {
        assert_eq!(
            normalize_quarter("Kuartal II / Second Quarter").unwrap(),
            "TW2"
        );
        assert_eq!(
            normalize_quarter("Kuartal III / Third Quarter").unwrap(),
            "TW3"
        );
        assert_eq!(
            normalize_quarter("Kuartal IV / Fourth Quarter").unwrap(),
            "TW4"
        );
        assert_eq!(
            normalize_quarter("Kuartal I / First Quarter").unwrap(),
            "TW1"
        );
        assert_eq!(
            normalize_quarter("Tahunan / Annual").unwrap(),
            "TW4"
        );
    }

    #[test]
    fn parse_sample_zip() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/src/inlineXBRL.zip");
        let bytes = fs::read(path).expect("inlineXBRL.zip");
        let parsed = parse_zip_bytes(&bytes).expect("parse");
        assert_eq!(parsed.meta.code, "AADI");
        assert_eq!(parsed.meta.quarter, "TW1");
        assert_eq!(parsed.meta.fiscal_year, 2026);
        assert!(parsed.ytd.net_income > 0.0);
        assert!(parsed.ytd.cash_from_operation > 0.0);
    }

    #[test]
    fn parse_properti_zip() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/src/inlineXBRL-properti.zip");
        let bytes = fs::read(path).expect("inlineXBRL-properti.zip");
        let parsed = parse_zip_bytes(&bytes).expect("parse properti");
        assert_eq!(parsed.meta.code, "DMAS");
        assert!(parsed.ytd.net_income > 0.0);
        assert!(parsed.ytd.cash_from_operation > 0.0);
        assert!(parsed.ytd.capital_expenditure > 0.0);
    }
}
