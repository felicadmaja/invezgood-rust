//! Parse inline XBRL ZIP IDX → metadata + metrik YTD.

use std::io::Read;

use chrono::{Datelike, NaiveDate, Utc};
use sha2::{Digest, Sha256};
use zip::ZipArchive;

use crate::model::{ParsedReportMeta, ParsedXlbrZip, YtdMetrics};

const FILE_DEI: &str = "1000000.html";
const FILE_IS_GENERAL: &str = "1321000.html";
const FILE_IS_BANKING: &str = "2311000.html";
const FILE_IS_CANDIDATES: &[&str] = &[FILE_IS_GENERAL, FILE_IS_BANKING];
const FILE_CF: &str = "1510000.html";

const LABEL_PERIOD_SUBMISSION: &str = "Periode penyampaian laporan keuangan";

const CONCEPT_CFO: &str = "idx-cor:NetCashFlowsReceivedFromUsedInOperatingActivities";
const CONCEPT_CFI: &str = "idx-cor:NetCashFlowsReceivedFromUsedInInvestingActivities";
const CONCEPT_CFF: &str = "idx-cor:NetCashFlowsReceivedFromUsedInFinancingActivities";
const CONCEPT_CAPEX: &str = "idx-cor:PaymentsForAcquisitionOfPropertyPlantAndEquipment";
const CONCEPT_NET_INCOME: &str = "idx-cor:ProfitLoss";

const CONTEXT_YTD: &str = "CurrentYearDuration";

pub fn parse_zip_bytes(bytes: &[u8]) -> Result<ParsedXlbrZip, String> {
    let source_zip_hash = hex_sha256(bytes);
    let mut archive =
        ZipArchive::new(std::io::Cursor::new(bytes)).map_err(|e| format!("buka zip gagal: {e}"))?;

    let dei_html = read_zip_entry(&mut archive, FILE_DEI)?;
    let is_html = read_zip_entry_any(&mut archive, FILE_IS_CANDIDATES)?;
    let cf_html = read_zip_entry(&mut archive, FILE_CF)?;

    let meta = parse_dei(&dei_html)?;
    let ytd = YtdMetrics {
        net_income: extract_non_fraction(&is_html, CONCEPT_NET_INCOME, CONTEXT_YTD)?,
        cash_from_operation: extract_non_fraction(&cf_html, CONCEPT_CFO, CONTEXT_YTD)?,
        cash_from_investment: extract_non_fraction(&cf_html, CONCEPT_CFI, CONTEXT_YTD)?,
        cash_from_financing: extract_non_fraction(&cf_html, CONCEPT_CFF, CONTEXT_YTD)?,
        capital_expenditure: extract_non_fraction(&cf_html, CONCEPT_CAPEX, CONTEXT_YTD)?,
    };

    Ok(ParsedXlbrZip {
        meta,
        ytd,
        source_zip_hash,
    })
}

fn read_zip_entry(archive: &mut ZipArchive<std::io::Cursor<&[u8]>>, name: &str) -> Result<String, String> {
    let mut file = archive
        .by_name(name)
        .map_err(|_| format!("file {name} tidak ditemukan di zip"))?;
    let mut buf = String::new();
    file.read_to_string(&mut buf)
        .map_err(|e| format!("baca {name} gagal: {e}"))?;
    Ok(buf)
}

fn read_zip_entry_any(
    archive: &mut ZipArchive<std::io::Cursor<&[u8]>>,
    names: &[&str],
) -> Result<String, String> {
    for name in names {
        if let Ok(html) = read_zip_entry(archive, name) {
            return Ok(html);
        }
    }
    let listed = names
        .iter()
        .map(|n| format!("'{n}'"))
        .collect::<Vec<_>>()
        .join(" atau ");
    Err(format!("file {listed} tidak ditemukan di zip"))
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

fn extract_non_fraction(html: &str, concept: &str, context: &str) -> Result<f64, String> {
    let pattern = format!(r#"name="{concept}" contextRef="{context}""#);
    let start = html
        .find(&pattern)
        .ok_or_else(|| format!("{concept} context={context} tidak ditemukan"))?;
    let tag_end = html[start..]
        .find('>')
        .ok_or_else(|| format!("tag {concept} tidak lengkap"))? + start
        + 1;
    let tag = &html[start..tag_end];
    if tag.contains("xsi:nil=\"true\"") {
        return Ok(0.0);
    }
    let inner = &html[tag_end..];
    if inner.starts_with("</") {
        return Ok(0.0);
    }
    let close = inner
        .find("</ix:nonFraction>")
        .ok_or_else(|| format!("penutup {concept} tidak ditemukan"))?;
    let display = inner[..close].trim();
    let scale = parse_attr_i32(tag, "scale").unwrap_or(0);
    parse_display_amount(display, scale)
}

fn parse_attr_i32(tag: &str, name: &str) -> Option<i32> {
    let needle = format!("{name}=\"");
    let start = tag.find(&needle)? + needle.len();
    let rest = &tag[start..];
    let end = rest.find('"')?;
    rest[..end].parse().ok()
}

fn parse_display_amount(display: &str, _scale: i32) -> Result<f64, String> {
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
}
