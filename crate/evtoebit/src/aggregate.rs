use std::collections::HashMap;

use crate::pb::MedianEvMultipleRow;
use crate::universe::UniverseRow;
use crate::yahoo::EmitenMetrics;

const SECTOR_ROLLUP_LABEL: &str = "(agregat sektor)";

#[derive(Debug, Clone)]
struct DetailRow {
    sektor: String,
    sub_sektor: String,
    ev_ebit: f64,
    ev_ebitda: f64,
}

pub fn aggregate_median(
    universe: &[UniverseRow],
    metrics: &[(String, Result<EmitenMetrics, String>)],
    max_multiple: f64,
) -> Vec<MedianEvMultipleRow> {
    let mut details = Vec::new();

    for (row, result) in universe.iter().zip(metrics.iter()) {
        let Ok(m) = &result.1 else { continue };
        if let Some(ev_ebit) = m.ev_ebit.filter(|v| *v > 0.0 && *v <= max_multiple) {
            details.push(DetailRow {
                sektor: row.sektor.clone(),
                sub_sektor: row.sub_sektor.clone(),
                ev_ebit,
                ev_ebitda: m.ev_ebitda.filter(|v| *v > 0.0 && *v <= max_multiple).unwrap_or(0.0),
            });
        }
    }

    let min_valid_n = min_valid_n();
    let mut groups: HashMap<(String, String), Vec<DetailRow>> = HashMap::new();

    for row in details {
        groups
            .entry((row.sektor.clone(), row.sub_sektor.clone()))
            .or_default()
            .push(row.clone());
        groups
            .entry((row.sektor.clone(), SECTOR_ROLLUP_LABEL.to_string()))
            .or_default()
            .push(row);
    }

    let mut out: Vec<MedianEvMultipleRow> = groups
        .into_iter()
        .map(|((sektor, sub_sektor), rows)| row_from_group(sektor, sub_sektor, &rows, min_valid_n))
        .filter(|row| row.n >= min_valid_n)
        .collect();

    out.sort_by(|a, b| {
        a.sektor
            .cmp(&b.sektor)
            .then_with(|| a.sub_sektor.cmp(&b.sub_sektor))
    });
    out
}

fn row_from_group(
    sektor: String,
    sub_sektor: String,
    rows: &[DetailRow],
    min_valid_n: i32,
) -> MedianEvMultipleRow {
    let mut ebit_vals: Vec<f64> = rows.iter().map(|r| r.ev_ebit).collect();
    ebit_vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let mut ebitda_vals: Vec<f64> = rows
        .iter()
        .filter_map(|r| (r.ev_ebitda > 0.0).then_some(r.ev_ebitda))
        .collect();
    ebitda_vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let n = ebit_vals.len() as i32;
    MedianEvMultipleRow {
        sektor,
        sub_sektor,
        n,
        median_ev_ebit: round2(quantile(&ebit_vals, 0.5)),
        p25_ev_ebit: round2(quantile(&ebit_vals, 0.25)),
        p75_ev_ebit: round2(quantile(&ebit_vals, 0.75)),
        median_ev_ebitda: round2(quantile(&ebitda_vals, 0.5)),
        flag: if n < min_valid_n {
            format!("n<{min_valid_n}")
        } else {
            String::new()
        },
    }
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

fn quantile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    if sorted.len() == 1 {
        return sorted[0];
    }
    let pos = q * (sorted.len() - 1) as f64;
    let lower = pos.floor() as usize;
    let upper = pos.ceil() as usize;
    if lower == upper {
        return sorted[lower];
    }
    let weight = pos - lower as f64;
    sorted[lower] * (1.0 - weight) + sorted[upper] * weight
}

pub fn min_valid_n() -> i32 {
    std::env::var("EVTOEBIT_MIN_VALID_N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20)
}

pub fn max_multiple() -> f64 {
    std::env::var("EVTOEBIT_MAX_MULTIPLE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100.0)
}
