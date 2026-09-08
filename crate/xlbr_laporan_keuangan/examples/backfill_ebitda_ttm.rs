//! Backfill kolom `ebitda_ttm` dari zip lokal `src/downloaded_xbrl`.
//!
//! Proses ascending (tahun ↑, Q1→Q4) agar zip referensi TTM (Q4 tahun lalu, kuartal sama tahun lalu)
//! sudah tersedia di disk saat dihitung.
//!
//! Usage:
//!   cargo run -p xlbr_laporan_keuangan --example backfill_ebitda_ttm
//!   cargo run -p xlbr_laporan_keuangan --example backfill_ebitda_ttm -- UNTR

use std::path::{Path, PathBuf};
use std::sync::Arc;

use scylla::client::session::Session;
use xlbr_laporan_keuangan::model::quarter_index;

const ZIP_PREFIX: &str = "inlineXBRL-";

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ZipJob {
    code: String,
    fiscal_year: i32,
    quarter: String,
    path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenvy::dotenv_override().ok();

    let filter_code = std::env::args().nth(1).map(|s| s.to_ascii_uppercase());
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/downloaded_xbrl");

    let mut jobs = collect_zip_jobs(&root, filter_code.as_deref())?;
    jobs.sort();

    if jobs.is_empty() {
        eprintln!("Tidak ada zip di {}", root.display());
        return Ok(());
    }

    let session = xlbr_laporan_keuangan::connect().await?;
    let mut ok = 0usize;
    let mut failed = 0usize;
    let mut with_ttm = 0usize;

    for job in &jobs {
        match process_zip(session.clone(), job).await {
            Ok(row) => {
                ok += 1;
                if row.ebitda_ttm != 0.0 {
                    with_ttm += 1;
                }
                if ok % 50 == 0 || row.ebitda_ttm != 0.0 {
                    eprintln!(
                        "OK {} {} {} ebitda_ttm={:.0}",
                        row.code, row.fiscal_year, row.quarter, row.ebitda_ttm
                    );
                }
            }
            Err(e) => {
                failed += 1;
                eprintln!(
                    "FAIL {} {} {} ({}): {e}",
                    job.code, job.fiscal_year, job.quarter, job.path.display()
                );
            }
        }
    }

    eprintln!(
        "Selesai: {ok} sukses ({with_ttm} dengan ebitda_ttm), {failed} gagal, total {}",
        ok + failed
    );
    Ok(())
}

fn collect_zip_jobs(
    root: &Path,
    filter_code: Option<&str>,
) -> Result<Vec<ZipJob>, Box<dyn std::error::Error + Send + Sync>> {
    let mut jobs = Vec::new();
    for entry in walkdir(root)? {
        let file_name = entry
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or("nama file zip tidak valid")?;
        if !file_name.ends_with(".zip") || !file_name.starts_with(ZIP_PREFIX) {
            continue;
        }
        let stem = file_name.strip_suffix(".zip").unwrap_or(file_name);
        let parts: Vec<&str> = stem.split('-').collect();
        if parts.len() < 4 {
            continue;
        }
        let code = parts[1].to_ascii_uppercase();
        if filter_code.is_some_and(|f| f != code) {
            continue;
        }
        let fiscal_year: i32 = parts[2].parse()?;
        let quarter = parts[3].to_ascii_uppercase();
        if quarter_index(&quarter).is_none() {
            continue;
        }
        jobs.push(ZipJob {
            code,
            fiscal_year,
            quarter,
            path: entry,
        });
    }
    Ok(jobs)
}

fn walkdir(root: &Path) -> Result<Vec<PathBuf>, Box<dyn std::error::Error + Send + Sync>> {
    let mut out = Vec::new();
    if !root.is_dir() {
        return Ok(out);
    }
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            out.extend(walkdir(&path)?);
        } else if path.is_file() {
            out.push(path);
        }
    }
    Ok(out)
}

async fn process_zip(
    session: Arc<Session>,
    job: &ZipJob,
) -> Result<xlbr_laporan_keuangan::XlbrLaporanKeuanganRow, String> {
    let bytes = tokio::fs::read(&job.path)
        .await
        .map_err(|e| format!("baca {}: {e}", job.path.display()))?;
    xlbr_laporan_keuangan::upload_from_zip_bytes(session, &bytes).await
}
