pub mod pb {
    tonic::include_proto!("xlbr_laporan_keuangan");
}

pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("xlbr_laporan_keuangan_descriptor");

mod database;
pub mod model;
mod parser;
pub mod repository;
mod service;

pub use database::connect;
pub use model::{KEYSPACE, QUARTERS, TABLE, XlbrLaporanKeuanganRow};
pub use pb::xlbr_laporan_keuangan_server::{XlbrLaporanKeuangan, XlbrLaporanKeuanganServer};
pub use service::XlbrLaporanKeuanganService;

use std::sync::Arc;

use scylla::client::session::Session;

use model::{required_prior_quarters, XlbrLaporanKeuanganRow as Row};
use parser::parse_zip_bytes;
use repository::{list_for_year, row_from_standalone, standalone_sum_prior_to, upsert};

const MAX_ZIP_BYTES: usize = 32 * 1024 * 1024;

/// Parse zip → dekumulasi → upsert `invezgood.xlbr_laporan_keuangan`.
pub async fn upload_from_zip_bytes(session: Arc<Session>, bytes: &[u8]) -> Result<Row, String> {
    if bytes.is_empty() {
        return Err("zip kosong".into());
    }
    if bytes.len() > MAX_ZIP_BYTES {
        return Err(format!("zip melebihi batas {MAX_ZIP_BYTES} byte"));
    }

    let parsed = parse_zip_bytes(bytes)?;

    let existing =
        list_for_year(session.as_ref(), &parsed.meta.code, parsed.meta.fiscal_year).await?;
    validate_prior_quarters(&existing, &parsed.meta.quarter)?;

    let prior_sum = standalone_sum_prior_to(&existing, &parsed.meta.quarter);
    let standalone = parsed.ytd.deaccumulate(&prior_sum);

    let row = row_from_standalone(
        &parsed.meta.code,
        parsed.meta.fiscal_year,
        &parsed.meta.quarter,
        parsed.meta.period_end,
        &parsed.meta.presentation_currency,
        parsed.meta.unit_scale,
        standalone,
        &parsed.source_zip_hash,
    );

    upsert(session.as_ref(), &row).await?;
    Ok(row)
}

fn validate_prior_quarters(existing: &[Row], quarter: &str) -> Result<(), String> {
    for &req in required_prior_quarters(quarter)? {
        if !existing.iter().any(|r| r.quarter.eq_ignore_ascii_case(req)) {
            return Err(format!(
                "upload {quarter} membutuhkan {req} sudah di-upload untuk tahun buku yang sama"
            ));
        }
    }
    Ok(())
}
