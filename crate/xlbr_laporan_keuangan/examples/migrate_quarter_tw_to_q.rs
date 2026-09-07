//! Migrasi clustering key `quarter`: TW1..TW4 → Q1..Q4.
//!
//! Usage: `cargo run -p xlbr_laporan_keuangan --example migrate_quarter_tw_to_q`

use futures::TryStreamExt;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use xlbr_laporan_keuangan::model::{normalize_quarter_label, XlbrLaporanKeuanganRow};
use xlbr_laporan_keuangan::repository;

const SELECT_ALL: &str =
    "SELECT code, fiscal_year, quarter, period_end, presentation_currency, unit_scale, \
    cash_from_operation, cash_from_investment, cash_from_financing, capital_expenditure, \
    free_cash_flow, net_income, interest_paid, tax_paid, st_bank_loans, current_maturities, \
    lt_loans, bonds, sukuk, lease_liabilities, hutang_berbunga, uploaded_at, source_zip_hash, catatan \
    FROM invezgood.xlbr_laporan_keuangan";

const DELETE_ROW: &str =
    "DELETE FROM invezgood.xlbr_laporan_keuangan WHERE code = ? AND fiscal_year = ? AND quarter = ?";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenvy::dotenv_override().ok();

    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".into());
    let user = std::env::var("SCYLLA_USER").unwrap_or_else(|_| "cassandra".into());
    let password = std::env::var("SCYLLA_PASSWORD").unwrap_or_default();

    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .user(user, password)
        .build()
        .await?;

    let mut rows = session
        .query_iter(SELECT_ALL, &[])
        .await?
        .rows_stream::<XlbrLaporanKeuanganRow>()?;

    let mut migrated = 0usize;
    let mut skipped = 0usize;

    while let Some(row) = rows.try_next().await? {
        let new_quarter = normalize_quarter_label(&row.quarter);
        if new_quarter.eq_ignore_ascii_case(&row.quarter) {
            skipped += 1;
            continue;
        }

        let mut updated = row.clone();
        updated.quarter = new_quarter.clone();
        repository::upsert(&session, &updated).await?;
        session
            .query_unpaged(
                DELETE_ROW,
                (&row.code, row.fiscal_year, row.quarter.as_str()),
            )
            .await?;
        migrated += 1;
        if migrated % 100 == 0 {
            eprintln!("migrated {migrated}...");
        }
    }

    eprintln!("Selesai: {migrated} baris TW→Q, {skipped} sudah Q.");
    Ok(())
}
