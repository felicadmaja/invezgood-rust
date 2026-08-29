//! Usage: cargo run -p xlbr_laporan_keuangan --example upload_from_file -- path/to/inlineXBRL.zip

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenvy::dotenv_override().ok();
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: upload_from_file <path-to.zip>")?;
    let bytes = tokio::fs::read(&path).await?;
    let session = xlbr_laporan_keuangan::connect().await?;
    let row = xlbr_laporan_keuangan::upload_from_zip_bytes(session, &bytes).await?;
    println!(
        "OK {} {} {} CFO={:.0} net_income={:.0} fcf={:.0}",
        row.code, row.fiscal_year, row.quarter, row.cash_from_operation, row.net_income, row.free_cash_flow
    );
    Ok(())
}
