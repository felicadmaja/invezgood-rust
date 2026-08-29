//! Usage: cargo run -p xlbr_laporan_keuangan --example upload_from_url -- <url>

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenvy::dotenv_override().ok();
    let url = std::env::args().nth(1).ok_or("usage: upload_from_url <url>")?;
    let session = xlbr_laporan_keuangan::connect().await?;
    let row = xlbr_laporan_keuangan::upload_from_url(session, &url).await?;
    println!(
        "OK {} {} {} CFO={:.0} net_income={:.0} fcf={:.0}",
        row.code, row.fiscal_year, row.quarter, row.cash_from_operation, row.net_income, row.free_cash_flow
    );
    Ok(())
}
