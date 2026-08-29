//! Invoke logic sama dengan RPC `GetMedianEVToEbitdaFromScylla` (tanpa gRPC auth).
//! `cargo run -p evtoebit --example invoke_from_scylla`

use evtoebit::repository;
use stock_list::connect;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = dotenvy::dotenv_override();

    let session = connect().await?;
    let db_rows = repository::find_all(session.as_ref()).await?;
    let rows: Vec<_> = db_rows.iter().map(repository::row_to_pb).collect();

    println!("success: true");
    println!("message: {} baris dari invezgood.evtoebit", rows.len());
    println!();
    println!(
        "{:<35} {:>4} {:>12} {:>12} {:>12} {:>12} {:>12} {:>4}",
        "sektor", "n", "med_ebit", "p25", "p75", "med_ebitda", "updated_at", "flag"
    );
    println!("{}", "-".repeat(115));
    for row in &rows {
        let ts = row
            .updated_at
            .map(|t| t.to_string())
            .unwrap_or_else(|| "-".into());
        println!(
            "{:<35} {:>4} {:>12.2} {:>12.2} {:>12.2} {:>12.2} {:>12} {:>4}",
            row.sektor,
            row.n,
            row.median_ev_ebit,
            row.p25_ev_ebit,
            row.p75_ev_ebit,
            row.median_ev_ebitda,
            ts,
            row.flag,
        );
    }
    Ok(())
}
