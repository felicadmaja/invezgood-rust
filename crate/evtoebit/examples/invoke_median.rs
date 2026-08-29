//! Invoke logic sama dengan RPC `GetMedianEVToEbitdaFromYahooFinance` (tanpa gRPC auth).
//! Full universe: `cargo run -p evtoebit --example invoke_median`

use std::sync::Arc;

use evtoebit::{compute_median, new_yahoo_client, YahooClient};
use stock_list::connect;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = dotenvy::dotenv_override();

    let session = connect().await?;
    let yahoo: Arc<YahooClient> = new_yahoo_client()?;
    let resp = compute_median(session, yahoo).await?;

    println!("success: {}", resp.success);
    println!("message: {}", resp.message);
    println!();
    println!(
        "{:<35} {:<30} {:>4} {:>12} {:>12} {:>12} {:>12} {:>4}",
        "sektor", "sub_sektor", "n", "med_ebit", "p25", "p75", "med_ebitda", "flag"
    );
    println!("{}", "-".repeat(130));
    for row in &resp.rows {
        println!(
            "{:<35} {:<30} {:>4} {:>12.2} {:>12.2} {:>12.2} {:>12.2} {:>4}",
            row.sektor,
            row.sub_sektor,
            row.n,
            row.median_ev_ebit,
            row.p25_ev_ebit,
            row.p75_ev_ebit,
            row.median_ev_ebitda,
            row.flag,
        );
    }
    Ok(())
}
