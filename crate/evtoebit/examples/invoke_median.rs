//! Invoke `EvToEbitService::fetch_median_from_yahoo_finance` (sama dengan RPC, tanpa gRPC/auth).
//! Full universe: `cargo run -p evtoebit --example invoke_median`

use evtoebit::{new_yahoo_client, EvToEbitService};
use stock_list::connect;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = dotenvy::dotenv_override();

    let session = connect().await?;
    let yahoo = new_yahoo_client()?;
    let service = EvToEbitService::new(session, yahoo);

    let resp = service.fetch_median_from_yahoo_finance().await?;

    println!("success: {}", resp.success);
    println!("message: {}", resp.message);
    println!();
    println!(
        "{:<35} {:>4} {:>12} {:>12} {:>12} {:>12} {:>4}",
        "sektor", "n", "med_ebit", "p25", "p75", "med_ebitda", "flag"
    );
    println!("{}", "-".repeat(100));
    for row in &resp.rows {
        println!(
            "{:<35} {:>4} {:>12.2} {:>12.2} {:>12.2} {:>12.2} {:>4}",
            row.sektor,
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
