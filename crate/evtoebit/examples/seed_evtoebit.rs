//! Compute median EV/EBIT lalu upsert ke `invezgood.evtoebit`.
//! Full universe: `cargo run -p evtoebit --example seed_evtoebit`

use std::sync::Arc;

use chrono::Utc;
use evtoebit::{compute_median, new_yahoo_client, repository, YahooClient};
use stock_list::connect;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = dotenvy::dotenv_override();

    let session = connect().await?;
    let yahoo: Arc<YahooClient> = new_yahoo_client()?;
    repository::recreate_table(session.as_ref()).await?;
    let resp = compute_median(session.clone(), yahoo).await?;
    let updated_at = Utc::now();
    let n = repository::upsert_all(session.as_ref(), &resp.rows, updated_at).await?;

    println!("success: {}", resp.success);
    println!("message: {}", resp.message);
    println!("upserted: {n} baris ke invezgood.evtoebit @ {updated_at}");
    Ok(())
}
