//! Compute median EV/EBIT lalu upsert ke `invezgood.evtoebit`.
//! Full universe: `cargo run -p evtoebit --example seed_evtoebit`

use std::sync::Arc;

use evtoebit::{new_yahoo_client, repository, sync_median_from_yahoo_to_scylla, YahooClient};
use stock_list::connect;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = dotenvy::dotenv_override();

    let session = connect().await?;
    let yahoo: Arc<YahooClient> = new_yahoo_client()?;
    repository::recreate_table(session.as_ref()).await?;
    let (n, message) = sync_median_from_yahoo_to_scylla(session, yahoo, None).await?;

    println!("success: true");
    println!("message: {message}");
    println!("upserted: {n} baris ke invezgood.evtoebit");
    Ok(())
}
