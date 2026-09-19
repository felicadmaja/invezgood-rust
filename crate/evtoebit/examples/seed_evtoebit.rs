//! Compute median EV/EBIT lalu upsert ke `invezgood.evtoebit`.
//! Full universe: `cargo run -p evtoebit --example seed_evtoebit`

use evtoebit::{new_yahoo_client, repository, EvToEbitService};
use stock_list::connect;
use user::new_session_store;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = dotenvy::dotenv_override();

    let session = connect().await?;
    let yahoo = new_yahoo_client()?;
    repository::recreate_table(session.as_ref()).await?;
    let service = EvToEbitService::new(session, yahoo, new_session_store());
    let resp = service.fetch_median_from_yahoo_finance().await?;

    println!("success: {}", resp.success);
    println!("message: {}", resp.message);
    Ok(())
}
