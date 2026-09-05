//! Reset `stockbit_profile_updated_at` ke epoch untuk semua emiten.
//!
//! Usage: `cargo run -p stock_list --example reset_stockbit_profile_updated_at`

use chrono::{TimeZone, Utc};
use futures::TryStreamExt;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

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

    let epoch = Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap();

    let mut rows = session
        .query_iter("SELECT code FROM invezgood.stock_list", &[])
        .await?
        .rows_stream::<(String,)>()?;

    let mut updated = 0usize;
    while let Some((code,)) = rows.try_next().await? {
        session
            .query_unpaged(
                "UPDATE invezgood.stock_list SET stockbit_profile_updated_at = ? WHERE code = ?",
                (epoch, code.as_str()),
            )
            .await?;
        updated += 1;
        if updated % 100 == 0 {
            eprintln!("updated {updated}...");
        }
    }

    eprintln!("Selesai: stockbit_profile_updated_at = 1970-01-01 untuk {updated} baris.");
    Ok(())
}
