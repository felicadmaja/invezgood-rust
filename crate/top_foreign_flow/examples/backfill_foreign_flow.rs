//! Backfill Invezgo top foreign → Scylla (sama fetch/upsert dengan RPC, tanpa gRPC).
//! `cargo run -p top_foreign_flow --example backfill_foreign_flow`

use std::sync::Arc;

use chrono::{Datelike, NaiveDate};
use top_foreign_flow::fetch_and_save;
use stock_list::connect;

fn parse_date(s: &str) -> NaiveDate {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").expect("YYYY-MM-DD")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = dotenvy::dotenv_override();

    let start = parse_date("2026-03-01");
    let end = parse_date("2026-09-01");
    let session = connect().await?;

    let mut date = start;
    let mut ok = 0usize;
    let mut skip = 0usize;
    let mut err = 0usize;

    while date <= end {
        if matches!(date.weekday(), chrono::Weekday::Sat | chrono::Weekday::Sun) {
            skip += 1;
            date = date.succ_opt().expect("date");
            continue;
        }

        let label = date.format("%Y-%m-%d").to_string();
        match fetch_and_save(Arc::clone(&session), date).await {
            Ok(n) => {
                ok += 1;
                eprintln!("{label}: upsert {n} baris (Invezgo)");
            }
            Err(e) => {
                err += 1;
                eprintln!("{label}: GAGAL — {e}");
            }
        }

        date = date.succ_opt().expect("date");
    }

    eprintln!("selesai: ok={ok} skip_weekend={skip} err={err}");
    Ok(())
}
