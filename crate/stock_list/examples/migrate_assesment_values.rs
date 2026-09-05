//! Migrasi nilai assessment di invezgood.stock_list:
//! - fundamental_assesment: -1→1, 1→2, 0 tetap
//! - valuation_assesment: -1→1, 1→3, 0 tetap
//!
//! Usage: `cargo run -p stock_list --example migrate_assesment_values`

use futures::TryStreamExt;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

fn map_fundamental(v: i8) -> i8 {
    match v {
        -1 => 1,
        1 => 2,
        other => other,
    }
}

fn map_valuation(v: i8) -> i8 {
    match v {
        -1 => 1,
        1 => 3,
        other => other,
    }
}

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

    let mut rows = session
        .query_iter(
            "SELECT code, fundamental_assesment, valuation_assesment FROM invezgood.stock_list",
            &[],
        )
        .await?
        .rows_stream::<(String, Option<i8>, Option<i8>)>()?;

    let mut updated = 0usize;
    let mut counts: std::collections::BTreeMap<(i8, i8), usize> = std::collections::BTreeMap::new();

    while let Some((code, fund, val)) = rows.try_next().await? {
        let fund = fund.unwrap_or(0);
        let val = val.unwrap_or(0);
        let new_fund = map_fundamental(fund);
        let new_val = map_valuation(val);

        *counts.entry((new_fund, new_val)).or_default() += 1;

        if new_fund == fund && new_val == val {
            continue;
        }

        session
            .query_unpaged(
                "UPDATE invezgood.stock_list SET fundamental_assesment = ?, valuation_assesment = ? WHERE code = ?",
                (new_fund, new_val, code.as_str()),
            )
            .await?;
        updated += 1;
        if updated % 100 == 0 {
            eprintln!("updated {updated}...");
        }
    }

    eprintln!("Selesai: {updated} baris di-update.");
    eprintln!("Distribusi setelah migrasi (fundamental, valuation):");
    for ((f, v), cnt) in counts {
        eprintln!("  ({f}, {v}): {cnt}");
    }
    Ok(())
}
