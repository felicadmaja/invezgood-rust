//! Sama logic RPC `GetTopForeignFlowByCode` (tanpa gRPC/JWT).
//! `cargo run -p top_foreign_flow --example invoke_by_code TINS`

use top_foreign_flow::find_by_code;
use stock_list::connect;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = dotenvy::dotenv_override();
    let code = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "TINS".to_string())
        .trim()
        .to_ascii_uppercase();

    let session = connect().await?;
    let mut rows = find_by_code(session.as_ref(), &code).await?;

    rows.sort_by(|a, b| b.tahun_bulan_tanggal.cmp(&a.tahun_bulan_tanggal));
    let take = rows.len().min(7);

    println!("code={code} total={} (tampil {} terbaru)\n", rows.len(), take);
    println!(
        "{:<12} {:<6} {:>8} {:>10} {:>14} {:>12} {:>8} {:>10}",
        "tanggal", "code", "price", "change", "value", "volume", "calc_val", "accum/dist"
    );
    println!("{}", "-".repeat(95));
    for row in rows.into_iter().take(take) {
        println!(
            "{:<12} {:<6} {:>8} {:>10.3} {:>14} {:>12} {:>8.2} {:>10}",
            row.tahun_bulan_tanggal,
            row.code,
            row.price.unwrap_or(0),
            row.change.unwrap_or(0.0),
            row.value,
            row.volume.unwrap_or(0),
            row.calculated_value.unwrap_or(0.0),
            row.accum_or_dist.as_deref().unwrap_or("-"),
        );
    }
    Ok(())
}
