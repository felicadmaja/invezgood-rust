//! Eksekusi langsung fungsi inti RPC `GetPBVPERBandByCode` (tanpa gRPC):
//! stockbit_browser::fetch_fundachart_both_json("BBCA") → cetak JSON PE band & PBV band.
//! Jalankan: set -a && source .env && set +a && cargo run --bin fundachart_bbca_debug

#[tokio::main]
async fn main() -> Result<(), String> {
    let code = std::env::args().nth(1).unwrap_or_else(|| "BBCA".to_string());
    eprintln!("Scrape fundachart PE+PBV untuk code={code} ...");

    let (raw_pe, raw_pbv) = stockbit_browser::fetch_fundachart_both_json(&code)
        .await
        .map_err(|e| e.to_string())?;

    let pretty = |s: &str| -> String {
        serde_json::from_str::<serde_json::Value>(s)
            .and_then(|v| serde_json::to_string_pretty(&v))
            .unwrap_or_else(|_| s.to_string())
    };

    println!("================ PE BAND (per_band) ================");
    println!("{}", pretty(&raw_pe));
    println!("================ PBV BAND (pbv_band) ================");
    println!("{}", pretty(&raw_pbv));

    Ok(())
}
