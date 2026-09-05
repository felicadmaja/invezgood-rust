//! Debug fetch Stockbit profile: `cargo run -p stock_list --example debug_stockbit_profile -- ABDA`
//!
//! Env: STOCKBIT_EMAIL, STOCKBIT_PASSWORD (untuk bearer Chrome).

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenvy::dotenv_override().ok();
    let code = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "ABDA".to_string());
    match stock_list::stockbit_profile::fetch_stockbit_profile(&code).await {
        Ok(profile) => {
            eprintln!("OK {code}: background len={}", profile.background.len());
        }
        Err(e) => {
            eprintln!("ERR {code}: {e}");
            std::process::exit(1);
        }
    }
    Ok(())
}
