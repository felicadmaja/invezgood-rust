//! Debug bertahap login Stockbit → screenshot urut sampai halaman START TRADING.
//! Jalankan: set -a && source ../.env && set +a && cargo run --bin stockbit_login_flow_debug

use stockbit_browser::{
    acquire_browser_session, debug_login_flow_with_screenshots, launch_page,
    reset_stockbit_chrome_login_profile, BrowserLockClass,
};

#[tokio::main]
async fn main() -> Result<(), String> {
    let email = std::env::var("STOCKBIT_EMAIL").map_err(|e| e.to_string())?;
    let password = std::env::var("STOCKBIT_PASSWORD").map_err(|e| e.to_string())?;

    let _guard = acquire_browser_session(BrowserLockClass::Interactive).await?;
    println!("Logout Chrome Stockbit (reset profil)...");
    reset_stockbit_chrome_login_profile()
        .await
        .map_err(|e| e.to_string())?;

    let (browser, page) = launch_page().await.map_err(|e| e.to_string())?;
    let shots = debug_login_flow_with_screenshots(&page, email.trim(), password.trim())
        .await
        .map_err(|e| e.to_string())?;

    println!("\n=== Screenshot urut ===");
    for (label, path) in &shots {
        println!("{label}: {path}");
    }
    browser.close().await;
    Ok(())
}
