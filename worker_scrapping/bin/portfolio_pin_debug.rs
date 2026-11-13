//! Debug: screenshot alur START TRADING + PIN (tanpa insert Scylla).
//! Jalankan: `cd worker_scrapping && set -a && source ../.env && set +a && cargo run --bin portfolio_pin_debug`

use std::time::Duration;

use stockbit_browser::{
    acquire_browser_session, launch_page, open_stream_or_login, save_debug_screenshot,
    BrowserLockClass, STOCKBIT_STREAM_URL,
};
use tokio::time::sleep;
use worker_scrapping::portofolio_worker;

#[tokio::main]
async fn main() -> Result<(), String> {
    let email = std::env::var("STOCKBIT_EMAIL").map_err(|e| e.to_string())?;
    let password = std::env::var("STOCKBIT_PASSWORD").map_err(|e| e.to_string())?;
    let _guard = acquire_browser_session(BrowserLockClass::Interactive).await?;
    let (browser, page) = launch_page().await.map_err(|e| e.to_string())?;
    open_stream_or_login(&page, email.trim(), password.trim())
        .await
        .map_err(|e| e.to_string())?;
    stockbit_browser::goto_stockbit(&page, STOCKBIT_STREAM_URL)
        .await
        .map_err(|e| e.to_string())?;
    sleep(Duration::from_secs(2)).await;
    let _ = save_debug_screenshot(&page, "01_stream_ready").await;

    println!("=== ensure_trading_session → worker_scrapping/screenshots/ ===");
    match portofolio_worker::ensure_trading_session(&page).await {
        Ok(pin_entered) => println!("ensure_trading_session OK pin_entered={pin_entered}"),
        Err(e) => eprintln!("ensure_trading_session ERR: {e}"),
    }
    let _ = save_debug_screenshot(&page, "99_final").await;
    browser.close().await;
    Ok(())
}
