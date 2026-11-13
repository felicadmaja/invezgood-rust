//! Buka https://stockbit.com/login di Chrome profil worker dan simpan screenshot.
//! Jalankan: set -a && source ../.env && set +a && cargo run --bin stockbit_login_page_screenshot

use std::time::Duration;

use stockbit_browser::{
    acquire_browser_session, goto_stockbit, launch_page, reset_stockbit_chrome_login_profile,
    save_debug_screenshot, BrowserLockClass, STOCKBIT_LOGIN_URL,
};
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), String> {
    let _guard = acquire_browser_session(BrowserLockClass::Interactive).await?;
    reset_stockbit_chrome_login_profile()
        .await
        .map_err(|e| e.to_string())?;

    let (browser, page) = launch_page().await.map_err(|e| e.to_string())?;
    goto_stockbit(&page, STOCKBIT_LOGIN_URL)
        .await
        .map_err(|e| e.to_string())?;
    sleep(Duration::from_secs(3)).await;
    let path = save_debug_screenshot(&page, "01_login_page")
        .await
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "(gagal simpan)".into());
    println!("Screenshot halaman login: {path}");
    browser.close().await;
    Ok(())
}
