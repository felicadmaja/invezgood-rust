//! Buka URL Stockbit (default Web Terminal watchlist ESSA) pakai Chrome persistent yang sudah
//! login (reuse profil browser_data, tanpa reset), lalu screenshot.
//! URL override via env STOCKBIT_SHOT_URL. Jalankan:
//!   set -a && source .env && set +a && cargo run --bin stockbit_terminal_screenshot

use std::time::Duration;

use stockbit_browser::{
    acquire_browser_session, goto_stockbit, has_app_error, launch_page, save_debug_screenshot,
    BrowserLockClass,
};

#[tokio::main]
async fn main() -> Result<(), String> {
    let url = std::env::var("STOCKBIT_SHOT_URL").unwrap_or_else(|_| {
        "https://terminal.stockbit.com/main?layout=watchlist&tabName=ESSA&symbol=ESSA".to_string()
    });
    let label = std::env::var("STOCKBIT_SHOT_LABEL")
        .unwrap_or_else(|_| "terminal_essa".to_string());

    let _guard = acquire_browser_session(BrowserLockClass::Interactive).await?;
    let (browser, page) = launch_page().await.map_err(|e| e.to_string())?;

    println!("Buka URL: {url}");
    goto_stockbit(&page, &url).await.map_err(|e| e.to_string())?;

    // Beri waktu SPA Web Terminal render; reload bila crash app-error.
    tokio::time::sleep(Duration::from_secs(6)).await;
    for i in 1..=3 {
        if has_app_error(&page).await {
            println!("App error — reload ({i}/3)...");
            let _ = goto_stockbit(&page, &url).await;
            tokio::time::sleep(Duration::from_secs(5)).await;
        } else {
            break;
        }
    }
    tokio::time::sleep(Duration::from_secs(3)).await;

    let final_url = page.url().await.ok().flatten().unwrap_or_default();
    println!("URL akhir: {final_url}");

    match save_debug_screenshot(&page, &label).await {
        Some(p) => println!("Screenshot: {}", p.display()),
        None => println!("Screenshot gagal disimpan"),
    }

    browser.close().await;
    Ok(())
}
