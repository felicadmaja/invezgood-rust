//! Verifikasi sesi Stockbit TANPA reset profil (meniru ensure_stockbit_bearer):
//! reuse profil browser_data → /stream → extract bearer. Dipakai untuk konfirmasi device sudah
//! trusted & cookie sesi masih valid setelah verifikasi OTP.
//! Jalankan: set -a && source .env && set +a && cargo run --bin stockbit_session_verify

use stockbit_browser::{
    acquire_browser_session, extract_stockbit_bearer, launch_page, open_stream_or_login,
    BrowserLockClass,
};

#[tokio::main]
async fn main() -> Result<(), String> {
    let email = std::env::var("STOCKBIT_EMAIL").map_err(|e| e.to_string())?;
    let password = std::env::var("STOCKBIT_PASSWORD").map_err(|e| e.to_string())?;

    let _guard = acquire_browser_session(BrowserLockClass::Interactive).await?;
    let (browser, page) = launch_page().await.map_err(|e| e.to_string())?;

    println!("Cek sesi /stream (reuse profil, tanpa reset)...");
    open_stream_or_login(&page, email.trim(), password.trim())
        .await
        .map_err(|e| e.to_string())?;

    match extract_stockbit_bearer(&page).await {
        Ok(token) => println!("BEARER OK (len={})", token.len()),
        Err(e) => println!("BEARER gagal: {e}"),
    }

    browser.close().await;
    Ok(())
}
