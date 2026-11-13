//! Debug login Stockbit dengan verifikasi kode OTP email.
//! Alur: logout+clear profil → /login → isi email+password → submit → berhenti di popup OTP,
//! tahan sesi & tunggu kode di file STOCKBIT_OTP_FILE (default /tmp/stockbit_otp_code.txt),
//! ketik kode → /stream interaktif → extract bearer.
//! Jalankan: set -a && source .env && set +a && cargo run --bin stockbit_login_otp_debug

use std::time::Duration;

use stockbit_browser::{
    acquire_browser_session, debug_login_with_email_otp, extract_stockbit_bearer, launch_page,
    reset_stockbit_chrome_login_profile, BrowserLockClass,
};

#[tokio::main]
async fn main() -> Result<(), String> {
    let email = std::env::var("STOCKBIT_EMAIL").map_err(|e| e.to_string())?;
    let password = std::env::var("STOCKBIT_PASSWORD").map_err(|e| e.to_string())?;
    let otp_file = std::env::var("STOCKBIT_OTP_FILE")
        .unwrap_or_else(|_| "/tmp/stockbit_otp_code.txt".to_string());
    let otp_wait_secs: u64 = std::env::var("STOCKBIT_OTP_WAIT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(600);

    let _guard = acquire_browser_session(BrowserLockClass::Interactive).await?;
    println!("Logout Chrome Stockbit (reset profil)...");
    reset_stockbit_chrome_login_profile()
        .await
        .map_err(|e| e.to_string())?;

    let (browser, page) = launch_page().await.map_err(|e| e.to_string())?;
    let shots = debug_login_with_email_otp(
        &page,
        email.trim(),
        password.trim(),
        &otp_file,
        Duration::from_secs(otp_wait_secs),
    )
    .await
    .map_err(|e| e.to_string())?;

    println!("\n=== Screenshot urut ===");
    for (label, path) in &shots {
        println!("{label}: {path}");
    }

    match extract_stockbit_bearer(&page).await {
        Ok(token) => println!("BEARER OK (len={})", token.len()),
        Err(e) => println!("BEARER gagal: {e}"),
    }

    browser.close().await;
    Ok(())
}
