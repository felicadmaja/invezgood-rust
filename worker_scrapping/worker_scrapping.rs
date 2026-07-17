//! Worker opsional — scrap Stockbit (Chrome). Tidak dijalankan oleh PM2 / binary utama.
//!
//! ```bash
//! cargo run -p worker_scrapping
//! ```
//! Buka langsung https://stockbit.com/stream.
//! Jika dialihkan ke https://stockbit.com/login (atau sesi habis), isi username/password natural,
//! lalu kembali ke /stream, scrap Top Gainer/Loser, insert Scylla.
//!
//! Env wajib saat perlu login: `STOCKBIT_EMAIL`, `STOCKBIT_PASSWORD`.
//! Env opsional: `CHROME_EXECUTABLE_PATH` (mis. `/usr/bin/chromium-browser`).
//! Env opsional: `STOCKBIT_2FA_TIMEOUT_SECS` (default 300 = 5 menit).
//! Env opsional: `STOCKBIT_SESSION_CHECK_SECS` (default 5) — tunggu popup sesi habis di `/stream`.
//! Env Scylla (insert `emiten_trending`, `emiten_list`, `bandarmology`): `SCYLLA_URI`, `SCYLLA_KEYSPACE`, opsional `SCYLLA_USER` / `SCYLLA_PASSWORD`.
//!
//! Setelah movers → Top Gainer/Loser → insert `emiten_trending`.
//! Lalu MV `emiten_trending_by_tahun_bulan_tanggal` (hari ini) → Key Stats + Corp. Action + Profile → insert `emiten_list`.
//! Kemudian Bandar Detector → Last 7D / Last 1M / Last 3M → insert `bandarmology` (d_7, M_1, M_3).
//!
//! Profil Chrome disimpan di `worker_scrapping/browser_data/` agar cookie/sesi login tetap ada antar run.

mod bandarmology;
mod emiten_list_worker;
mod emiten_trending_worker;

use chrono::Local;
use chromiumoxide::cdp::browser_protocol::page::CaptureScreenshotFormat;
use chromiumoxide::page::{Page, ScreenshotParams};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use stockbit_browser::{
    dismiss_profile_avatar_modal, goto_stockbit, launch_page, open_stream_or_login,
    STOCKBIT_STREAM_URL,
};
use tokio::time::sleep;

/// Root workspace. `CARGO_MANIFEST_DIR` = `worker_scrapping`.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".."))
}

fn screenshot_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("screenshots")
}

async fn clear_screenshot_dir() -> Result<(), Box<dyn std::error::Error>> {
    let dir = screenshot_dir();
    tokio::fs::create_dir_all(&dir).await?;
    let mut entries = match tokio::fs::read_dir(&dir).await {
        Ok(entries) => entries,
        Err(_) => return Ok(()),
    };
    let mut removed = 0usize;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_file() {
            tokio::fs::remove_file(entry.path()).await?;
            removed += 1;
        }
    }
    if removed > 0 {
        println!(
            "Screenshot lama dihapus: {removed} file dari {}",
            dir.display()
        );
    }
    Ok(())
}

async fn save_step_screenshot(
    page: &Page,
    step: &str,
    label: &str,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let dir = screenshot_dir();
    tokio::fs::create_dir_all(&dir).await?;
    let path = dir.join(format!("stockbit_{step}_{label}.png"));
    page.save_screenshot(
        ScreenshotParams::builder()
            .format(CaptureScreenshotFormat::Png)
            .build(),
        &path,
    )
    .await?;
    println!("Screenshot [{step} {label}]: {}", path.display());
    Ok(path)
}

fn keyspace() -> String {
    std::env::var("SCYLLA_KEYSPACE").unwrap_or_else(|_| "stockbit".to_string())
}

async fn connect_scylla() -> Result<Arc<Session>, Box<dyn std::error::Error>> {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let mut builder = SessionBuilder::new().known_node(uri.as_str());
    if let Ok(user) = std::env::var("SCYLLA_USER") {
        if let Ok(password) = std::env::var("SCYLLA_PASSWORD") {
            builder = builder.user(user, password);
        }
    }
    Ok(Arc::new(builder.build().await?))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workspace_env = workspace_root().join(".env");
    if workspace_env.exists() {
        let _ = dotenvy::from_path(&workspace_env);
    } else {
        dotenvy::dotenv().ok();
    }

    let email = std::env::var("STOCKBIT_EMAIL").unwrap_or_default();
    let password = std::env::var("STOCKBIT_PASSWORD").unwrap_or_default();

    clear_screenshot_dir().await?;

    let (mut browser, page) = launch_page().await.map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;

    // Awal: langsung /stream. Jika Stockbit redirect ke /login → isi username/password natural.
    open_stream_or_login(&page, &email, &password)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;

    // Safety: Skip lagi jika Profile Avatar muncul ulang sebelum klik movers.
    dismiss_profile_avatar_modal(&page)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;

    let session = connect_scylla().await?;
    let ks = keyspace();

    let (inserted_gainer, inserted_loser) =
        emiten_trending_worker::scrape_and_insert_movers(&page, &session, &ks).await?;
    save_step_screenshot(&page, "06", "movers").await?;
    println!(
        "OK: emiten_trending gainer={inserted_gainer}, loser={inserted_loser}."
    );

    let today = Local::now().date_naive();
    println!(
        "Query MV emiten_trending_by_tahun_bulan_tanggal untuk {}...",
        today.format("%Y-%m-%d")
    );
    let emitens = bandarmology::fetch_today_emiten_names(&session, &ks, today).await?;
    println!(
        "Ditemukan {} emiten unik hari ini (MV emiten_trending_by_tahun_bulan_tanggal).",
        emitens.len()
    );

    let key_stats_ok =
        emiten_list_worker::scrape_and_insert_key_stats(&page, &session, &ks, &emitens).await?;
    println!("OK: {key_stats_ok} emiten key_stats/profile diinsert ke emiten_list.");

    println!("Kembali ke /stream untuk bandarmology...");
    goto_stockbit(&page, STOCKBIT_STREAM_URL)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;
    sleep(Duration::from_secs(2)).await;
    dismiss_profile_avatar_modal(&page)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;

    let bandar_ok =
        bandarmology::scrape_and_insert_bandarmology(&page, &session, &ks, today, &emitens).await?;
    println!("OK: {bandar_ok} emiten diinsert ke bandarmology.");
    let screenshot_path = save_step_screenshot(&page, "09", "bandarmology").await?;

    let final_url = page.url().await?.unwrap_or_default();
    let final_title = page.get_title().await?.unwrap_or_default();
    println!("Siap — title: {final_title:?} | url: {final_url}");

    browser.close().await?;

    println!(
        "Selesai. Screenshot terakhir: {}",
        screenshot_path.display()
    );
    Ok(())
}
