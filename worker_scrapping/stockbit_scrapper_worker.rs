//! Worker opsional — scrap Stockbit (Chrome). Tidak dijalankan oleh PM2 / binary utama.
//!
//! ```bash
//! cargo run -p worker_scrapping
//! ```
//! Sebelum scrap: `pm2 stop stockbit_ws`. Setelah selesai (atau Ctrl+C): `pm2 start stockbit_ws`.
//! Buka langsung https://stockbit.com/stream.
//! Jika dialihkan ke https://stockbit.com/login (atau sesi habis), isi username/password natural,
//! lalu kembali ke /stream, scrap Top Gainer/Loser, insert Scylla.
//!
//! Env wajib saat perlu login: `STOCKBIT_EMAIL`, `STOCKBIT_PASSWORD`.
//! Env opsional: `CHROME_EXECUTABLE_PATH` (mis. `/usr/bin/chromium-browser`).
//! Env opsional: `STOCKBIT_2FA_TIMEOUT_SECS` (default 300 = 5 menit).
//! Env opsional: `STOCKBIT_SESSION_CHECK_SECS` (default 5) — tunggu popup sesi habis di `/stream`.
//! Env Scylla (insert `emiten_trending`, `emiten_list`, `bandarmology`, `portofolio`): `SCYLLA_URI`, `SCYLLA_KEYSPACE`, opsional `SCYLLA_USER` / `SCYLLA_PASSWORD`.
//! Env Trading PIN: `STOCKBUT_PIN` (atau `STOCKBIT_PIN`).
//!
//! Setelah movers → Top Gainer/Loser → insert `emiten_trending`.
//! Lalu MV `emiten_trending_by_tahun_bulan_tanggal` (hari ini) → Key Stats + Corp. Action + Profile → insert `emiten_list`.
//! Kemudian Bandar Detector → Last 7D / Last 1M / Last 3M / Last 1Y → insert `bandarmology` (d_7, M_1, M_3, M_12).
//! Lalu START TRADING (PIN) → Portfolio → insert `portofolio`.
//!
//! Profil Chrome disimpan di `worker_scrapping/browser_data/` agar cookie/sesi login tetap ada antar run.

use worker_scrapping::{
    bandarmology_worker, emiten_list_worker, emiten_trending_worker, portofolio_worker,
};

use chrono::Local;
use chromiumoxide::cdp::browser_protocol::page::CaptureScreenshotFormat;
use chromiumoxide::page::{Page, ScreenshotParams};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use stockbit_browser::{
    dismiss_profile_avatar_modal, goto_stockbit, launch_page, open_stream_or_login,
    STOCKBIT_STREAM_URL,
};
use tokio::time::sleep;

const PM2_APP_NAME: &str = "stockbit_ws";

fn run_pm2(args: &[&str]) -> Result<(), String> {
    let output = Command::new("pm2")
        .args(args)
        .output()
        .map_err(|e| format!("gagal menjalankan pm2 {}: {e}", args.join(" ")))?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    Err(format!(
        "pm2 {} gagal (exit {:?}): {}{}",
        args.join(" "),
        output.status.code(),
        stderr.trim(),
        if stdout.trim().is_empty() {
            String::new()
        } else {
            format!(" | {}", stdout.trim())
        }
    ))
}

fn pm2_stop_stockbit_ws() {
    println!("PM2: stop {PM2_APP_NAME}...");
    match run_pm2(&["stop", PM2_APP_NAME]) {
        Ok(()) => println!("PM2: {PM2_APP_NAME} di-stop."),
        Err(e) => eprintln!("Peringatan: {e}"),
    }
}

fn pm2_start_stockbit_ws() {
    println!("PM2: start {PM2_APP_NAME}...");
    match run_pm2(&["start", PM2_APP_NAME]) {
        Ok(()) => println!("PM2: {PM2_APP_NAME} di-start."),
        Err(e) => eprintln!("Peringatan: {e}"),
    }
}

/// Pastikan `pm2 start stockbit_ws` dijalankan sekali saat worker selesai / Ctrl+C.
struct Pm2RestartGuard {
    done: AtomicBool,
}

impl Pm2RestartGuard {
    fn arm() -> Arc<Self> {
        pm2_stop_stockbit_ws();
        Arc::new(Self {
            done: AtomicBool::new(false),
        })
    }

    fn start_once(&self) {
        if self.done.swap(true, Ordering::SeqCst) {
            return;
        }
        pm2_start_stockbit_ws();
    }
}

impl Drop for Pm2RestartGuard {
    fn drop(&mut self) {
        self.start_once();
    }
}

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

    // Stop gRPC `stockbit_ws` selama scrap; start ulang saat selesai atau Ctrl+C.
    let pm2_guard = Pm2RestartGuard::arm();
    {
        let guard = Arc::clone(&pm2_guard);
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                eprintln!("Ctrl+C diterima — start ulang PM2 {PM2_APP_NAME}...");
                guard.start_once();
                std::process::exit(130);
            }
        });
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
    let emitens = bandarmology_worker::fetch_today_emiten_names(&session, &ks, today).await?;
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
        bandarmology_worker::scrape_and_insert_bandarmology(&page, &session, &ks, today, &emitens)
            .await?;
    println!("OK: {bandar_ok} emiten diinsert ke bandarmology.");
    save_step_screenshot(&page, "09", "bandarmology").await?;

    println!("Lanjut scrape portofolio (START TRADING → PIN → Portfolio)...");
    let porto_ok = portofolio_worker::scrape_and_insert_portofolio(&page, &session, &ks).await?;
    println!("OK: {porto_ok} baris diinsert ke portofolio.");
    let screenshot_path = save_step_screenshot(&page, "10", "portofolio").await?;

    let final_url = page.url().await?.unwrap_or_default();
    let final_title = page.get_title().await?.unwrap_or_default();
    println!("Siap — title: {final_title:?} | url: {final_url}");

    browser.close().await?;

    println!(
        "Selesai. Screenshot terakhir: {}",
        screenshot_path.display()
    );
    pm2_guard.start_once();
    Ok(())
}
