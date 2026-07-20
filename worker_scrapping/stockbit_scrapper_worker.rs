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
//! Env Redis (cache `long_name`, TTL 1 tahun): `REDIS_URL`.
//! Env Trading PIN: `STOCKBUT_PIN` (atau `STOCKBIT_PIN`).
//!
//! Setelah movers via API `order-trade/market-mover` (TOP_GAINER / TOP_LOSER)
//! → insert `emiten_trending` (termasuk `long_name`: Redis → `emiten_list` → API movers).
//! Bila PK hari ini baru (insert murni), upsert juga `emiten_trending_count_by_name`
//! (`appearance_count + 1`, `last_tahun_bulan_tanggal` = hari ini, `updated_at` = now).
//! Lalu token-ring scan `emiten_list.code_name` → Key Stats + Corp. Action + Profile API
//! (`keystats/ratio`, `corpaction`, `emitten/{CODE}/profile`) → upsert `emiten_list`
//! (hanya kolom scrape; tidak mengisi `sector`, `is_konglomerasi`, `is_fundamental_solid`,
//! `is_blue_chip`, `catatan`, `catatan_owner`, `foto_owner`).
//! Kemudian Bandar Detector via API `exodus.stockbit.com/marketdetectors/{CODE}`
//! (Bearer dari sesi login; `to` = kemarin; kolom `broker_summary` saja) → insert `bandarmology`.
//! Throttle otomatis bila `x-rate-limit-remaining` hampir habis.
//! Jika API mengembalikan HTTP 4xx: worker dihentikan segera + `pm2 resume stockbit_ws`
//! (hindari diblokir server).
//! Lalu START TRADING (PIN bila perlu) → Bearer trading pasca-PIN →
//! `GET carina.stockbit.com/portfolio/v2/list` → pastikan emiten_list + bandarmology
//! → insert `portofolio` (termasuk `long_name` dari Redis / emiten_list / company.name).
//!
//! Profil Chrome disimpan di `worker_scrapping/browser_data/` agar cookie/sesi login tetap ada antar run.

use worker_scrapping::{
    bandarmology_worker, emiten_list_worker, emiten_trending_worker, portofolio_worker,
};

use chrono::Local;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use stockbit_browser::{
    dismiss_profile_avatar_modal, launch_page, open_stream_or_login,
};

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

    let (inserted_gainer, inserted_loser, mover_codes) =
        emiten_trending_worker::scrape_and_insert_movers(&page, &session, &ks).await?;
    println!(
        "OK: emiten_trending gainer={inserted_gainer}, loser={inserted_loser} (dengan long_name)."
    );

    let today = Local::now().date_naive();
    println!("Token-ring scan emiten_list.code_name (setelah seed movers)...");
    let existing = bandarmology_worker::fetch_emiten_list_code_names(&session, &ks)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;
    let mut seen = std::collections::HashSet::new();
    let mut emitens = Vec::with_capacity(existing.len() + mover_codes.len());
    for c in &mover_codes {
        let code = c.trim().to_ascii_uppercase();
        if !code.is_empty() && seen.insert(code.clone()) {
            emitens.push(code);
        }
    }
    let movers_front = emitens.len();
    for c in &existing {
        let code = c.trim().to_ascii_uppercase();
        if !code.is_empty() && seen.insert(code.clone()) {
            emitens.push(code);
        }
    }
    println!(
        "Ditemukan {} emiten untuk key_stats/profile/corp/bandarmology (movers dulu={}, scan={}).",
        emitens.len(),
        movers_front,
        existing.len()
    );

    // Upsert scrape-only: tidak mengisi sector, is_konglomerasi, is_blue_chip,
    // is_plan_to_trade, catatan, catatan_owner, foto_owner (dan is_fundamental_solid).
    let key_stats_ok =
        emiten_list_worker::scrape_and_insert_key_stats(&page, &session, &ks, &emitens).await?;
    println!("OK: {key_stats_ok} emiten key_stats/profile diupsert ke emiten_list.");

    println!("Bandarmology via marketdetectors API (Bearer dari sesi browser)...");
    let bandar_ok =
        bandarmology_worker::scrape_and_insert_bandarmology(&page, &session, &ks, today, &emitens)
            .await
            .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;
    println!("OK: {bandar_ok} emiten diinsert ke bandarmology.");

    println!("Lanjut portofolio (PIN bila perlu → API → emiten_list + bandarmology → upsert)...");
    let porto_ok = portofolio_worker::scrape_and_insert_portofolio(&page, &session, &ks).await?;
    println!("OK: {porto_ok} baris diinsert ke portofolio.");

    let final_url = page.url().await?.unwrap_or_default();
    let final_title = page.get_title().await?.unwrap_or_default();
    println!("Siap — title: {final_title:?} | url: {final_url}");

    browser.close().await?;

    println!("Selesai.");
    pm2_guard.start_once();
    Ok(())
}
