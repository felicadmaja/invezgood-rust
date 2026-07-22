//! On-demand one-shot: scrape bandarmology untuk **semua** `emiten_list.emiten_name`.
//!
//! Tidak dijalankan oleh PM2 / `stockbit_scrapper_worker`. Jalankan manual:
//!
//! ```bash
//! cargo run -p worker_scrapping --bin scrap_bandarmology_all
//! # atau
//! cargo build --release -p worker_scrapping --bin scrap_bandarmology_all
//! ./target/release/scrap_bandarmology_all
//! ```
//!
//! Alur:
//! 1. Load `.env` workspace
//! 2. `pm2 stop stockbit_ws` (hindari bentrok Chrome profil)
//! 3. Login Stockbit (browser) → Bearer
//! 4. Token-ring scan `emiten_list` → semua `emiten_name`
//! 5. Hapus Redis skip-cache bandarmology + `TRUNCATE` tabel `bandarmology` (timpa data lama)
//! 6. Scrape API `exodus.stockbit.com/marketdetectors/{CODE}` per emiten → upsert Scylla
//!    (bulan berjalan + historis max 12 bulan; aturan sama `bandarmology_worker`)
//! 7. `pm2 start stockbit_ws`
//!
//! Env: `STOCKBIT_EMAIL`, `STOCKBIT_PASSWORD`, `SCYLLA_*`, `REDIS_URL`, opsional Chrome.

use chrono::Local;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use stockbit_browser::{dismiss_profile_avatar_modal, launch_page, open_stream_or_login};
use worker_scrapping::bandarmology_worker;
use worker_scrapping::redis_bandarmology_skip;

const PM2_APP_NAME: &str = "stockbit_ws";

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

struct Pm2RestartGuard {
    done: AtomicBool,
}

impl Pm2RestartGuard {
    fn arm() -> Arc<Self> {
        println!("PM2: stop {PM2_APP_NAME}...");
        match run_pm2(&["stop", PM2_APP_NAME]) {
            Ok(()) => println!("PM2: {PM2_APP_NAME} di-stop."),
            Err(e) => eprintln!("Peringatan: {e}"),
        }
        Arc::new(Self {
            done: AtomicBool::new(false),
        })
    }

    fn start_once(&self) {
        if self.done.swap(true, Ordering::SeqCst) {
            return;
        }
        println!("PM2: start {PM2_APP_NAME}...");
        match run_pm2(&["start", PM2_APP_NAME]) {
            Ok(()) => println!("PM2: {PM2_APP_NAME} di-start."),
            Err(e) => eprintln!("Peringatan: {e}"),
        }
    }
}

impl Drop for Pm2RestartGuard {
    fn drop(&mut self) {
        self.start_once();
    }
}

async fn truncate_bandarmology(
    session: &Session,
    keyspace: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let cql = format!("TRUNCATE TABLE {keyspace}.bandarmology");
    println!("Scylla: {cql}");
    session.query_unpaged(cql, &[]).await?;
    println!("Scylla: bandarmology dikosongkan (TRUNCATE).");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workspace_env = workspace_root().join(".env");
    if workspace_env.exists() {
        let _ = dotenvy::from_path(&workspace_env);
    } else {
        dotenvy::dotenv().ok();
    }

    worker_scrapping::http_abort::enable_worker_abort_on_http_4xx();

    let started_at = Local::now().format("%Y-%m-%d %H:%M:%S %z");
    println!("=== scrap_bandarmology_all started at {started_at} ===");

    let email = std::env::var("STOCKBIT_EMAIL").unwrap_or_default();
    let password = std::env::var("STOCKBIT_PASSWORD").unwrap_or_default();
    if email.trim().is_empty() || password.trim().is_empty() {
        return Err("STOCKBIT_EMAIL dan STOCKBIT_PASSWORD wajib diisi di .env".into());
    }

    let pm2_guard = Pm2RestartGuard::arm();
    {
        let guard = Arc::clone(&pm2_guard);
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                eprintln!("Ctrl+C — start ulang PM2 {PM2_APP_NAME}...");
                guard.start_once();
                std::process::exit(130);
            }
        });
    }

    let (mut browser, page) = launch_page()
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;

    open_stream_or_login(&page, &email, &password)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;
    dismiss_profile_avatar_modal(&page)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;

    let session = connect_scylla().await?;
    let ks = keyspace();

    println!("Token-ring scan {ks}.emiten_list.emiten_name...");
    let emitens = bandarmology_worker::fetch_emiten_list_emiten_names(session.as_ref(), &ks)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;
    println!("Ditemukan {} emiten_name dari emiten_list.", emitens.len());
    if emitens.is_empty() {
        eprintln!("Tidak ada emiten — selesai.");
        browser.close().await?;
        pm2_guard.start_once();
        return Ok(());
    }

    println!("Hapus Redis skip-cache bandarmology (s/d 23:59)...");
    let n_redis = redis_bandarmology_skip::clear_all_skip_keys().await;
    println!("Redis: {n_redis} key skip dihapus.");

    truncate_bandarmology(session.as_ref(), &ks).await?;

    let today = Local::now().date_naive();
    println!(
        "Scrape bandarmology API untuk {} emiten (today={today}) — sequential upsert...",
        emitens.len()
    );
    let ok = bandarmology_worker::scrape_and_insert_bandarmology(
        &page,
        &session,
        &ks,
        today,
        &emitens,
    )
    .await
    .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;

    println!(
        "Selesai scrap_bandarmology_all: {ok}/{} emiten OK (upsert ke {ks}.bandarmology).",
        emitens.len()
    );

    browser.close().await?;
    pm2_guard.start_once();
    Ok(())
}
