//! One-shot: isi kolom **minggu aktif** saja di `bandarmology` untuk semua `emiten_list.emiten_name`.
//!
//! ```bash
//! cargo run -p worker_scrapping --bin bandarmology_fill_this_week
//! # atau
//! cargo build --release -p worker_scrapping --bin bandarmology_fill_this_week
//! ./target/release/bandarmology_fill_this_week
//! ```
//!
//! Alur:
//! 1. Load `.env` workspace
//! 2. `pm2 stop stockbit_ws` (hindari bentrok Chrome profil)
//! 3. Login Stockbit → Bearer
//! 4. Token-ring scan `emiten_list` → semua `emiten_name`
//! 5. API marketdetectors per emiten: **hanya slot minggu hari ini** (w1–w4 menurut tanggal)
//! 6. Force upsert Scylla `bandarmology` (abaikan `updated_at` / Redis skip)
//! 7. `pm2 start stockbit_ws`
//! Tidak menulis `portofolio_bandarmology`.
//!
//! Tidak scrape historis, tidak overwrite `broker_summary` bulan berjalan (tetap dari baris lama bila ada).
//!
//! Log: `worker_scrapping/bandarmology_fill_this_week.log` (di-clear tiap run).

use chrono::Local;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use std::fs::OpenOptions;
use std::io::{pipe, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use stockbit_browser::{dismiss_profile_avatar_modal, launch_page, open_stream_or_login};
use worker_scrapping::bandarmology_worker;

const PM2_APP_NAME: &str = "stockbit_ws";
const LOG_FILE: &str = "bandarmology_fill_this_week.log";

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".."))
}

fn keyspace() -> String {
    std::env::var("SCYLLA_KEYSPACE").unwrap_or_else(|_| "stockbit".to_string())
}

fn init_log() -> Result<(), Box<dyn std::error::Error>> {
    let log_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(LOG_FILE);
    let mut log = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&log_path)?;

    let started_at = Local::now().format("%Y-%m-%d %H:%M:%S %z");
    writeln!(
        log,
        "=== bandarmology_fill_this_week started at {started_at} ===\n"
    )?;
    log.flush()?;

    let log = Arc::new(Mutex::new(log));
    install_stdio_tee(Arc::clone(&log), libc::STDOUT_FILENO)?;
    install_stdio_tee(log, libc::STDERR_FILENO)?;

    std::thread::spawn(|| loop {
        let _ = std::io::stdout().flush();
        let _ = std::io::stderr().flush();
        std::thread::sleep(Duration::from_millis(200));
    });

    println!("Log file: {} (di-clear tiap awal run)", log_path.display());
    println!("Run started at {started_at}");
    Ok(())
}

fn install_stdio_tee(
    log: Arc<Mutex<std::fs::File>>,
    target_fd: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let (mut reader, writer) = pipe()?;
    let writer_fd = writer.as_raw_fd();

    let orig_fd = unsafe { libc::dup(target_fd) };
    if orig_fd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    if unsafe { libc::dup2(writer_fd, target_fd) } < 0 {
        unsafe {
            libc::close(orig_fd);
        }
        return Err(std::io::Error::last_os_error().into());
    }
    std::mem::forget(writer);

    std::thread::spawn(move || {
        let mut console = unsafe { std::fs::File::from_raw_fd(orig_fd) };
        let mut buf = [0u8; 8192];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    let chunk = &buf[..n];
                    let _ = console.write_all(chunk);
                    let _ = console.flush();
                    if let Ok(mut f) = log.lock() {
                        let _ = f.write_all(chunk);
                        let _ = f.flush();
                    }
                }
                Err(_) => break,
            }
        }
    });

    Ok(())
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workspace_env = workspace_root().join(".env");
    if workspace_env.exists() {
        let _ = dotenvy::from_path(&workspace_env);
    } else {
        dotenvy::dotenv().ok();
    }

    worker_scrapping::http_abort::enable_worker_abort_on_http_4xx();
    init_log()?;

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

    let today = Local::now().date_naive();

    let ok = bandarmology_worker::scrape_and_insert_this_week_only(
        &page,
        &session,
        &ks,
        today,
        &emitens,
    )
    .await
    .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;

    println!(
        "Selesai bandarmology_fill_this_week: bandarmology={ok}/{} (minggu aktif, today={today}).",
        emitens.len()
    );

    browser.close().await?;
    pm2_guard.start_once();
    Ok(())
}
