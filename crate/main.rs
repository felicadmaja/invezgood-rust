//! ```bash
//! cargo run -p create_database --bin stockbit_ws
//! ```
//! Buka Stockbit; jika sesi masih aktif langsung ke https://stockbit.com/stream (skip login).
//! Jika di halaman login: isi credential dari `.env`, klik Login,
//! lalu tunggu approve 2-Step Verification di aplikasi Stockbit (HP),
//! kemudian masuk ke /stream dan ambil screenshot.
//!
//! Env wajib saat perlu login: `STOCKBIT_EMAIL`, `STOCKBIT_PASSWORD`.
//! Env opsional: `CHROME_EXECUTABLE_PATH` (mis. `/usr/bin/chromium-browser`).
//! Env opsional: `STOCKBIT_2FA_TIMEOUT_SECS` (default 300 = 5 menit).
//!
//! Profil Chrome disimpan di `browser_data/` (root workspace) agar cookie/sesi login tetap ada antar run.

use chromiumoxide::browser::{Browser, BrowserConfig};
use chromiumoxide::cdp::browser_protocol::page::CaptureScreenshotFormat;
use chromiumoxide::page::{Page, ScreenshotParams};
use futures::StreamExt;
use rand::Rng;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::time::sleep;

const STOCKBIT_STREAM_URL: &str = "https://stockbit.com/stream";
const STOCKBIT_LOGIN_URL: &str = "https://stockbit.com/login";
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

fn is_login_url(url: &str) -> bool {
    url.contains("/login")
}

fn is_stream_url(url: &str) -> bool {
    url.contains("/stream")
}

fn is_2fa_pending_url(url: &str) -> bool {
    url.contains("/trusted-device") || url.contains("/two-factor") || url.contains("/2fa")
}

/// Root workspace. `CARGO_MANIFEST_DIR` = `crate/create_database`.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.."))
}

fn screenshot_dir() -> PathBuf {
    workspace_root().join("screenshots")
}

fn browser_data_dir() -> PathBuf {
    workspace_root().join("browser_data")
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

fn browser_config() -> Result<BrowserConfig, Box<dyn std::error::Error>> {
    let data_dir = browser_data_dir();
    std::fs::create_dir_all(&data_dir)?;

    let mut builder = BrowserConfig::builder()
        .user_data_dir(&data_dir)
        .args([
            "--headless=new",
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
            "--window-size=1440,900",
            "--no-first-run",
            "--no-default-browser-check",
        ]);

    if let Ok(path) = std::env::var("CHROME_EXECUTABLE_PATH") {
        if !path.is_empty() {
            builder = builder.chrome_executable(Path::new(&path));
        }
    }

    Ok(builder.build()?)
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

async fn type_naturally(
    page: &Page,
    selector: &str,
    value: &str,
    label: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let element = page.find_element(selector).await.map_err(|_| {
        format!("Error: Elemen {selector} ({label}) tidak ditemukan di halaman!")
    })?;

    element.click().await?;
    sleep(Duration::from_millis(400)).await;

    println!("Mulai mengetik {label} secara natural...");
    for karakter in value.chars() {
        let delay_acak = rand::thread_rng().gen_range(80..220);
        sleep(Duration::from_millis(delay_acak)).await;
        element.type_str(&karakter.to_string()).await?;
    }
    println!("{label} selesai dimasukkan.");
    Ok(())
}

/// True jika form login (#username) terlihat di halaman.
async fn has_login_form(page: &Page) -> bool {
    page.find_element("#username").await.is_ok()
}

/// Tunggu sampai user selesai approve 2-Step Verification di aplikasi Stockbit (HP).
async fn wait_for_2fa_phone_approval(
    page: &Page,
    timeout: Duration,
) -> Result<String, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let poll = Duration::from_secs(2);
    let mut last_url = String::new();

    println!();
    println!("=== 2-Step Verification ===");
    println!("Buka app Stockbit di handphone, lalu tap 'Yes, It's Me'.");
    println!(
        "Script menunggu sampai verifikasi selesai (timeout {} detik)...",
        timeout.as_secs()
    );
    println!();

    loop {
        let url = page.url().await?.unwrap_or_default();
        if url != last_url {
            println!("URL saat ini: {url}");
            last_url = url.clone();
        }

        if !url.is_empty() && !is_2fa_pending_url(&url) {
            sleep(Duration::from_secs(2)).await;
            let final_url = page.url().await?.unwrap_or(url);
            println!("2FA selesai — URL: {final_url}");
            return Ok(final_url);
        }

        if started.elapsed() >= timeout {
            save_step_screenshot(page, "04b", "2fa_timeout").await?;
            return Err(format!(
                "Timeout menunggu 2FA setelah {} detik. Approve di HP lalu jalankan ulang, atau naikkan STOCKBIT_2FA_TIMEOUT_SECS.",
                timeout.as_secs()
            )
            .into());
        }

        let elapsed = started.elapsed().as_secs();
        if elapsed > 0 && elapsed % 15 == 0 {
            println!(
                "... masih menunggu approve di HP ({elapsed}/{} dtk)",
                timeout.as_secs()
            );
        }

        sleep(poll).await;
    }
}

async fn perform_login(
    page: &Page,
    email: &str,
    password: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if email.is_empty() || password.is_empty() {
        return Err("STOCKBIT_EMAIL dan STOCKBIT_PASSWORD wajib diisi di .env".into());
    }

    type_naturally(page, "#username", email, "email/username").await?;
    sleep(Duration::from_millis(400)).await;
    save_step_screenshot(page, "02", "setelah_username").await?;

    type_naturally(page, "#password", password, "password").await?;
    sleep(Duration::from_millis(400)).await;
    save_step_screenshot(page, "03", "setelah_password").await?;

    sleep(Duration::from_millis(800)).await;

    if let Ok(btn) = page.find_element("#email-login-button").await {
        println!("Menekan tombol Login...");
        btn.click().await?;
    } else {
        return Err("Error: Tombol Login (#email-login-button) tidak ditemukan!".into());
    }

    println!("Menunggu halaman pasca-login...");
    sleep(Duration::from_secs(3)).await;

    let after_url = page.url().await?.unwrap_or_default();
    let after_title = page.get_title().await?.unwrap_or_default();
    println!("Setelah klik Login — title: {after_title:?} | url: {after_url}");
    save_step_screenshot(page, "04", "2fa_prompt").await?;

    let timeout_secs: u64 = std::env::var("STOCKBIT_2FA_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);

    if is_2fa_pending_url(&after_url) {
        wait_for_2fa_phone_approval(page, Duration::from_secs(timeout_secs)).await?;
    } else {
        println!("Tidak ada halaman 2FA — lanjut.");
    }

    Ok(())
}

/// Tutup modal "New! Profile Avatar" dengan klik tombol Skip (jika muncul).
async fn dismiss_profile_avatar_modal(page: &Page) -> Result<bool, Box<dyn std::error::Error>> {
    // Tunggu sebentar jika modal muncul lambat setelah /stream load.
    for attempt in 1..=8 {
        let clicked = page
            .evaluate(
                r#"(() => {
                    const texts = ['Skip', 'Lewati'];
                    const candidates = Array.from(
                        document.querySelectorAll('button, [role="button"], a')
                    );
                    const target = candidates.find((el) => {
                        const t = (el.innerText || el.textContent || '').trim();
                        return texts.some((x) => t === x || t.includes(x));
                    });
                    if (!target) return false;
                    target.click();
                    return true;
                })()"#,
            )
            .await?
            .into_value::<bool>()
            .unwrap_or(false);

        if clicked {
            println!("Modal Profile Avatar: tombol Skip diklik (attempt {attempt}).");
            sleep(Duration::from_secs(1)).await;
            return Ok(true);
        }
        sleep(Duration::from_millis(500)).await;
    }

    println!("Modal Profile Avatar: tombol Skip tidak ditemukan (mungkin sudah tertutup).");
    Ok(false)
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

    let config = browser_config()?;
    let (mut browser, mut handler) = Browser::launch(config).await?;

    tokio::task::spawn(async move {
        while let Some(h) = handler.next().await {
            if h.is_err() {
                break;
            }
        }
    });

    let page = browser.new_page("about:blank").await?;
    page.set_user_agent(USER_AGENT).await?;

    let stealth_script = r#"
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
    "#;
    page.evaluate_on_new_document(stealth_script).await?;

    // Cek sesi lewat /stream: jika belum login biasanya redirect ke /login.
    println!("Membuka {STOCKBIT_STREAM_URL} untuk cek sesi login...");
    page.goto(STOCKBIT_STREAM_URL).await?;
    sleep(Duration::from_secs(3)).await;

    let mut url = page.url().await?.unwrap_or_default();
    let title = page.get_title().await?.unwrap_or_default();
    println!("Halaman awal — title: {title:?} | url: {url}");
    save_step_screenshot(&page, "01", "cek_sesi").await?;

    if is_2fa_pending_url(&url) {
        println!("Sesi di tengah 2FA — menunggu approve di HP...");
        let timeout_secs: u64 = std::env::var("STOCKBIT_2FA_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300);
        wait_for_2fa_phone_approval(&page, Duration::from_secs(timeout_secs)).await?;
        url = page.url().await?.unwrap_or_default();
    }

    let need_login = is_login_url(&url) || has_login_form(&page).await;

    if need_login {
        println!("Belum login (halaman login). Mengisi username/password...");
        if !is_login_url(&url) {
            page.goto(STOCKBIT_LOGIN_URL).await?;
            sleep(Duration::from_secs(2)).await;
        }
        perform_login(&page, &email, &password).await?;

        println!("Login selesai — membuka {STOCKBIT_STREAM_URL} ...");
        page.goto(STOCKBIT_STREAM_URL).await?;
        sleep(Duration::from_secs(3)).await;
    } else if is_stream_url(&url) {
        println!("Sudah login — langsung di /stream. Skip isi username/password.");
    } else {
        println!("Sesi terdeteksi, tapi belum di /stream — membuka {STOCKBIT_STREAM_URL} ...");
        page.goto(STOCKBIT_STREAM_URL).await?;
        sleep(Duration::from_secs(3)).await;

        let again = page.url().await?.unwrap_or_default();
        if is_login_url(&again) || has_login_form(&page).await {
            println!("Redirect ke login — mengisi username/password...");
            perform_login(&page, &email, &password).await?;
            page.goto(STOCKBIT_STREAM_URL).await?;
            sleep(Duration::from_secs(3)).await;
        }
    }

    sleep(Duration::from_secs(2)).await;
    dismiss_profile_avatar_modal(&page).await?;

    println!("Mengklik right-menu-movers...");
    if let Ok(btn) = page.find_element(r#"[data-cy="right-menu-movers"]"#).await {
        btn.click().await?;
        sleep(Duration::from_secs(2)).await;
    } else {
        return Err("Error: Tombol [data-cy=\"right-menu-movers\"] tidak ditemukan!".into());
    }

    let final_url = page.url().await?.unwrap_or_default();
    let final_title = page.get_title().await?.unwrap_or_default();
    println!("Siap — title: {final_title:?} | url: {final_url}");

    let screenshot_path = save_step_screenshot(&page, "06", "movers").await?;

    browser.close().await?;

    println!("Selesai. Screenshot terakhir: {}", screenshot_path.display());
    Ok(())
}
