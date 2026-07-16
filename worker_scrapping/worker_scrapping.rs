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
//! Env Scylla (insert `emiten_trending` + `bandarmology`): `SCYLLA_URI`, `SCYLLA_KEYSPACE`, opsional `SCYLLA_USER` / `SCYLLA_PASSWORD`.
//!
//! Setelah movers → Top Gainer/Loser → insert `emiten_trending`.
//! Lalu MV `emiten_trending_by_tahun_bulan_tanggal` (hari ini) → Bandar Detector →
//! period Latest/Prev Day/Last 7D/1M/3M/6M/1Y → insert `bandarmology` (d_1..M_12).
//!
//! Profil Chrome disimpan di `worker_scrapping/browser_data/` agar cookie/sesi login tetap ada antar run.

mod bandarmology;

use chrono::Local;
use chromiumoxide::browser::{Browser, BrowserConfig};
use chromiumoxide::cdp::browser_protocol::page::CaptureScreenshotFormat;
use chromiumoxide::page::{Page, ScreenshotParams};
use futures::StreamExt;
use rand::Rng;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

const STOCKBIT_STREAM_URL: &str = "https://stockbit.com/stream";
const STOCKBIT_LOGIN_URL: &str = "https://stockbit.com/login";
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
/// Batas tunggu navigasi `goto` / fallback (detik). Default chromiumoxide ~30s terlalu lama.
const NAV_TIMEOUT_SECS: u64 = 10;

fn is_login_url(url: &str) -> bool {
    url.contains("/login")
}

fn is_stream_url(url: &str) -> bool {
    url.contains("/stream")
}

fn is_2fa_pending_url(url: &str) -> bool {
    url.contains("/trusted-device") || url.contains("/two-factor") || url.contains("/2fa")
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

fn browser_data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("browser_data")
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
    clear_stale_chrome_locks(&data_dir);

    let mut builder = BrowserConfig::builder()
        .user_data_dir(&data_dir)
        .request_timeout(Duration::from_secs(120))
        .launch_timeout(Duration::from_secs(60))
        .args([
            "--headless=new",
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
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

/// Hapus SingletonLock/Socket stale agar launch tidak macet setelah crash sebelumnya.
fn clear_stale_chrome_locks(data_dir: &Path) {
    for name in ["SingletonLock", "SingletonSocket", "SingletonCookie"] {
        let p = data_dir.join(name);
        if p.exists() {
            let _ = std::fs::remove_file(&p);
        }
    }
}

/// Navigate yang lebih tahan untuk SPA (Stockbit): `goto` dibatasi [`NAV_TIMEOUT_SECS`].
/// Jika `expect_path` diisi (mis. `"/login"`), wajib URL mengandung path itu — jangan anggap sukses
/// hanya karena masih di domain stockbit.com (mis. stuck di `/stream` + modal sesi).
async fn goto_stockbit(
    page: &Page,
    url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    goto_stockbit_expect(page, url, None).await
}

async fn goto_stockbit_expect(
    page: &Page,
    url: &str,
    expect_path: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let nav_timeout = Duration::from_secs(NAV_TIMEOUT_SECS);
    let path_ok = |current: &str| -> bool {
        if let Some(p) = expect_path {
            current.contains(p)
        } else {
            url_looks_navigated(current)
        }
    };

    match tokio::time::timeout(nav_timeout, page.goto(url)).await {
        Ok(Ok(_)) => {
            let current = page.url().await.ok().flatten().unwrap_or_default();
            if expect_path.is_none() || path_ok(&current) {
                return Ok(());
            }
            println!("goto selesai tapi URL belum {expect_path:?} (sekarang: {current}) — force assign...");
        }
        Ok(Err(e)) => {
            let msg = e.to_string();
            let current = page.url().await.ok().flatten().unwrap_or_default();
            if path_ok(&current) {
                println!("goto warning ({msg}) — lanjut, URL: {current}");
                return Ok(());
            }
            println!("goto gagal ({msg}, URL: {current}) — location.assign...");
        }
        Err(_) => {
            let current = page.url().await.ok().flatten().unwrap_or_default();
            if path_ok(&current) {
                println!("goto timeout {NAV_TIMEOUT_SECS}s — lanjut, URL: {current}");
                return Ok(());
            }
            println!(
                "goto timeout {NAV_TIMEOUT_SECS}s (URL masih: {current}) — location.assign..."
            );
        }
    }

    force_location_assign(page, url).await?;

    let started = Instant::now();
    loop {
        sleep(Duration::from_millis(400)).await;
        let current = page.url().await?.unwrap_or_default();
        if path_ok(&current) {
            println!("Navigasi OK — URL: {current}");
            return Ok(());
        }
        if started.elapsed() >= nav_timeout {
            // Coba lagi sekali dengan replace.
            force_location_replace(page, url).await?;
            sleep(Duration::from_secs(2)).await;
            let current = page.url().await?.unwrap_or_default();
            if path_ok(&current) {
                println!("Navigasi OK setelah replace — URL: {current}");
                return Ok(());
            }
            return Err(format!(
                "Timeout navigasi ke {url} (expect={expect_path:?}); URL sekarang: {current}"
            )
            .into());
        }
    }
}

async fn force_location_assign(page: &Page, url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let escaped = url.replace('\\', "\\\\").replace('"', "\\\"");
    page.evaluate(format!(r#"window.location.assign("{escaped}")"#))
        .await?;
    Ok(())
}

async fn force_location_replace(page: &Page, url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let escaped = url.replace('\\', "\\\\").replace('"', "\\\"");
    page.evaluate(format!(r#"window.location.replace("{escaped}")"#))
        .await?;
    Ok(())
}

fn url_looks_navigated(current: &str) -> bool {
    !current.is_empty()
        && (current.contains("stockbit.com")
            || current.contains("/login")
            || current.contains("/stream")
            || current.contains("/trusted-device"))
}

/// Tunggu sampai form login (#username) siap.
async fn wait_for_login_form(page: &Page, timeout: Duration) -> Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();
    while started.elapsed() < timeout {
        if has_login_form(page).await {
            return Ok(());
        }
        sleep(Duration::from_millis(400)).await;
    }
    Err("Form login (#username) tidak muncul".into())
}

/// Klik CTA popup sesi habis bila ada (membantu lepas dari modal sebelum ke /login).
async fn click_session_expired_cta(page: &Page) -> Result<bool, Box<dyn std::error::Error>> {
    let clicked = page
        .evaluate(
            r#"(() => {
                const candidates = Array.from(
                    document.querySelectorAll('button, [role="button"], a')
                );
                const target = candidates.find((el) => {
                    const t = (el.innerText || el.textContent || '').trim();
                    return t.includes('Kembali ke Halaman Utama');
                });
                if (!target) return false;
                target.click();
                return true;
            })()"#,
        )
        .await?
        .into_value::<bool>()
        .unwrap_or(false);
    Ok(clicked)
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
    let element = page
        .find_element(selector)
        .await
        .map_err(|_| format!("Error: Elemen {selector} ({label}) tidak ditemukan di halaman!"))?;

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

/// True jika popup "New! Profile Avatar" terlihat.
async fn has_profile_avatar_modal(page: &Page) -> bool {
    page.evaluate(
        r#"(() => {
            const body = (document.body && document.body.innerText) || '';
            return body.includes('New! Profile Avatar');
        })()"#,
    )
    .await
    .ok()
    .and_then(|v| v.into_value::<bool>().ok())
    .unwrap_or(false)
}

/// Tunggu sampai user selesai approve 2-Step Verification di aplikasi Stockbit (HP).
/// Jika muncul popup "New! Profile Avatar", anggap login sudah sukses — klik Skip, jangan tunggu 2FA.
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
    println!("Jika muncul popup 'New! Profile Avatar' → Skip, tanpa tunggu 2FA.");
    println!();

    loop {
        if has_profile_avatar_modal(page).await {
            println!("Popup 'New! Profile Avatar' terdeteksi saat tunggu 2FA — Skip, lewati 2FA.");
            dismiss_profile_avatar_modal(page).await?;
            let final_url = page.url().await?.unwrap_or_default();
            return Ok(final_url);
        }

        let url = page.url().await?.unwrap_or_default();
        if url != last_url {
            println!("URL saat ini: {url}");
            last_url = url.clone();
        }

        if !url.is_empty() && !is_2fa_pending_url(&url) {
            // Mungkin sudah masuk /stream dengan Profile Avatar — cek sekali lagi.
            if has_profile_avatar_modal(page).await {
                println!("Sudah di luar 2FA + Profile Avatar — Skip.");
                dismiss_profile_avatar_modal(page).await?;
            }
            sleep(Duration::from_secs(2)).await;
            let final_url = page.url().await?.unwrap_or(url);
            println!("2FA selesai / tidak diperlukan — URL: {final_url}");
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

    // Profile Avatar = login sudah sukses, tidak perlu tunggu 2FA.
    if has_profile_avatar_modal(page).await {
        println!(
            "Popup 'New! Profile Avatar' muncul di tahap 2fa_prompt — klik Skip, lewati tunggu 2FA."
        );
        dismiss_profile_avatar_modal(page).await?;
        return Ok(());
    }

    let timeout_secs: u64 = std::env::var("STOCKBIT_2FA_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);

    if is_2fa_pending_url(&after_url) {
        wait_for_2fa_phone_approval(page, Duration::from_secs(timeout_secs)).await?;
    } else if is_stream_url(&after_url) {
        // Kadang Profile Avatar muncul sedikit terlambat setelah screenshot.
        println!("Sudah di /stream — cek singkat Profile Avatar / 2FA...");
        for _ in 0..10 {
            if has_profile_avatar_modal(page).await {
                println!("Profile Avatar muncul — Skip, lewati 2FA.");
                dismiss_profile_avatar_modal(page).await?;
                break;
            }
            if is_2fa_pending_url(&page.url().await?.unwrap_or_default()) {
                wait_for_2fa_phone_approval(page, Duration::from_secs(timeout_secs)).await?;
                break;
            }
            sleep(Duration::from_millis(500)).await;
        }
    } else {
        println!("Tidak ada halaman 2FA — lanjut.");
    }

    Ok(())
}

/// Tutup modal "New! Profile Avatar" dengan klik tombol Skip (jika muncul).
async fn dismiss_profile_avatar_modal(page: &Page) -> Result<bool, Box<dyn std::error::Error>> {
    // Tunggu sebentar jika modal muncul lambat setelah /stream load.
    for attempt in 1..=8 {
        let has_modal = has_profile_avatar_modal(page).await;
        let clicked = page
            .evaluate(
                r#"(() => {
                    const body = (document.body && document.body.innerText) || '';
                    const avatarModal = body.includes('New! Profile Avatar');
                    const texts = ['Skip', 'Lewati'];
                    const candidates = Array.from(
                        document.querySelectorAll('button, [role="button"], a')
                    );
                    const target = candidates.find((el) => {
                        const t = (el.innerText || el.textContent || '').trim();
                        return texts.some((x) => t === x || t.includes(x));
                    });
                    // Jika modal avatar terdeteksi, wajib Skip; kalau tidak, Skip generik juga OK.
                    if (!target) return false;
                    if (avatarModal || texts.some((x) => (target.innerText || '').includes(x))) {
                        target.click();
                        return true;
                    }
                    return false;
                })()"#,
            )
            .await?
            .into_value::<bool>()
            .unwrap_or(false);

        if clicked {
            println!(
                "Modal Profile Avatar: tombol Skip diklik (attempt {attempt}, modal={has_modal})."
            );
            sleep(Duration::from_secs(1)).await;
            return Ok(true);
        }
        sleep(Duration::from_millis(500)).await;
    }

    println!("Modal Profile Avatar: tombol Skip tidak ditemukan (mungkin sudah tertutup).");
    Ok(false)
}

/// True jika popup "Sesi Kamu Sudah Habis" terlihat di DOM.
async fn has_session_expired_modal(page: &Page) -> bool {
    page.evaluate(
        r#"(() => {
            const body = (document.body && document.body.innerText) || '';
            return body.includes('Sesi Kamu Sudah Habis');
        })()"#,
    )
    .await
    .ok()
    .and_then(|v| v.into_value::<bool>().ok())
    .unwrap_or(false)
}

/// Tangani sesi habis / redirect login: pastikan benar-benar di `/login` + `#username`, lalu isi natural.
async fn login_and_return_to_stream(
    page: &Page,
    email: &str,
    password: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Lepas modal sesi habis dulu (jika ada), lalu paksa ke /login.
    if has_session_expired_modal(page).await {
        if click_session_expired_cta(page).await? {
            println!("CTA 'Kembali ke Halaman Utama' diklik.");
            sleep(Duration::from_secs(1)).await;
        }
    }

    if !has_login_form(page).await {
        println!("Membuka {STOCKBIT_LOGIN_URL} untuk login...");
        goto_stockbit_expect(page, STOCKBIT_LOGIN_URL, Some("/login")).await?;
    }

    wait_for_login_form(page, Duration::from_secs(15)).await?;
    println!("Form login siap — mengisi username/password secara natural...");
    perform_login(page, email, password).await?;

    println!("Login selesai — membuka {STOCKBIT_STREAM_URL} ...");
    goto_stockbit_expect(page, STOCKBIT_STREAM_URL, Some("/stream")).await?;
    sleep(Duration::from_secs(2)).await;
    Ok(())
}

/// Alur awal: akses `/stream`. Jika dialihkan ke `/login`, popup sesi habis, atau form login
/// muncul → isi username/password natural lalu kembali ke `/stream`.
async fn open_stream_or_login(
    page: &Page,
    email: &str,
    password: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Membuka {STOCKBIT_STREAM_URL} ...");
    goto_stockbit(page, STOCKBIT_STREAM_URL).await?;
    sleep(Duration::from_secs(1)).await;

    let url = page.url().await?.unwrap_or_default();
    let title = page.get_title().await?.unwrap_or_default();
    println!("Halaman awal — title: {title:?} | url: {url}");
    save_step_screenshot(page, "01", "cek_sesi").await?;

    // Poll singkat: redirect login / popup sesi habis / Profile Avatar.
    let wait_secs: u64 = std::env::var("STOCKBIT_SESSION_CHECK_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);
    let started = Instant::now();
    let mut need_login = is_login_url(&url) || has_login_form(page).await;

    while started.elapsed() < Duration::from_secs(wait_secs) {
        if has_profile_avatar_modal(page).await {
            println!("Popup 'New! Profile Avatar' — klik Skip.");
            dismiss_profile_avatar_modal(page).await?;
            break;
        }
        if has_session_expired_modal(page).await {
            println!("Popup 'Sesi Kamu Sudah Habis' — login ulang.");
            save_step_screenshot(page, "01b", "sesi_habis").await?;
            need_login = true;
            break;
        }
        let u = page.url().await?.unwrap_or_default();
        if is_login_url(&u) || has_login_form(page).await {
            println!("Dialihkan ke halaman login ({u}).");
            need_login = true;
            break;
        }
        if is_stream_url(&u) {
            // Tetap di stream tanpa popup bermasalah — lanjut poll sisa waktu singkat.
        }
        sleep(Duration::from_millis(400)).await;
    }

    // Cek sekali lagi setelah poll.
    let url = page.url().await?.unwrap_or_default();
    if !need_login {
        need_login = is_login_url(&url)
            || has_login_form(page).await
            || has_session_expired_modal(page).await;
    }

    if need_login {
        login_and_return_to_stream(page, email, password).await?;
    } else if is_stream_url(&url) {
        println!("Sudah di /stream dengan sesi aktif.");
    } else {
        println!("URL belum /stream ({url}) — membuka ulang {STOCKBIT_STREAM_URL} ...");
        goto_stockbit(page, STOCKBIT_STREAM_URL).await?;
        sleep(Duration::from_secs(1)).await;
        let again = page.url().await?.unwrap_or_default();
        if is_login_url(&again) || has_login_form(page).await {
            login_and_return_to_stream(page, email, password).await?;
        }
    }

    if has_profile_avatar_modal(page).await {
        dismiss_profile_avatar_modal(page).await?;
    }

    let final_url = page.url().await?.unwrap_or_default();
    if !is_stream_url(&final_url) {
        return Err(format!(
            "Gagal masuk /stream setelah cek sesi (URL: {final_url})"
        )
        .into());
    }
    Ok(())
}

#[derive(Debug, Clone, Deserialize)]
struct MoversRow {
    symbol: String,
    price: String,
    value: String,
    volume: String,
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

/// Normalisasi symbol: huruf saja, uppercase (contoh `kblv` → `KBLV`).
fn normalize_emiten_name(raw: &str) -> String {
    raw.chars()
        .filter(|c| c.is_ascii_alphabetic())
        .map(|c| c.to_ascii_uppercase())
        .collect()
}

async fn click_mover_tab(page: &Page, label: &str) -> Result<(), Box<dyn std::error::Error>> {
    for attempt in 1..=10 {
        // Prefer id prefix MOVER_TYPE_* bila ada; fallback teks label di <p>.
        let clicked = page
            .evaluate(format!(
                r#"(() => {{
                    const label = {label_js};
                    const byId = Array.from(document.querySelectorAll('[id^="MOVER_TYPE_"]'))
                        .find((el) => {{
                            const t = (el.innerText || el.textContent || '').trim();
                            return t === label || t.includes(label);
                        }});
                    if (byId) {{ byId.click(); return true; }}
                    const nodes = Array.from(document.querySelectorAll('p, span, button, div, a'));
                    const target = nodes.find((el) => {{
                        const t = (el.innerText || el.textContent || '').trim();
                        return t === label;
                    }});
                    if (!target) return false;
                    target.click();
                    return true;
                }})()"#,
                label_js = serde_json::to_string(label).unwrap_or_else(|_| format!("\"{label}\""))
            ))
            .await?
            .into_value::<bool>()
            .unwrap_or(false);
        if clicked {
            println!("'{label}' diklik (attempt {attempt}).");
            return Ok(());
        }
        sleep(Duration::from_millis(500)).await;
    }
    Err(format!("Elemen '{label}' tidak ditemukan").into())
}

/// Scrape tabel movers (kolom Symbol / Price / Value / Volume).
async fn scrape_movers_table(page: &Page) -> Result<Vec<MoversRow>, Box<dyn std::error::Error>> {
    // Tunggu sampai ada baris tbody.
    for _ in 0..20 {
        let ready = page
            .evaluate(
                r#"(() => {
                    const table = document.querySelector('table');
                    if (!table) return false;
                    return table.querySelectorAll('tbody tr').length > 0;
                })()"#,
            )
            .await?
            .into_value::<bool>()
            .unwrap_or(false);
        if ready {
            break;
        }
        sleep(Duration::from_millis(500)).await;
    }

    let json = page
        .evaluate(
            r#"(() => {
                const table = document.querySelector('table');
                if (!table) return '[]';
                const rows = Array.from(table.querySelectorAll('tbody tr'));
                const out = rows.map((tr) => {
                    const tds = Array.from(tr.querySelectorAll('td'));
                    // Kolom data (lewati gap-head kosong): symbol, price, value, volume, freq
                    const cells = tds.filter((td) => {
                        const w = td.getAttribute('style') || '';
                        const text = (td.innerText || '').trim();
                        // gap columns sering width 106.5px dan kosong
                        if (!text) return false;
                        return true;
                    });
                    const symbolEl = tr.querySelector('.symbol span, .symbol');
                    let symbol = symbolEl
                        ? (symbolEl.innerText || '').trim().split(/\s+/)[0]
                        : '';
                    if (!symbol && cells[0]) {
                        symbol = (cells[0].innerText || '').trim().split(/\s+/)[0];
                    }
                    const priceCell = cells[1] || null;
                    let price = '';
                    if (priceCell) {
                        const spans = priceCell.querySelectorAll('span');
                        price = spans.length > 0
                            ? (spans[0].innerText || '').trim()
                            : (priceCell.innerText || '').trim().split(/\s+/)[0];
                    }
                    const value = cells[2] ? (cells[2].innerText || '').trim() : '';
                    const volume = cells[3] ? (cells[3].innerText || '').trim() : '';
                    return { symbol, price, value, volume };
                }).filter((r) => r.symbol);
                return JSON.stringify(out);
            })()"#,
        )
        .await?
        .into_value::<String>()
        .unwrap_or_else(|_| "[]".to_string());

    let rows: Vec<MoversRow> = serde_json::from_str(&json)?;
    Ok(rows)
}

async fn insert_emiten_trending(
    session: &Session,
    rows: &[MoversRow],
    gainer_or_loser: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let ks = keyspace();
    let today = Local::now().date_naive();
    let date_str = today.format("%Y-%m-%d").to_string();

    let insert = session
        .prepare(format!(
            "INSERT INTO {ks}.emiten_trending (\
                agg_tahun_bulan_tanggal_emiten_name, \
                tahun_bulan_tanggal, \
                gainer_or_loser, \
                emiten_name, \
                price, \
                value, \
                volume\
            ) VALUES (?, ?, ?, ?, ?, ?, ?)"
        ))
        .await?;

    let mut n = 0usize;
    for row in rows {
        let emiten = normalize_emiten_name(&row.symbol);
        if emiten.is_empty() {
            continue;
        }
        let agg = format!("{date_str}_{emiten}");
        session
            .execute_unpaged(
                &insert,
                (
                    agg.as_str(),
                    today,
                    gainer_or_loser,
                    emiten.as_str(),
                    row.price.as_str(),
                    row.value.as_str(),
                    row.volume.as_str(),
                ),
            )
            .await?;
        n += 1;
    }
    Ok(n)
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

    // Handler wajib tetap jalan sampai browser putus; jangan `break` pada event error
    // non-fatal — itu menyebabkan Timeout / oneshot canceled pada goto/click.
    tokio::task::spawn(async move { while handler.next().await.is_some() {} });

    let page = browser.new_page("about:blank").await?;
    page.set_user_agent(USER_AGENT).await?;

    let stealth_script = r#"
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
    "#;
    page.evaluate_on_new_document(stealth_script).await?;

    // Awal: langsung /stream. Jika Stockbit redirect ke /login → isi username/password natural.
    open_stream_or_login(&page, &email, &password).await?;

    // Safety: Skip lagi jika Profile Avatar muncul ulang sebelum klik movers.
    dismiss_profile_avatar_modal(&page).await?;

    println!("Mengklik right-menu-movers...");
    if let Ok(btn) = page.find_element(r#"[data-cy="right-menu-movers"]"#).await {
        btn.click().await?;
        sleep(Duration::from_secs(2)).await;
    } else {
        return Err("Error: Tombol [data-cy=\"right-menu-movers\"] tidak ditemukan!".into());
    }
    save_step_screenshot(&page, "06", "movers").await?;

    println!("Mengklik Top Gainer...");
    click_mover_tab(&page, "Top Gainer").await?;
    sleep(Duration::from_secs(2)).await;
    save_step_screenshot(&page, "07", "top_gainer").await?;

    let gainer_rows = scrape_movers_table(&page).await?;
    println!("Top Gainer: {} baris di-scrape.", gainer_rows.len());
    if gainer_rows.is_empty() {
        return Err("Tabel Top Gainer kosong / tidak terbaca".into());
    }

    let session = connect_scylla().await?;
    let inserted_gainer = insert_emiten_trending(&session, &gainer_rows, "gainer").await?;
    println!("OK: {inserted_gainer} baris diinsert ke emiten_trending (gainer).");

    println!("Mengklik Top Loser...");
    click_mover_tab(&page, "Top Loser").await?;
    sleep(Duration::from_secs(2)).await;
    let screenshot_path = save_step_screenshot(&page, "08", "top_loser").await?;

    let loser_rows = scrape_movers_table(&page).await?;
    println!("Top Loser: {} baris di-scrape.", loser_rows.len());
    if loser_rows.is_empty() {
        return Err("Tabel Top Loser kosong / tidak terbaca".into());
    }

    let inserted_loser = insert_emiten_trending(&session, &loser_rows, "loser").await?;
    println!("OK: {inserted_loser} baris diinsert ke emiten_trending (loser).");

    let today = Local::now().date_naive();
    let ks = keyspace();
    println!(
        "Query MV emiten_trending_by_tahun_bulan_tanggal untuk {}...",
        today.format("%Y-%m-%d")
    );
    let emitens = bandarmology::fetch_today_emiten_names(&session, &ks, today).await?;
    println!(
        "Ditemukan {} emiten unik hari ini untuk bandarmology.",
        emitens.len()
    );

    let bandar_ok =
        bandarmology::scrape_and_insert_bandarmology(&page, &session, &ks, today, &emitens).await?;
    println!("OK: {bandar_ok} emiten diinsert ke bandarmology.");
    save_step_screenshot(&page, "09", "bandarmology").await?;

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
