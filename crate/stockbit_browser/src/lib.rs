//! Browser automation untuk Stockbit (Chrome headless) — sesi, login, navigasi `/stream`.
//!
//! Dipakai oleh `user` (RPC `IsStockbitReady`) dan `worker_scrapping`.
//!
//! Env: `STOCKBIT_EMAIL`, `STOCKBIT_PASSWORD`, opsional `CHROME_EXECUTABLE_PATH`,
//! `STOCKBIT_2FA_TIMEOUT_SECS`, `STOCKBIT_SESSION_CHECK_SECS` (default random 60–300 untuk
//! jendela cek di `/stream`; default 5 untuk worker), `STOCKBIT_BROWSER_DATA_DIR`,
//! `STOCKBIT_READY_POLL_MIN_SECS` / `STOCKBIT_READY_POLL_MAX_SECS` (default 3600–7200 —
//! interval background poller untuk `IsStockbitReady`).

use chromiumoxide::browser::{Browser, BrowserConfig};
use chromiumoxide::page::Page;
use futures::StreamExt;
use rand::Rng;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tokio::time::sleep;

/// Interval default antar pengecekan web Stockbit (detik).
pub const READY_POLL_MIN_SECS: u64 = 3600;
pub const READY_POLL_MAX_SECS: u64 = 7200;

pub const STOCKBIT_STREAM_URL: &str = "https://stockbit.com/stream";
pub const STOCKBIT_LOGIN_URL: &str = "https://stockbit.com/login";
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
const NAV_TIMEOUT_SECS: u64 = 10;

pub type StockbitError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Debug)]
pub struct ReadinessUpdate {
    pub ready: bool,
    pub message: String,
}

fn poll_interval_range() -> (u64, u64) {
    let min = std::env::var("STOCKBIT_READY_POLL_MIN_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(READY_POLL_MIN_SECS);
    let max = std::env::var("STOCKBIT_READY_POLL_MAX_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(READY_POLL_MAX_SECS);
    if min <= max {
        (min, max)
    } else {
        (max, min)
    }
}

fn next_poll_secs() -> u64 {
    let (min, max) = poll_interval_range();
    rand::thread_rng().gen_range(min..=max)
}

/// Background poller: cek stockbit.com setiap 3600–7200 detik.
/// RPC `IsStockbitReady` hanya membaca status terakhir — tidak trigger cek langsung.
#[derive(Clone)]
pub struct ReadinessPoller {
    latest: Arc<RwLock<Option<ReadinessUpdate>>>,
}

impl ReadinessPoller {
    /// Mulai loop polling di background.
    /// Cek pertama segera (bukan dari RPC); berikutnya setiap 3600–7200 detik.
    pub fn start() -> Arc<Self> {
        let poller = Arc::new(Self {
            latest: Arc::new(RwLock::new(None)),
        });
        let runner = Arc::clone(&poller);
        tokio::spawn(async move {
            runner.run_loop().await;
        });
        poller
    }

    /// Status terakhir dari pooling (None = belum pernah dicek).
    pub async fn latest(&self) -> Option<ReadinessUpdate> {
        self.latest.read().await.clone()
    }

    async fn publish(&self, update: ReadinessUpdate) {
        let mut guard = self.latest.write().await;
        *guard = Some(update);
    }

    async fn run_loop(self: Arc<Self>) {
        let mut first = true;
        loop {
            if first {
                first = false;
                println!("Stockbit readiness poller: cek awal (background, bukan dari RPC)");
            } else {
                let wait_secs = next_poll_secs();
                let (min, max) = poll_interval_range();
                println!(
                    "Stockbit readiness poller: cek berikutnya dalam {wait_secs}s (interval {min}–{max}s)"
                );
                sleep(Duration::from_secs(wait_secs)).await;
            }

            let (tx, mut rx) = mpsc::channel::<ReadinessUpdate>(8);
            let poller = Arc::clone(&self);
            let forward = tokio::spawn(async move {
                while let Some(update) = rx.recv().await {
                    poller.publish(update).await;
                }
            });

            match run_readiness_check(tx).await {
                Ok(()) => {}
                Err(e) => {
                    self.publish(ReadinessUpdate {
                        ready: false,
                        message: format!("Error: {e}"),
                    })
                    .await;
                }
            }
            let _ = forward.await;
        }
    }
}

fn is_login_url(url: &str) -> bool {
    url.contains("/login")
}

fn is_stream_url(url: &str) -> bool {
    url.contains("/stream")
}

fn is_2fa_pending_url(url: &str) -> bool {
    url.contains("/trusted-device") || url.contains("/two-factor") || url.contains("/2fa")
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..").join(".."))
}

pub fn browser_data_dir() -> PathBuf {
    std::env::var("STOCKBIT_BROWSER_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| workspace_root().join("worker_scrapping").join("browser_data"))
}

pub fn browser_config() -> Result<BrowserConfig, StockbitError> {
    let data_dir = browser_data_dir();
    std::fs::create_dir_all(&data_dir)?;
    kill_stale_chrome_processes(&data_dir);
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

/// Bunuh proses Chrome sisa run sebelumnya yang masih memegang profil `data_dir`,
/// agar tidak muncul error "Failed to create SingletonLock: File exists (17)".
fn kill_stale_chrome_processes(data_dir: &Path) {
    // 1) PID dari symlink SingletonLock (target biasanya "hostname-<pid>").
    let lock = data_dir.join("SingletonLock");
    if let Ok(target) = std::fs::read_link(&lock) {
        if let Some(pid) = target
            .to_string_lossy()
            .rsplit('-')
            .next()
            .and_then(|s| s.trim().parse::<i32>().ok())
        {
            kill_pid(pid);
        }
    }

    // 2) Fallback: pkill chrome yang memakai profil ini.
    // Pola tanpa awalan "--" agar tidak dianggap opsi pkill; "--" mengakhiri flags.
    // stdout/stderr di-null supaya usage/help tidak muncul di terminal.
    if let Some(dir) = data_dir.to_str() {
        let pattern = format!("user-data-dir={dir}");
        let _ = std::process::Command::new("pkill")
            .args(["-9", "-f", "--", &pattern])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }

    // Beri waktu OS melepas file lock sebelum relaunch.
    std::thread::sleep(Duration::from_millis(300));
}

fn kill_pid(pid: i32) {
    if pid <= 1 {
        return;
    }
    let _ = std::process::Command::new("kill")
        .args(["-9", &pid.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
}

fn clear_stale_chrome_locks(data_dir: &Path) {
    for name in ["SingletonLock", "SingletonSocket", "SingletonCookie"] {
        let p = data_dir.join(name);
        // symlink_metadata agar symlink yatim (broken) tetap terhapus.
        if p.exists() || std::fs::symlink_metadata(&p).is_ok() {
            let _ = std::fs::remove_file(&p);
        }
    }
}

pub async fn launch_page() -> Result<(Browser, Page), StockbitError> {
    let config = browser_config()?;
    let (browser, mut handler) = Browser::launch(config).await?;
    tokio::task::spawn(async move { while handler.next().await.is_some() {} });

    let page = browser.new_page("about:blank").await?;
    page.set_user_agent(USER_AGENT).await?;
    page.evaluate_on_new_document(
        r#"
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
    "#,
    )
    .await?;

    Ok((browser, page))
}

pub async fn goto_stockbit(page: &Page, url: &str) -> Result<(), StockbitError> {
    goto_stockbit_expect(page, url, None).await
}

pub async fn goto_stockbit_expect(
    page: &Page,
    url: &str,
    expect_path: Option<&str>,
) -> Result<(), StockbitError> {
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
        }
        Ok(Err(_)) | Err(_) => {}
    }

    force_location_assign(page, url).await?;

    let started = Instant::now();
    loop {
        sleep(Duration::from_millis(400)).await;
        let current = page.url().await?.unwrap_or_default();
        if path_ok(&current) {
            return Ok(());
        }
        if started.elapsed() >= nav_timeout {
            force_location_replace(page, url).await?;
            sleep(Duration::from_secs(2)).await;
            let current = page.url().await?.unwrap_or_default();
            if path_ok(&current) {
                return Ok(());
            }
            return Err(format!(
                "Timeout navigasi ke {url} (expect={expect_path:?}); URL sekarang: {current}"
            )
            .into());
        }
    }
}

async fn force_location_assign(page: &Page, url: &str) -> Result<(), StockbitError> {
    let escaped = url.replace('\\', "\\\\").replace('"', "\\\"");
    page.evaluate(format!(r#"window.location.assign("{escaped}")"#))
        .await?;
    Ok(())
}

async fn force_location_replace(page: &Page, url: &str) -> Result<(), StockbitError> {
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

async fn wait_for_login_form(page: &Page, timeout: Duration) -> Result<(), StockbitError> {
    let started = Instant::now();
    while started.elapsed() < timeout {
        if has_login_form(page).await {
            return Ok(());
        }
        sleep(Duration::from_millis(400)).await;
    }
    Err("Form login (#username) tidak muncul".into())
}

async fn click_session_expired_cta(page: &Page) -> Result<bool, StockbitError> {
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

async fn type_naturally(
    page: &Page,
    selector: &str,
    value: &str,
    label: &str,
) -> Result<(), StockbitError> {
    let element = page
        .find_element(selector)
        .await
        .map_err(|_| format!("Error: Elemen {selector} ({label}) tidak ditemukan di halaman!"))?;

    element.click().await?;
    sleep(Duration::from_millis(400)).await;

    for karakter in value.chars() {
        let delay_acak = rand::thread_rng().gen_range(80..220);
        sleep(Duration::from_millis(delay_acak)).await;
        element.type_str(&karakter.to_string()).await?;
    }
    Ok(())
}

async fn has_login_form(page: &Page) -> bool {
    page.find_element("#username").await.is_ok()
}

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

async fn wait_for_2fa_phone_approval(
    page: &Page,
    timeout: Duration,
) -> Result<String, StockbitError> {
    let started = Instant::now();
    let poll = Duration::from_secs(2);
    let mut last_url = String::new();

    loop {
        if has_profile_avatar_modal(page).await {
            dismiss_profile_avatar_modal(page).await?;
            let final_url = page.url().await?.unwrap_or_default();
            return Ok(final_url);
        }

        let url = page.url().await?.unwrap_or_default();
        if url != last_url {
            last_url = url.clone();
        }

        if !url.is_empty() && !is_2fa_pending_url(&url) {
            if has_profile_avatar_modal(page).await {
                dismiss_profile_avatar_modal(page).await?;
            }
            sleep(Duration::from_secs(2)).await;
            let final_url = page.url().await?.unwrap_or(url);
            return Ok(final_url);
        }

        if started.elapsed() >= timeout {
            return Err(format!(
                "Timeout menunggu 2FA setelah {} detik. Approve di HP lalu coba lagi.",
                timeout.as_secs()
            )
            .into());
        }

        sleep(poll).await;
    }
}

async fn perform_login(page: &Page, email: &str, password: &str) -> Result<(), StockbitError> {
    if email.is_empty() || password.is_empty() {
        return Err("STOCKBIT_EMAIL dan STOCKBIT_PASSWORD wajib diisi di .env".into());
    }

    type_naturally(page, "#username", email, "email/username").await?;
    sleep(Duration::from_millis(400)).await;
    type_naturally(page, "#password", password, "password").await?;
    sleep(Duration::from_millis(800)).await;

    if let Ok(btn) = page.find_element("#email-login-button").await {
        btn.click().await?;
    } else {
        return Err("Error: Tombol Login (#email-login-button) tidak ditemukan!".into());
    }

    sleep(Duration::from_secs(3)).await;

    let after_url = page.url().await?.unwrap_or_default();

    if has_profile_avatar_modal(page).await {
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
        for _ in 0..10 {
            if has_profile_avatar_modal(page).await {
                dismiss_profile_avatar_modal(page).await?;
                break;
            }
            if is_2fa_pending_url(&page.url().await?.unwrap_or_default()) {
                wait_for_2fa_phone_approval(page, Duration::from_secs(timeout_secs)).await?;
                break;
            }
            sleep(Duration::from_millis(500)).await;
        }
    }

    Ok(())
}

pub async fn dismiss_profile_avatar_modal(page: &Page) -> Result<bool, StockbitError> {
    for _ in 1..=8 {
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
            sleep(Duration::from_secs(1)).await;
            return Ok(true);
        }
        sleep(Duration::from_millis(500)).await;
    }
    Ok(false)
}

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

async fn login_and_return_to_stream(
    page: &Page,
    email: &str,
    password: &str,
) -> Result<(), StockbitError> {
    if has_session_expired_modal(page).await {
        if click_session_expired_cta(page).await? {
            sleep(Duration::from_secs(1)).await;
        }
    }

    if !has_login_form(page).await {
        goto_stockbit_expect(page, STOCKBIT_LOGIN_URL, Some("/login")).await?;
    }

    wait_for_login_form(page, Duration::from_secs(15)).await?;
    perform_login(page, email, password).await?;
    goto_stockbit_expect(page, STOCKBIT_STREAM_URL, Some("/stream")).await?;
    sleep(Duration::from_secs(2)).await;
    Ok(())
}

async fn send_update(tx: &mpsc::Sender<ReadinessUpdate>, ready: bool, message: &str) {
    let _ = tx
        .send(ReadinessUpdate {
            ready,
            message: message.to_string(),
        })
        .await;
}

/// Cek `/stream`, login bila perlu, kirim progres lewat channel.
pub async fn run_readiness_check(tx: mpsc::Sender<ReadinessUpdate>) -> Result<(), StockbitError> {
    let email = std::env::var("STOCKBIT_EMAIL").unwrap_or_default();
    let password = std::env::var("STOCKBIT_PASSWORD").unwrap_or_default();

    let (mut browser, page) = launch_page().await?;

    goto_stockbit(&page, STOCKBIT_STREAM_URL).await?;
    sleep(Duration::from_secs(1)).await;

    let url = page.url().await?.unwrap_or_default();
    // Durasi jendela + interval polling: random 60–300 detik (override: STOCKBIT_SESSION_CHECK_SECS).
    let wait_secs: u64 = std::env::var("STOCKBIT_SESSION_CHECK_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| rand::thread_rng().gen_range(60u64..=300));
    let started = Instant::now();
    let mut need_login = is_login_url(&url) || has_login_form(&page).await;
    let mut session_expired = false;

    while started.elapsed() < Duration::from_secs(wait_secs) {
        if has_profile_avatar_modal(&page).await {
            dismiss_profile_avatar_modal(&page).await?;
            break;
        }
        if has_session_expired_modal(&page).await {
            session_expired = true;
            need_login = true;
            break;
        }
        let u = page.url().await?.unwrap_or_default();
        if is_login_url(&u) || has_login_form(&page).await {
            need_login = true;
            break;
        }
        let remaining = wait_secs.saturating_sub(started.elapsed().as_secs());
        if remaining == 0 {
            break;
        }
        let poll_secs = rand::thread_rng().gen_range(60u64..=300).min(remaining);
        sleep(Duration::from_secs(poll_secs)).await;
    }

    let url = page.url().await?.unwrap_or_default();
    if !need_login {
        need_login = is_login_url(&url)
            || has_login_form(&page).await
            || has_session_expired_modal(&page).await;
        if has_session_expired_modal(&page).await {
            session_expired = true;
        }
    }

    if need_login {
        if session_expired {
            send_update(&tx, false, "Session expired").await;
        }
        send_update(&tx, false, "Sedang login stockbit.com").await;
        login_and_return_to_stream(&page, &email, &password).await?;
    } else if !is_stream_url(&url) {
        goto_stockbit(&page, STOCKBIT_STREAM_URL).await?;
        sleep(Duration::from_secs(1)).await;
        let again = page.url().await?.unwrap_or_default();
        if is_login_url(&again) || has_login_form(&page).await {
            send_update(&tx, false, "Sedang login stockbit.com").await;
            login_and_return_to_stream(&page, &email, &password).await?;
        }
    }

    if has_profile_avatar_modal(&page).await {
        dismiss_profile_avatar_modal(&page).await?;
    }

    let final_url = page.url().await?.unwrap_or_default();
    if !is_stream_url(&final_url) {
        return Err(format!("Gagal masuk /stream setelah cek sesi (URL: {final_url})").into());
    }

    send_update(&tx, true, "Stockbit ready").await;
    browser.close().await?;
    Ok(())
}

/// Alur awal worker: akses `/stream`, login bila sesi habis, kembali ke `/stream`.
pub async fn open_stream_or_login(
    page: &Page,
    email: &str,
    password: &str,
) -> Result<(), StockbitError> {
    goto_stockbit(page, STOCKBIT_STREAM_URL).await?;
    sleep(Duration::from_secs(1)).await;

    let url = page.url().await?.unwrap_or_default();
    let wait_secs: u64 = std::env::var("STOCKBIT_SESSION_CHECK_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);
    let started = Instant::now();
    let mut need_login = is_login_url(&url) || has_login_form(page).await;

    while started.elapsed() < Duration::from_secs(wait_secs) {
        if has_profile_avatar_modal(page).await {
            dismiss_profile_avatar_modal(page).await?;
            break;
        }
        if has_session_expired_modal(page).await {
            need_login = true;
            break;
        }
        let u = page.url().await?.unwrap_or_default();
        if is_login_url(&u) || has_login_form(page).await {
            need_login = true;
            break;
        }
        sleep(Duration::from_millis(400)).await;
    }

    let url = page.url().await?.unwrap_or_default();
    if !need_login {
        need_login = is_login_url(&url)
            || has_login_form(page).await
            || has_session_expired_modal(page).await;
    }

    if need_login {
        login_and_return_to_stream(page, email, password).await?;
    } else if !is_stream_url(&url) {
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
