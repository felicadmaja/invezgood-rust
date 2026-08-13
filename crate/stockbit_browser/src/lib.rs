//! Browser automation untuk Stockbit (Chrome headless) — sesi, login, navigasi `/stream`.
//!
//! Dipakai oleh `user` (RPC `IsStockbitReady`) dan `worker_scrapping`.
//!
//! Chrome **di-reuse** antar RPC on-demand / readiness (satu proses `stockbit_ws`):
//! `launch_page()` tidak menutup browser; `BrowserSession::close` no-op agar cookie/JWT
//! tetap hidup. Relaunch + soft-kill hanya bila sesi Chrome mati.
//!
//! Env: `STOCKBIT_EMAIL`, `STOCKBIT_PASSWORD`, opsional `CHROME_EXECUTABLE_PATH`,
//! `STOCKBIT_2FA_TIMEOUT_SECS`, `STOCKBIT_SESSION_CHECK_SECS` (default random 60–300 untuk
//! jendela cek readiness di `/stream`; default **2** untuk worker/on-demand),
//! `STOCKBIT_BEARER_CACHE_SECS` (default 300 — cache JWT antar scrape),
//! `STOCKBIT_BROWSER_DATA_DIR`,
//! `STOCKBIT_READY_POLL_MIN_SECS` / `STOCKBIT_READY_POLL_MAX_SECS` (default 540–600 —
//! interval poller readiness setelah cek pertama Senin–Jumat 09:00:00–09:00:59),
//! `REDIS_URL` (state readiness).
//!
//! Jika poller mendeteksi sesi habis: login ulang; bila gagal, retry dengan jeda acak 10–30 detik.

mod redis_readiness;

use chrono::{DateTime, Datelike, Duration as ChronoDuration, Local, NaiveDate, TimeZone, Timelike};
use chromiumoxide::browser::{Browser, BrowserConfig};
use chromiumoxide::handler::viewport::Viewport;
use chromiumoxide::cdp::browser_protocol::page::CaptureScreenshotFormat;
use chromiumoxide::page::{Page, ScreenshotParams};
use futures::StreamExt;
use rand::Rng;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch, Mutex, MutexGuard};
use tokio::time::{sleep, timeout};

/// Hook setelah setiap tick poller readiness (`ready` = status terakhir).
/// Return diabaikan (Yahoo spike tidak lewat IsStockbitReady).
pub type AfterPollHook = Arc<
    dyn Fn(bool) -> Pin<Box<dyn Future<Output = Option<Vec<PortofolioSpike>>> + Send>>
        + Send
        + Sync,
>;

/// Emiten spike Yahoo (disimpan di Redis readiness; stream spike di GetPriceSpikeFromYahooFinance).
#[derive(Clone, Debug, PartialEq)]
pub struct PortofolioSpike {
    pub emiten_name: String,
    /// `up` | `down` (close vs open hari ini; ambang dari `.env`).
    pub jenis_spike: String,
    /// Persentase perubahan close vs open (naik positif, turun negatif).
    pub value_spike_percentage: f64,
}

/// Mutex global: satu Chrome profil — readiness poller dan on-demand scrape
/// tidak boleh memakai browser bersamaan.
static BROWSER_SESSION_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

/// Jumlah task interactive (RPC client) yang sedang menunggu / memakai slot Chrome.
static INTERACTIVE_WAITERS: AtomicUsize = AtomicUsize::new(0);

/// Kelas akuisisi lock Chrome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrowserLockClass {
    /// RPC client — prioritas; tunggu lock (timeout) agar server tetap merespons.
    Interactive,
    /// Poller / auto-scrape — yield bila ada RPC client menunggu; skip cepat jika sibuk.
    Background,
}

/// Timeout tunggu lock untuk RPC interactive (hindari hang "server tidak merespons").
const INTERACTIVE_LOCK_TIMEOUT: Duration = Duration::from_secs(45);
/// Timeout singkat untuk background; gagal → skip auto-scrape / poller step.
const BACKGROUND_LOCK_TIMEOUT: Duration = Duration::from_secs(2);

pub fn browser_session_lock() -> &'static Mutex<()> {
    BROWSER_SESSION_LOCK.get_or_init(|| Mutex::new(()))
}

/// True bila ada RPC client yang menunggu Chrome (background harus yield/skip).
pub fn browser_interactive_waiters() -> usize {
    INTERACTIVE_WAITERS.load(Ordering::SeqCst)
}

struct InteractiveWaitGuard;

impl InteractiveWaitGuard {
    fn enter() -> Self {
        INTERACTIVE_WAITERS.fetch_add(1, Ordering::SeqCst);
        Self
    }
}

impl Drop for InteractiveWaitGuard {
    fn drop(&mut self) {
        INTERACTIVE_WAITERS.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Ambil exclusive Chrome session.
/// - `Interactive`: prioritas, timeout 45s → error jelas (bukan hang client).
/// - `Background`: skip segera bila user menunggu / lock tidak didapat cepat.
pub async fn acquire_browser_session(
    class: BrowserLockClass,
) -> Result<MutexGuard<'static, ()>, String> {
    match class {
        BrowserLockClass::Interactive => {
            let _waiter = InteractiveWaitGuard::enter();
            match timeout(INTERACTIVE_LOCK_TIMEOUT, browser_session_lock().lock()).await {
                Ok(guard) => Ok(guard),
                Err(_) => Err(
                    "Chrome sibuk dipakai poller/scrape background. Coba lagi sebentar \
                     (server merespons; bukan hang)."
                        .into(),
                ),
            }
        }
        BrowserLockClass::Background => {
            if browser_interactive_waiters() > 0 {
                return Err("browser skip: RPC client menunggu Chrome".into());
            }
            match timeout(BACKGROUND_LOCK_TIMEOUT, browser_session_lock().lock()).await {
                Ok(guard) => {
                    if browser_interactive_waiters() > 0 {
                        drop(guard);
                        return Err("browser skip: RPC client menunggu Chrome".into());
                    }
                    Ok(guard)
                }
                Err(_) => Err("browser skip: Chrome sibuk".into()),
            }
        }
    }
}

/// Chrome + page yang di-reuse antar scrape (hidup selama proses `stockbit_ws`).
struct PersistentChrome {
    browser: Browser,
    page: Page,
}

static PERSISTENT_CHROME: OnceLock<Mutex<Option<PersistentChrome>>> = OnceLock::new();

fn persistent_chrome() -> &'static Mutex<Option<PersistentChrome>> {
    PERSISTENT_CHROME.get_or_init(|| Mutex::new(None))
}

/// Handle yang dikembalikan `launch_page` — `close()` **tidak** mematikan Chrome
/// (sengaja, agar sesi Stockbit tetap hidup antar RPC).
pub struct BrowserSession;

impl BrowserSession {
    /// No-op: Chrome tetap jalan di pool persistent.
    pub async fn close(self) {
        // sengaja tidak menutup Chrome
    }
}

/// Interval default antar pengecekan web Stockbit (detik).
pub const READY_POLL_MIN_SECS: u64 = 9 * 60;
pub const READY_POLL_MAX_SECS: u64 = 10 * 60;

/// Jeda acak antar retry login bila sesi habis / login gagal (detik).
pub const LOGIN_RETRY_MIN_SECS: u64 = 10;
pub const LOGIN_RETRY_MAX_SECS: u64 = 30;

pub const STOCKBIT_STREAM_URL: &str = "https://stockbit.com/stream";
pub const STOCKBIT_LOGIN_URL: &str = "https://stockbit.com/login";
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
const NAV_TIMEOUT_SECS: u64 = 10;
/// Default tunggu popup sesi habis / redirect di worker & on-demand (`open_stream_or_login`).
const WORKER_SESSION_CHECK_SECS: u64 = 2;
/// TTL cache Bearer JWT setelah probe market-mover OK.
const BEARER_CACHE_TTL_SECS_DEFAULT: u64 = 300;

struct CachedBearer {
    token: String,
    cached_at: Instant,
}

static BEARER_CACHE: OnceLock<Mutex<Option<CachedBearer>>> = OnceLock::new();

fn bearer_cache() -> &'static Mutex<Option<CachedBearer>> {
    BEARER_CACHE.get_or_init(|| Mutex::new(None))
}

fn bearer_cache_ttl_secs() -> u64 {
    std::env::var("STOCKBIT_BEARER_CACHE_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(BEARER_CACHE_TTL_SECS_DEFAULT)
}

fn worker_session_check_secs() -> u64 {
    std::env::var("STOCKBIT_SESSION_CHECK_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(WORKER_SESSION_CHECK_SECS)
}

pub type StockbitError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Debug)]
pub struct ReadinessUpdate {
    pub ready: bool,
    pub message: String,
    /// Naik tiap hasil cek poller background. `0` = hydrate Redis (bukan tick poll).
    pub poll_seq: u64,
    /// Emiten is_plan_to_trade=true dengan spike (ambang UP_SPIKE_PERCENTAGE / DOWN_SPIKE_PERCENTAGE).
    pub portofolio: Vec<PortofolioSpike>,
}

fn next_poll_seq() -> u64 {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed) + 1
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

fn is_weekday(date: NaiveDate) -> bool {
    !matches!(
        date.weekday(),
        chrono::Weekday::Sat | chrono::Weekday::Sun
    )
}

fn local_at(date: NaiveDate, hour: u32, min: u32, sec: u32) -> DateTime<Local> {
    let naive = date
        .and_hms_opt(hour, min, sec)
        .expect("jam 09:00:ss valid");
    Local
        .from_local_datetime(&naive)
        .earliest()
        .expect("zona waktu lokal")
}

/// Target cek pertama: Senin–Jumat 09:00:00 + jitter detik (0–59).
fn next_mon_fri_0900(now: DateTime<Local>, jitter_secs: u32) -> DateTime<Local> {
    let jitter = jitter_secs.min(59);
    let date = now.date_naive();
    if is_weekday(date) {
        let target = local_at(date, 9, 0, jitter);
        if now < target {
            return target;
        }
    }
    next_mon_fri_0900_after_date(date, jitter)
}

fn next_mon_fri_0900_after_date(after: NaiveDate, jitter_secs: u32) -> DateTime<Local> {
    let mut date = after.succ_opt().expect("tanggal");
    while !is_weekday(date) {
        date = date.succ_opt().expect("tanggal");
    }
    local_at(date, 9, 0, jitter_secs.min(59))
}

fn in_first_check_window(now: DateTime<Local>) -> bool {
    is_weekday(now.date_naive()) && now.hour() == 9 && now.minute() == 0
}

fn after_first_check_window(now: DateTime<Local>) -> bool {
    is_weekday(now.date_naive()) && (now.hour() > 9 || (now.hour() == 9 && now.minute() > 0))
}

fn sleep_secs_until(target: DateTime<Local>, now: DateTime<Local>) -> u64 {
    (target - now).num_seconds().max(0) as u64
}

/// Cek pertama hari kerja di 09:00:00–09:00:59; seterusnya interval 9–10 menit
/// sampai melewati jendela 09:00 hari kerja berikutnya.
async fn wait_before_next_poll(last_first_check_date: &mut Option<NaiveDate>) {
    let now = Local::now();
    let today = now.date_naive();
    let first_done_today = *last_first_check_date == Some(today);

    if !first_done_today {
        if in_first_check_window(now) {
            println!(
                "Stockbit readiness poller: cek pertama hari ini (09:00:{:02})",
                now.second()
            );
            *last_first_check_date = Some(today);
            return;
        }
        if after_first_check_window(now) {
            println!(
                "Stockbit readiness poller: cek pertama hari ini (terlewat 09:00, langsung cek)"
            );
            *last_first_check_date = Some(today);
            return;
        }
        let jitter = rand::thread_rng().gen_range(0u32..=59);
        let target = next_mon_fri_0900(now, jitter);
        let wait_secs = sleep_secs_until(target, now);
        println!(
            "Stockbit readiness poller: cek pertama Senin-Jumat 09:00:{:02} pada {} (tunggu {wait_secs}s)",
            jitter,
            target.format("%Y-%m-%d %H:%M:%S")
        );
        sleep(Duration::from_secs(wait_secs)).await;
        *last_first_check_date = Some(Local::now().date_naive());
        return;
    }

    let wait_secs = next_poll_secs();
    let (min, max) = poll_interval_range();
    let jitter = rand::thread_rng().gen_range(0u32..=59);
    let next_first = next_mon_fri_0900_after_date(today, jitter);
    let wake = now + ChronoDuration::seconds(wait_secs as i64);
    if wake >= next_first {
        let wait_first = sleep_secs_until(next_first, now);
        println!(
            "Stockbit readiness poller: cek pertama berikutnya {} (tunggu {wait_first}s, bukan interval {min}–{max}s)",
            next_first.format("%Y-%m-%d %H:%M:%S")
        );
        sleep(Duration::from_secs(wait_first)).await;
        *last_first_check_date = Some(Local::now().date_naive());
        return;
    }

    println!(
        "Stockbit readiness poller: cek berikutnya dalam {wait_secs}s (interval {min}–{max}s)"
    );
    sleep(Duration::from_secs(wait_secs)).await;
}

/// Poller tunggal per proses: cek pertama Senin–Jumat 09:00:00–09:00:59 waktu server,
/// seterusnya 9–10 menit, sejak `ensure_loop_running` / start server
/// (tidak tergantung subscriber `IsStockbitReady`).
/// Banyak subscriber tidak menambah frekuensi. Status terakhir di Redis (`stockbit:readiness`);
/// stream hanya baca cache poller/Redis.
/// Opsional: `set_after_poll_hook` untuk auto-scrape (portofolio / emiten / pending) setelah tick.
#[derive(Clone)]
pub struct ReadinessPoller {
    notify: watch::Sender<Option<ReadinessUpdate>>,
    subscriber_count: Arc<AtomicUsize>,
    /// True bila loop poller sudah di-spawn.
    loop_started: Arc<Mutex<bool>>,
    after_poll: Arc<Mutex<Option<AfterPollHook>>>,
}

impl ReadinessPoller {
    /// Buat poller tanpa loop — panggil `ensure_loop_running` di app startup agar selalu jalan.
    pub fn new() -> Arc<Self> {
        let (notify, _) = watch::channel(None);
        Arc::new(Self {
            notify,
            subscriber_count: Arc::new(AtomicUsize::new(0)),
            loop_started: Arc::new(Mutex::new(false)),
            after_poll: Arc::new(Mutex::new(None)),
        })
    }

    /// Back-compat: sama `new()`.
    pub fn start() -> Arc<Self> {
        Self::new()
    }

    /// Daftarkan hook yang dipanggil setelah setiap tick poller (sukses atau gagal cek).
    pub async fn set_after_poll_hook(self: &Arc<Self>, hook: AfterPollHook) {
        *self.after_poll.lock().await = Some(hook);
    }

    /// Dipanggil saat client buka stream `IsStockbitReady` (hanya hitung subscriber stream).
    pub async fn register_subscriber(self: &Arc<Self>) {
        self.subscriber_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Dipanggil saat client tutup stream `IsStockbitReady` (tidak menghentikan poller).
    pub async fn unregister_subscriber(self: &Arc<Self>) {
        let prev = self.subscriber_count.load(Ordering::SeqCst);
        if prev > 0 {
            self.subscriber_count.fetch_sub(1, Ordering::SeqCst);
        }
    }

    pub fn subscriber_count(&self) -> usize {
        self.subscriber_count.load(Ordering::SeqCst)
    }

    /// Status terakhir dari Redis (None = belum pernah dicek / Redis miss).
    pub async fn latest(&self) -> Option<ReadinessUpdate> {
        redis_readiness::get().await
    }

    /// Subscribe hasil cek poller (termasuk `ready=true` setelah sesi OK).
    pub fn subscribe(&self) -> watch::Receiver<Option<ReadinessUpdate>> {
        self.notify.subscribe()
    }

    /// Mulai loop poller bila belum jalan (idempotent). Dipakai di app startup.
    pub async fn ensure_loop_running(self: &Arc<Self>) {
        let mut started = self.loop_started.lock().await;
        if *started {
            return;
        }

        if let Some(mut cached) = redis_readiness::get().await {
            cached.poll_seq = 0;
            println!(
                "Stockbit readiness poller: hydrate Redis ready={} msg={:?}",
                cached.ready, cached.message
            );
            let _ = self.notify.send(Some(cached));
        }

        let poller = Arc::clone(self);
        tokio::spawn(async move {
            poller.run_loop().await;
        });
        *started = true;
        println!("Stockbit readiness poller: dimulai (selalu aktif)");
    }

    async fn publish(&self, update: ReadinessUpdate) {
        redis_readiness::set(&update).await;
        let _ = self.notify.send(Some(update));
    }

    async fn run_loop(self: Arc<Self>) {
        let mut last_first_check_date: Option<NaiveDate> = None;
        loop {
            wait_before_next_poll(&mut last_first_check_date).await;

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
                        poll_seq: next_poll_seq(),
                        portofolio: Vec::new(),
                    })
                    .await;
                }
            }
            let _ = forward.await;

            let latest = redis_readiness::get().await.unwrap_or(ReadinessUpdate {
                ready: false,
                message: String::new(),
                poll_seq: 0,
                portofolio: Vec::new(),
            });
            let hook = self.after_poll.lock().await.clone();
            if let Some(hook) = hook {
                let _ = hook(latest.ready).await;
            }
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
    browser_config_inner(true)
}

fn browser_config_inner(kill_stale: bool) -> Result<BrowserConfig, StockbitError> {
    let data_dir = browser_data_dir();
    std::fs::create_dir_all(&data_dir)?;
    if kill_stale {
        // Soft-kill dulu (SIGTERM), baru SIGKILL bila masih hidup — agar cookie sempat flush.
        terminate_stale_chrome_processes(&data_dir);
        clear_stale_chrome_locks(&data_dir);
    }

    let viewport = Viewport {
        width: 1366,
        height: 900,
        device_scale_factor: Some(1.0),
        emulating_mobile: false,
        is_landscape: true,
        has_touch: false,
    };

    let mut builder = BrowserConfig::builder()
        .user_data_dir(&data_dir)
        .request_timeout(Duration::from_secs(120))
        .launch_timeout(Duration::from_secs(60))
        .viewport(viewport)
        .args([
            "--headless=new",
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-blink-features=AutomationControlled",
            "--window-size=1366,900",
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

/// Soft-kill proses Chrome yang memegang profil, lalu hard-kill bila perlu.
fn terminate_stale_chrome_processes(data_dir: &Path) {
    let lock = data_dir.join("SingletonLock");
    let mut pids = Vec::new();
    if let Ok(target) = std::fs::read_link(&lock) {
        if let Some(pid) = target
            .to_string_lossy()
            .rsplit('-')
            .next()
            .and_then(|s| s.trim().parse::<i32>().ok())
        {
            if pid > 1 {
                pids.push(pid);
            }
        }
    }

    for &pid in &pids {
        signal_pid(pid, false);
    }
    if let Some(dir) = data_dir.to_str() {
        let pattern = format!("user-data-dir={dir}");
        let _ = std::process::Command::new("pkill")
            .args(["-TERM", "-f", "--", &pattern])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
    std::thread::sleep(Duration::from_millis(800));

    for &pid in &pids {
        signal_pid(pid, true);
    }
    if let Some(dir) = data_dir.to_str() {
        let pattern = format!("user-data-dir={dir}");
        let _ = std::process::Command::new("pkill")
            .args(["-9", "-f", "--", &pattern])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
    std::thread::sleep(Duration::from_millis(300));
}

fn signal_pid(pid: i32, force: bool) {
    if pid <= 1 {
        return;
    }
    let sig = if force { "-9" } else { "-TERM" };
    let _ = std::process::Command::new("kill")
        .args([sig, &pid.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
}

fn clear_stale_chrome_locks(data_dir: &Path) {
    for name in ["SingletonLock", "SingletonSocket", "SingletonCookie"] {
        let p = data_dir.join(name);
        if p.exists() || std::fs::symlink_metadata(&p).is_ok() {
            let _ = std::fs::remove_file(&p);
        }
    }
}

async fn page_is_alive(page: &Page) -> bool {
    match tokio::time::timeout(Duration::from_secs(5), page.evaluate("1+1")).await {
        Ok(Ok(eval)) => eval.into_value::<i64>().ok() == Some(2),
        _ => false,
    }
}

fn is_stale_execution_context(err: &str) -> bool {
    err.contains("Cannot find context")
        || err.contains("-32000")
        || err.contains("Execution context was destroyed")
}

/// `page.evaluate` dengan retry bila CDP context hilang (navigasi SPA / reload).
pub async fn evaluate_resilient(
    page: &Page,
    expression: impl Into<String>,
) -> Result<chromiumoxide::js::EvaluationResult, StockbitError> {
    let expression = expression.into();
    let mut last_err = String::new();
    for attempt in 1..=10 {
        match page.evaluate(expression.as_str()).await {
            Ok(v) => return Ok(v),
            Err(e) => {
                let msg = e.to_string();
                if is_stale_execution_context(&msg) {
                    eprintln!(
                        "\x1b[33mChrome evaluate: stale context (attempt {attempt}/10) — jeda lalu retry\x1b[0m"
                    );
                    last_err = msg;
                    sleep(Duration::from_millis(500)).await;
                    continue;
                }
                return Err(msg.into());
            }
        }
    }
    Err(format!(
        "Chrome evaluate gagal setelah retry stale context: {last_err}"
    )
    .into())
}

async fn launch_fresh_browser() -> Result<(Browser, Page), StockbitError> {
    let config = browser_config_inner(true)?;
    let (browser, mut handler) = Browser::launch(config).await?;
    tokio::task::spawn(async move { while handler.next().await.is_some() {} });

    let page = browser.new_page("about:blank").await?;
    page.set_user_agent(USER_AGENT).await?;
    page.evaluate_on_new_document(
        r#"
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
        // Tangkap Authorization Bearer yang dipakai SPA (exodus.stockbit.com, dll.).
        (function () {
            try {
                if (window.__sbCaptureAuthInstalled) return;
                window.__sbCaptureAuthInstalled = true;
                window.__sbCapturedBearer = '';
                const remember = (v) => {
                    if (!v || typeof v !== 'string') return;
                    const t = v.replace(/^Bearer\s+/i, '').trim();
                    if (t.startsWith('eyJ')) window.__sbCapturedBearer = t;
                };
                const wrapHeaders = (headers) => {
                    if (!headers) return;
                    try {
                        if (typeof headers.get === 'function') {
                            remember(headers.get('Authorization') || headers.get('authorization'));
                        } else if (typeof headers === 'object') {
                            remember(headers.Authorization || headers.authorization);
                        }
                    } catch (_) {}
                };
                const ofetch = window.fetch;
                window.fetch = function (input, init) {
                    try {
                        if (init && init.headers) wrapHeaders(init.headers);
                        if (input && typeof input === 'object' && input.headers) wrapHeaders(input.headers);
                    } catch (_) {}
                    return ofetch.apply(this, arguments);
                };
                const oOpen = XMLHttpRequest.prototype.open;
                const oSet = XMLHttpRequest.prototype.setRequestHeader;
                XMLHttpRequest.prototype.open = function () {
                    this.__sbUrl = arguments[1];
                    return oOpen.apply(this, arguments);
                };
                XMLHttpRequest.prototype.setRequestHeader = function (k, v) {
                    try {
                        if (String(k).toLowerCase() === 'authorization') remember(v);
                    } catch (_) {}
                    return oSet.apply(this, arguments);
                };
            } catch (_) {}
        })();
    "#,
    )
    .await?;

    Ok((browser, page))
}

/// Ambil page Chrome bersama. Reuse bila masih hidup; relaunch hanya bila mati.
/// Caller sebaiknya memegang [`browser_session_lock`].
/// `BrowserSession::close` **tidak** mematikan Chrome (pool persistent).
pub async fn launch_page() -> Result<(BrowserSession, Page), StockbitError> {
    let mut slot = persistent_chrome().lock().await;

    if let Some(existing) = slot.as_ref() {
        if page_is_alive(&existing.page).await {
            println!("Chrome session: reuse (sesi tetap hidup, tanpa kill/relaunch)");
            return Ok((BrowserSession, existing.page.clone()));
        }
        println!("Chrome session: page tidak responsif — graceful relaunch...");
        if let Some(mut old) = slot.take() {
            let _ = old.browser.close().await;
        }
        sleep(Duration::from_millis(500)).await;
    } else {
        println!("Chrome session: belum ada — launch baru...");
    }

    let (browser, page) = launch_fresh_browser().await?;
    println!("Chrome session: ready (persistent pool)");
    *slot = Some(PersistentChrome {
        browser,
        page: page.clone(),
    });
    Ok((BrowserSession, page))
}

/// Paksa tutup Chrome persistent (opsional; shutdown bersih / one-shot worker).
pub async fn shutdown_shared_browser() -> Result<(), StockbitError> {
    let mut slot = persistent_chrome().lock().await;
    if let Some(mut old) = slot.take() {
        println!("Chrome session: shutdown_shared_browser — menutup Chrome...");
        let _ = old.browser.close().await;
        sleep(Duration::from_millis(500)).await;
        terminate_stale_chrome_processes(&browser_data_dir());
        clear_stale_chrome_locks(&browser_data_dir());
    }
    Ok(())
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

fn error_screenshot_dir() -> PathBuf {
    // Prefer folder worker screenshots (workspace), fallback ke crate ini.
    let from_crate = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../worker_scrapping/screenshots");
    from_crate
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("screenshots"))
}

/// Simpan screenshot debug saat error; path dicetak ke stderr.
pub async fn save_error_screenshot(page: &Page, label: &str) -> Option<PathBuf> {
    let dir = error_screenshot_dir();
    if let Err(e) = tokio::fs::create_dir_all(&dir).await {
        eprintln!("Peringatan: gagal buat folder screenshot error: {e}");
        return None;
    }
    let path = dir.join(format!("stockbit_error_{label}.png"));
    match page
        .save_screenshot(
            ScreenshotParams::builder()
                .format(CaptureScreenshotFormat::Png)
                .build(),
            &path,
        )
        .await
    {
        Ok(_) => {
            eprintln!("Screenshot error [{label}]: {}", path.display());
            Some(path)
        }
        Err(e) => {
            eprintln!("Peringatan: gagal simpan screenshot error [{label}]: {e}");
            None
        }
    }
}

async fn wait_for_login_form(page: &Page, timeout: Duration) -> Result<(), StockbitError> {
    let started = Instant::now();
    while started.elapsed() < timeout {
        if has_login_form(page).await {
            return Ok(());
        }
        sleep(Duration::from_millis(400)).await;
    }
    let url = page.url().await.ok().flatten().unwrap_or_default();
    let title = page.get_title().await.ok().flatten().unwrap_or_default();
    let shot = save_error_screenshot(page, "login_form_missing").await;
    let shot_info = shot
        .as_ref()
        .map(|p| format!(" | screenshot: {}", p.display()))
        .unwrap_or_default();
    Err(format!(
        "Form login (#username) tidak muncul (url={url:?} title={title:?}){shot_info}"
    )
    .into())
}

/// Klik tombol CTA di modal "Sesi Kamu Sudah Habis".
async fn click_session_expired_cta(page: &Page) -> Result<bool, StockbitError> {
    let clicked = page
        .evaluate(
            r#"(() => {
                const needle = 'kembali ke halaman utama';
                const candidates = Array.from(
                    document.querySelectorAll('button, [role="button"], a')
                );
                const target = candidates.find((el) => {
                    const t = (el.innerText || el.textContent || '').trim().toLowerCase();
                    return t.includes(needle);
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
    per_char_delay_ms: (u64, u64),
) -> Result<(), StockbitError> {
    let element = page
        .find_element(selector)
        .await
        .map_err(|_| format!("Error: Elemen {selector} ({label}) tidak ditemukan di halaman!"))?;

    element.click().await?;
    sleep(Duration::from_millis(400)).await;

    // Kosongkan nilai lama (username sering sudah terisi setelah sesi habis).
    let clear_js = format!(
        r#"(() => {{
            const el = document.querySelector({selector:?});
            if (!el) return false;
            el.focus();
            if (typeof el.select === 'function') el.select();
            el.value = '';
            el.dispatchEvent(new Event('input', {{ bubbles: true }}));
            el.dispatchEvent(new Event('change', {{ bubbles: true }}));
            return true;
        }})()"#
    );
    let _ = page.evaluate(clear_js.as_str()).await;

    for karakter in value.chars() {
        let (lo, hi) = per_char_delay_ms;
        if hi > 0 {
            let delay = if lo >= hi {
                lo
            } else {
                rand::thread_rng().gen_range(lo..=hi)
            };
            sleep(Duration::from_millis(delay)).await;
        }
        element.type_str(&karakter.to_string()).await?;
    }
    Ok(())
}

async fn has_login_form(page: &Page) -> bool {
    page.evaluate(
        r#"(() => {
            return !!(
                document.querySelector('#username')
                || document.querySelector('[data-cy="login-form"]')
                || document.querySelector('[data-cy="auth-card-layout"].login')
                || document.querySelector('[data-cy="login-form-username"]')
            );
        })()"#,
    )
    .await
    .ok()
    .and_then(|v| v.into_value::<bool>().ok())
    .unwrap_or(false)
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

    type_naturally(page, "#username", email, "email/username", (0, 0)).await?;
    sleep(Duration::from_millis(400)).await;
    type_naturally(page, "#password", password, "password", (0, 0)).await?;
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

/// Sudah di `/stream` tanpa form login dan tanpa modal sesi habis → sesi aktif.
async fn is_already_authenticated_on_stream(page: &Page) -> bool {
    let url = page.url().await.ok().flatten().unwrap_or_default();
    is_stream_url(&url)
        && !has_login_form(page).await
        && !has_session_expired_modal(page).await
}

async fn wait_for_authenticated_stream(page: &Page, timeout: Duration) -> Result<(), StockbitError> {
    let started = Instant::now();
    while started.elapsed() < timeout {
        if is_already_authenticated_on_stream(page).await {
            return Ok(());
        }
        sleep(Duration::from_millis(400)).await;
    }
    let url = page.url().await.ok().flatten().unwrap_or_default();
    let title = page.get_title().await.ok().flatten().unwrap_or_default();
    let shot = save_error_screenshot(page, "stream_not_authenticated").await;
    let shot_info = shot
        .as_ref()
        .map(|p| format!(" | screenshot: {}", p.display()))
        .unwrap_or_default();
    Err(format!(
        "Timeout menunggu /stream siap (url={url:?} title={title:?}){shot_info}"
    )
    .into())
}

/// Poll setelah akses `/login`: keluar cepat bila redirect ke `/stream` atau form login muncul.
async fn wait_for_stream_redirect_or_login_form(page: &Page, timeout: Duration) {
    let started = Instant::now();
    while started.elapsed() < timeout {
        if is_already_authenticated_on_stream(page).await || has_login_form(page).await {
            return;
        }
        sleep(Duration::from_millis(400)).await;
    }
}

/// Perlu login ulang hanya jika:
/// - di-redirect ke `/login`, atau
/// - modal "Sesi Kamu Sudah Habis", atau
/// - form login (`#username` / `data-cy=login-form`) muncul.
async fn needs_relogin(page: &Page) -> bool {
    if has_session_expired_modal(page).await {
        return true;
    }
    let url = page.url().await.ok().flatten().unwrap_or_default();
    if is_login_url(&url) {
        return true;
    }
    // Form login di `/stream` tanpa modal biasanya bukan halaman login — skip.
    // Setelah klik CTA sesi habis, form login bisa muncul di path mana pun.
    has_login_form(page).await && !is_stream_url(&url)
}

async fn login_and_return_to_stream(
    page: &Page,
    email: &str,
    password: &str,
) -> Result<(), StockbitError> {
    invalidate_bearer_cache().await;
    let session_expired = has_session_expired_modal(page).await;

    if session_expired {
        println!("Modal 'Sesi Kamu Sudah Habis' — klik 'Kembali ke Halaman Utama'...");
        if !click_session_expired_cta(page).await? {
            return Err(
                "Tombol 'Kembali ke Halaman Utama' tidak ditemukan di modal sesi habis".into(),
            );
        }
        println!("Menunggu form login (#username / data-cy=login-form)...");
        wait_for_login_form(page, Duration::from_secs(30)).await?;
        println!("Form login muncul — isi email/password...");
        perform_login(page, email, password).await?;
        goto_stockbit_expect(page, STOCKBIT_STREAM_URL, Some("/stream")).await?;
        wait_for_authenticated_stream(page, Duration::from_secs(10)).await?;
        return Ok(());
    }

    // Sudah di /stream tanpa form login → jangan paksa ke /login.
    if is_already_authenticated_on_stream(page).await {
        println!("Sesi aktif di /stream — skip login.");
        return Ok(());
    }

    if !has_login_form(page).await {
        // Jangan expect `/login` ketat: bila cookie masih valid, Stockbit redirect ke /stream.
        goto_stockbit(page, STOCKBIT_LOGIN_URL).await?;
        wait_for_stream_redirect_or_login_form(page, Duration::from_secs(10)).await;
        if is_already_authenticated_on_stream(page).await {
            println!("Akses /login di-redirect ke /stream — sesi masih aktif, skip login.");
            return Ok(());
        }
    }

    wait_for_login_form(page, Duration::from_secs(15)).await?;
    perform_login(page, email, password).await?;
    goto_stockbit_expect(page, STOCKBIT_STREAM_URL, Some("/stream")).await?;
    wait_for_authenticated_stream(page, Duration::from_secs(10)).await?;
    Ok(())
}

async fn send_update(tx: &mpsc::Sender<ReadinessUpdate>, ready: bool, message: &str) {
    let _ = tx
        .send(ReadinessUpdate {
            ready,
            message: message.to_string(),
            poll_seq: next_poll_seq(),
            portofolio: Vec::new(),
        })
        .await;
}

/// Cek `/stream`, login bila perlu, kirim progres lewat channel.
///
/// Lock Chrome **tidak** dipegang saat sleep/poll idle — supaya RPC client tetap bisa
/// invoke scrape/browser sementara poller menunggu.
pub async fn run_readiness_check(tx: mpsc::Sender<ReadinessUpdate>) -> Result<(), StockbitError> {
    let email = std::env::var("STOCKBIT_EMAIL").unwrap_or_default();
    let password = std::env::var("STOCKBIT_PASSWORD").unwrap_or_default();

    // Durasi jendela cek (override: STOCKBIT_SESSION_CHECK_SECS); poll singkat tanpa pegang lock.
    let wait_secs: u64 = std::env::var("STOCKBIT_SESSION_CHECK_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| rand::thread_rng().gen_range(60u64..=300));
    let started = Instant::now();
    let mut need_login = false;
    let mut session_expired = false;
    let mut decided = false;

    while started.elapsed() < Duration::from_secs(wait_secs) && !decided {
        // Yield ke RPC client bila mereka menunggu Chrome.
        if browser_interactive_waiters() > 0 {
            println!(
                "Stockbit readiness: tunda cek sesaat — {} RPC client menunggu Chrome",
                browser_interactive_waiters()
            );
            sleep(Duration::from_secs(2)).await;
            continue;
        }

        let _browser_guard = match acquire_browser_session(BrowserLockClass::Background).await {
            Ok(g) => g,
            Err(e) => {
                println!("Stockbit readiness: {e} — retry cek setelah jeda");
                sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        let (browser, page) = launch_page().await?;
        let step = async {
            goto_stockbit(&page, STOCKBIT_STREAM_URL).await?;
            sleep(Duration::from_secs(1)).await;

            if has_profile_avatar_modal(&page).await {
                dismiss_profile_avatar_modal(&page).await?;
                decided = true;
                need_login = false;
                return Ok::<(), StockbitError>(());
            }
            if has_session_expired_modal(&page).await {
                session_expired = true;
                need_login = true;
                decided = true;
                println!(
                    "Modal 'Sesi Kamu Sudah Habis' terdeteksi — akan klik 'Kembali ke Halaman Utama' lalu login..."
                );
                let _ = save_error_screenshot(&page, "session_expired_modal").await;
                return Ok(());
            }
            let u = page.url().await?.unwrap_or_default();
            if is_login_url(&u) {
                need_login = true;
                decided = true;
                return Ok(());
            }
            if is_stream_url(&u) && !has_login_form(&page).await {
                need_login = false;
                decided = true;
                return Ok(());
            }
            Ok(())
        }
        .await;

        browser.close().await;
        drop(_browser_guard); // lepaskan lock sebelum sleep

        step?;

        if decided {
            break;
        }

        let remaining = wait_secs.saturating_sub(started.elapsed().as_secs());
        if remaining == 0 {
            break;
        }
        // Poll singkat (2–5s) tanpa pegang lock — RPC client bisa masuk di celah ini.
        let poll_secs = rand::thread_rng().gen_range(2u64..=5).min(remaining);
        sleep(Duration::from_secs(poll_secs)).await;
    }

    if !decided {
        // Satu kali cek final di bawah lock.
        let _browser_guard = acquire_browser_session(BrowserLockClass::Background)
            .await
            .map_err(|e| -> StockbitError { e.into() })?;
        let (browser, page) = launch_page().await?;
        goto_stockbit(&page, STOCKBIT_STREAM_URL).await?;
        sleep(Duration::from_secs(1)).await;
        need_login = needs_relogin(&page).await;
        if has_session_expired_modal(&page).await {
            session_expired = true;
            need_login = true;
        }
        browser.close().await;
        drop(_browser_guard);
    }

    if need_login {
        if email.trim().is_empty() || password.is_empty() {
            return Err(
                "Sesi Stockbit habis / perlu login, tapi STOCKBIT_EMAIL / STOCKBIT_PASSWORD kosong"
                    .into(),
            );
        }
        if session_expired {
            send_update(&tx, false, "Session expired — login ulang").await;
        } else {
            send_update(&tx, false, "not authenticated — login ulang").await;
        }
        send_update(&tx, false, "Sedang login stockbit.com").await;
        login_and_return_to_stream_with_retry_unlocked(&email, &password, &tx).await?;
    } else {
        // Pastikan masih authenticated / di /stream (lock singkat).
        let _browser_guard = acquire_browser_session(BrowserLockClass::Background)
            .await
            .map_err(|e| -> StockbitError { e.into() })?;
        let (browser, page) = launch_page().await?;
        let url = page.url().await?.unwrap_or_default();
        if is_already_authenticated_on_stream(&page).await {
            send_update(&tx, false, "Sesi aktif di /stream — skip login").await;
        } else if !is_stream_url(&url) {
            goto_stockbit(&page, STOCKBIT_STREAM_URL).await?;
            sleep(Duration::from_secs(1)).await;
            if needs_relogin(&page).await {
                browser.close().await;
                drop(_browser_guard);
                if email.trim().is_empty() || password.is_empty() {
                    return Err(
                        "Perlu login stockbit.com, tapi STOCKBIT_EMAIL / STOCKBIT_PASSWORD kosong"
                            .into(),
                    );
                }
                send_update(&tx, false, "Sedang login stockbit.com").await;
                login_and_return_to_stream_with_retry_unlocked(&email, &password, &tx).await?;
                // verify below
                let _browser_guard = acquire_browser_session(BrowserLockClass::Background)
                    .await
                    .map_err(|e| -> StockbitError { e.into() })?;
                let (browser, page) = launch_page().await?;
                if has_profile_avatar_modal(&page).await {
                    dismiss_profile_avatar_modal(&page).await?;
                }
                let final_url = page.url().await?.unwrap_or_default();
                if !is_stream_url(&final_url) {
                    browser.close().await;
                    return Err(
                        format!("Gagal masuk /stream setelah cek sesi (URL: {final_url})").into(),
                    );
                }
                send_update(&tx, true, "Stockbit ready").await;
                browser.close().await;
                return Ok(());
            }
        }
        if has_profile_avatar_modal(&page).await {
            dismiss_profile_avatar_modal(&page).await?;
        }
        let final_url = page.url().await?.unwrap_or_default();
        if !is_stream_url(&final_url) {
            browser.close().await;
            return Err(format!("Gagal masuk /stream setelah cek sesi (URL: {final_url})").into());
        }
        send_update(&tx, true, "Stockbit ready").await;
        browser.close().await;
        return Ok(());
    }

    // Setelah login retry: verifikasi /stream.
    let _browser_guard = acquire_browser_session(BrowserLockClass::Background)
        .await
        .map_err(|e| -> StockbitError { e.into() })?;
    let (browser, page) = launch_page().await?;
    if has_profile_avatar_modal(&page).await {
        dismiss_profile_avatar_modal(&page).await?;
    }
    let final_url = page.url().await?.unwrap_or_default();
    if !is_stream_url(&final_url) {
        browser.close().await;
        return Err(format!("Gagal masuk /stream setelah cek sesi (URL: {final_url})").into());
    }
    send_update(&tx, true, "Stockbit ready").await;
    browser.close().await;
    Ok(())
}

/// Login dengan retry; **melepas** Chrome lock saat jeda retry agar RPC client tidak hang.
async fn login_and_return_to_stream_with_retry_unlocked(
    email: &str,
    password: &str,
    tx: &mpsc::Sender<ReadinessUpdate>,
) -> Result<(), StockbitError> {
    let mut attempt: u32 = 0;
    loop {
        if browser_interactive_waiters() > 0 {
            println!(
                "Stockbit readiness: tunda login — {} RPC client menunggu Chrome",
                browser_interactive_waiters()
            );
            sleep(Duration::from_secs(2)).await;
            continue;
        }

        attempt += 1;
        let login_result = {
            let _browser_guard = acquire_browser_session(BrowserLockClass::Background)
                .await
                .map_err(|e| -> StockbitError { e.into() })?;
            let (browser, page) = launch_page().await?;
            let result = login_and_return_to_stream(&page, email, password).await;
            browser.close().await;
            result
        };

        match login_result {
            Ok(()) => {
                if attempt > 1 {
                    println!("Stockbit readiness: login berhasil setelah {attempt} percobaan");
                }
                return Ok(());
            }
            Err(e) => {
                let wait_secs =
                    rand::thread_rng().gen_range(LOGIN_RETRY_MIN_SECS..=LOGIN_RETRY_MAX_SECS);
                let msg = format!(
                    "Login gagal (percobaan {attempt}): {e}; retry dalam {wait_secs}s"
                );
                eprintln!("Stockbit readiness: {msg}");
                send_update(tx, false, &msg).await;
                // Sleep di luar lock — RPC client bisa memakai Chrome di celah ini.
                sleep(Duration::from_secs(wait_secs)).await;
            }
        }
    }
}

const BEARER_PROBE_URL: &str = "https://exodus.stockbit.com/order-trade/market-mover?\
mover_type=MOVER_TYPE_TOP_GAINER&filter_stocks=FILTER_STOCKS_TYPE_MAIN_BOARD";

fn ensure_auth_capture_js() -> &'static str {
    r#"(() => {
        try {
            if (window.__sbCaptureAuthInstalled) {
                return (window.__sbCapturedBearer || '').length;
            }
            window.__sbCaptureAuthInstalled = true;
            window.__sbCapturedBearer = window.__sbCapturedBearer || '';
            const remember = (v) => {
                if (!v || typeof v !== 'string') return;
                const t = v.replace(/^Bearer\s+/i, '').trim();
                if (t.startsWith('eyJ')) window.__sbCapturedBearer = t;
            };
            const wrapHeaders = (headers) => {
                if (!headers) return;
                try {
                    if (typeof headers.get === 'function') {
                        remember(headers.get('Authorization') || headers.get('authorization'));
                    } else if (Array.isArray(headers)) {
                        for (const pair of headers) {
                            if (pair && String(pair[0]).toLowerCase() === 'authorization') remember(pair[1]);
                        }
                    } else if (typeof headers === 'object') {
                        remember(headers.Authorization || headers.authorization);
                    }
                } catch (_) {}
            };
            const ofetch = window.fetch;
            window.fetch = function (input, init) {
                try {
                    if (init && init.headers) wrapHeaders(init.headers);
                    if (input && typeof input === 'object' && input.headers) wrapHeaders(input.headers);
                } catch (_) {}
                return ofetch.apply(this, arguments);
            };
            const oSet = XMLHttpRequest.prototype.setRequestHeader;
            XMLHttpRequest.prototype.setRequestHeader = function (k, v) {
                try {
                    if (String(k).toLowerCase() === 'authorization') remember(v);
                } catch (_) {}
                return oSet.apply(this, arguments);
            };
            return (window.__sbCapturedBearer || '').length;
        } catch (_) { return 0; }
    })()"#
}

async fn probe_bearer_ok(http: &reqwest::Client, token: &str) -> Result<u16, String> {
    let resp = http
        .get(BEARER_PROBE_URL)
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/json, text/plain, */*")
        .header("Origin", "https://stockbit.com")
        .header("Referer", "https://stockbit.com/")
        .header("x-platform", "web")
        .send()
        .await
        .map_err(|e| e.to_string())?;
    Ok(resp.status().as_u16())
}

async fn bearer_http_client() -> Result<reqwest::Client, StockbitError> {
    reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| StockbitError::from(e.to_string()))
}

type BearerCandidate = (String, String, String);

async fn pick_probe_valid_bearer(candidates: &[BearerCandidate]) -> Result<String, StockbitError> {
    let mut ordered: Vec<BearerCandidate> = Vec::new();
    for h in candidates {
        let source = h.0.clone();
        let token = h.1.clone();
        let iss = h.2.clone();
        if !token.starts_with("eyJ") {
            continue;
        }
        if source == "network.capture" {
            ordered.insert(0, (source, token, iss));
            continue;
        }
        if source.to_lowercase().contains("eipo") {
            continue;
        }
        if source.to_lowercase().contains("refresh") {
            continue;
        }
        if source.to_lowercase().contains("securities") {
            continue;
        }
        if iss.to_uppercase() == "STOCKBIT" || source.contains("cookie") {
            ordered.push((source, token, iss));
        }
    }

    let mut seen_tok = std::collections::HashSet::new();
    ordered.retain(|(_, tok, _)| seen_tok.insert(tok.clone()));

    let http = bearer_http_client().await?;
    for (_source, token, _iss) in &ordered {
        match probe_bearer_ok(&http, token).await {
            Ok(status) if (200..300).contains(&status) => return Ok(token.clone()),
            _ => {}
        }
    }

    Err(
        "Tidak ada Bearer yang lolos probe market-mover. \
         Sesi web mungkin habis — login ulang di Chrome worker."
            .into(),
    )
}

async fn scan_bearer_candidates(page: &Page) -> Result<Vec<BearerCandidate>, StockbitError> {
    let mut cookie_blob = String::new();
    if let Ok(cookies) = page.get_cookies().await {
        for c in cookies {
            cookie_blob.push_str(&c.name);
            cookie_blob.push('=');
            cookie_blob.push_str(&c.value);
            cookie_blob.push(';');
        }
    }

    let cookie_js = serde_json::to_string(&cookie_blob).unwrap_or_else(|_| "\"\"".into());
    let scanned = page
        .evaluate(format!(
            r#"((cookieBlob) => {{
                const hits = [];
                const b64url = (s) => {{
                    try {{
                        const pad = '='.repeat((4 - (s.length % 4)) % 4);
                        const b64 = (s + pad).replace(/-/g, '+').replace(/_/g, '/');
                        return JSON.parse(atob(b64));
                    }} catch (_) {{ return null; }}
                }};
                const addToken = (raw, source) => {{
                    if (!raw || typeof raw !== 'string') return;
                    const m = raw.match(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g);
                    if (!m) return;
                    for (const token of m) {{
                        const p = b64url(token.split('.')[1] || '');
                        hits.push({{
                            token,
                            source,
                            iss: (p && p.iss) ? String(p.iss) : '',
                            token_type: (p && p.token_type) ? String(p.token_type) : '',
                        }});
                    }}
                }};
                const scanStorage = (storage, label) => {{
                    try {{
                        for (let i = 0; i < storage.length; i++) {{
                            const key = storage.key(i);
                            if (!key) continue;
                            addToken(storage.getItem(key) || '', label + ':' + key);
                        }}
                    }} catch (_) {{}}
                }};
                scanStorage(window.localStorage, 'localStorage');
                scanStorage(window.sessionStorage, 'sessionStorage');
                try {{ addToken(document.cookie || '', 'document.cookie'); }} catch (_) {{}}
                addToken(cookieBlob || '', 'cdp.cookies');
                try {{
                    if (window.__sbCapturedBearer) {{
                        hits.unshift({{
                            token: window.__sbCapturedBearer,
                            source: 'network.capture',
                            iss: (() => {{
                                const p = b64url(window.__sbCapturedBearer.split('.')[1] || '');
                                return (p && p.iss) ? String(p.iss) : '';
                            }})(),
                            token_type: '',
                        }});
                    }}
                }} catch (_) {{}}

                const seen = new Set();
                const uniq = [];
                for (const h of hits) {{
                    const key = h.source + '|' + h.token;
                    if (seen.has(key)) continue;
                    seen.add(key);
                    uniq.push(h);
                }}
                return JSON.stringify(uniq.map((h) => ({{
                    token: h.token,
                    source: h.source,
                    iss: h.iss,
                    token_type: h.token_type,
                }})));
            }})({cookie_js})"#
        ))
        .await?
        .into_value::<String>()
        .unwrap_or_else(|_| "[]".to_string());

    let raw: Vec<serde_json::Value> = serde_json::from_str(&scanned).unwrap_or_default();
    Ok(raw
        .iter()
        .filter(|h| {
            let token_type = h
                .get("token_type")
                .and_then(|x| x.as_str())
                .unwrap_or("");
            !token_type.to_uppercase().contains("EIPO")
        })
        .map(|h| {
            (
                h.get("source")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string(),
                h.get("token")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string(),
                h.get("iss")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string(),
            )
        })
        .collect())
}

async fn store_bearer_cache(token: String) {
    let mut cache = bearer_cache().lock().await;
    *cache = Some(CachedBearer {
        token,
        cached_at: Instant::now(),
    });
}

/// Hapus cache Bearer (mis. setelah login ulang / sesi habis).
pub async fn invalidate_bearer_cache() {
    let mut cache = bearer_cache().lock().await;
    *cache = None;
}

async fn try_cached_bearer() -> Option<String> {
    let token = {
        let cache = bearer_cache().lock().await;
        let entry = cache.as_ref()?;
        if entry.cached_at.elapsed() >= Duration::from_secs(bearer_cache_ttl_secs()) {
            return None;
        }
        entry.token.clone()
    };
    let http = bearer_http_client().await.ok()?;
    match probe_bearer_ok(&http, &token).await {
        Ok(status) if (200..300).contains(&status) => Some(token),
        _ => {
            invalidate_bearer_cache().await;
            None
        }
    }
}

/// Bearer untuk HTTP API tanpa caller menyediakan `Page`.
/// Cache hit → tanpa Chrome. Miss → lock interactive + login bila perlu + extract.
pub async fn ensure_stockbit_bearer() -> Result<String, StockbitError> {
    if let Some(token) = try_cached_bearer().await {
        println!("Bearer cache hit via ensure_stockbit_bearer (len={}).", token.len());
        return Ok(token);
    }

    let email = std::env::var("STOCKBIT_EMAIL").unwrap_or_default();
    let password = std::env::var("STOCKBIT_PASSWORD").unwrap_or_default();
    if email.trim().is_empty() || password.is_empty() {
        return Err(
            "STOCKBIT_EMAIL / STOCKBIT_PASSWORD wajib untuk ambil Bearer Stockbit".into(),
        );
    }

    let _browser_guard = acquire_browser_session(BrowserLockClass::Interactive)
        .await
        .map_err(|e| -> StockbitError { e.into() })?;
    let (browser, page) = launch_page().await?;
    open_stream_or_login(&page, email.trim(), password.trim()).await?;
    let token = extract_stockbit_bearer(&page).await?;
    browser.close().await;
    Ok(token)
}

/// Ambil Bearer JWT untuk API `exodus.stockbit.com`.
///
/// Urutan: cache (probe) → scan halaman saat ini → warm-up keystats + scan.
/// Warm-up halaman symbol agar SPA mengirim `Authorization` (network.capture),
/// lalu probe kandidat via HTTP dari Rust (bukan `fetch` di page — CORS status=0).
/// Abaikan securities* / EIPO / refresh.
pub async fn extract_stockbit_bearer(page: &Page) -> Result<String, StockbitError> {
    if let Some(token) = try_cached_bearer().await {
        println!("Bearer cache hit (len={}).", token.len());
        return Ok(token);
    }

    let _ = page.evaluate(ensure_auth_capture_js()).await?;

    println!("Bearer: scan halaman saat ini (tanpa navigasi keystats)...");
    if let Ok(candidates) = scan_bearer_candidates(page).await {
        if let Ok(token) = pick_probe_valid_bearer(&candidates).await {
            println!("Bearer OK dari halaman saat ini (len={}).", token.len());
            store_bearer_cache(token.clone()).await;
            return Ok(token);
        }
    }

    println!("Bearer: warm-up navigasi keystats...");
    let _ = page
        .evaluate(r#"(() => { window.__sbCapturedBearer = ''; return 0; })()"#)
        .await;
    goto_stockbit(page, "https://stockbit.com/symbol/BBCA/keystats").await?;
    let _ = page.evaluate(ensure_auth_capture_js()).await?;
    for _ in 0..25 {
        sleep(Duration::from_millis(400)).await;
        let n = page
            .evaluate(r#"(() => (window.__sbCapturedBearer || '').length)()"#)
            .await?
            .into_value::<u64>()
            .unwrap_or(0);
        if n > 0 {
            break;
        }
    }

    let candidates = scan_bearer_candidates(page).await?;
    let token = pick_probe_valid_bearer(&candidates).await?;
    println!("Bearer OK dari keystats (len={}).", token.len());
    store_bearer_cache(token.clone()).await;
    Ok(token)
}

/// Alur awal worker: akses `/stream`, login hanya bila di-redirect ke `/login` / sesi habis.
pub async fn open_stream_or_login(
    page: &Page,
    email: &str,
    password: &str,
) -> Result<(), StockbitError> {
    if is_already_authenticated_on_stream(page).await {
        println!("Chrome reuse: sudah di /stream — skip navigasi + sleep 1s");
        if !has_profile_avatar_modal(page).await {
            println!("Sesi aktif di /stream — skip login, lanjut scrape.");
            return Ok(());
        }
        dismiss_profile_avatar_modal(page).await?;
        println!("Sesi aktif di /stream — skip login, lanjut scrape.");
        return Ok(());
    }

    goto_stockbit(page, STOCKBIT_STREAM_URL).await?;

    let wait_secs = worker_session_check_secs();
    let started = Instant::now();

    // Tunggu sebentar: redirect ke /login, modal sesi habis, atau konfirmasi sudah di /stream.
    let mut saw_session_expired = false;
    while started.elapsed() < Duration::from_secs(wait_secs) {
        if has_profile_avatar_modal(page).await {
            dismiss_profile_avatar_modal(page).await?;
            break;
        }
        if has_session_expired_modal(page).await {
            saw_session_expired = true;
            invalidate_bearer_cache().await;
            println!(
                "Modal 'Sesi Kamu Sudah Habis' terdeteksi — akan klik 'Kembali ke Halaman Utama' lalu login..."
            );
            let _ = save_error_screenshot(page, "session_expired_modal").await;
            break;
        }
        let u = page.url().await?.unwrap_or_default();
        if is_login_url(&u) {
            // Akses /stream di-redirect ke /login → perlu login ulang.
            break;
        }
        if is_stream_url(&u) && !has_login_form(page).await {
            // Tetap di /stream tanpa form login → sudah login.
            break;
        }
        sleep(Duration::from_millis(400)).await;
    }

    if has_profile_avatar_modal(page).await {
        dismiss_profile_avatar_modal(page).await?;
    }

    if saw_session_expired || needs_relogin(page).await {
        println!("Perlu login ulang (redirect /login atau sesi habis)...");
        login_and_return_to_stream(page, email, password).await?;
    } else if is_already_authenticated_on_stream(page).await {
        println!("Sesi aktif di /stream — skip login, lanjut scrape.");
    } else {
        // Belum jelas: coba /stream lagi.
        goto_stockbit(page, STOCKBIT_STREAM_URL).await?;
        wait_for_authenticated_stream(page, Duration::from_secs(10)).await?;
        if needs_relogin(page).await {
            println!("Perlu login ulang setelah re-cek /stream...");
            login_and_return_to_stream(page, email, password).await?;
        } else if !is_already_authenticated_on_stream(page).await {
            let url = page.url().await?.unwrap_or_default();
            return Err(format!(
                "Gagal memastikan sesi Stockbit (URL: {url})"
            )
            .into());
        } else {
            println!("Sesi aktif di /stream — skip login, lanjut scrape.");
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
