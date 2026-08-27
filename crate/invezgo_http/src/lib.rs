//! HTTP helper Invezgo: serialisasi global, serial per emiten (chart+bandarmology), cooldown 429.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

const DEFAULT_MIN_INTERVAL_MS: u64 = 500;
const DEFAULT_JEDA_MS_ANTAR_EMITEN: u64 = 25;
const DEFAULT_429_MAX_RETRIES: u32 = 4;
const DEFAULT_429_BACKOFF_MS: u64 = 15_000;
const DEFAULT_429_COOLDOWN_SECS: u64 = 60;

static REQUEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static EMITEN_DETAIL_LOCKS: OnceLock<Mutex<HashMap<String, Arc<Mutex<()>>>>> = OnceLock::new();
static LAST_REQUEST_AT: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();
static COOLDOWN_UNTIL: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();
static HTTP_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

fn min_interval_ms() -> u64 {
    static CACHED: OnceLock<u64> = OnceLock::new();
    *CACHED.get_or_init(|| env_u64("INVEZGO_MIN_INTERVAL_MS", DEFAULT_MIN_INTERVAL_MS))
}

fn max_429_retries() -> u32 {
    static CACHED: OnceLock<u32> = OnceLock::new();
    *CACHED.get_or_init(|| env_u32("INVEZGO_429_MAX_RETRIES", DEFAULT_429_MAX_RETRIES))
}

fn backoff_base_ms() -> u64 {
    static CACHED: OnceLock<u64> = OnceLock::new();
    *CACHED.get_or_init(|| env_u64("INVEZGO_429_BACKOFF_MS", DEFAULT_429_BACKOFF_MS))
}

fn cooldown_secs_on_429() -> u64 {
    static CACHED: OnceLock<u64> = OnceLock::new();
    *CACHED.get_or_init(|| env_u64("INVEZGO_429_COOLDOWN_SECS", DEFAULT_429_COOLDOWN_SECS))
}

/// Jeda antar emiten dari `JEDA_MS_ANTAR_EMITEN` (ms).
pub fn jeda_ms_antar_emiten() -> u64 {
    static CACHED: OnceLock<u64> = OnceLock::new();
    *CACHED.get_or_init(|| env_u64("JEDA_MS_ANTAR_EMITEN", DEFAULT_JEDA_MS_ANTAR_EMITEN))
}

/// Sleep jeda antar emiten (spike poller, worker batch, dll.).
pub async fn delay_between_emitens() {
    tokio::time::sleep(Duration::from_millis(jeda_ms_antar_emiten())).await;
}

fn http_client() -> &'static reqwest::Client {
    HTTP_CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("invezgo_http: gagal buat reqwest client")
    })
}

async fn wait_min_interval() {
    let min = Duration::from_millis(min_interval_ms());
    if min.is_zero() {
        return;
    }
    let last = LAST_REQUEST_AT.get_or_init(|| Mutex::new(None));
    let mut guard = last.lock().await;
    if let Some(at) = *guard {
        let elapsed = at.elapsed();
        if elapsed < min {
            tokio::time::sleep(min - elapsed).await;
        }
    }
    *guard = Some(Instant::now());
}

async fn cooldown_remaining() -> Duration {
    let cooldown = COOLDOWN_UNTIL.get_or_init(|| Mutex::new(None));
    let guard = cooldown.lock().await;
    guard
        .map(|until| until.saturating_duration_since(Instant::now()))
        .unwrap_or(Duration::ZERO)
}

async fn extend_cooldown(extra: Duration) {
    let until = Instant::now() + extra;
    let cooldown = COOLDOWN_UNTIL.get_or_init(|| Mutex::new(None));
    let mut guard = cooldown.lock().await;
    let extend = match *guard {
        Some(current) if current > until => current,
        _ => until,
    };
    *guard = Some(extend);
}

async fn wait_cooldown() {
    loop {
        let remaining = cooldown_remaining().await;
        if remaining.is_zero() {
            break;
        }
        eprintln!(
            "Invezgo cooldown — tunggu {}ms sebelum request berikutnya",
            remaining.as_millis()
        );
        tokio::time::sleep(remaining).await;
    }
}

fn backoff_ms(attempt: u32) -> u64 {
    let base = backoff_base_ms();
    base.saturating_mul(1u64 << attempt.min(6))
}

fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
}

async fn wait_on_429(attempt: u32, retry_after: Option<Duration>) {
    let cooldown = Duration::from_secs(cooldown_secs_on_429());
    extend_cooldown(cooldown).await;
    let wait = retry_after
        .unwrap_or(cooldown)
        .max(Duration::from_millis(backoff_ms(attempt)))
        .max(cooldown);
    eprintln!(
        "Invezgo 429 — tunggu {}ms sebelum retry (attempt {})",
        wait.as_millis(),
        attempt + 1
    );
    tokio::time::sleep(wait).await;
}

fn log_token_fingerprint(token: &str) {
    static LOGGED: OnceLock<()> = OnceLock::new();
    LOGGED.get_or_init(|| {
        let fp = if token.len() >= 12 {
            format!(
                "{}…{} len={}",
                &token[..8],
                &token[token.len().saturating_sub(4)..],
                token.len()
            )
        } else {
            format!("len={}", token.len())
        };
        eprintln!("Invezgo invoke auth token fp={fp}");
    });
}

fn log_429_headers(headers: &reqwest::header::HeaderMap) {
    for name in ["retry-after", "x-ratelimit-limit", "x-ratelimit-remaining", "x-ratelimit-reset"] {
        if let Some(v) = headers.get(name) {
            if let Ok(s) = v.to_str() {
                eprintln!("Invezgo 429 header {name}: {s}");
            }
        }
    }
}

/// Path relatif untuk log (tanpa host).
fn log_path(url: &str) -> &str {
    url.strip_prefix("https://api.invezgo.com")
        .or_else(|| url.strip_prefix("http://api.invezgo.com"))
        .unwrap_or(url)
}

fn normalize_emiten_code(code: &str) -> String {
    code.trim().to_ascii_uppercase()
}

async fn emiten_detail_lock(code: &str) -> Arc<Mutex<()>> {
    let key = normalize_emiten_code(code);
    let map = EMITEN_DETAIL_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map.lock().await;
    guard
        .entry(key)
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

/// Serialkan fetch Invezgo chart + bandarmology untuk emiten yang sama (RPC paralel → antre).
pub async fn with_emiten_detail_serial<T, F, Fut>(code: &str, label: &str, f: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let key = normalize_emiten_code(code);
    if key.is_empty() {
        return f().await;
    }

    let lock = emiten_detail_lock(&key).await;
    eprintln!("Invezgo emiten serial {key} — tunggu giliran ({label})");
    let _guard = lock.lock().await;
    eprintln!("Invezgo emiten serial {key} — jalan ({label})");
    f().await
}

/// GET Invezgo dengan mutex global, jeda minimum, cooldown 429, dan retry exponential backoff.
pub async fn get(url: &str) -> Result<String, String> {
    let token = std::env::var("INVEZGO_BEARER_TOKEN")
        .map_err(|_| "INVEZGO_BEARER_TOKEN belum diset".to_string())?;
    log_token_fingerprint(&token);

    let lock = REQUEST_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = lock.lock().await;

    let max_retries = max_429_retries();
    let client = http_client();
    let path = log_path(url);

    for attempt in 0..=max_retries {
        wait_cooldown().await;
        wait_min_interval().await;

        let started = Instant::now();
        let attempt_no = attempt + 1;
        eprintln!(
            "Invezgo invoke GET {path} attempt={attempt_no}/{}",
            max_retries + 1
        );

        let response = match client
            .get(url)
            .bearer_auth(&token)
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                let elapsed = started.elapsed().as_millis();
                eprintln!("Invezgo invoke ERR GET {path} network {elapsed}ms — {e}");
                return Err(format!("Invezgo GET {url}: {e}"));
            }
        };

        let status = response.status();
        let retry_after = parse_retry_after(response.headers());

        if status.as_u16() == 429 {
            log_429_headers(response.headers());
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| String::from("(body kosong)"));
            let elapsed = started.elapsed().as_millis();
            eprintln!(
                "Invezgo invoke 429 GET {path} {elapsed}ms attempt={attempt_no}/{}",
                max_retries + 1
            );

            if attempt < max_retries {
                wait_on_429(attempt, retry_after).await;
                continue;
            }
            return Err(format!("Invezgo HTTP 429 {url} (habis retry): {body}"));
        }

        let body = response
            .text()
            .await
            .map_err(|e| format!("Invezgo body {url}: {e}"))?;
        let elapsed = started.elapsed().as_millis();

        if !status.is_success() {
            eprintln!(
                "Invezgo invoke ERR GET {path} HTTP {status} {elapsed}ms body={}b",
                body.len()
            );
            return Err(format!("Invezgo HTTP {status} {url}: {body}"));
        }

        eprintln!(
            "\x1b[32mInvezgo invoke OK GET {path} HTTP {status} {elapsed}ms body={}b\x1b[0m",
            body.len()
        );
        return Ok(body);
    }

    Err(format!("Invezgo GET {url}: retry 429 habis"))
}
