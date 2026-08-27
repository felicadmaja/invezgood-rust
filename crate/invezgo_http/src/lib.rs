//! HTTP helper Invezgo: serialisasi global antar request, jeda antar emiten, retry 429.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

const DEFAULT_MIN_INTERVAL_MS: u64 = 200;
const DEFAULT_JEDA_MS_ANTAR_EMITEN: u64 = 25;
const DEFAULT_429_MAX_RETRIES: u32 = 3;
const DEFAULT_429_BACKOFF_MS: u64 = 1000;

static REQUEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static LAST_REQUEST_AT: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();
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

fn backoff_ms(attempt: u32) -> u64 {
    let base = backoff_base_ms();
    base.saturating_mul(1u64 << attempt.min(10))
}

/// GET Invezgo dengan mutex global, jeda minimum antar request, dan retry exponential backoff bila 429.
pub async fn get(url: &str) -> Result<String, String> {
    let token = std::env::var("INVEZGO_BEARER_TOKEN")
        .map_err(|_| "INVEZGO_BEARER_TOKEN belum diset".to_string())?;

    let lock = REQUEST_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = lock.lock().await;

    let max_retries = max_429_retries();
    let client = http_client();

    for attempt in 0..=max_retries {
        wait_min_interval().await;

        let response = client
            .get(url)
            .bearer_auth(&token)
            .send()
            .await
            .map_err(|e| format!("Invezgo GET {url}: {e}"))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| format!("Invezgo body {url}: {e}"))?;

        if status.as_u16() == 429 {
            if attempt < max_retries {
                let wait = backoff_ms(attempt);
                eprintln!(
                    "Invezgo 429 {url} — retry {}/{} dalam {}ms",
                    attempt + 1,
                    max_retries,
                    wait
                );
                tokio::time::sleep(Duration::from_millis(wait)).await;
                continue;
            }
            return Err(format!("Invezgo HTTP 429 {url} (habis retry): {body}"));
        }

        if !status.is_success() {
            return Err(format!("Invezgo HTTP {status} {url}: {body}"));
        }

        return Ok(body);
    }

    Err(format!("Invezgo GET {url}: retry 429 habis"))
}
