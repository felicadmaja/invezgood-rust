//! HTTP helper Invezgo: token bucket 500 req/min (paralel OK), cooldown 429, retry backoff.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

const DEFAULT_MAX_REQ_PER_MINUTE: u64 = 500;
const DEFAULT_BURST: u64 = 20;
const DEFAULT_JEDA_MS_ANTAR_EMITEN: u64 = 25;
const DEFAULT_429_MAX_RETRIES: u32 = 3;
const DEFAULT_429_BACKOFF_MS: u64 = 2_000;
const DEFAULT_429_COOLDOWN_SECS: u64 = 10;

static RATE_LIMITER: OnceLock<TokenBucket> = OnceLock::new();
static COOLDOWN_UNTIL: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();
static HTTP_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

struct TokenBucketState {
    tokens: f64,
    last: Instant,
}

struct TokenBucket {
    state: Mutex<TokenBucketState>,
    max_tokens: f64,
    /// Token per millisecond (500/min → 500/60000).
    refill_per_ms: f64,
}

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

fn max_req_per_minute() -> u64 {
    static CACHED: OnceLock<u64> = OnceLock::new();
    *CACHED.get_or_init(|| env_u64("INVEZGO_MAX_REQ_PER_MINUTE", DEFAULT_MAX_REQ_PER_MINUTE))
}

fn burst_capacity() -> u64 {
    static CACHED: OnceLock<u64> = OnceLock::new();
    *CACHED.get_or_init(|| {
        let burst = env_u64("INVEZGO_BURST", DEFAULT_BURST);
        burst.max(1).min(max_req_per_minute())
    })
}

fn rate_limiter() -> &'static TokenBucket {
    RATE_LIMITER.get_or_init(|| {
        let rpm = max_req_per_minute().max(1) as f64;
        let burst = burst_capacity().max(1) as f64;
        TokenBucket {
            state: Mutex::new(TokenBucketState {
                tokens: burst,
                last: Instant::now(),
            }),
            max_tokens: burst,
            refill_per_ms: rpm / 60_000.0,
        }
    })
}

impl TokenBucket {
    /// Ambil 1 token; request paralel antre di sini bila bucket kosong.
    async fn acquire(&self) {
        loop {
            let wait = {
                let mut st = self.state.lock().await;
                let now = Instant::now();
                let elapsed_ms = now.duration_since(st.last).as_secs_f64() * 1000.0;
                st.tokens = (st.tokens + elapsed_ms * self.refill_per_ms).min(self.max_tokens);
                st.last = now;

                if st.tokens >= 1.0 {
                    st.tokens -= 1.0;
                    None
                } else {
                    let deficit = 1.0 - st.tokens;
                    let wait_ms = (deficit / self.refill_per_ms).ceil() as u64;
                    Some(Duration::from_millis(wait_ms.max(1)))
                }
            };

            match wait {
                None => return,
                Some(d) => tokio::time::sleep(d).await,
            }
        }
    }
}

fn log_rate_limit_config() {
    static LOGGED: OnceLock<()> = OnceLock::new();
    LOGGED.get_or_init(|| {
        let rpm = max_req_per_minute();
        let burst = burst_capacity();
        let avg_ms = 60_000u64.div_ceil(rpm.max(1));
        eprintln!(
            "Invezgo rate limit {rpm} req/min burst={burst} (~{avg_ms}ms avg antar req, paralel OK)"
        );
    });
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

/// Jeda antar emiten dari `JEDA_MS_ANTAR_EMITEN` (ms) — spike poller batch.
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
    for name in ["retry-after", "x-ratelimit-limit", "x-ratelimit-remaining", "x-ratelimit-reset"]
    {
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

/// GET Invezgo: paralel OK, dibatasi token bucket + cooldown 429.
pub async fn get(url: &str) -> Result<String, String> {
    let token = std::env::var("INVEZGO_BEARER_TOKEN")
        .map_err(|_| "INVEZGO_BEARER_TOKEN belum diset".to_string())?;
    log_rate_limit_config();
    log_token_fingerprint(&token);

    let max_retries = max_429_retries();
    let client = http_client();
    let path = log_path(url);
    let limiter = rate_limiter();

    for attempt in 0..=max_retries {
        wait_cooldown().await;
        limiter.acquire().await;

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
